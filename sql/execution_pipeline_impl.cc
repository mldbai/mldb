// This file is part of MLDB. Copyright 2015 Datacratic. All rights reserved.

/** execution_pipeline_impl.cc
    Jeremy Barnes, 27 August 2015
    Copyright (c) 2015 Datacratic Inc.  All rights reserved.

    Implementation of the new query execution pipeline.
*/

#include "execution_pipeline_impl.h"
#include "mldb/http/http_exception.h"
#include "mldb/types/basic_value_descriptions.h"
#include "table_expression_operations.h"
#include <algorithm>
#include "mldb/sql/sql_expression_operations.h"

using namespace std;


namespace Datacratic {
namespace MLDB {


/*****************************************************************************/
/* TABLE LEXICAL SCOPE                                                       */
/*****************************************************************************/

TableLexicalScope::
TableLexicalScope(TableOperations table_,
                  Utf8String asName_)
    : table(std::move(table_)), asName(std::move(asName_))
{
    auto rowInfo = table.getRowInfo();
    
    knownColumns = rowInfo->getKnownColumns();

    std::sort(knownColumns.begin(), knownColumns.end(),
              [] (const KnownColumn & first,
                  const KnownColumn & second)
              {
                  return first.columnName.toUtf8String()
                      < second.columnName.toUtf8String();
              });
    
    hasUnknownColumns = rowInfo->getSchemaCompleteness() == SCHEMA_OPEN;
}

VariableGetter
TableLexicalScope::
doGetVariable(const Utf8String & variableName, int fieldOffset)
{
    ColumnName columnName(variableName);
    ColumnHash columnHash(columnName);

    //cerr << "dataset lexical scope: fieldOffset = " << fieldOffset << endl;
    ExcAssertGreaterEqual(fieldOffset, 0);

    // TODO: we may know something about this column...
    return {[=] (const SqlRowScope & rowScope,
                 ExpressionValue & storage,
                 const VariableFilter & filter) -> const ExpressionValue &
            {
                auto & row = static_cast<const PipelineResults &>(rowScope);

                const ExpressionValue & rowContents
                    = row.values.at(fieldOffset + ROW_CONTENTS);

                //cerr << "dataset: getting variable " << variableName
                //     << " from row " << jsonEncode(row.values)
                //     << " offset " << fieldOffset + ROW_CONTENTS
                //     << " returns " << rowContents.getField(variableName)
                //     << endl;

                return storage = std::move(rowContents.getField(variableName, filter));
            },
            std::make_shared<AtomValueInfo>()};

}

GetAllColumnsOutput
TableLexicalScope::
doGetAllColumns(std::function<Utf8String (const Utf8String &)> keep, int fieldOffset)
{
    //cerr << "dataset lexical scope get columns: fieldOffset = " << fieldOffset << endl;
    ExcAssertGreaterEqual(fieldOffset, 0);

    std::vector<KnownColumn> columnsWithInfo;
    std::map<ColumnHash, ColumnName> index;

    for (auto & column: knownColumns) {
        Utf8String outputName = keep(column.columnName.toUtf8String());
        if (outputName.empty())
            continue;
        KnownColumn out = column;
        out.columnName = ColumnName(outputName);
        columnsWithInfo.emplace_back(std::move(out));
        index[column.columnName] = ColumnName(outputName);
    }
    
    auto exec = [=] (const SqlRowScope & rowScope) -> ExpressionValue
        {
            auto & row = static_cast<const PipelineResults &>(rowScope);

            const ExpressionValue & rowContents
            = row.values.at(fieldOffset + ROW_CONTENTS);

            StructValue result;

            auto onSubexpression = [&] (const Id & columnName,
                                        const Id & prefix,  // always null
                                        const ExpressionValue & value)
            {
                auto it = index.find(columnName);
                if (it == index.end()) {
                    return true;
                }
                result.emplace_back(it->second, std::move(value));
                return true;
            };

            rowContents.forEachSubexpression(onSubexpression);

            return std::move(result);
        };

    GetAllColumnsOutput result;
    result.info = std::make_shared<RowValueInfo>(columnsWithInfo, SCHEMA_CLOSED);
    result.exec = exec;
    return result;
}

BoundFunction
TableLexicalScope::
doGetFunction(const Utf8String & functionName,
              const std::vector<BoundSqlExpression> & args,
              int fieldOffset)
{
    // First, let the dataset either override or implement the function
    // itself.
    //auto override = dataset->overrideFunction(functionName, *this);
    //if (override)
    //    return override;
        
    if (functionName == "rowName") {
        return {[=] (const std::vector<ExpressionValue> & args,
                     const SqlRowScope & rowScope)
                {
                    auto & row = static_cast<const PipelineResults &>(rowScope);
                    return row.values.at(fieldOffset + ROW_NAME);
                },
                std::make_shared<Utf8StringValueInfo>()
                    };
    }

    else if (functionName == "rowHash") {
        return {[=] (const std::vector<ExpressionValue> & args,
                     const SqlRowScope & rowScope)
                {
                    auto & row = static_cast<const PipelineResults &>(rowScope);
                    RowHash result(Id(row.values.at(fieldOffset + ROW_NAME).toUtf8String()));
                    return ExpressionValue(result.hash(),
                                           Date::notADate());
                },
                std::make_shared<Uint64ValueInfo>()
                    };
    }

    return BoundFunction();
}

Utf8String
TableLexicalScope::
as() const
{
    return asName;
}

std::vector<std::shared_ptr<ExpressionValueInfo> >
TableLexicalScope::
outputAdded() const
{
    return { std::make_shared<Utf8StringValueInfo>(), table.getRowInfo() };
}


/*****************************************************************************/
/* GENERATE ROWS EXECUTOR                                                    */
/*****************************************************************************/

GenerateRowsExecutor::
GenerateRowsExecutor()
    : currentDone(0), finished(false)
{
}

bool
GenerateRowsExecutor::
generateMore()
{
    // HACK: for the moment, generators will generate all rows,
    // but not keep any state, so we arrange for them to be
    // called only once.
    if (!current.empty()) {
        finished = true;
        return false;
    }

    // Ask for some more
    current.clear();

    current = generator(1000, params);
    currentDone = 0;
    if (current.empty()) {
        finished = true;
    }

    return !current.empty();
}

std::shared_ptr<PipelineResults>
GenerateRowsExecutor::
take()
{
    if (currentDone == current.size()) {
        if (!generateMore())
            return nullptr;
    }
            
    // Return the row itself as the value, and the row's name as
    // metadata.
    auto result = source->take();

    result->values.emplace_back(current[currentDone].rowName.toUtf8String(),
                                Date::notADate());
    result->values.emplace_back(std::move(current[currentDone].columns));
    ++currentDone;

    return result;
}

void
GenerateRowsExecutor::
restart()
{
    current.clear();
    currentDone = 0;
    finished = false;
}


/*****************************************************************************/
/* GENERATE ROWS ELEMENT                                                     */
/*****************************************************************************/

GenerateRowsElement::
GenerateRowsElement(std::shared_ptr<PipelineElement> root,
                    SelectExpression select,
                    TableOperations from,
                    Utf8String as,
                    WhenExpression when,
                    std::shared_ptr<SqlExpression> where,
                    OrderByExpression orderBy)
    : root(root), select(select), from(from), as(as), when(when), where(where), orderBy(orderBy)
{
    ExcAssert(where);
}
    
std::shared_ptr<BoundPipelineElement>
GenerateRowsElement::
bind() const
{
    return std::make_shared<Bound>(this, root->bind());
}


/*****************************************************************************/
/* BOUND GENERATE ROWS ELEMENT                                               */
/*****************************************************************************/

GenerateRowsElement::Bound::
Bound(const GenerateRowsElement * parent,
      std::shared_ptr<BoundPipelineElement> source)
    : parent(std::dynamic_pointer_cast<const GenerateRowsElement>
             (parent->shared_from_this())),
      source_(std::move(source)),
      inputScope_(std::move(source_->outputScope())),
      outputScope_(/* Add a table to the outer scope */
                   inputScope_->tableScope
                   (std::make_shared<TableLexicalScope>
                    (parent->from, parent->as)))
{
}

std::shared_ptr<ElementExecutor>
GenerateRowsElement::Bound::
start(const BoundParameters & getParam,
      bool allowParallel) const
{
    auto result = std::make_shared<GenerateRowsExecutor>();
    result->source = source_->start(getParam, allowParallel);
    result->generator
        = parent->from.runQuery(*outputScope_,
                                parent->select,
                                parent->when,
                                *parent->where,
                                parent->orderBy,
                                0 /* offset */, -1 /* limit */,
                                allowParallel);
    result->params = getParam;
    ExcAssert(result->params);
    result->generateMore();
    return result;
}

std::shared_ptr<BoundPipelineElement>
GenerateRowsElement::Bound::
boundSource() const
{
    return source_;
}

std::shared_ptr<PipelineExpressionScope>
GenerateRowsElement::Bound::
outputScope() const
{
    return outputScope_;
}


/*****************************************************************************/
/* JOIN LEXICAL SCOPE                                                        */
/*****************************************************************************/

JoinLexicalScope::
JoinLexicalScope(std::shared_ptr<PipelineExpressionScope> inner,
                 std::shared_ptr<LexicalScope> left,
                 std::shared_ptr<LexicalScope> right)
    : inner(inner), left(left), right(right),
      leftOutputAdded(left->outputAdded().size())
{
}

VariableGetter
JoinLexicalScope::
doGetVariable(const Utf8String & variableName, int fieldOffset)
{
#if 0
    cerr << "join getting variable " << variableName << " with field offset "
         << fieldOffset << endl;
    cerr << inner->numOutputFields() << " output fields" << endl;

    cerr << "tables: left " << jsonEncode(left->tableNames())
         << " right " << jsonEncode(right->tableNames()) << endl;
    cerr << "offsets: left " << leftFieldOffset(fieldOffset)
         << " right " << rightFieldOffset(fieldOffset) << endl;
#endif

    auto check = [&] (LexicalScope & scope, int fieldOffset) -> VariableGetter
        {
            for (auto & t: scope.tableNames()) {
                Utf8String prefix = t + ".";
                if (variableName.startsWith(prefix)) {
                    //cerr << "matches this side" << endl;

                    Utf8String name = variableName;

                    // If this scope has an as() field which is equal
                    // to the table name we asked for, then it's a
                    // terminal table with non-prefixed variables and
                    // we need to remove the table name since it's no
                    // longer ambiguous.
                    if (scope.as() == t)
                        name.replace(0, prefix.length(), Utf8String());
                        
#if 0
                    cerr << "getting from lexical scope " << t
                         << " with name "
                         << name << " and as " << scope.as()
                         << " and field offset " << fieldOffset
                         << endl;
#endif
                        
                    return scope.doGetVariable(name, fieldOffset);
                }
            }

            return VariableGetter();
        };
        
    VariableGetter result = check(*left, leftFieldOffset(fieldOffset));
    if (result.exec) return result;
    result = check(*right, rightFieldOffset(fieldOffset));
    if (result.exec) return result;

    // We can pass through the same scope, since we will point to the
    // same object.
    result = inner->doGetVariable(Utf8String(), variableName);
        
    return result;
}

/** For a join, we can select over the columns for either one or the other. */
GetAllColumnsOutput
JoinLexicalScope::
doGetAllColumns(std::function<Utf8String (const Utf8String &)> keep, int fieldOffset)
{
    //cerr << "doGetAllColums for join with field offset " << fieldOffset << endl;

    Utf8String leftPrefix = left->as();
    Utf8String rightPrefix = right->as();

    auto leftOutput = left->doGetAllColumns(keep, leftFieldOffset(fieldOffset));
    auto rightOutput = right->doGetAllColumns(keep, rightFieldOffset(fieldOffset));

    GetAllColumnsOutput result;
    result.exec = [=] (const SqlRowScope & scope) -> ExpressionValue
        {
            ExpressionValue leftResult = leftOutput.exec(scope);
            ExpressionValue rightResult = rightOutput.exec(scope);

            //cerr << "get all columns merging "
            //     << jsonEncode(leftResult) << " and "
            //     << jsonEncode(rightResult) << endl;

                
            StructValue output;
            leftResult.appendToRow(ColumnName(leftPrefix), output);
            rightResult.appendToRow(ColumnName(rightPrefix), output);

            return std::move(output);
        };

    auto cols1 = leftOutput.info->getKnownColumns();
    auto cols2 = rightOutput.info->getKnownColumns();

    if (!leftPrefix.empty()) {
        leftPrefix += ".";
    }

    if (!rightPrefix.empty()) {
        rightPrefix += ".";
    }

    std::vector<KnownColumn> knownColumns;
    for (auto & c: cols1) {
        if (!leftPrefix.empty())
            c.columnName = ColumnName(leftPrefix + c.columnName.toUtf8String());
        knownColumns.emplace_back(std::move(c));
    }
    for (auto & c: cols2) {
        if (!rightPrefix.empty())
            c.columnName = ColumnName(rightPrefix + c.columnName.toUtf8String());
        knownColumns.emplace_back(std::move(c));
    }

    SchemaCompleteness unk1 = leftOutput.info->getSchemaCompleteness();
    SchemaCompleteness unk2 = rightOutput.info->getSchemaCompleteness();

    result.info = std::make_shared<RowValueInfo>
        (knownColumns,
         (unk1 == SCHEMA_OPEN || unk2 == SCHEMA_OPEN
          ? SCHEMA_OPEN : SCHEMA_CLOSED));
        
    return result;
}

BoundFunction
JoinLexicalScope::
doGetFunction(const Utf8String & functionName,
              const std::vector<BoundSqlExpression> & args,
              int fieldOffset)
{
    //cerr << "Asking join for function " << functionName
    //     << " with field offset " << fieldOffset << endl;

    if (functionName == "rowName") {
        auto leftRowName
            = left->doGetFunction(functionName, args, leftFieldOffset(fieldOffset));
        auto rightRowName
            = right->doGetFunction(functionName, args, rightFieldOffset(fieldOffset));
            
        auto exec = [=] (const std::vector<ExpressionValue> & args,
                         const SqlRowScope & context)
            -> ExpressionValue
            {
                return ExpressionValue
                (leftRowName(args, context).toUtf8String()
                 + "-"
                 + rightRowName(args, context).toUtf8String(),
                 Date::notADate());
            };

        return { exec, leftRowName.resultInfo };
    }

    // For now, don't allow joins to override functions
    return inner->doGetFunction(Utf8String(), functionName, args);
}

/** Joins don't introduce a scope name for the join. */
Utf8String
JoinLexicalScope::
as() const
{
    return Utf8String();
}

std::set<Utf8String>
JoinLexicalScope::
tableNames() const
{
    std::set<Utf8String> leftNames = left->tableNames();
    std::set<Utf8String> rightNames = right->tableNames();
    leftNames.insert(rightNames.begin(), rightNames.end());
    return leftNames;
}

std::vector<std::shared_ptr<ExpressionValueInfo> >
JoinLexicalScope::
outputAdded() const
{
    auto leftAdded = (*left).outputAdded();
    auto rightAdded = (*right).outputAdded();

    leftAdded.insert(leftAdded.end(),
                     std::make_move_iterator(rightAdded.begin()),
                     std::make_move_iterator(rightAdded.end()));

    return leftAdded;
}


/*****************************************************************************/
/* JOIN ELEMENT                                                              */
/*****************************************************************************/

JoinElement::
JoinElement(std::shared_ptr<PipelineElement> root,
            std::shared_ptr<TableExpression> left,
            std::shared_ptr<TableExpression> right,
            std::shared_ptr<SqlExpression> on,
            JoinQualification joinQualification,
            SelectExpression select,
            std::shared_ptr<SqlExpression> where,
            OrderByExpression orderBy)
    : root(root), left(left), right(right), on(on),
      select(select), where(where), orderBy(orderBy),
      condition(left, right, on, where), joinQualification(joinQualification)
{
    switch (condition.style) {
    case AnnotatedJoinCondition::CROSS_JOIN:
    case AnnotatedJoinCondition::EQUIJOIN:
        break;
    default:
        throw HttpReturnException(400, "Join expression requires an equality operator; needs to be in the form f(left) = f(right)",
                                  "joinOn", on,
                                  "condition", condition);
    }

    SelectExpression selectAll = SelectExpression::parse("*");

    // JOIN do not support when expression
    auto when = WhenExpression::parse("true");

    bool outerLeft = joinQualification == JOIN_LEFT || joinQualification == JOIN_FULL;
    bool outerRight = joinQualification == JOIN_RIGHT || joinQualification == JOIN_FULL;

    auto constantWhere = condition.constantWhere;

    //These are the values that we need to compute to see if the rows "match"
    std::vector<std::shared_ptr<SqlExpression> > leftclauses = { condition.left.selectExpression };    
    std::vector<std::shared_ptr<SqlExpression> > rightclauses= { condition.right.selectExpression };    

    auto leftCondition = condition.left.where;
    auto rightCondition = condition.right.where;

    // if outer join, we need to grab all row on one or both sides  

    auto fixOuterSide = [&] (std::shared_ptr<SqlExpression>& condition, AnnotatedJoinCondition::Side& side, std::vector<std::shared_ptr<SqlExpression> >& clauses)
        {
            //remove the condition, we want all rows from this side
            condition = SqlExpression::TRUE;

            auto notnullExpr = std::make_shared<IsTypeExpression>(side.where, true, "null");
            auto conditionExpr = std::make_shared<BooleanOperatorExpression>(BooleanOperatorExpression(side.where, constantWhere, "AND"));
            auto complementExpr = std::make_shared<BooleanOperatorExpression>(BooleanOperatorExpression(conditionExpr, notnullExpr, "AND"));

            //add the condition to the select expression instead

            clauses.push_back(complementExpr);
        };

    if (outerLeft)
        fixOuterSide(leftCondition, condition.left, leftclauses);      

    if (outerRight)
        fixOuterSide(leftCondition, condition.left, leftclauses);      

    if (outerLeft || outerRight)
        constantWhere = SqlExpression::TRUE;

    auto leftEmbedding = std::make_shared<EmbeddingLiteralExpression>(leftclauses);
    auto rightEmbedding = std::make_shared<EmbeddingLiteralExpression>(rightclauses);

    leftImpl= root
        ->where(constantWhere)
        ->from(left, when, selectAll, leftCondition,
               condition.left.orderBy)
        ->select(leftEmbedding);

    rightImpl = root
        ->where(constantWhere)
        ->from(right, when, selectAll, rightCondition,
               condition.right.orderBy)
        ->select(rightEmbedding);
}

std::shared_ptr<BoundPipelineElement>
JoinElement::
bind() const
{
    return std::make_shared<Bound>(root->bind(),
                                   leftImpl->bind(),
                                   rightImpl->bind(),
                                   condition,
                                   joinQualification);
}


/*****************************************************************************/
/* CROSS JOIN EXECUTOR                                                       */
/*****************************************************************************/
    
JoinElement::CrossJoinExecutor::
CrossJoinExecutor(const Bound * parent,
                  std::shared_ptr<ElementExecutor> root,
                  std::shared_ptr<ElementExecutor> left,
                  std::shared_ptr<ElementExecutor> right)
    : parent(parent),
      root(std::move(root)),
      left(std::move(left)),
      right(std::move(right))
{
    ExcAssert(parent && this->root && this->left && this->right);
    l = this->left->take();
    r = this->right->take();
}

std::shared_ptr<PipelineResults>
JoinElement::CrossJoinExecutor::
take()
{
    for (;;) {

        if (!l) {
            this->left->restart();
            l = this->left->take();
            r = this->right->take();
        }
        if (!l || !r)
            return nullptr;

        // Got a row!
        //cerr << "Cross join got a row" << endl;
        //cerr << "l = " << jsonEncode(l) << endl;
        //cerr << "r = " << jsonEncode(r) << endl;

        // Pop the selected join condition from l
        l->values.pop_back();

        for (auto & v: r->values)
            l->values.emplace_back(v);

        // Pop the selected join condition from r
        l->values.pop_back();

        //cerr << "cross returning " << jsonEncode(l) << endl;

        auto result = l;

        l = this->left->take();

        ExpressionValue storage;
        if (!parent->crossWhere_(*result, storage).isTrue())
            continue;


        return result;
    }
}

void
JoinElement::CrossJoinExecutor::
restart()
{
    left->restart();
    right->restart();
    l = left->take();
    r = right->take();
}


/*****************************************************************************/
/* EQUI JOIN EXECUTOR                                                        */
/*****************************************************************************/

JoinElement::EquiJoinExecutor::
EquiJoinExecutor(const Bound * parent,
                 std::shared_ptr<ElementExecutor> root,
                 std::shared_ptr<ElementExecutor> left,
                 std::shared_ptr<ElementExecutor> right)
    : parent(parent),
      root(std::move(root)),
      left(std::move(left)),
      right(std::move(right))
{
    l = this->left->take();
    r = this->right->take();
    takeMoreInput();
}

void
JoinElement::EquiJoinExecutor::
takeMoreInput()
{
    bool outerLeft = parent->joinQualification_ == JOIN_LEFT || parent->joinQualification_ == JOIN_FULL;
    bool outerRight = parent->joinQualification_ == JOIN_RIGHT || parent->joinQualification_ == JOIN_FULL;

    auto takeValueFromSide = [] (std::shared_ptr<PipelineResults>& s, std::shared_ptr<ElementExecutor>& executor, bool doOuter)
    {
        do
        {
            while (s && s->values.back().empty())
                s = executor->take();

            if (s)
            {
                ExpressionValue & embedding = s->values.back();
                ExpressionValue field = embedding.getField(0);

                //if we want to do an outer join we need all rows
                if (!field.empty() || doOuter)
                {
                    break;
                }
                else 
                {
                    s = executor->take();
                }

            }
        }
        while (s);
    };

    takeValueFromSide(l, this->left, outerLeft);
    takeValueFromSide(r, this->right, outerRight);   
}
            
std::shared_ptr<PipelineResults>
JoinElement::EquiJoinExecutor::
take()
{
    bool outerLeft = parent->joinQualification_ == JOIN_LEFT || parent->joinQualification_ == JOIN_FULL;
    bool outerRight = parent->joinQualification_ == JOIN_RIGHT || parent->joinQualification_ == JOIN_FULL;

    while (l && r) {

        ExpressionValue & lEmbedding = l->values.back();
        ExpressionValue & rEmbedding = r->values.back();

        ExpressionValue lField = lEmbedding.getField(0);
        ExpressionValue rField = rEmbedding.getField(0);

        //in case of outer join
        //check the where condition that we took out and put in the embedding instead
        auto checkOuterWhere = [] ( std::shared_ptr<PipelineResults>& s, std::shared_ptr<ElementExecutor>& executor, ExpressionValue& field, ExpressionValue & embedding) -> bool
        {
            ExpressionValue where = embedding.getField(1);
            //if the condition would have failed, or the select value is null, return the row.
            if (field.empty() || !where.asBool())
            {
                s->values.pop_back();
                s->values.emplace_back(ExpressionValue("", Date()));
                s->values.emplace_back(ExpressionValue("", Date()));
                return true;
            }

            return false;
        };

        if (outerLeft && checkOuterWhere(l, left, lField, rEmbedding))
        {
            auto result = std::move(r);                
            r = right->take();
            return result;    
        }

        if (outerRight && checkOuterWhere(r, right, rField, lEmbedding))
        {
            auto result = std::move(r);                
            r = right->take();
            return result;
        }

        if (lField == rField) {
            // Got a row!
            //cerr << "*** got row match on " << jsonEncode(lField) << endl;

            // Pop the selected join condition from l
            l->values.pop_back();

            // Pop the selected join condition from r
            r->values.pop_back();

            for (auto & v: r->values)
                l->values.emplace_back(std::move(v));

            //cerr << "returning " << jsonEncode(l) << endl;

            auto result = std::move(l);

            l = left->take();
            r = right->take();

            //cerr << "applying where " << parent->crossWhere_.expr->print()
            //     << " to " << jsonEncode(result->values) << endl;

            ExpressionValue storage;
            if (!parent->crossWhere_(*result, storage).isTrue())
            {
                continue;
            }

            return result;
        }
        else if (lField < rField) {
            do {
                l = this->left->take();     
            } while (l && l->values.back() < rField);
        }
        else {
            do {
                r = this->right->take();
            } while (r && r->values.back() < lField);
        }
    }

    if (outerLeft && l)
    {
        l->values.pop_back();
        l->values.emplace_back(ExpressionValue("", Date()));
        l->values.emplace_back(ExpressionValue("", Date()));
        auto result = std::move(l);                
        l = left->take();
        return result;
    }

    if (outerRight && r)
    {
        r->values.pop_back();
        r->values.insert(r->values.begin(), ExpressionValue("", Date()));
        r->values.insert(r->values.begin(), ExpressionValue("", Date()));
        auto result = std::move(r);

        r = right->take();
        return result;
    }   
            
    // Nothing more found
    return nullptr;
}

void
JoinElement::EquiJoinExecutor::
restart()
{
    //cerr << "**** equijoin restart" << endl;
    left->restart();
    right->restart();
    l = left->take();
    r = right->take();
    takeMoreInput();
}


/*****************************************************************************/
/* BOUND JOIN EXECUTOR                                                       */
/*****************************************************************************/

JoinElement::Bound::
Bound(std::shared_ptr<BoundPipelineElement> root,
      std::shared_ptr<BoundPipelineElement> left,
      std::shared_ptr<BoundPipelineElement> right,
      AnnotatedJoinCondition condition,
      JoinQualification joinQualification)
    : root_(std::move(root)),
      left_(std::move(left)),
      right_(std::move(right)),
      outputScope_(createOutputScope()),
      crossWhere_(condition.crossWhere->bind(*outputScope_)),
      condition_(std::move(condition)),
      joinQualification_(joinQualification)
{
}

std::shared_ptr<PipelineExpressionScope>
JoinElement::Bound::
createOutputScope()
{
    auto rootScope = root_->outputScope();
    auto leftScope = left_->outputScope()->defaultScope();
    auto rightScope = right_->outputScope()->defaultScope();

    auto tableScope = rootScope
        //->tableScope(leftScope)
        //->tableScope(rightScope)
        ->tableScope(std::make_shared<JoinLexicalScope>(rootScope, leftScope, rightScope));

#if 0
    cerr << "root is " << ML::type_name(*root_) << " left is "
         << ML::type_name(*left_) << " right is " << ML::type_name(*right_)
         << endl;
    cerr << "output scope for join: rootScope size is " << rootScope->numOutputFields()
         << " leftScope is " << leftScope->outputAdded().size()
         << " rightScope is " << rightScope->outputAdded().size()
         << " total scope is " << tableScope->numOutputFields()
         << endl;

    cerr << "known tables: " << endl;
    for (auto & t: tableScope->tables) {
        cerr << t.first << " " << t.second.fieldOffset << " " << ML::type_name(*t.second.scope) << endl;
    }
#endif

    return tableScope;
}
        
std::shared_ptr<ElementExecutor>
JoinElement::Bound::
start(const BoundParameters & getParam,
      bool allowParallel) const
{
    switch (condition_.style) {

    case AnnotatedJoinCondition::CROSS_JOIN:
        return std::make_shared<CrossJoinExecutor>
            (this,
             root_->start(getParam, allowParallel),
             left_->start(getParam, allowParallel),
             right_->start(getParam, allowParallel));

    case AnnotatedJoinCondition::EQUIJOIN:
        return std::make_shared<EquiJoinExecutor>
            (this,
             root_->start(getParam, allowParallel),
             left_->start(getParam, allowParallel),
             right_->start(getParam, allowParallel));

    default:
        throw HttpReturnException(400, "Can't execute that kind of join",
                                  "condition", condition_);
    }
}

std::shared_ptr<BoundPipelineElement>
JoinElement::Bound::
boundSource() const
{
    return left_->boundSource();
}

std::shared_ptr<PipelineExpressionScope>
JoinElement::Bound::
outputScope() const
{
    return outputScope_;
}


/*****************************************************************************/
/* ROOT ELEMENT                                                              */
/*****************************************************************************/

RootElement::
RootElement(std::shared_ptr<SqlBindingScope> outer)
    : outer(outer)
{
}

std::shared_ptr<BoundPipelineElement>
RootElement::
bind() const
{
    return std::make_shared<Bound>(outer);
}

/*****************************************************************************/
/* ROOT ELEMENT EXECUTOR                                                     */
/*****************************************************************************/

std::shared_ptr<PipelineResults>
RootElement::Executor::
take()
{
    return std::make_shared<PipelineResults>();
}

void
RootElement::Executor::
restart()
{
}

/*****************************************************************************/
/* BOUND ROOT ELEMENT                                                        */
/*****************************************************************************/

RootElement::Bound::
Bound(std::shared_ptr<SqlBindingScope> outer)
    : scope_(new PipelineExpressionScope(outer))
{
}

std::shared_ptr<ElementExecutor>
RootElement::Bound::
start(const BoundParameters & getParam, bool allowParallel) const
{
    return std::make_shared<Executor>();
}

std::shared_ptr<BoundPipelineElement>
RootElement::Bound::
boundSource() const
{
    return nullptr;
}

std::shared_ptr<PipelineExpressionScope>
RootElement::Bound::
outputScope() const
{
    return scope_;
}


/*****************************************************************************/
/* FROM ELEMENT                                                              */
/*****************************************************************************/

FromElement::
FromElement(std::shared_ptr<PipelineElement> root_,
            std::shared_ptr<TableExpression> from_,
            WhenExpression when_,
            SelectExpression select_,
            std::shared_ptr<SqlExpression> where_,
            OrderByExpression orderBy_)
    : root(std::move(root_)), from(std::move(from_)),
      select(std::move(select_)), when(std::move(when_)), where(std::move(where_)),
      orderBy(std::move(orderBy_))
{
    ExcAssert(this->from);
    ExcAssert(this->root);

    UnboundEntities unbound = from->getUnbound();
    //cerr << "unbound for from = " << jsonEncode(unbound) << endl;


    if (!from || from->getType() == "null") {
        // No from clause
        // (TODO: generate single value)
        throw HttpReturnException(400, "Can't deal with no from clause",
                                  "exprType", from->getType());
    }
    else if (from->getType() == "join") {
        std::shared_ptr<JoinExpression> join
            = std::dynamic_pointer_cast<JoinExpression>(from);
        ExcAssert(join);

        impl.reset(new JoinElement(root, join->left, join->right, join->on, join->qualification, select, where, orderBy_));
        // TODO: order by for join output
            
    }
    else {
        auto rootBound = root->bind();
        auto scope = rootBound->outputScope();

        if (!unbound.params.empty())
            throw HttpReturnException(400, "Can't deal with from expression "
                                      "with unbound parameters",
                                      "exprType", from->getType(),
                                      "unbound", unbound);
            

        // Need to bound here to get the dataset
        BoundTableExpression bound = from->bind(*scope);
        impl.reset(new GenerateRowsElement(root,
                                           select,
                                           bound.table,
                                           bound.asName,
                                           when, 
                                           where,
                                           orderBy));
    }
}

std::shared_ptr<BoundPipelineElement>
FromElement::
bind() const
{
    return impl->bind();
}


/*****************************************************************************/
/* FILTER WHERE ELEMENT                                                      */
/*****************************************************************************/

FilterWhereElement::
FilterWhereElement(std::shared_ptr<PipelineElement> source,
                   std::shared_ptr<SqlExpression> where)
    : where_(where), source_(source)
{
    ExcAssert(where_);
    ExcAssert(source_);
}

std::shared_ptr<BoundPipelineElement>
FilterWhereElement::
bind() const
{
    return std::make_shared<Bound>(source_->bind(), *where_);
}


/*****************************************************************************/
/* FILTER WHERE EXECUTOR                                                     */
/*****************************************************************************/

std::shared_ptr<PipelineResults>
FilterWhereElement::Executor::
take()
{
    while (true) {
        std::shared_ptr<PipelineResults> input = source_->take();

        // If nothing left to give, then return an empty vector
        if (!input)
            return input;
                
        // Evaluate the where expression...
        ExpressionValue storage;
        const ExpressionValue & pass = parent_->where_(*input, storage);
                
        // If it doesn't evaluate to true, then on to the next row
        if (!pass.isTrue())
            continue;

        // Otherwise, we have our result
        return input;
    }
}

void
FilterWhereElement::Executor::
restart()
{
    source_->restart();
}


/*****************************************************************************/
/* BOUND FILTER WHERE                                                        */
/*****************************************************************************/

FilterWhereElement::Bound::
Bound(std::shared_ptr<BoundPipelineElement> source,
      const SqlExpression & where)
    : source_(std::move(source)),
      scope_(source_->outputScope())
{
    where_ = where.bind(*scope_);
}

std::shared_ptr<ElementExecutor>
FilterWhereElement::Bound::
start(const BoundParameters & getParam,
      bool allowParallel) const
{
    auto result = std::make_shared<Executor>();
    result->parent_ = this;
    result->source_ = source_->start(getParam, allowParallel);
    return result;
}

std::shared_ptr<BoundPipelineElement>
FilterWhereElement::Bound::
boundSource() const
{
    return source_;
}

std::shared_ptr<PipelineExpressionScope>
FilterWhereElement::Bound::
outputScope() const
{
    return scope_;
}


/*****************************************************************************/
/* SELECT ELEMENT                                                            */
/*****************************************************************************/

SelectElement::
SelectElement(std::shared_ptr<PipelineElement> source,
              SelectExpression select)
    : select(std::make_shared<SelectExpression>(select)), source(source)
{
    ExcAssert(this->select);
}

SelectElement::
SelectElement(std::shared_ptr<PipelineElement> source,
              std::shared_ptr<SqlExpression> expr)
    : select(expr), source(source)
{
    ExcAssert(this->select);
}

std::shared_ptr<BoundPipelineElement>
SelectElement::
bind() const
{
    return std::make_shared<Bound>(source->bind(), *select);
}

/*****************************************************************************/
/* SELECT ELEMENT EXECUTOR                                                   */
/*****************************************************************************/

std::shared_ptr<PipelineResults>
SelectElement::Executor::
take()
{
    while (true) {
        std::shared_ptr<PipelineResults> input = source->take();
                
        // If nothing left to give, then return an empty vector
        if (!input)
            return input;
                
        // Run the select expression in this input's context
        ExpressionValue selected = parent->select_(*input);

        input->values.emplace_back(std::move(selected));

        return input;
    }
}

void
SelectElement::Executor::
restart()
{
    source->restart();
}


/*****************************************************************************/
/* BOUND SELECT ELEMENT                                                      */
/*****************************************************************************/


SelectElement::Bound::
Bound(std::shared_ptr<BoundPipelineElement> source,
      const SqlExpression & select)
    : source_(std::move(source)),
      select_(select.bind(*source_->outputScope())),
      outputScope_(source_->outputScope()->selectScope({select_.info}))
{
    ExcAssert(source_->outputScope()->inLexicalScope());
}

std::shared_ptr<ElementExecutor>
SelectElement::Bound::
start(const BoundParameters & getParam,
      bool allowParallel) const
{
    auto result = std::make_shared<Executor>();
    result->parent = this;
    result->source = source_->start(getParam, allowParallel);
    return result;
}

std::shared_ptr<BoundPipelineElement>
SelectElement::Bound::
boundSource() const
{
    return source_;
}

std::shared_ptr<PipelineExpressionScope>
SelectElement::Bound::
outputScope() const
{
    return outputScope_;
}


/*****************************************************************************/
/* ORDER BY ELEMENT                                                          */
/*****************************************************************************/

OrderByElement::
OrderByElement(std::shared_ptr<PipelineElement> source,
               OrderByExpression orderBy)
    : source(source), orderBy(orderBy)
{
}

std::shared_ptr<BoundPipelineElement>
OrderByElement::
bind() const
{
    return std::make_shared<Bound>(source->bind(), orderBy);
}


/*****************************************************************************/
/* ORDER BY ELEMENT EXECUTOR                                                 */
/*****************************************************************************/

OrderByElement::Executor::
Executor(const Bound * parent,
         std::shared_ptr<ElementExecutor> source)
    : parent(parent), source(std::move(source)),
      numDone(-1)
{
}

std::shared_ptr<PipelineResults>
OrderByElement::Executor::
take()
{
    // We haven't returned anything yet.  Grab the entire set of results
    // from the input, sort it, and get it ready to serve up as results
    // of the query.
    if (numDone == -1) {
        // Get and sort the input

        while (true) {
            std::shared_ptr<PipelineResults> input = source->take();
            if (!input)
                break;
            sorted.emplace_back(std::move(input));
        }

        // We assume that the fields to sort on are at the end of the
        // list of fields.
        int offset
            = parent->scope_->numOutputFields()
            - parent->orderBy_.clauses.size();

        auto compare = [&] (const std::shared_ptr<PipelineResults> & p1,
                            const std::shared_ptr<PipelineResults> & p2)
            -> bool
            {
                return parent->orderBy_.less(p1->values, p2->values,
                                             offset);
            };
                
        std::sort(sorted.begin(), sorted.end(), compare);
                
        numDone = 0;
    }

    // OK, sorting is done.  Do we have anything left?  If not, return null
    if (numDone == sorted.size()) {
        sorted.clear();
        return nullptr;
    }

    // If so, return it
    return sorted[numDone++];
}

void
OrderByElement::Executor::
restart()
{
    // Don't re-sort the elements...
    numDone = 0;
}


/*****************************************************************************/
/* BOUND ORDER BY ELEMENT                                                    */
/*****************************************************************************/

OrderByElement::Bound::
Bound(std::shared_ptr<BoundPipelineElement> source,
      const OrderByExpression & orderBy)
    : source_(std::move(source)),
      scope_(source_->outputScope()),
      orderBy_(orderBy.bindAll(*scope_))
{
    ExcAssert(scope_->inLexicalScope());
}

std::shared_ptr<ElementExecutor>
OrderByElement::Bound::
start(const BoundParameters & getParam,
      bool allowParallel) const
{
    return std::make_shared<Executor>(this,
                                      source_->start(getParam, allowParallel));
}

std::shared_ptr<BoundPipelineElement>
OrderByElement::Bound::
boundSource() const
{
    return source_;
}

std::shared_ptr<PipelineExpressionScope>
OrderByElement::Bound::
outputScope() const
{
    return scope_;
}


/*****************************************************************************/
/* AGGREGATE LEXICAL SCOPE                                                   */
/*****************************************************************************/

AggregateLexicalScope::
AggregateLexicalScope(std::shared_ptr<PipelineExpressionScope> inner)
    : inner(inner)
{
}

VariableGetter
AggregateLexicalScope::
doGetVariable(const Utf8String & variableName,
              int fieldOffset)
{
    //cerr << "aggregate scope getting variable " << variableName
    //     << " at field offset " << fieldOffset << endl;

    // We can pass through the same scope, since we will point to the
    // same object.
    auto innerGetter = inner->doGetVariable(Utf8String(), variableName);

    return innerGetter;
}

GetAllColumnsOutput
AggregateLexicalScope::
doGetAllColumns(std::function<Utf8String (const Utf8String &)> keep,
                int fieldOffset)
{
    throw HttpReturnException(400, "Can't use wildcards within group by");
}

BoundFunction
AggregateLexicalScope::
doGetFunction(const Utf8String & functionName,
              const std::vector<BoundSqlExpression> & args,
              int fieldOffset)
{
    auto aggregate = inner->doGetAggregator(functionName, args);

    if (aggregate) {
        auto exec = [=] (const std::vector<ExpressionValue> & argValues,
                         const SqlRowScope & rowScope) -> ExpressionValue
            {
                auto & row = static_cast<const PipelineResults &>(rowScope);

                std::shared_ptr<void> storage = aggregate.init();
                    
                std::vector<ExpressionValue> rowArgs(args.size());

                for (auto & r: row.group) {
                    // Apply the arguments to the row

                    for (unsigned i = 0;  i != args.size();  ++i)
                        rowArgs[i] = args[i](*r);

                    aggregate.process(&rowArgs[0], args.size(), storage.get());
                }

                return aggregate.extract(storage.get());
            };

        return { exec, aggregate.resultInfo };
    }
    else {
        return inner->doGetFunction(Utf8String(), functionName, args);
    }

    return BoundFunction();
}

Utf8String
AggregateLexicalScope::
as() const
{
    return Utf8String();
}

std::vector<std::shared_ptr<ExpressionValueInfo> >
AggregateLexicalScope::
outputAdded() const
{
    return { };
}


/*****************************************************************************/
/* PARTITION ELEMENT                                                         */
/*****************************************************************************/


PartitionElement::
PartitionElement(std::shared_ptr<PipelineElement> source,
                 int numValues)
    : source(source), numValues(numValues)
{
}

std::shared_ptr<BoundPipelineElement>
PartitionElement::
bind() const
{
    return std::make_shared<Bound>(source->bind(), numValues);
}


/*****************************************************************************/
/* PARTITION ELEMENT EXECUTOR                                                */
/*****************************************************************************/

PartitionElement::Executor::
Executor(const Bound * parent,
         std::shared_ptr<ElementExecutor> source,
         int firstIndex,
         int lastIndex)
    : parent(parent),
      source(std::move(source)),
      firstIndex(firstIndex), lastIndex(lastIndex)
{
    first = this->source->take();
}

bool
PartitionElement::Executor::
sameGroup(const std::vector<ExpressionValue> & group1,
          const std::vector<ExpressionValue> & group2) const
{
    for (unsigned i = firstIndex;  i < lastIndex;  ++i) {
        const ExpressionValue & e1 = group1[i];
        const ExpressionValue & e2 = group2[i];
        int cmp = e1.compare(e2);
        if (cmp != 0)
            return false;
    }
            
    return true;
};

// Take a group at a time
std::shared_ptr<PipelineResults>
PartitionElement::Executor::
take()
{
    // If no more, we're done
    if (!first)
        return nullptr;

    std::shared_ptr<PipelineResults> key = first;
    std::vector<std::shared_ptr<PipelineResults> > group;
            
    while (first && sameGroup(first->values, key->values)) {
        group.emplace_back(std::move(first));
        first = source->take();
    }

    // Now return the result of our group
    auto result = key;
    result->group = std::move(group);

    //cerr << "got group " << jsonEncode(result->group) << endl;

    return result;
}

void
PartitionElement::Executor::
restart()
{
    source->restart();
    first = source->take();
}


/*****************************************************************************/
/* BOUND PARTITION ELEMENT                                                   */
/*****************************************************************************/

PartitionElement::Bound::
Bound(std::shared_ptr<BoundPipelineElement> source,
      int numValues)
    : source_(std::move(source)),
      outputScope_(source_->outputScope()
                   ->tableScope(std::make_shared<AggregateLexicalScope>
                                (source_->outputScope())))
{
}

std::shared_ptr<ElementExecutor>
PartitionElement::Bound::
start(const BoundParameters & getParam,
      bool allowParallel) const
{
    return std::make_shared<Executor>
        (this, source_->start(getParam, allowParallel),
         source_->numOutputFields() - numValues_,
         source_->numOutputFields());
}
        
std::shared_ptr<BoundPipelineElement>
PartitionElement::Bound::
boundSource() const
{
    return source_;
}

std::shared_ptr<PipelineExpressionScope>
PartitionElement::Bound::
outputScope() const
{
    return outputScope_;
}


/*****************************************************************************/
/* PARAMS ELEMENT                                                            */
/*****************************************************************************/

ParamsElement::
ParamsElement(std::shared_ptr<PipelineElement> source,
              GetParamInfo getParamInfo)
    : source_(std::move(source)),
      getParamInfo_(std::move(getParamInfo))
{
}

std::shared_ptr<BoundPipelineElement>
ParamsElement::
bind() const
{
    return std::make_shared<Bound>(source_->bind(), getParamInfo_);
}


/*****************************************************************************/
/* PARAMS ELEMENT EXECUTOR                                                   */
/*****************************************************************************/

ParamsElement::Executor::
Executor(std::shared_ptr<ElementExecutor> source,
         BoundParameters getParam)
    : source_(std::move(source)),
      getParam_(std::move(getParam))
{
    ExcAssert(getParam_);
}

std::shared_ptr<PipelineResults>
ParamsElement::Executor::
take()
{
    auto result = source_->take();
    result->getParam = getParam_;
    return result;
}

void
ParamsElement::Executor::
restart()
{
}


/*****************************************************************************/
/* BOUND PARAMS ELEMENT                                                      */
/*****************************************************************************/

ParamsElement::Bound::
Bound(std::shared_ptr<BoundPipelineElement> source,
      GetParamInfo getParamInfo)
    : source_(std::move(source)),
      outputScope_(source_->outputScope()->parameterScope(std::move(getParamInfo), {} /* no extra output fields */))
{
}
        
std::shared_ptr<ElementExecutor>
ParamsElement::Bound::
start(const BoundParameters & getParam, bool allowParallel) const
{
    return std::make_shared<Executor>(source_->start(getParam, allowParallel),
                                      getParam);
}

std::shared_ptr<BoundPipelineElement>
ParamsElement::Bound::
boundSource() const
{
    return source_;
}

std::shared_ptr<PipelineExpressionScope>
ParamsElement::Bound::
outputScope() const
{
    return outputScope_;
}


} // namespace MLDB
} // namespaec Datacratic

