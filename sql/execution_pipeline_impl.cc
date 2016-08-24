/** execution_pipeline_impl.cc
    Jeremy Barnes, 27 August 2015
    Copyright (c) 2015 Datacratic Inc.  All rights reserved.

    This file is part of MLDB. Copyright 2015 Datacratic. All rights reserved.

    Implementation of the new query execution pipeline.
*/

#include "execution_pipeline_impl.h"
#include "mldb/http/http_exception.h"
#include "mldb/types/basic_value_descriptions.h"
#include "mldb/types/set_description.h"
#include "mldb/types/tuple_description.h"
#include "table_expression_operations.h"
#include <algorithm>
#include "mldb/sql/sql_expression_operations.h"
#include "mldb/types/vector_description.h"
#include "mldb/base/scope.h"

using namespace std;


namespace Datacratic {
namespace MLDB {

/*****************************************************************************/
/* TABLE LEXICAL SCOPE                                                       */
/*****************************************************************************/

TableLexicalScope::
TableLexicalScope(std::shared_ptr<RowValueInfo> rowInfo,
                  Utf8String asName_)
    : rowInfo(rowInfo), asName(std::move(asName_))
{
    knownColumns = rowInfo->getKnownColumns();

    std::sort(knownColumns.begin(), knownColumns.end(),
              [] (const KnownColumn & first,
                  const KnownColumn & second)
              {
                  return first.columnName < second.columnName;
              });
    
    hasUnknownColumns = rowInfo->getSchemaCompletenessRecursive() == SCHEMA_OPEN;
}

ColumnGetter
TableLexicalScope::
doGetColumn(const ColumnName & columnName, int fieldOffset)
{
    //cerr << "dataset lexical scope: fieldOffset = " << fieldOffset << endl;
    ExcAssertGreaterEqual(fieldOffset, 0);

    // TODO: we may know something about this column...
    return {[=] (const SqlRowScope & rowScope,
                 ExpressionValue & storage,
                 const VariableFilter & filter) -> const ExpressionValue &
            {
                auto & row = rowScope.as<PipelineResults>();

                const ExpressionValue & rowContents
                    = row.values.at(fieldOffset + ROW_CONTENTS);

                //cerr << "dataset: getting variable " << columnName
                //     << " from row " << jsonEncode(row.values)
                //     << " offset " << fieldOffset + ROW_CONTENTS
                //     << " returns " << rowContents.getField(columnName)
                //     << endl;

                return storage = std::move(rowContents.getNestedColumn(columnName,
                                                                       filter));
            },
            std::make_shared<AtomValueInfo>()};

}

GetAllColumnsOutput
TableLexicalScope::
doGetAllColumns(const Utf8String & tableName,
                const ColumnFilter& keep,
                int fieldOffset)
{
    //cerr << "dataset lexical scope get columns: fieldOffset = "
    //     << fieldOffset << endl;

    ExcAssertGreaterEqual(fieldOffset, 0);

    std::vector<KnownColumn> columnsWithInfo;
    std::map<ColumnHash, ColumnName> index;

    for (auto & column: knownColumns) {

        ColumnName outputName = keep(column.columnName);

        if (outputName.empty() && !asName.empty()) {
            // BAD SMELL
            //try with the table alias
            outputName = keep(PathElement(asName) + column.columnName);
        }

        if (outputName.empty()) {
            continue;
        }

        KnownColumn out = column;
        out.columnName = ColumnName(outputName);
        columnsWithInfo.emplace_back(std::move(out));
        index[column.columnName] = ColumnName(outputName);
    }

    auto exec = [=] (const SqlRowScope & rowScope, const VariableFilter & filter)
        -> ExpressionValue
    {
        auto & row = rowScope.as<PipelineResults>();

        const ExpressionValue & rowContents
            = row.values.at(fieldOffset + ROW_CONTENTS);

        RowValue result;

        auto onAtom = [&] (const Path & columnName,
                       const Path & prefix,
                       const CellValue & val,
                       Date ts)
        {
            ColumnName newColumnName = prefix + columnName;
            auto it = index.find(newColumnName);
            if (it == index.end()) {
                if (hasUnknownColumns) {
                    ColumnName outputName = keep(newColumnName);
                    if (!outputName.empty())
                        result.emplace_back(std::move(outputName), val, ts);
                }
                return true;
            }
            result.emplace_back(it->second, val, ts);
            return true;
        };

        rowContents.forEachAtom(onAtom);

        ExpressionValue val(std::move(result));
        return val.getFilteredDestructive(filter);
    };

    GetAllColumnsOutput result;
    result.info = std::make_shared<RowValueInfo>
        (columnsWithInfo, hasUnknownColumns ? SCHEMA_OPEN : SCHEMA_CLOSED);
    result.exec = exec;
    return result;
}

BoundFunction
TableLexicalScope::
doGetFunction(const Utf8String & functionName,
              const std::vector<BoundSqlExpression> & args,
              int fieldOffset,
              SqlBindingScope & argScope)
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
                    auto & row = rowScope.as<PipelineResults>();
                    const ExpressionValue& rowPath
                        = row.values.at(fieldOffset + ROW_PATH);

                    //Can be empty in case of unmatched outerjoin
                    if (rowPath.empty()) {
                        return ExpressionValue::null(Date::Date::notADate());
                    }
                    else {
                        return ExpressionValue
                            (rowPath.toUtf8String(),
                             row.values.at(fieldOffset + ROW_PATH)
                             .getEffectiveTimestamp());
                    }
                },
                std::make_shared<Utf8StringValueInfo>()
            };
    }
    else if (functionName == "rowPath") {
        return {[=] (const std::vector<ExpressionValue> & args,
                     const SqlRowScope & rowScope)
                {
                    auto & row = rowScope.as<PipelineResults>();
                    return row.values.at(fieldOffset + ROW_PATH);
                },
                std::make_shared<PathValueInfo>()
                };
    }

    else if (functionName == "rowHash") {
        return {[=] (const std::vector<ExpressionValue> & args,
                     const SqlRowScope & rowScope)
                {
                    auto & row = rowScope.as<PipelineResults>();
                    RowHash result(row.values.at(fieldOffset + ROW_PATH)
                                   .coerceToPath());
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
    return { std::make_shared<Utf8StringValueInfo>(), rowInfo };
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
generateMore(SqlRowScope & rowScope)
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

    current = generator(1000, rowScope, params);
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
    // Return the row itself as the value, and the row's name as
    // metadata.
    auto result = source->take();

    if (!result)
        return result;

    if (currentDone == current.size() && !generateMore(*result))
        return nullptr;

    //cerr << "got row " << current[currentDone].rowName << " "
    //     << jsonEncodeStr(current[currentDone].columns) << endl;

    result->values.emplace_back(current[currentDone].rowName,
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
                    (parent->from.getRowInfo(), parent->as)))
{
}

std::shared_ptr<ElementExecutor>
GenerateRowsElement::Bound::
start(const BoundParameters & getParam) const
{
    auto result = std::make_shared<GenerateRowsExecutor>();
    result->source = source_->start(getParam);
    result->generator
        = parent->from.runQuery(*outputScope_,
                                parent->select,
                                parent->when,
                                *parent->where,
                                parent->orderBy,
                                0 /* offset */, -1 /* limit */);
    result->params = getParam;
    ExcAssert(result->params);
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
/* SUB SELECT LEXICAL SCOPE                                                  */
/*****************************************************************************/

/** Lexical scope for a sub select.  It allows for the output of the SELECT to be
    used in wildcards (SELECT * from (SELECT 1 AS X))
*/

SubSelectLexicalScope::
SubSelectLexicalScope(std::shared_ptr<PipelineExpressionScope> inner, std::shared_ptr<RowValueInfo> selectInfo, Utf8String asName_)
                    : TableLexicalScope(selectInfo, asName_),
                    inner(inner), selectInfo(selectInfo) {

}

GetAllColumnsOutput
SubSelectLexicalScope::
doGetAllColumns(const Utf8String & tableName,
                const ColumnFilter& keep,
                int fieldOffset)
{
    //We want the last two that were added by the sub pipeline.
    //for example if the subpipeline queries from a dataset, it will add 4
    //but if its not from a dataset, it will add two...

    ExcAssert(outputAdded().size() >= 2);
    size_t offset = outputAdded().size() - 2;

    return TableLexicalScope::doGetAllColumns(tableName, keep, fieldOffset + offset);
}

ColumnGetter
SubSelectLexicalScope::
doGetColumn(const ColumnName & columnName, int fieldOffset)
{
    //We want the last two that were added by the sub pipeline.
    //for example if the subpipeline queries from a dataset, it will add 4
    //but if its not from a dataset, it will add two...

    ExcAssert(outputAdded().size() >= 2);
    size_t offset = outputAdded().size() - 2;

    return TableLexicalScope::doGetColumn(columnName, fieldOffset + offset);

}

BoundFunction
SubSelectLexicalScope::
doGetFunction(const Utf8String & functionName,
              const std::vector<BoundSqlExpression> & args,
              int fieldOffset,
              SqlBindingScope & argScope)
{
    ExcAssert(outputAdded().size() >= 2);
    size_t offset = outputAdded().size() - 2;
    return TableLexicalScope::doGetFunction(functionName, args, fieldOffset + offset, argScope);
}

std::set<Utf8String>
SubSelectLexicalScope::
tableNames() const {
    return {asName};
}

std::vector<std::shared_ptr<ExpressionValueInfo> >
SubSelectLexicalScope::
outputAdded() const {

    return inner->outputInfo(); // We add the result of the sub pipeline.
}

/*****************************************************************************/
/* SUB SELECT EXECUTOR                                                       */
/*****************************************************************************/

SubSelectExecutor::
SubSelectExecutor(std::shared_ptr<BoundPipelineElement> boundSelect,
                  const BoundParameters & getParam)
{
    pipeline = boundSelect->start(getParam);
}

std::shared_ptr<PipelineResults>
SubSelectExecutor::
take()
{
    auto subResult = pipeline->take();
    if (subResult)
        subResult->group.clear();

    return subResult;
}

void
SubSelectExecutor::
restart()
{
    pipeline->restart();
}


/*****************************************************************************/
/* SUB SELECT ELEMENT                                                        */
/*****************************************************************************/

SubSelectElement::
SubSelectElement(std::shared_ptr<PipelineElement> root,
                 SelectStatement& stm,
                 OrderByExpression& orderBy,
                 GetParamInfo getParamInfo,
                 const Utf8String& asName) : root(root), asName(asName) {
    if (!orderBy.clauses.empty())
        stm.orderBy = orderBy;
    pipeline = root->statement(stm, getParamInfo);
}

std::shared_ptr<BoundPipelineElement>
SubSelectElement::
bind() const
{
    return std::make_shared<Bound>(this, root->bind());
}

/*****************************************************************************/
/* BOUND SUB SELECT ELEMENT                                                  */
/*****************************************************************************/

SubSelectElement::Bound::
Bound(const SubSelectElement * parent,
      std::shared_ptr<BoundPipelineElement> source)
    : parent(std::dynamic_pointer_cast<const SubSelectElement>
      (parent->shared_from_this())),
      source_(std::move(source))
{
    boundSelect = parent->pipeline->bind();
    inputScope_ = boundSelect->outputScope();
    shared_ptr<SelectElement::Bound> castBoundSelect = dynamic_pointer_cast<SelectElement::Bound>(boundSelect);

    ExcAssert(castBoundSelect);

    std::shared_ptr<RowValueInfo> rowInfo = dynamic_pointer_cast<RowValueInfo>(castBoundSelect->select_.info);

    ExcAssert(rowInfo);

    outputScope_ = source_->outputScope()->tableScope(std::make_shared<SubSelectLexicalScope>(inputScope_, rowInfo, parent->asName));
}

std::shared_ptr<ElementExecutor>
SubSelectElement::Bound::
start(const BoundParameters & getParam) const
{
    auto result = std::make_shared<SubSelectExecutor>(boundSelect, getParam);
    return result;
}

std::shared_ptr<BoundPipelineElement>
SubSelectElement::Bound::
boundSource() const
{
    return source_;
}

std::shared_ptr<PipelineExpressionScope>
SubSelectElement::Bound::
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

ColumnGetter
JoinLexicalScope::
doGetColumn(const ColumnName & columnName, int fieldOffset)
{
#if 0
    cerr << "join getting variable " << columnName << " with field offset "
         << fieldOffset << endl;
    cerr << inner->numOutputFields() << " output fields" << endl;

    cerr << "tables: left " << jsonEncode(left->tableNames())
         << " right " << jsonEncode(right->tableNames()) << endl;
    cerr << "offsets: left " << leftFieldOffset(fieldOffset)
         << " right " << rightFieldOffset(fieldOffset) << endl;
#endif

    auto check = [&] (LexicalScope & scope, int fieldOffset) -> ColumnGetter
        {
            for (auto & t: scope.tableNames()) {
                PathElement prefix(t);
                if (columnName.startsWith(prefix)) {
                    //cerr << "matches this side" << endl;

                    ColumnName name = columnName;

                    // If this scope has an as() field which is equal
                    // to the table name we asked for, then it's a
                    // terminal table with non-prefixed variables and
                    // we need to remove the table name since it's no
                    // longer ambiguous.
                    if (scope.as() == t)
                        name = name.removePrefix(prefix);
                        
#if 0
                    cerr << "getting from lexical scope " << t
                         << " with name "
                         << name << " and as " << scope.as()
                         << " and field offset " << fieldOffset
                         << endl;
#endif
                        
                    return scope.doGetColumn(name, fieldOffset);
                }
            }

            return ColumnGetter();
        };
        
    ColumnGetter result = check(*left, leftFieldOffset(fieldOffset));
    if (result.exec) return result;
    result = check(*right, rightFieldOffset(fieldOffset));
    if (result.exec) return result;

    // We can pass through the same scope, since we will point to the
    // same object.
    result = inner->doGetColumn(Utf8String(), columnName);
        
    return result;
}

/** For a join, we can select over the columns for either one or the other. */
GetAllColumnsOutput
JoinLexicalScope::
doGetAllColumns(const Utf8String & tableName,
                const ColumnFilter& keep,
                int fieldOffset)
{
    //cerr << "doGetAllColums for join with field offset " << fieldOffset << "table name" << tableName << endl;

    PathElement leftPrefix;
    if (!left->as().empty())
        leftPrefix = left->as();
    PathElement rightPrefix;
    if (!right->as().empty())
        rightPrefix = right->as();

    bool useLeft = tableName.empty() || tableName == leftPrefix;
    bool useRight = tableName.empty() || tableName == rightPrefix;

    auto leftOutput = left->doGetAllColumns(tableName, keep, leftFieldOffset(fieldOffset));
    auto rightOutput = right->doGetAllColumns(tableName, keep, rightFieldOffset(fieldOffset));

    GetAllColumnsOutput result;
    result.exec = [=] (const SqlRowScope & scope, const VariableFilter & filter) -> ExpressionValue
        {
            ExpressionValue leftResult, rightResult;

            if (useLeft)
                leftResult = leftOutput.exec(scope, filter);

            if (useRight)
                rightResult = rightOutput.exec(scope, filter);

            //cerr << "get all columns merging "
            //     << jsonEncode(leftResult) << " and "
            //     << jsonEncode(rightResult) << endl;
                
            StructValue output;
            if (useLeft) {
                if (!leftPrefix.empty()) {
                    output.emplace_back(leftPrefix, std::move(leftResult));
                }
                else {
                    leftResult.mergeToRowDestructive(output);
                }
            }

            if (useRight) {
                 if (!rightPrefix.empty()) {
                    output.emplace_back(rightPrefix, std::move(rightResult));
                }
                else {
                    rightResult.mergeToRowDestructive(output);
                }
            }

            return std::move(output);
        };

    std::vector<KnownColumn> knownColumns;
    if (useLeft)
        knownColumns.emplace_back(leftPrefix, leftOutput.info, COLUMN_IS_DENSE,
                                  0 /* fixed offset */);
    if (useRight)
        knownColumns.emplace_back(rightPrefix, rightOutput.info, COLUMN_IS_DENSE,
                                  1 /* fixed offset */);

    SchemaCompleteness unk1 = leftOutput.info->getSchemaCompleteness();
    SchemaCompleteness unk2 = rightOutput.info->getSchemaCompleteness();

    result.info = std::make_shared<RowValueInfo>
        (knownColumns,
         ((unk1 == SCHEMA_OPEN && useLeft) || (unk2 == SCHEMA_OPEN && useRight)
          ? SCHEMA_OPEN : SCHEMA_CLOSED));
        
    return result;
}

BoundFunction
JoinLexicalScope::
doGetFunction(const Utf8String & functionName,
              const std::vector<BoundSqlExpression> & args,
              int fieldOffset,
              SqlBindingScope & argScope)
{
    //cerr << "Asking join for function " << functionName
    //     << " with field offset " << fieldOffset << endl;

    if (functionName == "rowPath" || functionName == "rowName") {
        auto leftRowPath
            = left->doGetFunction("rowPath", args, leftFieldOffset(fieldOffset), argScope);
        auto rightRowPath
            = right->doGetFunction("rowPath", args, rightFieldOffset(fieldOffset), argScope);
        
        bool isRowName = functionName == "rowName";
        std::shared_ptr<ExpressionValueInfo> info;
        if (isRowName)
            info = std::make_shared<Utf8StringValueInfo>();
        else info = std::make_shared<PathValueInfo>();
        
        auto exec = [=] (const std::vector<ExpressionValue> & args,
                         const SqlRowScope & context)
            -> ExpressionValue
            {
                ExpressionValue left = leftRowPath(args, context);
                ExpressionValue right = rightRowPath(args, context);

#if 0  // structured row names               
                Path both = left.coerceToPath() + right.coerceToPath();
#else
                Utf8String rowName;
                rowName = left.empty() ? "[]-" : "[" + left.toUtf8String() + "]-";
                rowName += right.empty() ? "[]" : "[" + right.toUtf8String() + "]";
                Path both(rowName);
#endif
                Date ts = std::min(left.getEffectiveTimestamp(),
                                   right.getEffectiveTimestamp());
                if (isRowName)
                    return ExpressionValue(both.toUtf8String(), ts);
                else return ExpressionValue(std::move(both), ts);
            };

        return { exec, info };
    }

    if (functionName == "leftRowName") {
        return left->doGetFunction("rowName", args,
                                   leftFieldOffset(fieldOffset), argScope);
    }

    if (functionName == "leftRowPath") {
        return left->doGetFunction("rowPath", args,
                                     leftFieldOffset(fieldOffset), argScope);
    }

    if (functionName == "rightRowName") {
        return right->doGetFunction("rowName", args,
                                    rightFieldOffset(fieldOffset), argScope);
    }

    if (functionName == "rightRowPath") {
        return right->doGetFunction("rowPath", args,
                                    rightFieldOffset(fieldOffset), argScope);
    }

    // For now, don't allow joins to override functions
    return inner->doGetFunction(Utf8String(), functionName, args, argScope);
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
            BoundTableExpression boundLeft,
            std::shared_ptr<TableExpression> right,
            BoundTableExpression boundRight,
            std::shared_ptr<SqlExpression> on,
            JoinQualification joinQualification,
            SelectExpression select,
            std::shared_ptr<SqlExpression> where,
            OrderByExpression orderBy)
    : root(root),
      left(left), boundLeft(boundLeft), right(right), boundRight(boundRight),
      on(on), select(select), where(where), orderBy(orderBy),
      condition(left, right, on, where, joinQualification), joinQualification(joinQualification)
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
    cerr << "LEFT " << jsonEncode(left) << endl;
    SelectExpression selectAll = SelectExpression::parse("*");

    // JOIN do not support when expression
    auto when = WhenExpression::parse("true");

    bool outerLeft = joinQualification == JOIN_LEFT || joinQualification == JOIN_FULL;
    bool outerRight = joinQualification == JOIN_RIGHT || joinQualification == JOIN_FULL;

    auto constantWhere = condition.constantWhere;

    //These are the values that we need to compute to see if the rows "match"
    TupleExpression leftclauses, rightclauses;
    leftclauses.clauses.push_back(condition.left.selectExpression);
    rightclauses.clauses.push_back(condition.right.selectExpression);

    auto leftCondition = condition.left.where;
    auto rightCondition = condition.right.where;
    cerr << "left condition " << jsonEncode(leftCondition) << endl;
    cerr << "right condition " << jsonEncode(rightCondition) << endl;
    // if outer join, we need to grab all rows on one or both sides  
    auto fixOuterSide = [&] (std::shared_ptr<SqlExpression>& condition,
                             AnnotatedJoinCondition::Side& side,
                             TupleExpression & clauses)
        {
            //remove the condition, we want all rows from this side
            condition = SqlExpression::TRUE;

            auto notnullExpr = std::make_shared<IsTypeExpression>(side.where, true, "null");
            auto conditionExpr = std::make_shared<BooleanOperatorExpression>
                (BooleanOperatorExpression(side.where, constantWhere, "AND"));
            auto complementExpr = std::make_shared<BooleanOperatorExpression>
                (BooleanOperatorExpression(conditionExpr, notnullExpr, "AND"));

            //add the condition to the select expression instead
            clauses.clauses.push_back(complementExpr);
        };

    //if (outerLeft)
        fixOuterSide(leftCondition, condition.left, leftclauses);      

        //if (outerRight)
        fixOuterSide(rightCondition, condition.right, rightclauses);      
    cerr << "left condition " << jsonEncode(leftCondition) << endl;
    cerr << "right condition " << jsonEncode(rightCondition) << endl;
    if (outerLeft || outerRight)
        constantWhere = SqlExpression::TRUE;

    // TODO: this shouldn't be an embedding... the type system for those
    // is too restrictive to be used as a select clause here
    auto leftEmbedding = std::make_shared<EmbeddingLiteralExpression>(leftclauses.clauses);
    auto rightEmbedding = std::make_shared<EmbeddingLiteralExpression>(rightclauses.clauses);

    leftImpl= root
        ->where(constantWhere)
        ->from(left, boundLeft, when, selectAll, leftCondition,
               condition.left.orderBy)
        ->select(leftEmbedding);
    cerr << "order by " << jsonEncode(condition.left.orderBy) << endl;
    rightImpl = root
        ->where(constantWhere)
        ->from(right, boundRight, when, selectAll, rightCondition,
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
    bool outerLeft = parent->joinQualification_ == JOIN_LEFT
        || parent->joinQualification_ == JOIN_FULL;
    bool outerRight = parent->joinQualification_ == JOIN_RIGHT
        || parent->joinQualification_ == JOIN_FULL;

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

        ExpressionValue & lEmbedding = l->values.back();
        ExpressionValue & rEmbedding = r->values.back();

        if (outerLeft){
            ExpressionValue where = lEmbedding.getColumn(1, GET_ALL);
            if (!where.asBool()) {

                //take left
                // Pop the selected join condition from left
                l->values.pop_back();

                //empty values for right without the selected join condition
                size_t numR = r->values.size();
                for (int i = 0; i < numR - 1; ++i) {
                    l->values.emplace_back(ExpressionValue());
                }

                auto result = l;

                l = this->left->take();

                //cerr << "cross outer left returning " << jsonEncode(result) << endl;

                return result;
            }
        }

        if (outerRight){
            ExpressionValue where = rEmbedding.getColumn(1, GET_ALL);
            if (!where.asBool()) {

                size_t numL = l->values.size();

                //empty values for left without the selected join condition
                l->values.clear();
                for (int i = 0; i < numL - 1; ++i) {
                    l->values.emplace_back(ExpressionValue());
                }

                // Add r
                for (auto & v: r->values)
                    l->values.emplace_back(v);

                // Pop the selected join condition from r
                l->values.pop_back();

                auto result = l;

                l = this->left->take();

                //cerr << "cross outer right returning " << jsonEncode(result) << endl;

                return result;
            }
        }

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
        if (!parent->crossWhere_(*result, storage, GET_LATEST).isTrue())
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
      right(std::move(right)),
      alreadySeenLeftRow(false)
{
    auto lresult = this->left->take();
    bufferedLeftValues.push_back(lresult);
    l = bufferedLeftValues.begin();
    firstDuplicate = l;
    r = this->right->take();
}

/**
    Whevever the left side value of the pivot is greater
    than the right side we get the next item on the right side
    and rewind the left side until its value is equal or
    greater than the new right side value.  Such rewinding is
    necessary because there might be several identical values
    on each side and in this case, we need to return all the rows
    in the cross product.
*/
std::shared_ptr<PipelineResults>
JoinElement::EquiJoinExecutor::
take()
{
    bool outerLeft = parent->joinQualification_ == JOIN_LEFT
        || parent->joinQualification_ == JOIN_FULL;
    bool outerRight = parent->joinQualification_ == JOIN_RIGHT
        || parent->joinQualification_ == JOIN_FULL;

    auto takeFromBuffer = [&] ( bufferType::iterator l ) -> bufferType::iterator
    {
        if (l != bufferedLeftValues.end()) {
            ++l;
            if (l == bufferedLeftValues.end()) {
                auto lresult = this->left->take();
                if (lresult) {
                    // buffer the next element and return a pointer to it
                    bufferedLeftValues.push_back(lresult);
                    l = --bufferedLeftValues.end();
                    return l;
                }
                else {
                    // this is the last element
                    return bufferedLeftValues.end();
                }
            }
            else
                return l;
        }
        else {
            ExcAssert(!this->left->take());
            return bufferedLeftValues.end();
        }
    };

    while (l != bufferedLeftValues.end() && r) {
        ExpressionValue  & lEmbedding = (*l)->values.back();
        ExpressionValue  & rEmbedding = r->values.back();

        ExpressionValue lField = lEmbedding.getColumn(0, GET_ALL);
        ExpressionValue rField = rEmbedding.getColumn(0, GET_ALL);
        //cerr << "right row " << r->values.front().toString();
        //in case of outer join
        //check the where condition that we took out and put in the embedding instead
        auto checkOuterWhere = [] ( std::shared_ptr<PipelineResults>& s,
                                    std::shared_ptr<ElementExecutor>& executor,
                                    ExpressionValue& field,
                                    ExpressionValue & embedding) -> bool
        {
            ExpressionValue where = embedding.getColumn(1, GET_ALL);
            //if the condition would have failed, or the select value is null, return the row.
            if (field.empty() || !where.asBool())
            {
                s->values.pop_back();
                s->values.emplace_back(ExpressionValue::null(Date::notADate()));
                s->values.emplace_back(ExpressionValue::null(Date::notADate()));
                return true;
            }

            return false;
        };    

        if (lField == rField) {
            auto setLastLeftValue = ScopeSuccess([&]() noexcept {lastLeftValue = lField;});
            // Got a row!
            //cerr << "*** got row match on " << jsonEncode(lField) << endl;
         
            // return a copy since we are buffering the original left value
            auto result = make_shared<PipelineResults>(**l);
            // Pop the selected join conditions from left
            result->values.pop_back();

            auto numL = result->values.size();
            auto numR = r->values.size() - 1;

            for (auto i = 0; i < numR; ++i)
                result->values.push_back(r->values[i]);

            ExpressionValue storage;
            auto crossWhereTrue = parent->crossWhere_(*result, storage, GET_LATEST).isTrue();
            auto lWhere = lEmbedding.getColumn(1, GET_ALL).isTrue();
            auto rWhere = rEmbedding.getColumn(1, GET_ALL).isTrue();
            //cerr << "lWhere " << lWhere << endl;
            //cerr << "rWhere " << rWhere << " ";
            if ((!crossWhereTrue || !lWhere || !rWhere) && outerLeft) {
                //ExpressionValue where = rEmbedding.getColumn(1, GET_ALL);
                //                cerr << "cross where false and outer left" << endl;
                //if (!where.asBool()) {
                    for (auto i = 0; i < numR; i++)
                        result->values.pop_back();
                    for (auto i = 0; i < numR; i++)
                        result->values.push_back(ExpressionValue());
                    //}
            }
            else if ((!crossWhereTrue || !lWhere || !rWhere) && outerRight) {
                //cerr << "cross where false and outer right" << endl;
                //ExpressionValue where = lEmbedding.getColumn(1, GET_ALL);
                //if (!where.asBool()) {
                     for (auto i = 0; i < numL; i++)
                         result->values[i] = ExpressionValue();
                     // }
            }
            else if ((!crossWhereTrue || !lWhere || !rWhere) && !outerRight && !outerLeft) {
                l = takeFromBuffer(l);
                cerr << " skip row " << endl;
                if (l == bufferedLeftValues.end()) {
                    cerr << "rewind since we are at the end of the left side" << endl;
                    r = right->take();
                    l = firstDuplicate;
                    alreadySeenLeftRow = true;
                }
                continue;
            }

            if (lastLeftValue != lField) {
                cerr << "lastLeftValue " << lastLeftValue << " lField " << lField << endl;
                firstDuplicate = l;
                alreadySeenLeftRow = false;
            }

            l = takeFromBuffer(l);

            bool setAlreadySeen = false;
            if (l == bufferedLeftValues.end() && firstDuplicate != --bufferedLeftValues.end()) {
                cerr << "rewind since we are at the end of the left side" << endl;
                r = right->take();
                l = firstDuplicate;
                setAlreadySeen = true;
            }

            cerr << "crossWhere " << crossWhereTrue << " left where " << lWhere << " right where " << rWhere << " alreadySeeen " << alreadySeenLeftRow << endl;
            if (outerLeft && (!crossWhereTrue || !lWhere || !rWhere) && alreadySeenLeftRow) {
                cerr << "seen rows " << endl;
                continue;
            }

            if (setAlreadySeen)
                alreadySeenLeftRow = true;
            auto leftShit = result->values.front().empty() ? "empty" : result->values.front().toString();

            cerr <<  this << "1 left " << leftShit  << " right " <<
                jsonEncode(result->values[2].empty() ? "empty" : result->values[2].toString()) ;
            
            return std::move(result);
        }
        else if (lField < rField) {
            // loop until left field value is equal to the right field value
            // returning nulls if left outer
            do {
                auto result = make_shared<PipelineResults>(**l);
                if (outerLeft && checkOuterWhere(result, left, lField, rEmbedding)) {
                    l = takeFromBuffer(l);
                    cerr << ">>> 2 " << jsonEncode(result->values.front().toString());
                    return std::move(result);
                } else {
                    if (l == bufferedLeftValues.end())
                        cerr << "HERER1" << endl;
                    l = takeFromBuffer(l);
                    if (l == bufferedLeftValues.end())
                        cerr << "HERER2" << endl;
                }     
            } while (l != bufferedLeftValues.end()  && (*l)->values.back().getColumn(0, GET_ALL) < rField);
            if (l == bufferedLeftValues.end())
                return nullptr;
        }
        else {  
            ExcAssert(lField > rField);
            cerr << "1 taking from right" << endl;
            r = right->take();
            if (r) {
                if (r->values.back().getColumn(0, GET_ALL) == lastLeftValue) {
                    cerr << "rewind" << endl;
                    l = firstDuplicate;
                    alreadySeenLeftRow = true;
                    continue;
                }
                else {
                    // loop until right field value is equal or greater than the left field value
                    // returning nulls if right outer
                    while (r && r->values.back().getColumn(0, GET_ALL) < lField) {
                        if (outerRight && checkOuterWhere(r, right, rField, lEmbedding)) {
                            auto result = std::move(r);
                            cerr << "2 taking from right" << endl;
                            r = right->take();
                            cerr << "3 " << jsonEncode(result) << endl;
                            return std::move(result);
                        } else {
                            cerr << " 3 taking from right" << endl;
                            r = right->take();
                        }
                    }
                }
            }
        }
    }

    //Return unmatched rows if we have a LEFT/RIGHT/OUTER join
    //Fill unmatched with empty values
    if (outerLeft && l != bufferedLeftValues.end() && !alreadySeenLeftRow)
    {
        auto result = shared_ptr<PipelineResults>(new PipelineResults(**l));
        result->values.pop_back();
        result->values.emplace_back(ExpressionValue::null(Date::notADate()));
        result->values.emplace_back(ExpressionValue::null(Date::notADate()));
        l = takeFromBuffer(l);
        cerr << "4 " << jsonEncode(result) << endl;
        return result;
    }

    if (outerRight && r)
    {
        r->values.pop_back();
        r->values.insert(r->values.begin(), ExpressionValue::null(Date::notADate()));
        r->values.insert(r->values.begin(), ExpressionValue::null(Date::notADate()));
        auto result = std::move(r);
        r = right->take();
        cerr << "5 " << jsonEncode(result) << endl;
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
    bufferedLeftValues.resize(0);
    auto lresult = this->left->take();
    bufferedLeftValues.push_back(lresult);
    l = bufferedLeftValues.begin();
    firstDuplicate = l;
    r = right->take();
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
start(const BoundParameters & getParam) const
{
    switch (condition_.style) {

    case AnnotatedJoinCondition::CROSS_JOIN:
        return std::make_shared<CrossJoinExecutor>
            (this,
             root_->start(getParam),
             left_->start(getParam),
             right_->start(getParam));

    case AnnotatedJoinCondition::EQUIJOIN:
        return std::make_shared<EquiJoinExecutor>
            (this,
             root_->start(getParam),
             left_->start(getParam),
             right_->start(getParam));

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
start(const BoundParameters & getParam) const
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
            BoundTableExpression boundFrom_,
            WhenExpression when_,
            SelectExpression select_,
            std::shared_ptr<SqlExpression> where_,
            OrderByExpression orderBy_,
            GetParamInfo params_)
    : root(std::move(root_)), 
      from(std::move(from_)),
      boundFrom(std::move(boundFrom_)),
      select(std::move(select_)), 
      when(std::move(when_)), 
      where(std::move(where_)),
      orderBy(std::move(orderBy_))
{
    ExcAssert(this->from);
    ExcAssert(this->root);

    UnboundEntities unbound = from->getUnbound();
    //cerr << "unbound for from = " << jsonEncode(unbound) << endl;   

    if (from->getType() == "join") {
        std::shared_ptr<JoinExpression> join
            = std::dynamic_pointer_cast<JoinExpression>(from);
        ExcAssert(join);

        impl.reset(new JoinElement(root,
                                   join->left, BoundTableExpression(),
                                   join->right, BoundTableExpression(),
                                   join->on, join->qualification,
                                   select, where, orderBy));
        // TODO: order by for join output
            
    }
    else if (from->getType() == "select") {
        std::shared_ptr<SelectSubtableExpression> subSelect
            = std::dynamic_pointer_cast<SelectSubtableExpression>(from);

        ExcAssert(subSelect);

        GetParamInfo getParamInfo = [&] (const Utf8String & paramName)
            -> std::shared_ptr<ExpressionValueInfo>
            {
                throw HttpReturnException(500, "No query parameter " + paramName);
            };

        if (params_)
            getParamInfo = params_;

        impl.reset(new SubSelectElement(root, subSelect->statement, orderBy, getParamInfo, from->getAs()));
    }
    else {
#if 0
        if (!unbound.params.empty())
            throw HttpReturnException(400, "Can't deal with from expression "
                                      "with unbound parameters",
                                      "exprType", from->getType(),
                                      "unbound", unbound);
#endif

        if (!from || from->getType() == "null")
        {
            //We have no from so we add a dummy TableOperations that will return a single row with no values

            TableOperations dummyTable;

            // Allow us to query row information from the dataset
            dummyTable.getRowInfo = [=] () { return make_shared<RowValueInfo>(std::vector<KnownColumn>()); };

            // Allow the dataset to override functions
            dummyTable.getFunction = [=] (SqlBindingScope & context,
                                            const Utf8String & tableName,
                                            const Utf8String & functionName,
                                            const std::vector<std::shared_ptr<ExpressionValueInfo> > & args)
                -> BoundFunction 
                {
                    return BoundFunction();
                };

            // Allow the dataset to run queries
            dummyTable.runQuery = [=] (const SqlBindingScope & context,
                                         const SelectExpression & select,
                                         const WhenExpression & when,
                                         const SqlExpression & where,
                                         const OrderByExpression & orderBy,
                                         ssize_t offset,
                                         ssize_t limit)
                -> BasicRowGenerator
                {

                    auto generateRows = [=] (ssize_t numToGenerate,
                                            SqlRowScope & rowScope,
                                            const BoundParameters & params)
                    ->std::vector<NamedRowValue>
                    {
                        std::vector<NamedRowValue> result;

                        if (offset == 0) {
                            NamedRowValue row;
                            row.rowName = RowName("result");
                            row.rowHash = row.rowName;
                            result.push_back(std::move(row));
                        }

                        return result;
                    };

                    return BasicRowGenerator(generateRows);
                };

            dummyTable.getChildAliases = [=] ()
                {
                    return std::vector<Utf8String>();
                };

            impl.reset(new GenerateRowsElement(root,
                                               select,
                                               dummyTable,
                                               "",
                                               when,
                                               where,
                                               orderBy));
        }
        else if (!!boundFrom) {
            // We have a pre-bound version of the dataset; use that
            impl.reset(new GenerateRowsElement(root,
                                               select,
                                               boundFrom.table,
                                               boundFrom.asName,
                                               when, 
                                               where,
                                               orderBy));
        }
        else { 
            // Need to bound here to get the dataset
            auto rootBound = root->bind();
            auto scope = rootBound->outputScope();

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
        const ExpressionValue & pass = parent_->where_(*input, storage, GET_LATEST);
                
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
start(const BoundParameters & getParam) const
{
    auto result = std::make_shared<Executor>();
    result->parent_ = this;
    result->source_ = source_->start(getParam);
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
        ExpressionValue selected = parent->select_(*input, GET_ALL);

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
}

std::shared_ptr<ElementExecutor>
SelectElement::Bound::
start(const BoundParameters & getParam) const
{
    auto result = std::make_shared<Executor>();
    result->parent = this;
    result->source = source_->start(getParam);
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
start(const BoundParameters & getParam) const
{
    return std::make_shared<Executor>(this,
                                      source_->start(getParam));
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
AggregateLexicalScope(std::shared_ptr<PipelineExpressionScope> inner, int numValues)
    : inner(inner), numValues_(numValues)
{
}

ColumnGetter
AggregateLexicalScope::
doGetColumn(const ColumnName & columnName, int fieldOffset)
{
    //cerr << "aggregate scope getting variable " << columnName
    //     << " at field offset " << fieldOffset << endl;

    // We can pass through the same scope, since we will point to the
    // same object.
    auto innerGetter = inner->doGetColumn(Utf8String(), columnName);

    return innerGetter;
}

GetAllColumnsOutput
AggregateLexicalScope::
doGetAllColumns(const Utf8String & tableName,
                const ColumnFilter& keep,
                int fieldOffset)
{
    return inner->doGetAllColumns("" /* table name */, keep);
}

BoundFunction
AggregateLexicalScope::
doGetFunction(const Utf8String & functionName,
              const std::vector<BoundSqlExpression> & args,
              int fieldOffset,
              SqlBindingScope & argScope)
{
    auto aggregate = inner->doGetAggregator(functionName, args);

    if (aggregate) {
        auto exec = [=] (const std::vector<ExpressionValue> & argValues,
                         const SqlRowScope & rowScope) -> ExpressionValue
            {
                auto & row = rowScope.as<PipelineResults>();

                std::shared_ptr<void> storage = aggregate.init();
                    
                std::vector<ExpressionValue> rowArgs(args.size());

                for (auto & r: row.group) {
                    // Apply the arguments to the row

                    for (unsigned i = 0;  i != args.size();  ++i)
                        rowArgs[i] = args[i](*r, GET_LATEST);

                    aggregate.process(&rowArgs[0], args.size(), storage.get());
                }

                return aggregate.extract(storage.get());
            };

        return { exec, aggregate.resultInfo };
    }
    else if (functionName == "rowPath" 
             || functionName == "rowName" 
                || functionName == "rowHash") {
        auto getRowName = [=] (const SqlRowScope & rowScope) {
            auto & row = rowScope.as<PipelineResults>();

            // cerr << "rowPath from: " << jsonEncode(row) << " offset: " << fieldOffset << endl;

            static VectorDescription<ExpressionValue>
                desc(getExpressionValueDescriptionNoTimestamp());

            std::string result;
            result.reserve(116);  /// try to force a 128 byte allocation
            StringJsonPrintingContext scontext(result);
            scontext.writeUtf8 = true;
            std::vector<ExpressionValue> key;

            for (int i = 0; i < numValues_; ++i) {
                key.push_back(row.values.at(fieldOffset - numValues_ + i));
            }

            desc.printJsonTyped(&key, scontext);

            return result;
        };

        if (functionName == "rowPath") {
            auto exec = [=] (const std::vector<ExpressionValue> & argValues,
                             const SqlRowScope & rowScope) -> ExpressionValue
            {
                auto result = getRowName(rowScope);

                return ExpressionValue(Path(result),
                                       Date::negativeInfinity());
            };

            return { exec, std::make_shared<PathValueInfo>() };
        }
        else if (functionName == "rowName"){
            auto exec = [=] (const std::vector<ExpressionValue> & argValues,
                             const SqlRowScope & rowScope) -> ExpressionValue
            {
                auto result = getRowName(rowScope);

                return ExpressionValue(PathElement(result).toUtf8String(),
                                       Date::negativeInfinity());
            };

            return { exec, std::make_shared<StringValueInfo>() };
        }
        else {

             return {[=] (const std::vector<ExpressionValue> & args,
                     const SqlRowScope & rowScope)
                {
                    auto result = getRowName(rowScope);
                    return ExpressionValue(Path(result).hash(),
                                           Date::notADate());
                },
                std::make_shared<Uint64ValueInfo>()
                };
        }
    }
    else {
        return inner->doGetFunction(Utf8String(), functionName, args, argScope);
    }
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
                                (source_->outputScope(), numValues))),
      numValues_(numValues)
{
}

std::shared_ptr<ElementExecutor>
PartitionElement::Bound::
start(const BoundParameters & getParam) const
{
    return std::make_shared<Executor>
        (this, source_->start(getParam),
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
start(const BoundParameters & getParam) const
{
    return std::make_shared<Executor>(source_->start(getParam),
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
} // namespace Datacratic

