// This file is part of MLDB. Copyright 2015 mldb.ai inc. All rights reserved.

/** joined_dataset.cc                                              -*- C++ -*-
    Jeremy Barnes, 28 February 2015
    Copyright (c) 2015 mldb.ai inc.  All rights reserved.

*/

#include "joined_dataset.h"
#include "mldb/server/dataset_context.h"
#include "mldb/server/parallel_merge_sort.h"
#include "mldb/sql/sql_expression.h"
#include "mldb/sql/sql_expression_operations.h"
#include "mldb/sql/execution_pipeline_impl.h"
#include "mldb/jml/utils/lightweight_hash.h"
#include "mldb/jml/utils/profile.h"
#include "mldb/sql/join_utils.h"
#include "mldb/types/any_impl.h"
#include "mldb/types/structure_description.h"
#include "mldb/types/vector_description.h"
#include "mldb/http/http_exception.h"
#include "mldb/types/hash_wrapper_description.h"
#include "mldb/utils/compact_vector.h"
#include <functional>

using namespace std;
using namespace std::placeholders;


namespace MLDB {


/*****************************************************************************/
/* JOINED DATASET CONFIG                                                     */
/*****************************************************************************/

DEFINE_STRUCTURE_DESCRIPTION(JoinedDatasetConfig);

JoinedDatasetConfigDescription::
JoinedDatasetConfigDescription()
{
    nullAccepted = true;

    addField("left", &JoinedDatasetConfig::left,
             "Left dataset for the join");
    addField("right", &JoinedDatasetConfig::right,
             "Right dataset for the join");
    addField("on", &JoinedDatasetConfig::on,
             "Field to join on");
    addField("qualification", &JoinedDatasetConfig::qualification,
             "Type of join");
}

struct JoinedDataset::Itl
    : public MatrixView, public ColumnIndex {

    struct RowEntry {
        RowHash rowHash;   ///< Row hash of joined row
        RowPath rowName;   ///< Name of joined row
        RowPath leftName, rightName;  ///< Names of joined rows from input datasets
        //compact_vector<RowHash, 2> rowHashes;   ///< Row hash from input datasets
    };   

    struct JoinedRowStream : public RowStream {

        JoinedRowStream(JoinedDataset::Itl* source) : source(source)
        {
            
        }

        virtual std::shared_ptr<RowStream> clone() const{
            auto ptr = std::make_shared<JoinedRowStream>(source);
            return ptr;
        }

        virtual void initAt(size_t start){
            iter = source->rows.begin() + start;
        }

        virtual const RowPath & rowName(RowPath & storage) const
        {
            return iter->rowName;
        }

        virtual RowPath next() {
            return (iter++)->rowName;
        }
        
        virtual void advance() {
            ++iter;
        }

    private:
        std::vector<RowEntry>::const_iterator iter;
        JoinedDataset::Itl* source;
    };

    /// Rows in the joined dataset
    std::vector<RowEntry> rows;

    /// Map from row hash to the row
    Lightweight_Hash<RowHash, int64_t> rowIndex;

    /// Index of a row hash for a left or right dataset to a list of
    /// rows it's part of in the output.
    typedef std::map<RowHash, compact_vector<RowPath, 1> > SideRowIndex;

    /// Left row hash to list of row hashes it's present in for colummn index
    SideRowIndex leftRowIndex;

    /// Right row hash to list of row hashes it's present in for colummn index
    SideRowIndex rightRowIndex;

    struct ColumnEntry {
        ColumnPath columnName;       ///< Name of the column in this dataset
        ColumnPath childColumnName;  ///< Name of the column in the child dataset
        uint32_t bitmap;             ///< Which rows contribute to this column?
    };

    /// Index of columns on both sides of the join
    std::unordered_map<ColumnHash, ColumnEntry> columnIndex;

    /// Mapping from the table column hash to output column name
    std::unordered_map<ColumnHash, ColumnPath> leftColumns, rightColumns;

    /// Mapping from the table column hash to output column name for structured methods
    std::unordered_map<uint64_t, PathElement> leftColumnsExpr, rightColumnsExpr;

    /// Structure used to implement operations from each table
    TableOperations leftOps, rightOps;

    /// Datasets on the left and right side
    std::shared_ptr<Dataset> leftDataset, rightDataset;

    // is the dataset on the left a join too?
    int chainedJoinDepth;

    /// Names of tables, so that we can correctly identify where each
    /// column came from.
    std::vector<Utf8String> sideChildNames[JOIN_SIDE_MAX]; //Sub (non -direct) tables left and right
    Utf8String childAliases[JOIN_SIDE_MAX]; //Alias of the (direct) joined tables left and right
    std::vector<Utf8String> tableNames; //sub tables from both side + direct childs left and right

    Itl(SqlBindingScope & scope,
        std::shared_ptr<TableExpression> leftExpr,
        BoundTableExpression left,
        std::shared_ptr<TableExpression> rightExpr,
        BoundTableExpression right,
        std::shared_ptr<SqlExpression> on,
        JoinQualification qualification)
    {
        bool debug = false;

        // vector to set
        auto v2s = [] (std::vector<Utf8String> vec)
            {
                return std::set<Utf8String>(std::make_move_iterator(vec.begin()),
                                            std::make_move_iterator(vec.end()));
            };

        std::set<Utf8String> leftTables = v2s(left.table.getChildAliases());
        std::set<Utf8String> rightTables = v2s(right.table.getChildAliases());

        JoinedDataset* left_joined_dataset = dynamic_cast<JoinedDataset*>(left.dataset.get());

        chainedJoinDepth = left_joined_dataset != nullptr ? left_joined_dataset->getChainedJoinDepth() + 1 : 0;

        if (!left.dataset) {
            throw HttpReturnException
                (400, "A materialized join must be between materialized "
                 "datasets on both the left and the right side.  In practice, "
                 "this means it must be a subselect, a transpose, a join or a "
                 "dataset.  The dataset "
                 "expression on the left side of the join ('"
                 + leftExpr->surface
                 + "') is not materialized.  You can make it so by using the "
                 "transform procedure to record the output of the query into a "
                 "dataset, and then using this dataset in the join");
        }

        if (!right.dataset) {
            throw HttpReturnException
                (400, "A materialized join must be between materialized "
                 "datasets on both the left and the right side.  In practice, "
                 "this means it must be a subselect, a transpose, a join or a "
                 "dataset.  The dataset "
                 "expression on the right side of the join ('"
                 + rightExpr->surface
                 + "') is not materialized.  You can make it so by using the "
                 "transform procedure to record the output of the query into a "
                 "dataset, and then using this dataset in the join");
        }

        leftOps = left.table;
        rightOps = right.table;

        leftDataset = left.dataset;
        rightDataset = right.dataset;

        childAliases[JOIN_SIDE_LEFT] = left.asName;
        childAliases[JOIN_SIDE_RIGHT] = right.asName;

        tableNames = {left.asName, right.asName};

        auto addAliases = [&] (std::vector<Utf8String> aliases)
            {
                tableNames.insert(tableNames.end(),
                                  std::make_move_iterator(aliases.begin()),
                                  std::make_move_iterator(aliases.end()));
            };

        addAliases(leftOps.getChildAliases());
        addAliases(rightOps.getChildAliases());

        sideChildNames[JOIN_SIDE_LEFT]  = leftOps.getChildAliases();
        sideChildNames[JOIN_SIDE_RIGHT] = rightOps.getChildAliases();
      
        AnnotatedJoinCondition condition(leftExpr, rightExpr, on, 
                                         nullptr, //where
                                         qualification,
                                         debug);

        if (debug)
            cerr << "Analyzed join condition: " << jsonEncode(condition) << endl;

        // Run the constant expression
        if (!condition.constantWhere->constantValue().isTrue()
            && qualification == JoinQualification::JOIN_INNER)
        {
            return;
        }

        if (!condition.crossWhere || condition.crossWhere->isConstant()) {
            if (condition.crossWhere
                && !condition.crossWhere->constantValue().isTrue())
                return;
            
            // We can use a fast path, since we have simple non-filtered
            // equijoin
            makeJoinConstantWhere(condition, scope, left, right,
                                  qualification);            

        } else {

            // Complex join condition.  We need to generate the full set of
            // values.  To do this, we use the new executor.
            auto gotElement = [&] (std::shared_ptr<PipelineResults> & res) -> bool {

                ssize_t numValues = res->values.size();
                
                RowPath leftName = res->values.at(numValues-2).coerceToPath();
                RowPath rightName = res->values.at(numValues-1).coerceToPath();

                recordJoinRow(leftName, leftName, rightName, rightName);
                
                return true;
            };
            
            auto getParam = [&] (const Utf8String & paramName)
                -> ExpressionValue
                {
                    throw HttpReturnException(400, "No parameters bound in");
                };

            PipelineElement::root(scope)
                ->join(leftExpr, left, rightExpr, right, on, qualification)
                ->select(SqlExpression::parse("leftRowPath()"))
                ->select(SqlExpression::parse("rightRowPath()"))
                ->bind()
                ->start(getParam)
                ->takeAll(gotElement);
        }

        // Finally, the column indexes
        for (auto & c: leftDataset->getFlattenedColumnNames()) {

            ColumnPath newColumnName;
            if (!left.asName.empty())
                newColumnName = ColumnPath(left.asName) + c;
            else newColumnName = c;

            ColumnHash newColumnHash(newColumnName);

            ColumnEntry entry;
            entry.columnName = newColumnName;
            entry.childColumnName = c;
            entry.bitmap = 1;

            columnIndex[newColumnHash] = std::move(entry);
            leftColumns[c] = newColumnName;
        }

        for (auto & c: rightDataset->getFlattenedColumnNames()) {

            ColumnPath newColumnName;

            if (!right.asName.empty())
                newColumnName = ColumnPath(right.asName) + c;
            else newColumnName = c;
            ColumnHash newColumnHash(newColumnName);

            ColumnEntry entry;
            entry.columnName = newColumnName;
            entry.childColumnName = c;
            entry.bitmap = 2;

            columnIndex[newColumnHash] = std::move(entry);
            rightColumns[c] = newColumnName;
        }

        for (auto & c: leftDataset->getColumnPaths()) {
            leftColumnsExpr[c[0].hash()] = c[0];
        }

        for (auto & c: rightDataset->getColumnPaths()) {
            rightColumnsExpr[c[0].hash()] = c[0];
        }

        if (debug) {
            cerr << "total of " << columnIndex.size() << " columns and "
                 << rows.size() << " rows returned from join" << endl;
                
            cerr << jsonEncode(getColumnPaths());
        }
    }

     /* This is called to record a new entry from the join. */
    void recordJoinRow(const RowPath & leftName, RowHash leftHash,
                       const RowPath & rightName, RowHash rightHash)
    {
        bool debug = false;
        RowPath rowName;

        if (chainedJoinDepth > 0 && !leftName.empty()) {
            rowName = RowPath(leftName.toUtf8String() + "-" + "[" + rightName.toUtf8String() + "]");
        }
        else if (chainedJoinDepth == 0) {
            rowName = RowPath("[" + leftName.toUtf8String() + "]" + "-" + "[" + rightName.toUtf8String() + "]");
        }
        else {
            Utf8String left;
            for (int i = 0; i <= chainedJoinDepth; ++i) {
                left += "[]-";
            }

            rowName = RowPath(left + "[" + rightName.toUtf8String() + "]");
        }

#if 0
        cerr << "rowName = " << rowName << endl;
        cerr << "leftName = " << leftName << endl;
        cerr << "rightName = " << rightName << endl;
        if (!leftName.empty()) {
            if (!leftDataset->getMatrixView()->knownRow(leftName)) {
                cerr << "known names are " << jsonEncodeStr(leftDataset->getMatrixView()->getRowPaths()) << endl;
            }
            ExcAssert(leftDataset->getMatrixView()->knownRow(leftName));
        }
        if (!rightName.empty())
            ExcAssert(rightDataset->getMatrixView()->knownRow(rightName));
#endif

        RowHash rowHash(rowName);

        RowEntry entry;
        entry.rowName = rowName;
        entry.rowHash = rowHash;
        entry.leftName = leftName;
        entry.rightName = rightName;

        if (debug)
            cerr << "added entry number " << rows.size()
                 << " named " << "("<< rowName <<")"
                 << " from left (" << leftName <<")"
                 << " and right (" << rightName <<")"
                 << endl;

        rows.emplace_back(std::move(entry));
        rowIndex[rowHash] = rows.size() - 1;

        leftRowIndex[leftHash].push_back(rowName);
        rightRowIndex[rightHash].push_back(rowName);
    };

    //Easiest case with constant Where
    void makeJoinConstantWhere(AnnotatedJoinCondition& condition,
                               SqlBindingScope& scope,
                               BoundTableExpression& left,
                               BoundTableExpression& right,
                               JoinQualification qualification)
    {
        bool debug = false;
        bool outerLeft = qualification == JOIN_LEFT || qualification == JOIN_FULL;
        bool outerRight = qualification == JOIN_RIGHT || qualification == JOIN_FULL;

        // Where expressions for the left and right side
        auto runSide = [&] (const AnnotatedJoinCondition::Side & side,
                            const Dataset & dataset,
                            bool outer,
                            const std::function<void (const RowPath&,
                                                      const RowHash& )>
                                & recordOuterRow)
            -> std::vector<std::tuple<ExpressionValue, RowPath, RowHash> >
            {
                auto sideCondition = side.where;

                std::vector<std::shared_ptr<SqlExpression> > clauses
                    = { side.selectExpression };

                if (outer) {
                    //return all rows
                    sideCondition = SqlExpression::TRUE;

                    shared_ptr<SqlExpression> sideWhere;
                    if (condition.constantWhere->constantValue().isTrue()) {
                        sideWhere = side.where;
                    }
                    else {
                        // The constant part is false, hereby the original
                        // sideWhere is irrelevant and should be evaluated to
                        // false.
                        sideWhere = condition.constantWhere;
                    }

                    //but evaluate if the row is valid to join with the other side
                    auto notnullExpr = std::make_shared<IsTypeExpression>
                        (sideWhere, true, "null");
                    auto complementExpr
                        = std::make_shared<BooleanOperatorExpression>
                        (BooleanOperatorExpression(sideWhere, notnullExpr, "AND"));

                    clauses.push_back(complementExpr);
                }

                auto embedding = std::make_shared<EmbeddingLiteralExpression>
                    (clauses);
                auto rowExpression = std::make_shared<NamedColumnExpression>
                    (PathElement("var"), embedding);

                SelectExpression queryExpression;
                queryExpression.clauses.push_back(rowExpression);

                auto generator = dataset.queryBasic
                (scope, queryExpression, side.when, *sideCondition, side.orderBy,
                 0, -1);

                // Because we know that our outer scope is an
                // SqlExpressionMldbScope, we know that it takes an
                // empty rowScope with nothing that depends on the current
                // row.

                SqlRowScope rowScope;
                //Todo: destroying this can be really expensive.
                auto rows = generator(-1, rowScope);
            
                if (debug)
                    cerr << "got rows " << jsonEncode(rows) << endl;

                // Now we extract all values 
                std::vector<std::tuple<ExpressionValue, RowPath, RowHash> > sorted;
                std::vector<std::tuple<RowPath, RowHash> > outerRows;

                for (auto & r: rows) {
                    ExcAssertEqual(r.columns.size(), 1);

                    const ExpressionValue & embedding = std::get<1>(r.columns[0]);

                    if (outer) {
                        const ExpressionValue & embeddingCondition
                            = embedding.getColumn(1);
                        if (!embeddingCondition.asBool()) {
                            // if side.orderBy is not valid, the result will not
                            // be deterministic and we want a deterministic
                            // result, so output once sorted.
                            outerRows.emplace_back(r.rowName, r.rowHash);
                            continue;
                        }
                    }

                    const ExpressionValue & value = embedding.getColumn(0);
                    sorted.emplace_back(value, r.rowName, r.rowHash);
                }

                parallelQuickSortRecursive(sorted);
                parallelQuickSortRecursive(outerRows);

                for (auto & r: outerRows) {
                    recordOuterRow(std::get<0>(r), std::get<1>(r));
                }

                return sorted;
            };

        auto recordOuterLeft = [&] (const RowPath& rowPath, const RowHash& rowHash)
        {
            recordJoinRow(rowPath, rowHash, RowPath(), RowHash());
        };

        auto recordOuterRight = [&] (const RowPath& rowPath, const RowHash& rowHash)
        {
            recordJoinRow( RowPath(), RowHash(), rowPath, rowHash);
        };

        std::vector<std::tuple<ExpressionValue, RowPath, RowHash> >
            leftRows, rightRows;

        leftRows = runSide(condition.left, *left.dataset, outerLeft,
                           recordOuterLeft);

        rightRows = runSide(condition.right, *right.dataset, outerRight,
                            recordOuterRight);

        switch (condition.style) {
        case AnnotatedJoinCondition::CROSS_JOIN: {
            // Join with no restrictions on the joined column
            if (leftRows.size() * rightRows.size() > 100000000) {
                throw HttpReturnException
                    (400, "Cross join too big: cowardly refusing to materialize "
                     "row IDs for a dataset with > 100,000,000 rows",
                     "leftSize", leftRows.size(),
                     "rightSize", rightRows.size());//,
                //"joinOn", joinConfig.on);
            }
            break;
        }
        case AnnotatedJoinCondition::EQUIJOIN: {
            // Join on f(leftrow) = f(rightrow)
            break;
        }
        default:
            throw HttpReturnException(400, "Unknown or empty Join expression",
                                      //"joinOn", joinConfig.on,
                                      "condition", condition);
        }

        // Finally, perform the join
        // We keep a list of the row hashes of those that join up
        auto it1 = leftRows.begin(), end1 = leftRows.end();
        auto it2 = rightRows.begin(), end2 = rightRows.end();

        while (it1 != end1 && it2 != end2) {
            // TODO: there could be multiple values...

            const ExpressionValue & val1 = std::get<0>(*it1);
            const ExpressionValue & val2 = std::get<0>(*it2);
            
            if (debug)
                cerr << "joining " << jsonEncodeStr(val1)
                     << " and " << jsonEncodeStr(val2) << endl;

            if (val1 < val2) {
                if (outerLeft)
                    recordJoinRow(std::get<1>(*it1), std::get<2>(*it1),
                                  RowPath(), RowHash()); //For LEFT and FULL joins
                ++it1;
            }
            else if (val2 < val1) {
                if (outerRight)
                    recordJoinRow(RowPath(), RowHash(),std::get<1>(*it2),
                                  std::get<2>(*it2)); //For RIGHT and FULL joins
                ++it2;
            }
            else {

                ExcAssertEqual(val1, val2);

                // We got a match on the join condition.  So now
                // we take the cross product of the matching rows.

                // First figure out how many of each are there?
                auto erng1 = it1;  ++erng1;
                while (erng1 < end1 && std::get<0>(*erng1) == val1)
                    ++erng1;

                auto erng2 = it2;  ++erng2;
                while (erng2 < end2 && std::get<0>(*erng2) == val2)
                    ++erng2;

                // Now the cross product
                if (!val1.empty()) {
                    for (auto it1a = it1; it1a < erng1;  ++it1a) {
                        for (auto it2a = it2; it2a < erng2;  ++it2a) {
                            const RowPath & leftName = std::get<1>(*it1a);
                            const RowPath & rightName = std::get<1>(*it2a);
                            const RowHash & leftHash = std::get<2>(*it1a);
                            const RowHash & rightHash = std::get<2>(*it2a);

                            if (debug)
                                cerr << "rows " << leftName << " and "
                                     << rightName << " join on value "
                                     << val1 << endl;
                            
                            recordJoinRow(leftName, leftHash, rightName, rightHash);
                        }
                    }
                }
                else if (qualification != JOIN_INNER) {
                    for (auto it1a = it1; it1a < erng1 && outerLeft;  ++it1a) {
                        // For LEFT and FULL joins
                        recordJoinRow(std::get<1>(*it1a), std::get<2>(*it1a),
                                      RowPath(), RowHash());
                    }
                    
                    for (auto it2a = it2; it2a < erng2 && outerRight;  ++it2a) {
                        // For RIGHT and FULL joins
                        recordJoinRow(RowPath(), RowHash(),std::get<1>(*it2a),
                                      std::get<2>(*it2a));
                    }
                }

                it1 = erng1;
                it2 = erng2;
            }
        }

        while (outerLeft && it1 != end1) {
            // For LEFT and FULL joins
            recordJoinRow(std::get<1>(*it1), std::get<2>(*it1),
                          RowPath(), RowHash()); 
            ++it1;
        }

        while (outerRight && it2 != end2) {
            // For RIGHT and FULL joins
            recordJoinRow(RowPath(), RowHash(),std::get<1>(*it2),
                          std::get<2>(*it2));
            ++it2;
        }
    }

    virtual std::vector<RowPath>
    getRowPaths(ssize_t start = 0, ssize_t limit = -1) const
    {
        std::vector<RowPath> result;

        for (auto & r: rows) {
            result.push_back(r.rowName);
        }

        return result;
    }

    virtual std::vector<RowHash>
    getRowHashes(ssize_t start = 0, ssize_t limit = -1) const
    {
        std::vector<RowHash> result;

        for (auto & r: rows) {
            result.push_back(r.rowHash);
        }

        //cerr << "getRowHashes returned " << result.size() << " rows" << endl;
        
        return result;
    }

    virtual bool knownRow(const RowPath & rowName) const
    {
        return rowIndex.count(rowName);
    }

    virtual bool knownRowHash(const RowHash & rowHash) const
    {
        return rowIndex.count(rowHash);
    }

    virtual MatrixNamedRow getRow(const RowPath & rowName) const
    {
        auto it = rowIndex.find(rowName);
        if (it == rowIndex.end())
            return MatrixNamedRow();
        
        const RowEntry & row = rows.at(it->second);

        if (rowName != row.rowName)
            return MatrixNamedRow();

        MatrixNamedRow result;
        result.rowName = rowName;
        result.rowHash = rowName;

        auto doRow = [&] (const Dataset & dataset,
                          const RowPath & rowName,
                          const std::unordered_map<ColumnHash, ColumnPath> & mapping)
            {
                ExpressionValue rowValue;
                if (!rowName.empty())
                    rowValue = dataset.getRowExpr(rowName);

                auto onAtom = [&] (RowPath & rowName,
                                   CellValue & val,
                                   Date ts)
                {
                    ColumnHash colHash = rowName;
                    auto it = mapping.find(colHash);

                    if (it != mapping.end()) {

                        result.columns.emplace_back(it->second,
                                                    std::move(val),
                                                    ts);
                    }
                    return true;
                };
                
                rowValue.forEachAtomDestructive(onAtom);
            };                          

        doRow(*leftDataset, row.leftName, leftColumns);
        doRow(*rightDataset, row.rightName, rightColumns);

        return result;

    }

    ExpressionValue
    getRowExpr(const RowPath & rowName) const
    {
        StructValue result;

        auto it = rowIndex.find(rowName);
        if (it == rowIndex.end())
            return result;

        const RowEntry & row = rows.at(it->second);
        if (rowName != row.rowName)
            return result;

        auto doRow = [&] (const Dataset & dataset,
                      const RowPath & rowName,
                      const std::unordered_map<uint64_t, PathElement> & mapping,
                      const Utf8String& childName)
        {
            StructValue childresult;
            ExpressionValue rowValue;
            if (!rowName.empty())
                rowValue = dataset.getRowExpr(rowName);

            if (rowValue.empty())
                return;


            auto onColumn = [&] (const PathElement & columnName,
                                 const ExpressionValue & val)
            {
                uint64_t colHash = columnName.hash();
                auto it = mapping.find(colHash);

                if (it != mapping.end()) {

                    if (childName.empty())
                        result.emplace_back(columnName, val);
                    else
                        childresult.emplace_back(columnName, val);
                }
                return true;
            };

            rowValue.forEachColumn(onColumn);

            if (!childName.empty())
                result.emplace_back(PathElement(childName), ExpressionValue(childresult));
        };

        doRow(*leftDataset, row.leftName, leftColumnsExpr, childAliases[JOIN_SIDE_LEFT]);
        doRow(*rightDataset, row.rightName, rightColumnsExpr, childAliases[JOIN_SIDE_RIGHT]);

        return result;

    }

    virtual RowPath getRowPath(const RowHash & rowHash) const
    {
        auto it = rowIndex.find(rowHash);
        if (it == rowIndex.end())
            throw HttpReturnException(500, "Joined dataset did not find row with given hash",
                                      "rowHash", rowHash);

        const RowEntry & row = rows.at(it->second);

        return row.rowName;
    }

    virtual bool knownColumn(const ColumnPath & column) const
    {
        return columnIndex.count(column);
    }

    virtual ColumnPath getColumnPath(ColumnHash columnHash) const
    {
        auto it = columnIndex.find(columnHash);

        if (it == columnIndex.end())
            throw HttpReturnException(500, "Joined dataset did not find column with given hash",
                                      "columnHash", columnHash);

        return it->second.columnName;
    }

    /** Return a list of all columns. */
    virtual std::vector<ColumnPath> getColumnPaths() const
    {
        std::vector<ColumnPath> result;

        for (auto & c: columnIndex) {
            result.emplace_back(c.second.columnName);
        }

        std::sort(result.begin(), result.end());

        return result;
    }

    /** Return the value of the column for all rows and timestamps. */
    virtual MatrixColumn getColumn(const ColumnPath & columnName) const
    {
        auto it = columnIndex.find(columnName);

        if (it == columnIndex.end())
            throw HttpReturnException(500, "Joined dataset did not find column ",
                                      "columnName", columnName);
        
        auto doGetColumn = [&] (const Dataset & dataset,
                                const SideRowIndex & index,
                                const ColumnPath & columnName) -> MatrixColumn
            {
                MatrixColumn result;

                // First, get the column
                MatrixColumn column
                    = dataset.getColumnIndex()->getColumn(columnName);

                // Now for each row, find which index it's in
                for (auto & r: column.rows) {
                    RowHash rowHash = std::get<0>(r);

                    // Does this row appear in the output?  If not, nothing to
                    // do with it
                    auto it = index.find(rowHash);
                    if (it == index.end())
                        continue;

                    CellValue & value = std::get<1>(r);
                    Date ts = std::get<2>(r);

                    // Otherwise, copy it the number of times needed
                    if (it->second.size() == 1) {
                        result.rows.emplace_back(it->second[0], std::move(value), ts);
                    }
                    else {
                        // Can't move the value to avoid it becoming null
                        for (const RowPath & outputRowName: it->second) {
                            result.rows.emplace_back(outputRowName, value, ts);
                        }
                    }
                }

                return result;
            };
        MatrixColumn result;

        if (it->second.bitmap == 1) {
            // on the left
            result = doGetColumn(*leftDataset, leftRowIndex, it->second.childColumnName);
        }
        else {
            result = doGetColumn(*rightDataset, rightRowIndex, it->second.childColumnName);
        }

        result.columnHash = result.columnName = it->second.columnName;

        return result;
    }

    virtual size_t getRowCount() const
    {
        return rowIndex.size();
    }

    virtual size_t getColumnCount() const
    {
        return columnIndex.size();
    }

    RowPath getSubRowName(const RowPath & name, JoinSide side) const
    {   
        ExcAssert(side < JOIN_SIDE_MAX);
        RowHash rowHash(name);
        auto iter = rowIndex.find(rowHash);
        if (iter == rowIndex.end())
            return RowPath();

        int64_t index = iter->second;
        const RowEntry& entry = rows[index];

        return JOIN_SIDE_LEFT == side ? entry.leftName : entry.rightName;
    };

    RowHash getSubRowHash(const RowPath & name, JoinSide side) const
    {
        RowPath subName = getSubRowName(name, side);
        return RowHash(subName);      
    };

    //Query the original row name down the tree of joined datasets on that side
    //The alternative would be to store a variable-size list of <alias,rowName> tuples for each row entry
    RowPath
    getSubRowNameFromChildTable(const Utf8String& tableName,
                                const RowPath & name, JoinSide side) const
    {
        ExcAssert(side < JOIN_SIDE_MAX);
        RowHash rowHash(name);
        auto iter = rowIndex.find(rowHash);
        if (iter == rowIndex.end())
            return RowPath();
   

        int64_t index = iter->second;
        const RowEntry& entry = rows[index];

        RowPath subRowPath = JOIN_SIDE_LEFT == side ? entry.leftName : entry.rightName;

        return (JOIN_SIDE_LEFT == side ? *leftDataset : *rightDataset)
            .getOriginalRowName(tableName, subRowPath);
    }

    //Query the original row name down the tree of joined datasets on that side
    //The alternative would be to store a variable-size list of <alias,rowName> tuples for each row entry
    RowHash
    getSubRowHashFromChildTable(const Utf8String& tableName,
                                const RowPath & name, JoinSide side) const
    {
        ExcAssert(side < JOIN_SIDE_MAX);
        RowPath childName = getSubRowNameFromChildTable(tableName, name, side);
        return RowHash(name);
    }

    //Alias of the direct child on that side
    Utf8String getTableAlias(JoinSide side) const
    {
        return childAliases[side];
    }

    //Is this table alias found down the tree of joined datasets on that side
    bool isChildTable(const Utf8String& tableName, JoinSide side) const
    {
        return std::find(sideChildNames[side].begin(), sideChildNames[side].end(), tableName) != sideChildNames[side].end();
    }

    //As getSubRowNameFromChildTable, but we dont know which side, or whether is a direct child or not.
    RowPath
    getOriginalRowName(const Utf8String& tableName, const RowPath & name) const
    {
        JoinSide tableSide = JOIN_SIDE_MAX;

        if (tableName == getTableAlias(JOIN_SIDE_LEFT))
            tableSide = JOIN_SIDE_LEFT;
        else if (tableName == getTableAlias(JOIN_SIDE_RIGHT))
            tableSide = JOIN_SIDE_RIGHT;

        if (tableSide != JOIN_SIDE_MAX)
        {
            //its one of our childs
            return getSubRowName(name, tableSide);
        }

        if (isChildTable(tableName, JOIN_SIDE_LEFT))
            tableSide = JOIN_SIDE_LEFT;
        else if (isChildTable(tableName, JOIN_SIDE_RIGHT))
            tableSide = JOIN_SIDE_RIGHT;

        if (tableSide != JOIN_SIDE_MAX)
        {
            //its a child of our child somewhere down this side
            return getSubRowNameFromChildTable(tableName, name, tableSide);
        }

        return RowPath();
    }
};


/*****************************************************************************/
/* JOINED DATASET                                                            */
/*****************************************************************************/

JoinedDataset::
JoinedDataset(MldbServer * owner,
              PolyConfig config,
              const ProgressFunc & onProgress)
    : Dataset(owner)
{
    auto joinConfig = config.params.convert<JoinedDatasetConfig>();
    config_ = make_shared<PolyConfig>();
    config_->type = "joined";
    config_->params = joinConfig;

    SqlExpressionMldbScope scope(owner);

    /* The assumption is that both sides have the same number
       of items to process.  This is obviously not always the case
       so the progress may differ in speed when switching from the 
       left dataset to right dataset.
    */ 
    ProgressState joinState(100);
    auto joinedProgress = [&](uint side, const ProgressState & state) {
        joinState = state.count / *state.total * 100 + 50 * side;
        return onProgress(joinState);
    };

    // Create a scope to get our datasets from
    SqlExpressionMldbScope mldbScope(server);

    // Obtain our datasets
    BoundTableExpression left = joinConfig.left->bind(mldbScope, bind(joinedProgress, 0, _1));
    BoundTableExpression right = joinConfig.right->bind(mldbScope, bind(joinedProgress, 1, _1));
    
    
    itl.reset(new Itl(scope,
                      joinConfig.left, std::move(left),
                      joinConfig.right, std::move(right),
                      joinConfig.on, joinConfig.qualification));
}

JoinedDataset::
JoinedDataset(SqlBindingScope & scope,
              std::shared_ptr<TableExpression> leftExpr,
              BoundTableExpression left,
              std::shared_ptr<TableExpression> rightExpr,
              BoundTableExpression right,
              std::shared_ptr<SqlExpression> on,
              JoinQualification qualification)
    : Dataset(scope.getMldbServer())
{
    JoinedDatasetConfig config;
    config.left = leftExpr;
    config.right = rightExpr;
    config.on = on;
    config.qualification = qualification;

    config_ = make_shared<PolyConfig>();
    config_->type = "joined";
    config_->params = config;

    itl.reset(new Itl(scope,
                      leftExpr, std::move(left),
                      rightExpr, std::move(right),
                      on, qualification));
}

JoinedDataset::
~JoinedDataset()
{
}

Any
JoinedDataset::
getStatus() const
{
    return Any();
}

std::shared_ptr<MatrixView>
JoinedDataset::
getMatrixView() const
{
    return itl;
}

std::shared_ptr<ColumnIndex>
JoinedDataset::
getColumnIndex() const
{
    return itl;
}

std::shared_ptr<RowStream> 
JoinedDataset::
getRowStream() const
{
    return make_shared<JoinedDataset::Itl::JoinedRowStream>(itl.get());
}

void 
JoinedDataset::
getChildAliases(std::vector<Utf8String> & outAliases) const
{
    outAliases.insert(outAliases.begin(), itl->tableNames.begin(), itl->tableNames.end());
}

BoundFunction
JoinedDataset::
overrideFunctionFromSide(JoinSide tableSide,
                         const Utf8String & tableName,
                         const Utf8String & functionName,
                         SqlBindingScope & scope) const {

    ExcAssert(tableSide != JOIN_SIDE_MAX);
    if (functionName == "rowName") {
        return {[&, tableSide] (const std::vector<ExpressionValue> & args,
                     const SqlRowScope & scope)
                {
                    auto & row = scope.as<SqlExpressionDatasetScope::RowScope>();
                    return ExpressionValue
                        (itl->getSubRowName(row.getRowPath(), tableSide)
                             .toUtf8String(),
                         Date::negativeInfinity());
                },
                std::make_shared<Utf8StringValueInfo>()
            };
    }
    else if (functionName == "rowHash"){
        return {[&, tableSide] (const std::vector<ExpressionValue> & args,
                     const SqlRowScope & scope)
                {
                    auto & row = scope.as<SqlExpressionDatasetScope::RowScope>();
                    return ExpressionValue(itl->getSubRowHash(row.getRowPath(), tableSide), Date::negativeInfinity());
                },
                std::make_shared<Uint64ValueInfo>()
            };
    }
    else {
        ExcAssert(functionName == "rowPath");
        return {[&, tableSide] (const std::vector<ExpressionValue> & args,
                     const SqlRowScope & context)
                {
                    auto & row = context.as<SqlExpressionDatasetScope::RowScope>();
                    return ExpressionValue(CellValue(itl->getSubRowName(row.getRowPath(), tableSide)),
                                           Date::negativeInfinity());
                },
                std::make_shared<PathValueInfo>()
                };
    }
}

BoundFunction
JoinedDataset::
overrideFunctionFromChild(JoinSide tableSide,
                         const Utf8String & tableName,
                         const Utf8String & functionName,
                         SqlBindingScope & scope) const {

    ExcAssert(tableSide != JOIN_SIDE_MAX);
    if (functionName == "rowName") {
        return {[&, tableSide] (const std::vector<ExpressionValue> & args,
                     const SqlRowScope & scope)
                {
                    auto & row = scope.as<SqlExpressionDatasetScope::RowScope>();
                    return ExpressionValue
                        (itl->getSubRowNameFromChildTable
                         (tableName, row.getRowPath(), tableSide).toUtf8String(),
                         Date::negativeInfinity());
                },
                std::make_shared<Utf8StringValueInfo>()
            };
    }
    else if (functionName == "rowHash"){
        return {[&, tableSide] (const std::vector<ExpressionValue> & args,
                     const SqlRowScope & scope)
                {
                    auto & row = scope.as<SqlExpressionDatasetScope::RowScope>();
                    return ExpressionValue(itl->getSubRowHashFromChildTable(tableName, row.getRowPath(), tableSide), Date::negativeInfinity());
                },
                std::make_shared<Uint64ValueInfo>()
            };
    }
    else{
        ExcAssert(functionName == "rowPath");
        return {[&, tableSide] (const std::vector<ExpressionValue> & args,
                     const SqlRowScope & context)
                {
                    auto & row = context.as<SqlExpressionDatasetScope::RowScope>();
                    return ExpressionValue(CellValue(itl->getSubRowNameFromChildTable(tableName, row.getRowPath(), tableSide)),
                                           Date::negativeInfinity());
                },
                std::make_shared<PathValueInfo>()
                };
    }
}

BoundFunction
JoinedDataset::
overrideFunction(const Utf8String & tableName,
                 const Utf8String & functionName,
                 SqlBindingScope & scope) const
{
    //cerr << "JoinedDataset function name: " << functionName << " from table: " << tableName << endl;
    if (functionName == "rowName" || functionName == "rowHash" || functionName == "rowPath") {

        if (tableName.empty())
            return BoundFunction();

        //we do as much "side-checking" as we can at binding time

        JoinSide tableSide = JOIN_SIDE_MAX;

        if (tableName == itl->getTableAlias(JOIN_SIDE_LEFT))
            tableSide = JOIN_SIDE_LEFT;
        else if (tableName == itl->getTableAlias(JOIN_SIDE_RIGHT))
            tableSide = JOIN_SIDE_RIGHT;

        if (tableSide != JOIN_SIDE_MAX)
            return overrideFunctionFromSide(tableSide, tableName, functionName, scope);

        if (itl->isChildTable(tableName, JOIN_SIDE_LEFT))
            tableSide = JOIN_SIDE_LEFT;
        else if (itl->isChildTable(tableName, JOIN_SIDE_RIGHT))
            tableSide = JOIN_SIDE_RIGHT;

        if (tableSide != JOIN_SIDE_MAX)
            return overrideFunctionFromChild(tableSide, tableName, functionName, scope);

    }
    else if (tableName.empty() && functionName.endsWith(".rowName"))
    {
        const Utf8String newFunctionName("rowName");
        Utf8String newTableName = functionName;
        newTableName.removeSuffix(".rowName");
        return overrideFunction(newTableName, newFunctionName, scope);
    }
    if (functionName == "leftRowName") {
        return {[&] (const std::vector<ExpressionValue> & args,
                     const SqlRowScope & scope)
                {
                    auto & row = scope.as<SqlExpressionDatasetScope::RowScope>();
                    RowHash rowHash(row.getRowHash());
                    return ExpressionValue(itl->rows[itl->rowIndex[rowHash]].leftName, Date::negativeInfinity());
                },
                std::make_shared<Utf8StringValueInfo>()
            };
    }
    else if (functionName == "rightRowName") {
      return {[&] (const std::vector<ExpressionValue> & args,
                     const SqlRowScope & scope)
                {
                    auto & row = scope.as<SqlExpressionDatasetScope::RowScope>();
                    RowHash rowHash(row.getRowHash());
                    return ExpressionValue(itl->rows[itl->rowIndex[rowHash]].rightName, Date::negativeInfinity());
                },
                std::make_shared<Utf8StringValueInfo>()
            };
    }

    return BoundFunction();
}

RowPath
JoinedDataset::
getOriginalRowName(const Utf8String& tableName, const RowPath & name) const
{
    return itl->getOriginalRowName(tableName, name);
}

int
JoinedDataset::
getChainedJoinDepth() const
{
    return itl->chainedJoinDepth;
}

ExpressionValue
JoinedDataset::
getRowExpr(const RowPath & row) const
{
    return itl->getRowExpr(row);
}


static RegisterDatasetType<JoinedDataset, JoinedDatasetConfig> 
regJoined(builtinPackage(),
          "joined",
          "Joins together several datasets into one virtual dataset",
          "datasets/JoinedDataset.md.html",
          nullptr,
          {MldbEntity::INTERNAL_ENTITY});

extern std::shared_ptr<Dataset>
(*createJoinedDatasetFn) (SqlBindingScope &,
                          std::shared_ptr<TableExpression>,
                          BoundTableExpression,
                          std::shared_ptr<TableExpression>,
                          BoundTableExpression,
                          std::shared_ptr<SqlExpression>,
                          JoinQualification);

std::shared_ptr<Dataset>
createJoinedDataset(SqlBindingScope & scope,
                    std::shared_ptr<TableExpression> left,
                    BoundTableExpression boundLeft,
                    std::shared_ptr<TableExpression> right,
                    BoundTableExpression boundRight,
                    std::shared_ptr<SqlExpression> on,
                    JoinQualification q)
{
    return std::make_shared<JoinedDataset>
        (scope, left, boundLeft, right, boundRight, on, q);
}

namespace {
struct AtInit {
    AtInit()
    {
        createJoinedDatasetFn = createJoinedDataset;
    }
} atInit;

} // file scope

} // namespace MLDB

