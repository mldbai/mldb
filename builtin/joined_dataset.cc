// This file is part of MLDB. Copyright 2015 Datacratic. All rights reserved.

/** joined_dataset.cc                                              -*- C++ -*-
    Jeremy Barnes, 28 February 2015
    Copyright (c) 2015 Datacratic Inc.  All rights reserved.

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
#include "mldb/jml/utils/compact_vector.h"

using namespace std;


namespace Datacratic {
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
        RowName rowName;   ///< Name of joined row
        RowName leftName, rightName;  ///< Names of joined rows from input datasets
        //ML::compact_vector<RowHash, 2> rowHashes;   ///< Row hash from input datasets
    };

    enum JoinSide {
        JOIN_SIDE_LEFT = 0,
        JOIN_SIDE_RIGHT,
        JOIN_SIDE_MAX
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

        virtual RowName next() {
            return (iter++)->rowName;
        }

    private:
        std::vector<RowEntry>::const_iterator iter;
        JoinedDataset::Itl* source;
    };

    /// Rows in the joined dataset
    std::vector<RowEntry> rows;

    /// Map from row hash to the row
    ML::Lightweight_Hash<RowHash, int64_t> rowIndex;

    /// Index of a row hash for a left or right dataset to a list of
    /// rows it's part of in the output.
    typedef std::map<RowHash, ML::compact_vector<RowName, 1> > SideRowIndex;

    /// Left row hash to list of row hashes it's present in for colummn index
    SideRowIndex leftRowIndex;

    /// Right row hash to list of row hashes it's present in for colummn index
    SideRowIndex rightRowIndex;

    struct ColumnEntry {
        ColumnName columnName;       ///< Name of the column in this dataset
        ColumnName childColumnName;  ///< Name of the column in the child dataset
        uint32_t bitmap;             ///< Which rows contribute to this column?
    };

    /// Index of columns on both sides of the join
    std::unordered_map<ColumnHash, ColumnEntry> columnIndex;

    /// Mapping from the table column hash to output column name
    std::unordered_map<ColumnHash, ColumnName> leftColumns, rightColumns;

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
        ExpressionValue k = condition.constantWhere->constantValue();
        if (!k.isTrue())
            return;

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
                //cerr << "got rows complex " << res->values.size() << endl;
                Utf8String leftNameUtf8 = "";

                //results come in rowName/Columns pair
                //last two are the joined row (+2)
                //the two before that are the right row (+2)
                //and before that the left row which can be chained (chainedJoinDepth*2)
                //Deeper than the chained depth could be a subselect, for example.
                
                ssize_t i = res->values.size() - (chainedJoinDepth*2+4);
                ExcAssert(i >= 0);

                if (!res->values.at(i).empty())
                    leftNameUtf8 = res->values.at(i).toUtf8String();

                i += 2;

                for (; i + 2 < res->values.size(); i+=2) {
                    if (i == 2)
                        leftNameUtf8 = "[" + leftNameUtf8 + "]";
                    
                    leftNameUtf8 += res->values.at(0).empty() ? "-[]" :
                        "-[" + res->values.at(i).toUtf8String() + "]";
                }      
                  
                RowName leftName = RowName::parse(leftNameUtf8);
                
                Utf8String rightNameUtf8 = "";
                if (!res->values.at(i).empty())
                    rightNameUtf8 = res->values.at(i).toUtf8String();
                RowName rightName = RowName::parse(rightNameUtf8);

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
                ->bind()
                ->start(getParam)
                ->takeAll(gotElement);
        }

        // Finally, the column indexes
        for (auto & c: leftDataset->getColumnNames()) {
            ColumnName newColumnName;
            if (!left.asName.empty())
                newColumnName = ColumnName(left.asName) + c;
            else newColumnName = c;

            ColumnHash newColumnHash(newColumnName);

            ColumnEntry entry;
            entry.columnName = newColumnName;
            entry.childColumnName = c;
            entry.bitmap = 1;

            columnIndex[newColumnHash] = std::move(entry);
            leftColumns[c] = newColumnName;
        }

        // Finally, the column indexes
        for (auto & c: rightDataset->getColumnNames()) {
            ColumnName newColumnName;

            if (!right.asName.empty())
                newColumnName = ColumnName(right.asName) + c;
            else newColumnName = c;
            ColumnHash newColumnHash(newColumnName);

            ColumnEntry entry;
            entry.columnName = newColumnName;
            entry.childColumnName = c;
            entry.bitmap = 2;

            columnIndex[newColumnHash] = std::move(entry);
            rightColumns[c] = newColumnName;
        }

        if (debug) {
            cerr << "total of " << columnIndex.size() << " columns and "
                 << rows.size() << " rows returned from join" << endl;
                
            cerr << jsonEncode(getColumnNames());
        }
    }

     /* This is called to record a new entry from the join. */
    void recordJoinRow(const RowName & leftName, RowHash leftHash,
                       const RowName & rightName, RowHash rightHash)
    {
        bool debug = false;
        RowName rowName;

        if (chainedJoinDepth > 0 && !leftName.empty()) {
            rowName = std::move(RowName(leftName.toUtf8String() + "-" + "[" + rightName.toUtf8String() + "]"));
        }
        else if (chainedJoinDepth == 0) {
            rowName = std::move(RowName("[" + leftName.toUtf8String() + "]" + "-" + "[" + rightName.toUtf8String() + "]"));
        }
        else {
            Utf8String left;
            for (int i = 0; i <= chainedJoinDepth; ++i) {
                left += "[]-";
            }

            rowName = std::move(RowName(left + "[" + rightName.toUtf8String() + "]"));
        }

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
                            const std::function<void (const RowName&,
                                                      const RowHash& )>
                                & recordOuterRow)
            -> std::vector<std::tuple<ExpressionValue, RowName, RowHash> >
            {
                auto sideCondition = side.where;

                std::vector<std::shared_ptr<SqlExpression> > clauses
                    = { side.selectExpression };

                if (outer) {
                    //return all rows
                    sideCondition = SqlExpression::TRUE;

                    //but evaluate if the row is valid to join with the other side
                    auto notnullExpr = std::make_shared<IsTypeExpression>
                        (side.where, true, "null");
                    auto complementExpr
                        = std::make_shared<BooleanOperatorExpression>
                        (BooleanOperatorExpression(side.where, notnullExpr, "AND"));

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
                std::vector<std::tuple<ExpressionValue, RowName, RowHash> > sorted;
                std::vector<std::tuple<RowName, RowHash> > outerRows;

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

        auto recordOuterLeft = [&] (const RowName& rowName, const RowHash& rowHash)
        {
            recordJoinRow(rowName, rowHash, RowName(), RowHash());
        };

        auto recordOuterRight = [&] (const RowName& rowName, const RowHash& rowHash)
        {
            recordJoinRow( RowName(), RowHash(), rowName, rowHash);
        };

        std::vector<std::tuple<ExpressionValue, RowName, RowHash> >
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
                                  RowName(), RowHash()); //For LEFT and FULL joins
                ++it1;
            }
            else if (val2 < val1) {
                if (outerRight)
                    recordJoinRow(RowName(), RowHash(),std::get<1>(*it2),
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
                            const RowName & leftName = std::get<1>(*it1a);
                            const RowName & rightName = std::get<1>(*it2a);
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
                                      RowName(), RowHash());
                    }
                    
                    for (auto it2a = it2; it2a < erng2 && outerRight;  ++it2a) {
                        // For RIGHT and FULL joins
                        recordJoinRow(RowName(), RowHash(),std::get<1>(*it2a),
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
                          RowName(), RowHash()); 
            ++it1;
        }

        while (outerRight && it2 != end2) {
            // For RIGHT and FULL joins
            recordJoinRow(RowName(), RowHash(),std::get<1>(*it2),
                          std::get<2>(*it2));
            ++it2;
        }
    }

    virtual std::vector<RowName>
    getRowNames(ssize_t start = 0, ssize_t limit = -1) const
    {
        std::vector<RowName> result;

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

    virtual bool knownRow(const RowName & rowName) const
    {
        return rowIndex.count(rowName);
    }

    virtual bool knownRowHash(const RowHash & rowHash) const
    {
        return rowIndex.count(rowHash);
    }

    virtual MatrixNamedRow getRow(const RowName & rowName) const
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
                          const RowName & rowName,
                          const std::unordered_map<ColumnHash, ColumnName> & mapping)
            {
                ExpressionValue rowValue;
                if (!rowName.empty())
                    rowValue = dataset.getRowExpr(rowName);

                auto onAtom = [&] (RowName & rowName,
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

    virtual RowName getRowName(const RowHash & rowHash) const
    {
        auto it = rowIndex.find(rowHash);
        if (it == rowIndex.end())
            throw HttpReturnException(500, "Joined dataset did not find row with given hash",
                                      "rowHash", rowHash);

        const RowEntry & row = rows.at(it->second);

        return row.rowName;
    }

    virtual bool knownColumn(const ColumnName & column) const
    {
        return columnIndex.count(column);
    }

    virtual ColumnName getColumnName(ColumnHash columnHash) const
    {
        auto it = columnIndex.find(columnHash);

        if (it == columnIndex.end())
            throw HttpReturnException(500, "Joined dataset did not find column with given hash",
                                      "columnHash", columnHash);

        return it->second.columnName;
    }

    /** Return a list of all columns. */
    virtual std::vector<ColumnName> getColumnNames() const
    {
        std::vector<ColumnName> result;

        for (auto & c: columnIndex) {
            result.emplace_back(c.second.columnName);
        }

        std::sort(result.begin(), result.end());

        return result;
    }

    /** Return the value of the column for all rows and timestamps. */
    virtual MatrixColumn getColumn(const ColumnName & columnName) const
    {
        auto it = columnIndex.find(columnName);

        if (it == columnIndex.end())
            throw HttpReturnException(500, "Joined dataset did not find column ",
                                      "columnName", columnName);
        
        auto doGetColumn = [&] (const Dataset & dataset,
                                const SideRowIndex & index,
                                const ColumnName & columnName) -> MatrixColumn
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
                        for (const RowName & outputRowName: it->second) {
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

    RowName getSubRowName(const RowName & name, JoinSide side) const
    {   
        ExcAssert(side < JOIN_SIDE_MAX);
        RowHash rowHash(name);
        auto iter = rowIndex.find(rowHash);
        if (iter == rowIndex.end())
            return RowName();

        int64_t index = iter->second;
        const RowEntry& entry = rows[index];
        return JOIN_SIDE_LEFT == side ? entry.leftName : entry.rightName;
    };

    //Query the original row name down the tree of joined datasets on that side
    //The alternative would be to store a variable-size list of <alias,rowName> tuples for each row entry
    RowName
    getSubRowNameFromChildTable(const Utf8String& tableName,
                                const RowName & name, JoinSide side) const
    {
        ExcAssert(side < JOIN_SIDE_MAX);
        RowHash rowHash(name);
        auto iter = rowIndex.find(rowHash);
        if (iter == rowIndex.end())
            return RowName();

        int64_t index = iter->second;
        const RowEntry& entry = rows[index];

        RowName subRowName = JOIN_SIDE_LEFT == side ? entry.leftName : entry.rightName;

        return (JOIN_SIDE_LEFT == side ? *leftDataset : *rightDataset)
            .getOriginalRowName(tableName, subRowName);
    }

    //As getSubRowNameFromChildTable, but we dont know which side, or whether is a direct child or not.
    RowName
    getOriginalRowName(const Utf8String& tableName, const RowName & name) const
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

        return RowName();
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
};


/*****************************************************************************/
/* JOINED DATASET                                                            */
/*****************************************************************************/

JoinedDataset::
JoinedDataset(MldbServer * owner,
              PolyConfig config,
              const std::function<bool (const Json::Value &)> & onProgress)
    : Dataset(owner)
{
    auto joinConfig = config.params.convert<JoinedDatasetConfig>();

    SqlExpressionMldbScope scope(owner);

    // Create a scope to get our datasets from
    SqlExpressionMldbScope mldbScope(server);

    // Obtain our datasets
    BoundTableExpression left = joinConfig.left->bind(mldbScope);
    BoundTableExpression right = joinConfig.right->bind(mldbScope);
    
    
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
overrideFunction(const Utf8String & tableName,
                 const Utf8String & functionName,
                 SqlBindingScope & scope) const
{
    //cerr << "JoinedDataset function name: " << functionName << " from table: " << tableName << endl;
    if (functionName == "rowName") {

        if (tableName.empty())
            return BoundFunction();

        //we do as much "side-checking" as we can at binding time

        JoinedDataset::Itl::JoinSide tableSide = JoinedDataset::Itl::JOIN_SIDE_MAX;

        if (tableName == itl->getTableAlias(JoinedDataset::Itl::JOIN_SIDE_LEFT))
            tableSide = JoinedDataset::Itl::JOIN_SIDE_LEFT;
        else if (tableName == itl->getTableAlias(JoinedDataset::Itl::JOIN_SIDE_RIGHT))
            tableSide = JoinedDataset::Itl::JOIN_SIDE_RIGHT;

        if (tableSide != JoinedDataset::Itl::JOIN_SIDE_MAX)
        {
            return {[&, tableSide] (const std::vector<ExpressionValue> & args,
                     const SqlRowScope & scope)
                { 
                    auto & row = scope.as<SqlExpressionDatasetScope::RowScope>();
                    return ExpressionValue(itl->getSubRowName(row.row.rowName, tableSide).toUtf8String(), Date::negativeInfinity());
                },
                std::make_shared<Utf8StringValueInfo>()
            };
        }

        if (itl->isChildTable(tableName, JoinedDataset::Itl::JOIN_SIDE_LEFT))
            tableSide = JoinedDataset::Itl::JOIN_SIDE_LEFT;
        else if (itl->isChildTable(tableName, JoinedDataset::Itl::JOIN_SIDE_RIGHT))
            tableSide = JoinedDataset::Itl::JOIN_SIDE_RIGHT;

        if (tableSide != JoinedDataset::Itl::JOIN_SIDE_MAX)
        {
            return {[&, tableName, tableSide] (const std::vector<ExpressionValue> & args,
                     const SqlRowScope & scope)
                {
                    auto & row = scope.as<SqlExpressionDatasetScope::RowScope>();
                    return ExpressionValue(itl->getSubRowNameFromChildTable(tableName, row.row.rowName, tableSide).toUtf8String(), Date::negativeInfinity());
                },
                std::make_shared<Utf8StringValueInfo>()
            };
        }
    }
    else if (tableName.empty() && functionName.endsWith(".rowName"))
    {
        const Utf8String newFunctionName("rowName");
        Utf8String newTableName = functionName;
        newTableName.removeSuffix(".rowName");
        return overrideFunction(newTableName, newFunctionName, scope);
    }

    return BoundFunction();
}

RowName
JoinedDataset::
getOriginalRowName(const Utf8String& tableName, const RowName & name) const
{
    return itl->getOriginalRowName(tableName, name);
}

int
JoinedDataset::
getChainedJoinDepth() const
{
    return itl->chainedJoinDepth;
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
} // namespace Datacratic
