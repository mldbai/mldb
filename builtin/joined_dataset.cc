// This file is part of MLDB. Copyright 2015 Datacratic. All rights reserved.

/** joined_dataset.cc                                              -*- C++ -*-
    Jeremy Barnes, 28 February 2015
    Copyright (c) 2015 Datacratic Inc.  All rights reserved.

*/

#include "joined_dataset.h"
#include "mldb/server/dataset_context.h"
#include "mldb/sql/sql_expression.h"
#include "mldb/sql/sql_expression_operations.h"
#include "mldb/sql/execution_pipeline_impl.h"
#include "mldb/jml/utils/lightweight_hash.h"
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

    /// Datasets that were actually joined.  There will be a maximum of 31
    /// of them, as any more will be sub-joined
    std::vector<std::shared_ptr<Dataset> > datasets;

    /// Matrix view.  Length is the same as that of datasets.
    std::vector<std::shared_ptr<MatrixView> > matrices;

    /// Names of tables, so that we can correctly identify where each
    /// column came from.
    std::vector<Utf8String> tableNames;

    Itl(MldbServer * server, JoinedDatasetConfig joinConfig)
    {
        SqlExpressionMldbContext context(server);

        // Create a context to get our datasets from
        SqlExpressionMldbContext mldbContext(server);

        // Obtain our datasets
        BoundTableExpression left = joinConfig.left->bind(mldbContext);
        BoundTableExpression right = joinConfig.right->bind(mldbContext);

        bool debug = true;

        std::set<Utf8String> leftTables = joinConfig.left->getTableNames();
        std::set<Utf8String> rightTables = joinConfig.right->getTableNames();
        
        datasets.emplace_back(left.dataset);
        datasets.emplace_back(right.dataset);

        matrices.emplace_back(left.dataset->getMatrixView());
        matrices.emplace_back(right.dataset->getMatrixView());

        tableNames = {left.asName, right.asName};

        //if table aliases contains a dot '.', surround it with quotes to prevent ambiguity
        Utf8String quotedLeftName = left.asName;
        if (quotedLeftName.find('.') != quotedLeftName.end())
        {
            quotedLeftName = "\"" + left.asName + "\"";
        }
        Utf8String quotedRightName = right.asName;
        if (quotedRightName.find('.') != quotedRightName.end())
        {
            quotedRightName = "\"" + right.asName + "\"";
        }

        // ...
        AnnotatedJoinCondition condition(joinConfig.left, joinConfig.right,
                                         joinConfig.on, 
                                         nullptr, //where
                                         debug);

        if (debug)
            cerr << "Analyzed join condition: " << jsonEncode(condition) << endl;

        // Run the constant expression
        auto boundConstant = condition.constantWhere->bind(context);
        SqlRowScope scope;
        ExpressionValue storage;
        if (!boundConstant(scope, storage).isTrue())
            return;

        if (!condition.crossWhere || condition.crossWhere->isConstant()) {

            cerr << "FAST PATH" << endl;
            cerr << "qualify: " << joinConfig.qualification << endl;

            if (condition.crossWhere
                && !condition.crossWhere->constantValue().isTrue())
                return;
            
            // We can use a fast path, since we have simple non-filtered
            // equijoin

            makeJoinFast(condition, context, left, right, joinConfig.qualification);            

        } else {
            // Complex join condition.  We need to generate the full set of
            // values.  To do this, we use the new executor.

            cerr << "COMPLEX PATH" << endl;

            auto gotElement = [&] (std::shared_ptr<PipelineResults> & res) -> bool
                {
                    // This has xxx results
                    //gotRow(...);

                    //cerr << "got rows complex " << res->values.size() << endl;
                    Utf8String leftNameUtf8 = res->values.at(0).toUtf8String();
                    size_t i = 2;
                    for (; i+ 2 < res->values.size(); i+=2)
                    {
                        cerr << "adding join name " << endl;
                        leftNameUtf8 += "-" + res->values.at(i).toUtf8String();
                    }                        

                    RowName leftName(leftNameUtf8);
                    RowName rightName(res->values.at(i).toUtf8String());

                    recordJoinRow(leftName, leftName, rightName, rightName);

                    return true;
                };
            
            auto getParam = [&] (const Utf8String & paramName)
                -> ExpressionValue
                {
                    throw HttpReturnException(400, "No parameters bound in");
                };

            PipelineElement::root(context)
                ->join(joinConfig.left, joinConfig.right, joinConfig.on, joinConfig.qualification)
                ->bind()
                ->start(getParam, true)
                ->takeAll(gotElement);
        }

        // Finally, the column indexes
        for (auto & c: left.dataset->getColumnNames()) {
            ColumnName newColumnName(quotedLeftName.empty()
                                     ? c.toUtf8String()
                                     : quotedLeftName + "." + c.toUtf8String());
            ColumnHash newColumnHash(newColumnName);

            ColumnEntry entry;
            entry.columnName = newColumnName;
            entry.childColumnName = c;
            entry.bitmap = 1;

            columnIndex[newColumnHash] = std::move(entry);
            leftColumns[c] = newColumnName;
        }

        // Finally, the column indexes
        for (auto & c: right.dataset->getColumnNames()) {
            ColumnName newColumnName(quotedRightName.empty()
                                     ? c.toUtf8String()
                                     : quotedRightName + "." + c.toUtf8String());
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
    void recordJoinRow(const RowName & leftName, RowHash leftHash, const RowName & rightName, RowHash rightHash)
    {
        bool debug = true;

        RowName rowName(leftName.toUtf8String() + "-" + rightName.toUtf8String());
        RowHash rowHash(rowName);

        RowEntry entry;
        entry.rowName = rowName;
        entry.rowHash = rowHash;
        entry.leftName = leftName;
        entry.rightName = rightName;

        if (debug)
            cerr << "added entry number " << rows.size()
                 << "named " << "("<< rowName.toUtf8String() <<")"
                 << endl;

        rows.emplace_back(std::move(entry));
        rowIndex[rowHash] = rows.size() - 1;

        leftRowIndex[leftHash].push_back(rowName);
        rightRowIndex[rightHash].push_back(rowName);
    };

    void makeOuterSide(SqlExpressionMldbContext& context, 
                  const AnnotatedJoinCondition::Side & side, 
                  const Dataset & dataset, 
                  const std::function<void (const RowName &, RowHash)> & record)
    {
        //We create an expression that will return the complement

        std::shared_ptr<SqlExpression> lhs;
        auto notExpr = std::make_shared<BooleanOperatorExpression>(BooleanOperatorExpression(lhs, side.where, "NOT"));
        auto nullExpr = std::make_shared<IsTypeExpression>(side.where, false, "null");
        auto complementExpr = std::make_shared<BooleanOperatorExpression>(BooleanOperatorExpression(notExpr, nullExpr, "OR"));
        auto generator = dataset.queryBasic(context, side.select, side.when, *complementExpr, side.orderBy, 0, -1, true /* allowParallel */);
        auto rows = generator(-1);
        for (auto & r: rows)
            record(r.rowName, r.rowHash);
    }

    void makeJoinFast(AnnotatedJoinCondition& condition, SqlExpressionMldbContext& context, BoundTableExpression& left, BoundTableExpression& right, JoinQualification qualification)
    {
        bool debug = false;
        bool doLeft = qualification == JOIN_LEFT || qualification == JOIN_FULL;
        bool doRight = qualification == JOIN_RIGHT || qualification == JOIN_FULL;

        //cerr << "join fast path" << doLeft << doRight << endl;

        // Where expressions for the left and right side
        auto runSide = [&] (const AnnotatedJoinCondition::Side & side,
                            const Dataset & dataset)
            -> std::vector<std::tuple<ExpressionValue, RowName, RowHash> >
            {
                auto generator = dataset.queryBasic
                (context, side.select, side.when, *side.where, side.orderBy,
                 0, -1, true /* allowParallel */);
                auto rows = generator(-1);
            
                if (debug)
                    cerr << "got rows " << jsonEncode(rows) << endl;
                
                // Now we extract all values and re-sort.  This is necessary because
                // a row may have multiple values for the same column.
                std::vector<std::tuple<ExpressionValue, RowName, RowHash> > sorted;

                for (auto & r: rows) {
                    for (auto & c: r.columns) {
                        sorted.emplace_back(std::get<1>(c), r.rowName, r.rowHash);
                    }
                }

                std::sort(sorted.begin(), sorted.end());

                return sorted;
            };

        std::vector<std::tuple<ExpressionValue, RowName, RowHash> > leftRows, rightRows;

        leftRows = runSide(condition.left, *left.dataset);
        rightRows = runSide(condition.right, *right.dataset);

        switch (condition.style) {
        case AnnotatedJoinCondition::CROSS_JOIN: {
            // Join with no restrictions on the joined column
            if (leftRows.size() * rightRows.size() > 100000000) {
                throw HttpReturnException(400, "Cross join too big: cowardly refusing to materialize row IDs for a dataset with > 100,000,000 rows",
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
            throw HttpReturnException(400, "Join expression requires an equality operator; needs to be in the form f(left) = f(right)",
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
                cerr << "joining " << jsonEncodeStr(val1) << " and " << jsonEncodeStr(val2) << endl;

            if (val1 < val2) {
                if (doLeft)
                    recordJoinRow(std::get<1>(*it1), std::get<2>(*it1), RowName(), RowHash()); //For LEFT and FULL joins
                ++it1;
            }
            else if (val2 < val1) {
                if (doRight)
                    recordJoinRow(RowName(), RowHash(),std::get<1>(*it2), std::get<2>(*it2)); //For RIGHT and FULL joins
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
                                cerr << "rows " << leftName << " and " << rightName << " join on value " << val1 << endl;
                            
                            recordJoinRow(leftName, leftHash, rightName, rightHash);
                        }
                    }
                }
                else if (qualification != JOIN_INNER)
                {
                    for (auto it1a = it1; it1a < erng1 && doLeft;  ++it1a)
                    {
                        recordJoinRow(std::get<1>(*it1), std::get<2>(*it1), RowName(), RowHash()); //For LEFT and FULL joins
                    }

                    for (auto it2a = it2; it2a < erng2 && doRight;  ++it2a)
                    {
                        recordJoinRow(RowName(), RowHash(),std::get<1>(*it2), std::get<2>(*it2)); //For RIGHT and FULL joins
                    }
                }

                it1 = erng1;
                it2 = erng2;
            }
        }


        if (qualification == JOIN_LEFT || qualification == JOIN_FULL)
        {
            //cerr << "doing LEFT side" << endl;
            auto record = [&] (const RowName & leftName, RowHash leftHash)
            {
                recordJoinRow(leftName, leftHash, RowName(), RowHash());
            };

            makeOuterSide(context, condition.left, *(left.dataset), record);
        }
        if (qualification == JOIN_RIGHT || qualification == JOIN_FULL)
        {
            //cerr << "doing RIGHT side" << endl;
            auto record = [&] (const RowName & rightName, RowHash rightHash)
            {
                recordJoinRow(RowName(), RowHash(), rightName, rightHash);
            };

            makeOuterSide(context, condition.right, *(right.dataset), record);
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

        auto leftRow = matrices[0]->getRow(row.leftName);
        auto rightRow = matrices[1]->getRow(row.rightName);
        
        /// This function copies columns from a sub-row to the result of
        /// the function.
        auto copyColumns = [&] (const MatrixNamedRow & row,
                                const std::unordered_map<ColumnHash, ColumnName> & mapping)
            {
                for (auto & c: row.columns) {
                    ColumnHash colHash = std::get<0>(c);
                    auto it = mapping.find(colHash);
                    if (it == mapping.end())
                        continue;
                    result.columns.emplace_back(it->second,
                                                std::move(std::get<1>(c)),
                                                std::get<2>(c));
                }
            };

        copyColumns(leftRow, leftColumns);
        copyColumns(rightRow, rightColumns);

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
            throw HttpReturnException(500, "Joined dataset did not find column with given hash",
                                      "columnName", columnName);
        
        auto doGetColumn = [&] (const Dataset & dataset,
                                const SideRowIndex & index,
                                const ColumnName & columnName) -> MatrixColumn
            {
                MatrixColumn result;

                // First, get the column
                MatrixColumn column = std::move(dataset.getColumnIndex()->getColumn(columnName));

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
            result = doGetColumn(*datasets[0], leftRowIndex, it->second.childColumnName);
        }
        else {
            result = doGetColumn(*datasets[1], rightRowIndex, it->second.childColumnName);
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
    
    itl.reset(new Itl(server, joinConfig));
}

JoinedDataset::
JoinedDataset(MldbServer * owner,
              JoinedDatasetConfig config)
    : Dataset(owner)
{
    itl.reset(new Itl(server, config));
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

void 
JoinedDataset::
getChildAliases(std::vector<Utf8String> & outAliases) const
{
    outAliases.insert(outAliases.begin(), itl->tableNames.begin(), itl->tableNames.end());
}

static RegisterDatasetType<JoinedDataset, JoinedDatasetConfig> 
regJoined(builtinPackage(),
          "joined",
          "Joins together several datasets into one virtual dataset",
          "datasets/JoinedDataset.md.html");

extern std::shared_ptr<Dataset> (*createJoinedDatasetFn) (MldbServer *, const JoinedDatasetConfig &);

std::shared_ptr<Dataset> createJoinedDataset(MldbServer * server, const JoinedDatasetConfig & config)
{
    return std::make_shared<JoinedDataset>(server, config);
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
