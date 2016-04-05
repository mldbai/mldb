/* dataset.cc
   Jeremy Barnes, 21 January 2014
   Copyright (c) 2014 Datacratic Inc.  All rights reserved.

   This file is part of MLDB. Copyright 2015 Datacratic. All rights reserved.

   Dataset support.
*/

#include "mldb/core/dataset.h"
#include "mldb/types/structure_description.h"
#include "mldb/sql/sql_expression_operations.h"
#include "mldb/types/tuple_description.h"
#include "mldb/types/vector_description.h"
#include "mldb/server/analytics.h"
#include "mldb/sql/sql_expression.h"
#include "mldb/sql/sql_utils.h"
#include "mldb/jml/utils/lightweight_hash.h"
#include "mldb/server/dataset_context.h"
#include "mldb/server/per_thread_accumulator.h"
#include "mldb/server/bucket.h"
#include "mldb/server/parallel_merge_sort.h"
#include "mldb/jml/utils/environment.h"
#include "mldb/ml/jml/buckets.h"
#include "mldb/base/parallel.h"
#include "mldb/types/any_impl.h"
#include "mldb/http/http_exception.h"
#include "mldb/rest/rest_request_router.h"
#include "mldb/types/hash_wrapper_description.h"
#include <mutex>


using namespace std;

extern "C" {
    // For TCMalloc.  TODO: similar functionality exists in other memory
    // allocators.
    void MallocExtension_ReleaseFreeMemory(void) __attribute__((weak));
    void MallocExtension_GetStats(char * buffer, int buffer_length) __attribute__((weak));

    // Create weak versions of these symbols for when we're not using
    // tcmalloc.
    void MallocExtension_ReleaseFreeMemory(void)
    {
    }

    void MallocExtension_GetStats(char * buffer, int buffer_length)
    {
        if (buffer_length)
            *buffer = 0;
    }
} // extern "C"

namespace Datacratic {
namespace MLDB {

namespace {

struct SortByRowHash {
    bool operator () (const RowName & row1, const RowName & row2)
    {
        RowHash h1(row1), h2(row2);

        return h1 < h2 || (h1 == h2 && row1 < row2);
    }
};

} // file scope


/*****************************************************************************/
/* DATASET                                                                    */
/*****************************************************************************/

DEFINE_STRUCTURE_DESCRIPTION(MatrixRow);

MatrixRowDescription::
MatrixRowDescription()
{
    addField("rowName", &MatrixRow::rowName, "Name of the row");
    addField("rowHash", &MatrixRow::rowHash, "Hash of the row");
    addField("columns", &MatrixRow::columns, "Columns active for this row");
}

DEFINE_STRUCTURE_DESCRIPTION(MatrixNamedRow);

MatrixNamedRowDescription::
MatrixNamedRowDescription()
{
    addField("rowName", &MatrixNamedRow::rowName, "Name of the row");
    addField("rowHash", &MatrixNamedRow::rowHash, "Hash of the row");
    addField("columns", &MatrixNamedRow::columns, "Columns active for this row");
}

DEFINE_STRUCTURE_DESCRIPTION(MatrixEvent);

MatrixEventDescription::
MatrixEventDescription()
{
    addField("rowName", &MatrixEvent::rowName, "Name of the row");
    addField("rowHash", &MatrixEvent::rowHash, "Hash of the row");
    addField("timestamp", &MatrixEvent::timestamp, "Timestamp of event");
    addField("columns", &MatrixEvent::columns, "Columns active for this event");
}

DEFINE_STRUCTURE_DESCRIPTION(MatrixNamedEvent);

MatrixNamedEventDescription::
MatrixNamedEventDescription()
{
    addField("rowName", &MatrixNamedEvent::rowName, "Name of the row");
    addField("rowHash", &MatrixNamedEvent::rowHash, "Hash of the row");
    addField("timestamp", &MatrixNamedEvent::timestamp, "Timestamp of event");
    addField("columns", &MatrixNamedEvent::columns, "Columns active for this event");
}

DEFINE_STRUCTURE_DESCRIPTION(MatrixColumn);

MatrixColumnDescription::
MatrixColumnDescription()
{
    addField("columnName", &MatrixColumn::columnName, "Name of the column");
    addField("columnHash", &MatrixColumn::columnHash, "Hash of the column");
    addField("rows", &MatrixColumn::rows, "Row values for this column");
}

struct DatasetPolyConfigDescription    
    :  public Datacratic::StructureDescription<PolyConfigT<Dataset> > {
    DatasetPolyConfigDescription();

    DatasetPolyConfigDescription(const Datacratic::ConstructOnly &);

    virtual void initialize()
    {
        DatasetPolyConfigDescription newMe;
        *this = std::move(newMe);
    }

    virtual void parseJson(void * val,
                           JsonParsingContext & context) const;

    struct Regme;
    static Regme regme;
};

struct DatasetPolyConfigDescription::Regme {
    bool done;
    Regme()
        : done(false)
    {
        Datacratic::registerValueDescription
            (typeid(PolyConfigT<Dataset> ), [] () { return new DatasetPolyConfigDescription(); }, true);
    }
};

DatasetPolyConfigDescription::DatasetPolyConfigDescription(const Datacratic::ConstructOnly &)
{
    regme.done = true;
}

Datacratic::ValueDescriptionT<PolyConfigT<Dataset> > *
getDefaultDescription(PolyConfigT<Dataset> *)
{
    return new DatasetPolyConfigDescription();
}

Datacratic::ValueDescriptionT<PolyConfigT<Dataset> > *
getDefaultDescriptionUninitialized(PolyConfigT<Dataset> *)
{
    return new DatasetPolyConfigDescription(::Datacratic::constructOnly);
}

DatasetPolyConfigDescription::Regme DatasetPolyConfigDescription::regme;

DatasetPolyConfigDescription::
DatasetPolyConfigDescription()
{
    addParent<PolyConfig>();
    setTypeName("OutputDatasetSpec");
    documentationUri = "/doc/builtin/procedures/OutputDatasetSpec.md";
}

void
DatasetPolyConfigDescription::
parseJson(void * val,
          JsonParsingContext & context) const
{
    if (context.isString()) {
        PolyConfigT<Dataset> * val2 = reinterpret_cast<PolyConfigT<Dataset > *>(val);
        val2->id = context.expectStringAscii();
    }
    else 
        StructureDescription::parseJson(val, context);
}

DEFINE_STRUCTURE_DESCRIPTION_NAMED(ConstDatasetPolyConfigDescription,
                                   PolyConfigT<const Dataset>);

ConstDatasetPolyConfigDescription::
ConstDatasetPolyConfigDescription()
{
    addParent<PolyConfig>();
    setTypeName("Dataset (read-only)");
    documentationUri = "/doc/builtin/datasets/DatasetConfig.md";
}

DEFINE_STRUCTURE_DESCRIPTION(PersistentDatasetConfig);

PersistentDatasetConfigDescription::
PersistentDatasetConfigDescription()
{
    nullAccepted = true;

    addField("dataFileUrl", &PersistentDatasetConfig::dataFileUrl,
             "URL of the data file from which to load the dataset.");
}

/*****************************************************************************/
/* MATRIX VIEW                                                               */
/*****************************************************************************/

MatrixView::
~MatrixView()
{
}

uint64_t
MatrixView::
getRowColumnCount(const RowName & row) const
{
    ML::Lightweight_Hash_Set<ColumnHash> cols;
    for (auto & c: getRow(row).columns)
        cols.insert(std::get<0>(c));
    return cols.size();
}


/*****************************************************************************/
/* COLUMN INDEX                                                              */
/*****************************************************************************/

ColumnIndex::
~ColumnIndex()
{
}

uint64_t
ColumnIndex::
getColumnRowCount(const ColumnName & column) const
{
    ColumnStats toStoreResult;
    return getColumnStats(column, toStoreResult).rowCount();
}

bool
ColumnIndex::
forEachColumnGetStats(const OnColumnStats & onColumnStats) const
{
    for (auto & c: getColumnNames()) {
        ColumnStats toStore;
        if (!onColumnStats(c, getColumnStats(c, toStore)))
            return false;
    }

    return true;
}

const ColumnStats &
ColumnIndex::
getColumnStats(const ColumnName & column, ColumnStats & stats) const
{
    auto col = getColumnValues(column);

    stats = ColumnStats();

    ML::Lightweight_Hash_Set<RowHash> rows;
    bool oneOnly = true;
    bool isNumeric = true;

    for (auto & r: col) {
        RowHash rh = std::get<0>(r);
        const CellValue & v = std::get<1>(r);

        if (!rows.insert(rh).second)
            oneOnly = false;
        
        if (!v.isNumber())
            isNumeric = false;
        
        // TODO: not really true...
        stats.values[v].rowCount_ += 1;
    }

    stats.isNumeric_ = isNumeric && !col.empty();
    stats.rowCount_ = rows.size();
    return stats;
}

std::vector<std::tuple<RowName, CellValue> >
ColumnIndex::
getColumnValues(const ColumnName & column,
                const std::function<bool (const CellValue &)> & filter) const
{
    auto col = getColumn(column);

    std::vector<std::tuple<RowName, CellValue> > result;
    result.reserve(col.rows.size());

    bool sorted = true;
    
    for (auto & r: col.rows) {
        if (filter && !filter(std::get<1>(r)))
            continue;
        std::tuple<RowName, CellValue>
            current(std::move(std::get<0>(r)),
                    std::move(std::get<1>(r)));
        if (result.empty()) {
            result.emplace_back(std::move(current));
            continue;
        }

        auto & last = result.back();

        if (current == last)
            continue;
        if (current < last)
            sorted = false;
        if (!sorted || current > last)
            result.emplace_back(std::move(current));
    }

    if (!sorted) {
        std::sort(result.begin(), result.end());
        result.erase(std::unique(result.begin(), result.end()),
                     result.end());
    }

    return std::move(result);
}

std::vector<CellValue>
ColumnIndex::
getColumnDense(const ColumnName & column) const
{
    auto columnValues = getColumn(column);
    // getRowNames can return row names in an arbitrary order as long as it is deterministic.
    std::vector<RowName> rowNames = getRowNames();
    std::vector<CellValue> result;
    result.reserve(rowNames.size());
    const auto rowNamesBegin = rowNames.begin();
    const auto rowNamesEnd = rowNames.end();

    std::unordered_map<RowName, std::pair<CellValue, Date> > values;

    for (auto & c: columnValues.rows) {
        Date dateToInsert = std::get<2>(c);
        std::pair<CellValue, Date> valToInsert = std::make_pair<CellValue, Date>(std::move(std::get<1>(c)), std::move(dateToInsert));
        auto keyPair = std::make_pair<RowName, std::pair<CellValue, Date> >(std::move(std::get<0>(c)), std::move(valToInsert));
        auto res = values.insert(keyPair);
        if (!res.second) {
            if ( dateToInsert > res.first->second.second)
                res.first->second = valToInsert;
        }
    }

    for (auto& name : rowNames) {
        result.push_back(values.find(name)->second.first);
    } 

    return std::move(result);
}

std::tuple<BucketList, BucketDescriptions>
ColumnIndex::
getColumnBuckets(const ColumnName & column,
                 int maxNumBuckets) const
{
    auto vals = getColumnDense(column);

    std::unordered_map<CellValue, size_t> values;
    std::vector<CellValue> valueList;

    size_t totalRows = vals.size();

    for (auto& v : vals) {
        if (values.insert({v,0}).second)
            valueList.push_back(std::move(v));
    }

    BucketDescriptions descriptions;
    descriptions.initialize(valueList, maxNumBuckets);

    for (auto & v: values) {
        v.second = descriptions.getBucket(v.first);            
    }
        
    // Finally, perform the bucketed lookup
    WritableBucketList buckets(totalRows, descriptions.numBuckets());

    for (auto& v : vals) {
        uint32_t bucket = values[v];
        buckets.write(bucket);
    }

    return std::make_tuple(std::move(buckets), std::move(descriptions));
}


/*****************************************************************************/
/* DATASET                                                                    */
/*****************************************************************************/

Dataset::
Dataset(MldbServer * server)
    : server(server)
{
}

ML::Env_Option<int> RETURN_OS_MEMORY("RETURN_OS_MEMORY", 1);
ML::Env_Option<int> PRINT_OS_MEMORY("PRINT_OS_MEMORY", 0);


Dataset::
~Dataset()
{
    // MLDBFB-329
    // Once a dataset is deleted, try to free its memory from the system
    if (PRINT_OS_MEMORY) {
        char buf[8192];
        MallocExtension_GetStats(buf, 8192);
        cerr << buf << endl;
    }

    if (RETURN_OS_MEMORY) {
        MallocExtension_ReleaseFreeMemory();    
    
        if (PRINT_OS_MEMORY) {
            char buf[8192];
            MallocExtension_GetStats(buf, 8192);
            cerr << buf << endl;
        }
    }
}

void
Dataset::
recordRow(const RowName & rowName,
          const std::vector<std::tuple<ColumnName, CellValue, Date> > & vals)
{
    validateNames(rowName, vals);
    recordRowItl(rowName, vals);
}

void
Dataset::
recordRowItl(const RowName & rowName,
             const std::vector<std::tuple<ColumnName, CellValue, Date> > & vals)
{
    throw ML::Exception(("Dataset type '" + getType() + "' doesn't allow recording").rawString());
}

std::pair<Date, Date>
Dataset::
getTimestampRange() const
{
    static const SelectExpression select
        = SelectExpression::parseList("min(earliest_timestamp({*})) as earliest, max(latest_timestamp({*})) as latest");

    std::vector<MatrixNamedRow> res
        = queryStructured(select,
                          WhenExpression::TRUE,
                          *SqlExpression::TRUE /* where */,
                          OrderByExpression(),
                          TupleExpression(),
                          *SqlExpression::TRUE /* having */,
                          *SqlExpression::TRUE,
                          0, 1, "" /* alias */);
    
    std::pair<Date, Date> result;

    ExcAssertEqual(res.size(), 1);
    ExcAssertEqual(res[0].columns.size(), 2);

    static ColumnName cmin("earliest"), cmax("latest");

    for (auto & c: res[0].columns) {
        if (std::get<0>(c) == cmin)
            result.first = std::get<1>(c).toTimestamp();
        else if (std::get<0>(c) == cmax)
            result.second = std::get<1>(c).toTimestamp();
        else throw ML::Exception("unknown output of timestamp range query");
    }
    
    return result;
}

Date
Dataset::
quantizeTimestamp(Date timestamp) const
{
    throw ML::Exception(("Dataset type '" + getType() + "' doesn't allow recording and thus doesn't quantize timestamps").rawString());
}

void 
Dataset::
validateNames(const RowName & rowName,
              const std::vector<std::tuple<ColumnName, CellValue, Date> > & vals)
{
    if (rowName.empty())
        throw HttpReturnException(400, "empty row names are not allowed");
    for (auto & val : vals) {
        if (get<0>(val).empty())
            throw HttpReturnException(400, "empty column names are not allowed");
    }
}

void 
Dataset::
validateNames(const std::vector<std::pair<RowName, std::vector<std::tuple<ColumnName, CellValue, Date> > > > & rows)
{
    for (auto& r : rows)
    {
        validateNames(r.first, r.second);
    }
}

void
Dataset::
recordRows(const std::vector<std::pair<RowName, std::vector<std::tuple<ColumnName, CellValue, Date> > > > & rows)
{
    for (auto & r: rows)
        recordRow(r.first, r.second);
}

void
Dataset::
recordColumn(const ColumnName & columnName,
             const std::vector<std::tuple<RowName, CellValue, Date> > & vals)
{
    recordColumns({{columnName, vals}});
}

void
Dataset::
recordColumns(const std::vector<std::pair<ColumnName, std::vector<std::tuple<RowName, CellValue, Date> > > > & cols)
{
    std::map<RowName, std::vector<std::tuple<ColumnName, CellValue, Date> > > transposed;

    for (auto & c: cols) {
        for (auto & r: c.second) {
            transposed[std::get<0>(r)].emplace_back(c.first, std::get<1>(r), std::get<2>(r));
        }
    }

    std::vector<std::pair<RowName, std::vector<std::tuple<ColumnName, CellValue, Date> > > > rows
        (std::make_move_iterator(transposed.begin()),
         std::make_move_iterator(transposed.end()));

    recordRows(rows);
}

void
Dataset::
recordRowExpr(const RowName & rowName,
              const ExpressionValue & expr)
{
    RowValue row;
    expr.appendToRow(ColumnName(), row);
    recordRow(rowName, std::move(row));
}

void
Dataset::
recordRowsExpr(const std::vector<std::pair<RowName, ExpressionValue> > & rows)
{
    std::vector<std::pair<RowName, RowValue> > rowsOut;
    rowsOut.reserve(rows.size());
    for (auto & r: rows) {
        const RowName & rowName = r.first;
        const ExpressionValue & expr = r.second;
        RowValue row;
        expr.appendToRow(ColumnName(), row);
        rowsOut.emplace_back(rowName, std::move(row));
    }
    recordRows(std::move(rowsOut));
}

void
Dataset::
recordEmbedding(const std::vector<ColumnName> & columnNames,
                const std::vector<std::tuple<RowName, std::vector<float>, Date> > & rows)
{
    vector<pair<RowName, vector<tuple<ColumnName, CellValue, Date> > > > rowsOut;

    for (auto & r: rows) {
        vector<tuple<ColumnName, CellValue, Date> > row;
        row.reserve(columnNames.size());

        const RowName & rowName = std::get<0>(r);
        const std::vector<float> & embedding = std::get<1>(r);
        Date ts = std::get<2>(r);

        ExcAssertEqual(embedding.size(), columnNames.size());

        for (unsigned j = 0; j < columnNames.size();  ++j) {
            row.emplace_back(columnNames[j], embedding[j], ts);
        }

        rowsOut.emplace_back(rowName, std::move(row));
    }

    recordRows(rowsOut);
}

KnownColumn
Dataset::
getKnownColumnInfo(const ColumnName & columnName) const
{
    // TODO: do a better job with this... we are conservative but the column may have
    // a much tighter domain than this.
    return KnownColumn(columnName, std::make_shared<AtomValueInfo>(),
                       COLUMN_IS_SPARSE);
}

std::vector<KnownColumn>
Dataset::
getKnownColumnInfos(const std::vector<ColumnName> & columnNames) const
{
    std::vector<KnownColumn> result;
    result.reserve(columnNames.size());
    for (auto & columnName: columnNames)
        result.emplace_back(getKnownColumnInfo(columnName));
    return result;
}

std::shared_ptr<RowValueInfo>
Dataset::
getRowInfo() const
{
    std::vector<KnownColumn> knownColumns;

    for (auto & c: getColumnNames()) {
        knownColumns.emplace_back(std::move(getKnownColumnInfo(c)));
    }

    return std::make_shared<RowValueInfo>(std::move(knownColumns),
                                          SCHEMA_CLOSED);
}

std::vector<MatrixNamedRow>
Dataset::
queryStructured(const SelectExpression & select,
                const WhenExpression & when,
                const SqlExpression & where,
                const OrderByExpression & orderBy,
                const TupleExpression & groupBy,
                const SqlExpression & having,
                const SqlExpression & rowName,
                ssize_t offset,
                ssize_t limit,
                Utf8String alias,
                bool allowMT) const
{
    std::mutex lock;
    std::vector<MatrixNamedRow> output;

    if (!having.isConstantTrue() && groupBy.clauses.empty())
        throw HttpReturnException(400, "HAVING expression requires a GROUP BY expression");

    std::vector< std::shared_ptr<SqlExpression> > aggregators = select.findAggregators(!groupBy.clauses.empty());
    std::vector< std::shared_ptr<SqlExpression> > havingaggregators = having.findAggregators(!groupBy.clauses.empty());

    // Do it ungrouped if possible
    if (groupBy.clauses.empty() && aggregators.empty()) {
        auto aggregator = [&] (NamedRowValue & row_,
                               const std::vector<ExpressionValue> & calc)
            {
                MatrixNamedRow row = row_.flattenDestructive();
                row.rowName = GetValidatedRowName(calc.at(0));
                row.rowHash = row.rowName;
                std::unique_lock<std::mutex> guard(lock);
                output.emplace_back(std::move(row));
                return true;
            };

        // MLDB-154: if we have a limit or offset, we probably want a stable ordering
        // Due to a bug that led to it being always enabled we will always do this
        OrderByExpression orderBy_ = orderBy;
        if (limit != -1 || offset != 0 || true) {
            orderBy_.clauses.emplace_back(SqlExpression::parse("rowHash()"), ASC);
        }
        
        //cerr << "orderBy_ = " << jsonEncode(orderBy_) << endl;
        iterateDataset(select, *this, alias, when, where,
                       { rowName.shallowCopy() }, aggregator, orderBy_, offset, limit,
                       nullptr);
    }
    else {

        aggregators.insert(aggregators.end(), havingaggregators.begin(), havingaggregators.end());

        // Otherwise do it grouped...
        auto aggregator = [&] (NamedRowValue & row_)
            {
                MatrixNamedRow row = row_.flattenDestructive();
                std::unique_lock<std::mutex> guard(lock);
                output.emplace_back(row);
                return true;
            };

        iterateDatasetGrouped(select, *this, alias, when, where,
                              groupBy, aggregators, having, rowName,
                              aggregator, orderBy, offset, limit,
                              nullptr, allowMT);
    }

    return output;
}

template<typename Filter>
static std::pair<std::vector<RowName>, Any>
executeFilteredColumnExpression(const Dataset & dataset,
                                ssize_t numToGenerate, Any token,
                                const BoundParameters & params,
                                const ColumnName & columnName,
                                const Filter & filter)
{
    auto columnIndex = dataset.getColumnIndex();

    if (columnIndex->knownColumn(columnName)) {
        auto col = (*dataset.getColumnIndex()).getColumnValues(columnName, filter);
    
        std::vector<RowName> rows;

        auto matrix = dataset.getMatrixView();

        for (auto & r: col) {
            RowName & rh = std::get<0>(r);
            rows.emplace_back(std::move(rh));
        }

        std::sort(rows.begin(), rows.end(), SortByRowHash());
        rows.erase(std::unique(rows.begin(), rows.end()),
                   rows.end());

        return std::pair<std::vector<RowName>, Any>(std::move(rows), std::move(Any()));
    }
    else {
        return {};
    }
    
}

template<typename Filter>
static GenerateRowsWhereFunction
generateFilteredColumnExpression(const Dataset & dataset,
                                 const ColumnName & columnName,
                                 const Filter & filter,
                                 const std::string & explanation)
{
    return { std::bind(&executeFilteredColumnExpression<Filter>,
                       std::cref(dataset),
                       std::placeholders::_1,
                       std::placeholders::_2,
                       std::placeholders::_3,
                       columnName,
                       filter),
            explanation };
}

static GenerateRowsWhereFunction
generateVariableEqualsConstant(const Dataset & dataset,
                               const Utf8String& alias,
                               const ReadVariableExpression & variable,
                               const ConstantExpression & constant)
{
    ColumnName columnName(removeTableName(alias,variable.variableName).rawString());
    CellValue constantValue(constant.constant.getAtom());

    auto filter = [=] (const CellValue & val)
        {
            return val == constantValue;
        };

    return generateFilteredColumnExpression
        (dataset, columnName, filter,
         "generate rows where var '" + variable.variableName.rawString()
         + "' matches value '"
         + constantValue.toString() + "'");
}

static GenerateRowsWhereFunction
generateVariableIsTrue(const Dataset & dataset,
                       const Utf8String& alias,
                       const ReadVariableExpression & variable)
{
    ColumnName columnName(removeTableName(alias,variable.variableName).rawString());
    
    auto filter = [&] (const CellValue & val)
        {
            return val.isTrue();
        };

    return generateFilteredColumnExpression
        (dataset, columnName, filter,
         "generate rows where var '" + variable.variableName.rawString() + "' is true");
}

static GenerateRowsWhereFunction
generateVariableIsNotNull(const Dataset & dataset,
                          const Utf8String& alias,
                          const ReadVariableExpression & variable)
{
    ColumnName columnName(removeTableName(alias,variable.variableName).rawString());
    
    auto filter = [&] (const CellValue & val)
        {
            return !val.empty();
        };

    return generateFilteredColumnExpression
        (dataset, columnName, filter,
         "generate rows where var '" + variable.variableName.rawString() + "' is not null");
}

static GenerateRowsWhereFunction
generateRownameIsConstant(const Dataset & dataset,
                          const ConstantExpression & rowNameExpr)
{
    auto datasetPtr = &dataset;
    RowName rowName(rowNameExpr.constant.toString());
    return {[=] (ssize_t numToGenerate, Any token,
                 const BoundParameters & params)
            -> std::pair<std::vector<RowName>, Any>
            {
                // There should be exactly one row
                if (datasetPtr->getMatrixView()->knownRow(rowName))
                    return { { rowName }, token };
                else return { {}, token };
            },
            "generate single row matching rowName()"};
}

/*
    Must return the *exact* set of rows or a stream that will do the same
    because the where expression will not be evaluated outside of this method
    if this method is called.

    Ordering can be arbitrary but need to be deterministic
*/    
GenerateRowsWhereFunction
Dataset::
generateRowsWhere(const SqlBindingScope & scope,
                  const Utf8String& alias,
                  const SqlExpression & where,
                  ssize_t offset,
                  ssize_t limit) const
{
    auto getConstant = [] (const SqlExpression & expression) -> const ConstantExpression *
        {
            return dynamic_cast<const ConstantExpression *>(&expression);
        };

    auto getVariable = [] (const SqlExpression & expression) -> const ReadVariableExpression *
        {
            return dynamic_cast<const ReadVariableExpression *>(&expression);
        };

    auto getFunction = [] (const SqlExpression & expression) -> const FunctionCallWrapper *
        {
            return dynamic_cast<const FunctionCallWrapper *>(&expression);
        };

    auto getIsType = [] (const SqlExpression & expression) -> const IsTypeExpression *
        {
            return dynamic_cast<const IsTypeExpression *>(&expression);
        };

    auto getBoolean = [] (const SqlExpression & expression) -> const BooleanOperatorExpression *
        {
            return dynamic_cast<const BooleanOperatorExpression *>(&expression);
        };

    auto getArith = [] (const SqlExpression & expression) -> const ArithmeticExpression *
        {
            return dynamic_cast<const ArithmeticExpression *>(&expression);
        };

    auto boolean = getBoolean(where);

    if (boolean) {
        // Optimize a boolean operator

        if (boolean->op == "AND") {
            GenerateRowsWhereFunction lhsGen = generateRowsWhere(scope, alias, *boolean->lhs, 0, -1);
            GenerateRowsWhereFunction rhsGen = generateRowsWhere(scope, alias, *boolean->rhs, 0, -1);
            cerr << "AND between " << lhsGen.explain << " and " << rhsGen.explain
                 << endl;

            if (lhsGen.explain != "scan table" && rhsGen.explain != "scan table") {

                return {[=] (ssize_t numToGenerate, Any token,
                             const BoundParameters & params)
                        -> std::pair<std::vector<RowName>, Any>
                        {
                            auto lhsRows = lhsGen(-1, Any(), params).first;
                            auto rhsRows = rhsGen(-1, Any(), params).first;

                            std::sort(lhsRows.begin(), lhsRows.end(), SortByRowHash());
                            std::sort(rhsRows.begin(), rhsRows.end(), SortByRowHash());

                            vector<RowName> intersection;
                            std::set_intersection(lhsRows.begin(), lhsRows.end(),
                                                  rhsRows.begin(), rhsRows.end(),
                                                  std::back_inserter(intersection),
                                                  SortByRowHash());

                            return { std::move(intersection), Any() };
                        },
                        "set intersection for AND " + boolean->print().rawString() };
            }
        }
        else if (boolean->op == "OR") {
            GenerateRowsWhereFunction lhsGen = generateRowsWhere(scope, alias, *boolean->lhs, 0, -1);
            GenerateRowsWhereFunction rhsGen = generateRowsWhere(scope, alias, *boolean->rhs, 0, -1);
            cerr << "OR between " << lhsGen.explain << " and " << rhsGen.explain
                 << endl;

            if (lhsGen.explain != "scan table" && rhsGen.explain != "scan table") {
                return {[=] (ssize_t numToGenerate, Any token,
                             const BoundParameters & params)
                        -> std::pair<std::vector<RowName>, Any>
                        {
                            auto lhsRows = lhsGen(-1, Any(), params).first;
                            auto rhsRows = rhsGen(-1, Any(), params).first;

                            std::sort(lhsRows.begin(), lhsRows.end(), SortByRowHash());
                            std::sort(rhsRows.begin(), rhsRows.end(), SortByRowHash());

                            vector<RowName> u;
                            std::set_union(lhsRows.begin(), lhsRows.end(),
                                           rhsRows.begin(), rhsRows.end(),
                                           std::back_inserter(u),
                                           SortByRowHash());

                            return { std::move(u), Any() };
                        },
                        "set union for OR " + boolean->print().rawString() };
            }
        }
        else if (boolean->op == "NOT") {
            // TODO.  Mostly useful (in optimization) when we have AND NOT.
        }
    }

    auto variable = getVariable(where);

    if (variable) {
        // Optimize just a variable
        return generateVariableIsTrue(*this, alias, *variable);
    }

    // Optimize for rowName() IN (constant, constant, constant)
    // Optimize for rowName() IN ROWS / IN KEYS (...)
    auto inExpression = dynamic_cast<const InExpression *>(&where);
    if (inExpression) 
    {
        auto fexpr = getFunction(*(inExpression->expr));
        if (fexpr && removeTableName(alias, fexpr->functionName) == "rowName" ) {
            if (inExpression->tuple && inExpression->tuple->isConstant()) {
                return {[=] (ssize_t numToGenerate, Any token,
                             const BoundParameters & params)
                        -> std::pair<std::vector<RowName>, Any>
                        {
                            std::vector<RowName> filtered;
                            auto matrixView = this->getMatrixView();

                            for (auto& c : inExpression->tuple->clauses) {
                                ExpressionValue v = c->constantValue();

                                //casting other types to string will give a different result than non-optimized path.
                                if (!v.isString())
                                    continue;

                                RowName rowName(v.toUtf8String());

                                if (matrixView->knownRow(rowName))
                                    filtered.push_back(rowName);
                            }
                            return { std::move(filtered), Any() };
                        },
                        "rowName in tuple " + inExpression->tuple->print().rawString() };
            }
            else if (inExpression->setExpr) {
                // in keys or in values expression
                // Make sure they are constant or depend only upon the
                // bound parameters
                auto unbound = inExpression->setExpr->getUnbound();
                if (unbound.vars.empty() && unbound.tables.empty()
                    && unbound.wildcards.empty()) {
                    //cerr << "*** rowName() IN (constant set expr)" << endl;

                    SqlExpressionParamScope paramScope;

                    auto boundSet = inExpression->setExpr->bind(paramScope);
                    auto matrixView = this->getMatrixView();

                    if (inExpression->setExpr) {

                        bool keys = (inExpression->kind == InExpression::KEYS);
                        bool values = (inExpression->kind == InExpression::VALUES);
                        ExcAssert(keys || values);

                        return {[=] (ssize_t numToGenerate, Any token,
                                     const BoundParameters & params)
                                -> std::pair<std::vector<RowName>, Any>
                                {
                                    SqlExpressionParamScope::RowScope rowScope(params);
                                    ExpressionValue evaluatedSet
                                        = boundSet(rowScope, GET_LATEST);

                                    std::vector<RowName> filtered;

                                    // Lambda for KEYS, which looks for a
                                    // matching row from the key
                                    auto onKey = [&] (const ColumnName & key,
                                                      const ColumnName & prefix,
                                                      const ExpressionValue & val)
                                        {
                                            if (matrixView->knownRow(key)) {
                                                filtered.push_back(key);
                                            }
                                            return true;
                                        };
                                
                                    // Lambda for VALUES, which looks for a
                                    // matching row from the value
                                    auto onValue = [&] (const ColumnName & key,
                                                        const ColumnName & prefix,
                                                        const ExpressionValue & val)
                                        {
                                            //casting other types to string will give a different result than non-optimized path.
                                            if (!val.isString())
                                                return true;

                                            auto str = RowName(val.toUtf8String());
                                            if (matrixView->knownRow(str)) {
                                                filtered.push_back(str);
                                            }
                                            return true;
                                        };
                                    
                                    
                                    if (keys)
                                        evaluatedSet.forEachSubexpression(onKey);
                                    else evaluatedSet.forEachSubexpression(onValue);
                                    
                                    return { std::move(filtered), Any() };
                                },
                                "rowName in keys of (expr) " + inExpression->print().rawString()
                                    };
                    }
                }
            }
        }
    }
    
    auto comparison = dynamic_cast<const ComparisonExpression *>(&where);

    if (comparison) {
        // To optimize a comparison, we need to have variable == constant, or
        // rowName() == constant

        //cerr << "comparison " << comparison->print() << endl;

        auto clhs = getConstant(*comparison->lhs);
        auto crhs = getConstant(*comparison->rhs);
        auto flhs = getFunction(*comparison->lhs);
        auto frhs = getFunction(*comparison->rhs);
        auto vlhs = getVariable(*comparison->lhs);
        auto vrhs = getVariable(*comparison->rhs);
        auto alhs = getArith(*comparison->lhs);

        // Optimization for rowName() == constant.  In this case, we can generate a
        // single row.
        if (flhs && crhs && comparison->op == "=") {
            if (removeTableName(alias, flhs->functionName) == "rowName") {
                return generateRownameIsConstant(*this, *crhs);
            }
        }
        // Optimization for constant == rowName().  In this case, we can generate a
        // single row.
        if (frhs && clhs && comparison->op == "=") {
            if (removeTableName(alias, frhs->functionName) == "rowName" ) {
                return generateRownameIsConstant(*this, *clhs);
            }
        }

        // Optimization for rowHash() % x op y
        if (alhs && alhs->op == "%" && crhs && crhs->constant.isInteger()) {
            //cerr << "compare x % y op c" << endl;

            auto flhs2 = getFunction(*alhs->lhs);
            auto crhs2 = getConstant(*alhs->rhs);
            

            if (flhs2 && removeTableName(alias, flhs2->functionName) == "rowHash" && crhs2 && crhs2->constant.isInteger()) {

                std::function<bool (uint64_t, uint64_t)> op;

                if (comparison->op == "=" || comparison->op == "==") {
                    op = std::equal_to<uint64_t>();
                }
                else if (comparison->op == "!=")
                    op = std::not_equal_to<uint64_t>();
                else if (comparison->op == "<")
                    op = std::less<uint64_t>();
                else if (comparison->op == "<=")
                    op = std::less_equal<uint64_t>();
                else if (comparison->op == ">")
                    op = std::greater<uint64_t>();
                else if (comparison->op == ">=")
                    op = std::greater_equal<uint64_t>();
                else throw HttpReturnException(400, "unknown operator for comparison",
                                               "op", comparison->op);
                
                uint64_t m = crhs2->constant.getAtom().toUInt();
                uint64_t c = crhs->constant.getAtom().toUInt();

                return {[=] (ssize_t numToGenerate, Any token,
                             const BoundParameters & params)
                        -> std::pair<std::vector<RowName>, Any>
                        {
                            std::vector<RowName> filtered;

                            // getRowNames can return row names in an arbitrary order as long as it is deterministic.
                            for (const RowName & n: this->getMatrixView()
                                     ->getRowNames()) {
                                uint64_t hash = RowHash(n).hash();
                                
                                if (op(hash % m, c))
                                    filtered.push_back(n);
                            }

                            cerr << "row hash modulus expression returned "
                                 << filtered.size() << " of "
                                 << this->getMatrixView()
                                ->getRowHashes().size() << " rows" << endl;

                            return { std::move(filtered), Any() };
                        },
                        "rowName modulus expression " + comparison->print().rawString() };
            }
        }

        // Optimization for variable == constant
        if (vlhs && crhs && comparison->op == "=") {
            return generateVariableEqualsConstant(*this, alias, *vlhs, *crhs);
        }
        if (vrhs && clhs && comparison->op == "=") {
            return generateVariableEqualsConstant(*this, alias, *vrhs, *clhs);
        }
    }

    auto isType = getIsType(where);

    if (isType) {

        auto vlhs = getVariable(*isType->expr);
        
        // Optimize variable IS NOT NULL
        if (vlhs && isType->type == "null" && isType->notType) {
            return generateVariableIsNotNull(*this, alias, *vlhs);
        }

        // Optimize variable IS TRUE
        if (vlhs && isType->type == "true" && !isType->notType) {
            return generateVariableIsTrue(*this, alias, *vlhs);
        }
    }

    // Where constant
    if (where.isConstant()) {
        if (where.constantValue().isTrue()) {
            GenerateRowsWhereFunction wheregen= {[=] (ssize_t numToGenerate, Any token,
                         const BoundParameters & params)
                    {
                        ssize_t start = 0;
                        ssize_t limit = numToGenerate;

                        ExcAssertNotEqual(limit, 0);
            
                        if (!token.empty())
                            start = token.convert<size_t>();

                        //Row names can be returned in an arbitrary order as long as it is deterministic.
                        auto rows = this->getMatrixView()
                            ->getRowNames(start, limit);

                        start += rows.size();
                        Any newToken;
                        if (rows.size() == limit)
                            newToken = start;
                
                        return std::move(make_pair(std::move(rows), std::move(newToken)));
                    },
                    "Scan table keeping all rows"};

            wheregen.upperBound = this->getMatrixView()->getRowCount();
            wheregen.rowStream = this->getRowStream();

            return wheregen;

        }
        else {
            return { [=] (ssize_t numToGenerate, Any token,
                          const BoundParameters & params) -> std::pair<std::vector<RowName>, Any>
                    {
                        return { {}, Any() };
                    },
                    "Return nothing as constant where expression doesn't evaluate true"};
        }
    }

    // Couldn't optimize.  Fall through to scanning, evaluating the where
    // expression at each point

    SqlExpressionDatasetContext dsScope(*this, alias);
    auto whereBound = where.bind(dsScope);

    // Detect if where needs columns or not, by looking at what is unbound
    // in the expression.  For example rowName() or rowHash() don't need
    // the columns at all.
    UnboundEntities unbound = where.getUnbound();

    // Look for a free variable
    bool needsColumns = unbound.hasUnboundVariables();

    //cerr << "needsColumns for " << where.print() << " returned "
    //     << jsonEncode(unbound) << " and result " << needsColumns << endl;

    //no need to check for where == true, it was checked above...

    return {[=] (ssize_t numToGenerate, Any token,
                 const BoundParameters & params)
            {
                ssize_t start = 0;
                ssize_t limit = numToGenerate;

                ExcAssertNotEqual(limit, 0);
            
                if (!token.empty())
                    start = token.convert<size_t>();

                auto matrix = this->getMatrixView();

                //Row names can be returned in an arbitrary order as long as it is deterministic.
                auto rows = matrix->getRowNames(start, limit);

                std::vector<RowName> rowsToKeep;

                PerThreadAccumulator<std::vector<RowName> > accum;
                
                auto onRow = [&] (size_t n)
                    {
                        const RowName & r = rows[n];

                        MatrixNamedRow row;
                        if (needsColumns)
                            row = std::move(matrix->getRow(r));
                        else {
                            row.rowHash = row.rowName = r;
                        }

                        auto rowScope = dsScope.getRowContext(row, &params);
                        
                        bool keep = whereBound(rowScope, GET_LATEST).isTrue();
                        
                        if (keep)
                            accum.get().push_back(r);
                    };

                if (rows.size() >= 1000) {
                    // Scan the whole lot with the when in parallel
                    parallelMap(0, rows.size(), onRow);
                } else {
                    // Serial, since probably it's not worth the overhead
                    // to run them in parallel.
                    for (unsigned i = 0;  i < rows.size();  ++i)
                        onRow(i);
                }

                // Now merge together the results of all the threads
                auto onThreadOutput = [&] (std::vector<RowName> * vec)
                    {
                        rowsToKeep.insert(rowsToKeep.end(),
                                          std::make_move_iterator(vec->begin()),
                                          std::make_move_iterator(vec->end()));
                    };
                
                accum.forEach(onThreadOutput);

                std::sort(rowsToKeep.begin(), rowsToKeep.end(), SortByRowHash());

                start += rows.size();
                Any newToken;
                if (rows.size() == limit)
                    newToken = start;
                
                return std::move(make_pair(std::move(rowsToKeep),
                                           std::move(newToken)));
            },
            "scan table filtering by where expression"};
}

BasicRowGenerator
Dataset::
queryBasic(const SqlBindingScope & scope,
           const SelectExpression & select,
           const WhenExpression & when,
           const SqlExpression & where,
           const OrderByExpression & orderBy,
           ssize_t offset,
           ssize_t limit,
           bool allowParallel) const
{
    // 1.  Get the rows that match the where clause
    auto rowGenerator = generateRowsWhere(scope, "" /*alias*/ , where, 0 /* offset */, -1 /* limit */);

    // 2.  Find all the variables needed by the orderBy
    // Remove any constants from the order by clauses
    OrderByExpression newOrderBy;
    for (auto & x: orderBy.clauses) {

        // TODO: Better constant detection
        if (x.first->getType() == "constant")
            continue;  

        newOrderBy.clauses.push_back(x);
    }

    SqlExpressionDatasetContext selectScope(*this, "");
    SqlExpressionWhenScope whenScope(selectScope);
    auto boundWhen = when.bind(whenScope);

    auto boundSelect = select.bind(selectScope);
    
    SqlExpressionOrderByContext orderByScope(selectScope);
    
    auto boundOrderBy = newOrderBy.bindAll(orderByScope);

    // Do we select *?  In that case we can avoid a lot of copying
    bool selectStar = select.isIdentitySelect(selectScope);

    // Do we have when TRUE?  In that case we can avoid filtering
    bool whenTrue = when.when->isConstantTrue();

    auto exec = [=] (ssize_t numToGenerate,
                     SqlRowScope & rowScope,
                     const BoundParameters & params)
        {
            // Get a list of rows that we run over
            auto rows = rowGenerator(-1, Any(), params).first;
    
            if (true) {
                //cerr << "order by is " << jsonEncodeStr(orderBy) << endl;

                // Two phases:
                // 1.  Generate rows that match the where expression, in the correct order
                // 2.  Select over those rows to get our result

   
                // For each one, generate the order by key

                typedef std::tuple<std::vector<ExpressionValue>, NamedRowValue, std::vector<ExpressionValue> > SortedRow;
                typedef std::vector<SortedRow> SortedRows;
        
                PerThreadAccumulator<SortedRows> accum;

                std::atomic<int64_t> rowsAdded(0);

                SortedRows rowsSorted;
                
                std::shared_ptr<MatrixView> matrix = this->getMatrixView();
                
                auto doRow = [&] (int rowNum) -> bool
                    {
                        auto row = matrix->getRow(rows[rowNum]);

                        NamedRowValue outputRow;
                        outputRow.rowName = row.rowName;
                        outputRow.rowHash = row.rowName;

                        auto rowScope = selectScope.getRowContext(row, &params);

                        // Filter the tuple using the WHEN expression
                        if (!whenTrue)
                            boundWhen.filterInPlace(row, rowScope);

                        std::vector<ExpressionValue> calcd;
                        std::vector<ExpressionValue> sortFields;

                        if (selectStar) {

                            outputRow.columns.reserve(row.columns.size());
                            for (auto & c: row.columns) {
                                outputRow.columns.emplace_back
                                    (std::get<0>(c),
                                     ExpressionValue(std::move(std::get<1>(c)),
                                                     std::move(std::get<2>(c))));
                            }

                            // We can move things out of the row scope,
                            // since they will be found in the output
                            // row anyway
                            auto orderByRowScope
                                = orderByScope.getRowContext(rowScope,
                                                             outputRow);

                            
                            sortFields = boundOrderBy.apply(orderByRowScope);

                        }
                        else {
                            ExpressionValue selectOutput
                            = boundSelect(rowScope, GET_LATEST);
                            
                            selectOutput.mergeToRowDestructive(outputRow.columns);

                            // Get the order by scope, which can read from both the result
                            // of the select and the underlying row.
                            auto orderByRowScope
                                = orderByScope.getRowContext(rowScope, outputRow);

                            sortFields
                                = boundOrderBy.apply(orderByRowScope);

                            
                        }

                        //cerr << "sort fields for row " << rowNum << " are "
                        //<< jsonEncode(sortFields) << endl;

                        SortedRows & threadAccum = accum.get();
                        ++rowsAdded;
                        threadAccum.emplace_back(std::move(sortFields),
                                                 std::move(outputRow),
                                                 std::move(calcd));
                        return true;
                    };
                
                if (allowParallel)
                    parallelMap(0, rows.size(), doRow);
                else {
                    for (unsigned i = 0;  i < rows.size();  ++i)
                        doRow(i);
                }

                size_t totalRows = 0;
                vector<size_t> startAt;

                auto onThreadRow = [&] (SortedRows * rows)
                    {
                        startAt.push_back(totalRows);
                        totalRows += rows->size();
                
                    };

                accum.forEach(onThreadRow);

                rowsSorted.resize(totalRows);
                std::atomic<int64_t> rowsDone(0);

                auto copyRow = [&] (int i)
                    {
                        rowsDone += accum.threads[i]->size();
                        for (unsigned j = 0;  j < accum.threads[i]->size();  ++j) {
                            ExcAssert(std::get<0>(rowsSorted[j + startAt[i]]).empty());
                            rowsSorted[j + startAt[i]] = std::move((*accum.threads[i])[j]);
                        }
                    };
        
                if (allowParallel)
                    parallelMap(0, accum.numThreads(), copyRow);
                else {
                    for (unsigned i = 0;  i < accum.numThreads();  ++i)
                        copyRow(i);
                }

                ExcAssertEqual(rowsDone, totalRows);

                // Compare two rows according to the sort criteria
                auto compareRows = [&] (const SortedRow & row1,
                                        const SortedRow & row2)
                    {
                        return boundOrderBy.less(std::get<0>(row1), std::get<0>(row2));
                    };

                parallelQuickSortRecursive<SortedRow>(rowsSorted.begin(), rowsSorted.end(), compareRows);

                ssize_t realLimit = -1;
                if (realLimit == -1)
                    realLimit = rowsSorted.size();

                ExcAssertGreaterEqual(offset, 0);

                ssize_t begin = std::min<ssize_t>(offset, rowsSorted.size());
                ssize_t end = std::min<ssize_t>(offset + realLimit, rowsSorted.size());

                //cerr << "begin = " << begin << endl;
                //cerr << "end = " << end << endl;

                vector<NamedRowValue> result;
                result.reserve(end - begin);
                for (unsigned i = begin;  i < end;  ++i) {
                    result.emplace_back(std::move(std::get<1>(rowsSorted[i])));
                }
                return result;
            }
        };

    return { exec };
}

RestRequestMatchResult
Dataset::
handleRequest(RestConnection & connection,
              const RestRequest & request,
              RestRequestParsingContext & context) const
{
    Json::Value error;
    error["error"] = "Dataset of type '" + ML::type_name(*this)
        + "' does not respond to custom route '" + context.remaining + "'";
    error["details"]["verb"] = request.verb;
    error["details"]["resource"] = request.resource;
    connection.sendErrorResponse(400, error);
    return RestRequestRouter::MR_ERROR;
}

std::vector<MatrixNamedRow>
Dataset::
queryString(const Utf8String & query) const
{
    auto stm = SelectStatement::parse(query);
    ExcCheck(!stm.from, "FROM clauses are not allowed on dataset queries");
    ExcAssert(stm.where && stm.having && stm.rowName);

    return queryStructured(
            stm.select,
            stm.when,
            *stm.where,
            stm.orderBy,
            stm.groupBy,
            *stm.having,
            *stm.rowName,
            stm.offset, stm.limit);
}

Json::Value
Dataset::
selectExplainString(const Utf8String & select,
             const Utf8String & where) const
{
    std::vector<std::shared_ptr<SqlRowExpression> > selectParsed
        = SqlRowExpression::parseList(select);
    std::shared_ptr<SqlExpression> whereParsed
        = SqlExpression::parse(where);

    Json::Value explain;
    explain["where"] = Json::Value();
    explain["where"]["str"] = where;
    explain["where"]["parsed"] = whereParsed->print();

    explain["select"] = Json::Value();
    explain["select"]["str"] = select;
    explain["select"]["parsed"] = Json::Value(Json::ValueType::arrayValue);
    for(const auto & p_select : selectParsed)
        explain["select"]["parsed"].append(p_select->print());

    return explain;
}

template<typename T>
std::vector<T> frame(std::vector<T> & vec, ssize_t offset, ssize_t limit)
{
    if (offset < 0)
        throw ML::Exception("Offset can't be negative");
    if (limit < -1)
        throw ML::Exception("Limit can be positive, 0 or -1");

    if (offset > vec.size())
        offset = vec.size();
    
    ssize_t end = offset;
    if (limit == -1)
        end = vec.size();
    else {
        end = offset + limit;
        if (end > vec.size())
            end = vec.size();
    }

    ExcAssert(end >= offset);

    vec.erase(vec.begin() + end, vec.end());
    vec.erase(vec.begin(), vec.begin() + offset);

    return std::move(vec);
}

std::vector<ColumnName>
Dataset::
getColumnNames(ssize_t offset, ssize_t limit) const
{
    auto names = getMatrixView()->getColumnNames();
    return frame(names, offset, limit);
}

void
Dataset::
commit()
{
}

BoundFunction
Dataset::
overrideFunction(const Datacratic::Utf8String&,
                 const Utf8String & functionName,
                 SqlBindingScope & context) const
{
    return BoundFunction();
}

RowName 
Dataset::
getOriginalRowName(const Utf8String& tableName, const RowName & name) const
{
    return name;
}

} // namespace MLDB
} // namespace Datacratic

