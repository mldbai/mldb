// This file is part of MLDB. Copyright 2015 mldb.ai inc. All rights reserved.

/** dataset_collection.cc
    Jeremy Barnes, 24 November 2014
    Copyright (c) 2014 mldb.ai inc.  All rights reserved.

    Collection of datasets.

*/
#include "mldb/server/dataset_collection.h"
#include "mldb/rest/poly_collection_impl.h"
#include "mldb/server/mldb_server.h"
#include "mldb/jml/utils/string_functions.h"
#include "mldb/rest/rest_request_binding.h"
#include "mldb/jml/utils/lightweight_hash.h"
#include "mldb/sql/sql_expression.h"
#include "mldb/types/map_description.h"
#include "mldb/types/vector_description.h"
#include "mldb/types/pointer_description.h"
#include "mldb/types/tuple_description.h"

using namespace std;



namespace MLDB {

std::shared_ptr<DatasetCollection>
createDatasetCollection(MldbServer * server, RestRouteManager & routeManager)
{
    return createCollection<DatasetCollection>(2, L"dataset", L"datasets",
                                               server, routeManager);
}

std::shared_ptr<Dataset>
obtainDataset(MldbServer * server,
              const PolyConfig & config,
              const MldbServer::OnProgress & onProgress)
{
    return server->datasets->obtainEntitySync(config, onProgress);
}

std::shared_ptr<Dataset>
createDataset(MldbServer * server,
              const PolyConfig & config,
              const std::function<bool (const Json::Value & progress)> & onProgress,
              bool overwrite)
{
    return server->datasets->createEntitySync(config, onProgress, overwrite);
}

std::shared_ptr<DatasetType>
registerDatasetType(const Package & package,
                    const Utf8String & name,
                    const Utf8String & description,
                    std::function<Dataset * (RestDirectory *,
                                             PolyConfig,
                                             const std::function<bool (const Json::Value)> &)>
                    createEntity,
                    TypeCustomRouteHandler docRoute,
                    TypeCustomRouteHandler customRoute,
                    std::shared_ptr<const ValueDescription> config,
                    std::set<std::string> registryFlags)
{
    return DatasetCollection::registerType(package, name, description, createEntity,
                                           docRoute, customRoute, config, registryFlags);
}

void runHttpQuery(std::function<std::vector<MatrixNamedRow> ()> runQuery,
                  RestConnection & connection,
                  const std::string & format,
                  bool createHeaders,
                  bool rowNames,
                  bool rowHashes,
                  bool sortColumns)
{
    std::vector<MatrixNamedRow> sparseOutput = runQuery();

    if (sortColumns) {
        for (auto & r: sparseOutput) {
            std::sort(r.columns.begin(), r.columns.end());
        }
    }

    if (format == "full" || format == "") {
        connection.sendResponse(200, jsonEncodeStr(sparseOutput),
                                "application/json");
    }
    else if (format == "sparse") {
        std::vector<std::vector<std::pair<ColumnPath, CellValue> > > output;
        output.reserve(sparseOutput.size());

        for (auto & row: sparseOutput) {

            std::vector<std::pair<ColumnPath, CellValue> > rowOut;
            rowOut.reserve(row.columns.size() + rowNames + rowHashes);

            if (rowNames)
                rowOut.emplace_back(ColumnPath("_rowName"), row.rowName.toUtf8String());
            if (rowHashes)
                rowOut.emplace_back(ColumnPath("_rowHash"), row.rowHash.toString());

            for (auto & c: row.columns) {
                rowOut.emplace_back(std::get<0>(c), std::get<1>(c));
            }

            std::sort(rowOut.begin() + rowNames + rowHashes, rowOut.end());

            output.emplace_back(std::move(rowOut));
        }

        connection.sendResponse(200, jsonEncodeStr(output),
                                "application/json");
    }
    else if (format == "soa") {
        // Structure of arrays; one array per column
        std::map<ColumnPath, std::vector<CellValue> > output;
        for (unsigned i = 0;  i < sparseOutput.size();  ++i) {
            if (rowNames)
                output[ColumnPath("_rowName")]
                    .push_back(sparseOutput[i].rowName.toUtf8String());
            if (rowHashes)
                output[ColumnPath("_rowHash")]
                    .push_back(sparseOutput[i].rowHash.toString());

            for (auto & c: sparseOutput[i].columns) {
                const ColumnPath & col = std::get<0>(c);
                const CellValue & val = std::get<1>(c);

                std::vector<CellValue> & vals = output[col];
                if (vals.empty())
                    vals.resize(sparseOutput.size());
                vals[i] = val;
            }
        }
        connection.sendResponse(200, jsonEncodeStr(output),
                                "application/json");
    }
    else if (format == "aos") {
        // Array of structures; one structure per row
        std::vector<std::map<ColumnPath, CellValue> > output;
        for (unsigned i = 0;  i < sparseOutput.size();  ++i) {

            std::map<ColumnPath, CellValue> row;

            if (rowNames)
                row[ColumnPath("_rowName")] = sparseOutput[i].rowName.toUtf8String();
            if (rowHashes)
                row[ColumnPath("_rowHash")] = sparseOutput[i].rowHash.toString();

            for (auto & c: sparseOutput[i].columns) {
                const ColumnPath & col = std::get<0>(c);
                const CellValue & val = std::get<1>(c);
                row[col] = val;
            }

            output.emplace_back(std::move(row));
        }
        connection.sendResponse(200, jsonEncodeStr(output),
                                "application/json");
    }
    else if (format == "table") {
        // TODO: the SQL knows what columns could be created... this could
        // be greatly optimized.

        // First, find all columns
        std::vector<ColumnPath> columns;
        Lightweight_Hash<ColumnHash, int> columnIndex;
        for (auto & o: sparseOutput) {
            for (auto & c: o.columns) {
                auto & columnName = std::get<0>(c);
                if (columnIndex.insert({columnName, columns.size()}).second) {
                    columns.push_back(columnName);
                }
            }
        }

        if (sortColumns) {
            std::sort(columns.begin(), columns.end());
            for (size_t i = 0;  i < columns.size();  ++i) {
                columnIndex[columns[i]] = i;
            }
        }

        // Now, send them back
        std::vector<std::vector<CellValue> > output;
        output.reserve(sparseOutput.size() + createHeaders);

        if (createHeaders) {

            std::vector<CellValue> headers;
            if (rowNames)
                headers.push_back("_rowName");
            if (rowHashes)
                headers.push_back("_rowHash");

            for (auto & c: columns) {
                headers.push_back(c.toUtf8String());
            }
            output.push_back(headers);
        }

        for (auto & row: sparseOutput) {
            std::vector<CellValue> rowOut(columns.size() + rowNames + rowHashes);
            if (rowNames)
                rowOut[0] = row.rowName.toUtf8String();
            if (rowHashes)
                rowOut[rowNames] = row.rowHash.toString();

            for (auto & c: row.columns) {
                const ColumnPath & columnName = std::get<0>(c);
                CellValue cellValue = std::get<1>(c);

                //Special output options for table format
                if (cellValue.isTimestamp())
                {
                    //in table format print dates as epoch
                    cellValue = cellValue.coerceToString();
                }
                else if (cellValue.isTimeinterval())
                {
                    //in table format print time intervals as string
                    cellValue = cellValue.coerceToString();
                }
                else if (cellValue.isDouble())
                {
                    //in table format print 'special' floats as string
                    double value = cellValue.toDouble();
                    if (std::isnan(value))
                    {
                        std::string stringVal = std::signbit(value) ? "-NaN" : "NaN";
                        cellValue = CellValue(stringVal);
                    }
                    else if (std::isinf(value))
                    {
                        std::string stringVal = std::signbit(value) ? "-Inf" : "Inf";
                        cellValue = CellValue(stringVal);
                    }
                }
                else if (cellValue.isPath()) {
                    cellValue = CellValue(cellValue.coerceToPath().toUtf8String());
                }

                rowOut[columnIndex[columnName] + rowHashes + rowNames] = std::move(cellValue);
            }

            output.push_back(rowOut);
        }

        connection.sendResponse(200, jsonEncodeStr(output),
                                "application/json");
    }
    else if (format == "atom") {
        if (sparseOutput.size() > 1) {
            connection.sendErrorResponse(400, "Query with atom format returning multiple rows. Consider using limit.");
            return;
        }

        if (sparseOutput.size() == 0) {
            connection.sendErrorResponse(400, "Query with atom format returned no rows.");
            return;
        }

        const auto& columns = sparseOutput[0].columns;

        if (columns.size() == 0) {
            connection.sendErrorResponse(400, "Query with atom format returned no columns.");
            return;
        }

        if (columns.size() > 1) {
            connection.sendErrorResponse(400, "Query with atom format returned multiple columns.");
            return;
        }

        const auto& val = std::get<1>(columns[0]);

        connection.sendResponse(200, jsonEncodeStr(val),
                                "application/json"); 
    }
    else {
        connection.sendErrorResponse(400, "Unknown output format '" + format + "'");
    }
}


/*****************************************************************************/
/* DATASET COLLECTION                                                         */
/*****************************************************************************/

DatasetCollection::
DatasetCollection(MldbServer * server)
    : PolyCollection<Dataset>(L"dataset", L"datasets", server)
{
}

Any
DatasetCollection::
getEntityStatus(const Dataset & dataset) const
{
    return dataset.getStatus();
}

template<typename T>
T getParam(const RestRequest & req, const std::string & name, T defValue)
{
    for (auto & p: req.params) {
        if (p.first == name) {
            return boost::lexical_cast<T>(p.second);
        }
    }

    return defValue;
}

void
DatasetCollection::
initRoutes(RouteManager & manager)
{
    PolyCollection<Dataset>::initRoutes(manager);

    std::function<Dataset * (const RestRequestParsingContext & cxt) > getDataset
        = [] (const RestRequestParsingContext & cxt)
        {
            return static_cast<Dataset *>(cxt.getSharedPtrAs<PolyEntity>(2).get());
        };

    addRouteSync(*manager.valueNode, "/commit", { "POST" },
                 "Commit dataset",
                 &Dataset::commit,
                 getDataset);

    addRouteSyncJsonReturn(*manager.valueNode, "/timestampRange", { "GET" },
                           "Return timestamp range for dataset",
                           "[ earliest, latest ] timestamp",
                           &Dataset::getTimestampRange,
                           getDataset);

    //auto & matrix
    //    = manager.valueNode->addSubRouter("/matrix", "Operations on dataset as matrix");


    /************************
     *      /rows
     * ***************/

    auto & rows
        = manager.valueNode->addSubRouter("/rows", "Operations on matrix rows");

    RestRequestRouter::OnProcessRequest getRows
        = [=] (RestConnection & connection,
               const RestRequest & req,
               const RestRequestParsingContext & cxt)
        {
            try {
                //auto collection = manager.getCollection(cxt);
                //auto key = manager.getKey(cxt);

                MLDB_TRACE_EXCEPTIONS(false);

                auto dataset = std::static_pointer_cast<Dataset>
                    (cxt.getSharedPtrAs<PolyEntity>(2));

                int start = getParam(req, "start", 0);
                int limit = getParam(req, "limit", -1);

                // getRowPaths can return row names in an arbitrary order as long as it is deterministic.
                auto result = dataset->getMatrixView()->getRowPaths(start, limit);

                connection.sendHttpResponse(200, jsonEncodeStr(result),
                                            "application/json", {});

                return RestRequestRouter::MR_YES;
            } catch (const std::exception & exc) {
                //return sendExceptionResponse(connection, exc);
                connection.sendResponse(400, exc.what(), "text/plain");
                return RestRequestRouter::MR_YES;
            }
        };


    Json::Value help;
    rows.addRoute("", { "GET" },
                  "Get a list of the row names",
                  getRows, help);

    addRouteSync(rows, "", { "POST" },
                 "Record the given row into the dataset",
                 &Dataset::recordRow,
                 getDataset,
                 JsonParam<RowPath>("rowName", "Name of the row to record"),
                 JsonParam<std::vector<std::tuple<ColumnPath, CellValue, Date> > >
                 ("columns", "[ column name, value, date ] tuples to record"));

    addRouteSync(*manager.valueNode, "/multirows", { "POST" },
                 "Record many rows into the dataset",
                 &Dataset::recordRows,
                 getDataset,
                 JsonParam<std::vector<std::pair<RowPath, std::vector<std::tuple<ColumnPath, CellValue, Date> > > > >
                 ("", "[ [ row name, [ [ column name, value, timestamp ], ... ] ], ...] tuples to record"));

    auto & row MLDB_UNUSED
        = rows.addSubRouter(Rx("/([0-9a-z]{16})", "/<rowHash>"),
                            "operations on an individual row");


    auto & columns
        = manager.valueNode->addSubRouter("/columns", "Operations on matrix columns");

    addRouteSync(columns, "", { "POST" },
                 "Record the given column into the dataset",
                 &Dataset::recordColumn,
                 getDataset,
                 JsonParam<ColumnPath>("columnName", "Name of the column to record"),
                 JsonParam<std::vector<std::tuple<RowPath, CellValue, Date> > >
                 ("rows", "[ row name, value, date ] tuples to record"));

    addRouteSync(*manager.valueNode, "/multicolumns", { "POST" },
                 "Record many columns into the dataset",
                 &Dataset::recordColumns,
                 getDataset,
                 JsonParam<std::vector<std::pair<ColumnPath, std::vector<std::tuple<RowPath, CellValue, Date> > > > >
                 ("", "[ [ col name, [ [ row name, value, timestamp ], ... ] ], ...] tuples to record"));


    addRouteSyncJsonReturn(columns, "", { "GET" },
                           "Get a list of column names in the dataset",
                           "List of column names",
                           &Dataset::getColumnPaths,
                           getDataset,
#if 0
                           RestParamDefault<std::string>("regex",
                                                         "Regex of columns to select",
                                                         ""),
#endif
                           RestParamDefault<ssize_t>("offset",
                                                     "Where to start selecting",
                                                     0),
                           RestParamDefault<ssize_t>("limit",
                                                     "Maximum number to return",
                                                     -1));

    //auto & column MLDB_UNUSED
    //    = columns.addSubRouter(Rx("/([0-9a-z]{16})", "/<columnHash>"),
    //                           "operations on an individual column");

    auto & column MLDB_UNUSED
        = columns.addSubRouter(Rx("/([^/]*)", "/<columnName>"),
                               "operations on an individual column");

    RequestParam<ColumnPath> columnParam(6, "<columnName>",
                                         "Column to operate on");

    RestRequestRouter::OnProcessRequest getColumnValues
        = [=] (RestConnection & connection,
               const RestRequest & req,
               const RestRequestParsingContext & cxt)
        {
            try {
                //auto collection = manager.getCollection(cxt);
                //auto key = manager.getKey(cxt);

                MLDB_TRACE_EXCEPTIONS(false);

                //cerr << "cxt.resources = " << cxt.resources << endl;
                auto dataset = std::static_pointer_cast<Dataset>
                    (cxt.getSharedPtrAs<PolyEntity>(2));

                auto index = dataset->getColumnIndex();

                ColumnStats columnStatsStorage;
                auto stats = index->getColumnStats(ColumnPath(cxt.resources.at(6)),
                                                   columnStatsStorage);

                vector<CellValue> result;
                for (auto & v: stats.values)
                    result.emplace_back(v.first);

                connection.sendHttpResponse(200, jsonEncodeStr(result),
                                            "application/json", {});

                return RestRequestRouter::MR_YES;
            } catch (const std::exception & exc) {
                //return sendExceptionResponse(connection, exc);
                connection.sendResponse(400, exc.what(), "text/plain");
                return RestRequestRouter::MR_YES;
            }
        };

    column.addRoute("/values", { "GET" },
                    "Get a list of the values of the column",
                    getColumnValues, help);

    addRouteSyncJsonReturn(column, "/valuecounts", { "GET" },
                           "Get a list of values and row counts in the dataset for a column",
                           "List of column names with counts",
                           &DatasetCollection::getColumnValueCounts,
                           manager.getCollection,
                           getDataset,
                           columnParam,
                           RestParamDefault<ssize_t>("offset",
                                                     "Where to start selecting",
                                                     0),
                           RestParamDefault<ssize_t>("limit",
                                                     "Maximum number to return",
                                                     -1));

    // Make the plugin handle a route
    RestRequestRouter::OnProcessRequest handlePluginRoute
        = [=] (RestConnection & connection,
               const RestRequest & req,
               RestRequestParsingContext & cxt)
        {
            Dataset * dataset = getDataset(cxt);
            auto key = manager.getKey(cxt);

            try {
                return dataset->handleRequest(connection, req, cxt);
            }
            catch (const HttpReturnException & exc) {
                return sendExceptionResponse(connection, exc);
            } catch (const std::exception & exc) {
                return sendExceptionResponse(connection, exc);
            } MLDB_CATCH_ALL {
                connection.sendErrorResponse(400, "Unknown exception was thrown");
                return RestRequestRouter::MR_ERROR;
            }
        };

    RestRequestRouter & subRouter
        = manager.valueNode->addSubRouter("/routes", "Dataset type-specific routes");

    subRouter.rootHandler = handlePluginRoute;

}

std::vector<std::pair<CellValue, int64_t> >
DatasetCollection::
getColumnValueCounts(const Dataset * dataset,
                     const ColumnPath & columnName,
                     ssize_t offset,
                     ssize_t limit) const
{
    auto index = dataset->getColumnIndex();

    ColumnStats columnStatsStorage;
    auto stats = index->getColumnStats(columnName, columnStatsStorage);

    vector<pair<CellValue, int64_t> > result;
    for (auto & v: stats.values)
        result.emplace_back(v.first, v.second.rowCount());

    return result;
}

void
DatasetCollection::
queryStructured(const Dataset * dataset,
                RestConnection & connection,
                const std::string & format,
                const Utf8String & select,
                const Utf8String & when,
                const Utf8String & where,
                const Utf8String & orderBy,
                const Utf8String & groupBy,
                const Utf8String & having,
                const Utf8String & rowName,
                ssize_t offset,
                ssize_t limit,
                bool createHeaders,
                bool rowNames,
                bool rowHashes,
                bool sortColumns) const
{
    std::vector<std::shared_ptr<SqlRowExpression> > selectParsed
        = SqlRowExpression::parseList(select);
    WhenExpression whenParsed
        = WhenExpression::parse(when);
    std::shared_ptr<SqlExpression> whereParsed
        = SqlExpression::parse(where);
    OrderByExpression orderByParsed
        = OrderByExpression::parse(orderBy);
    TupleExpression groupByParsed
        = TupleExpression::parse(groupBy);
    std::shared_ptr<SqlExpression> havingParsed
        = SqlExpression::TRUE;
    if (!having.empty())
        havingParsed = SqlExpression::parse(having);
    std::shared_ptr<SqlExpression> rowNameParsed;
    if (!rowName.empty())
        rowNameParsed = SqlExpression::parse(rowName);
    else rowNameParsed = SqlExpression::parse("rowPath()");

    //cerr << "limit = " << limit << endl;
    //cerr << "offset = " << offset << endl;

    auto runQuery = [&] ()
        {
            return dataset->queryStructured
                (selectParsed, whenParsed, *whereParsed, orderByParsed,
                 groupByParsed,havingParsed, rowNameParsed, offset, limit);
        };

    runHttpQuery(runQuery, connection, format, createHeaders,rowNames, rowHashes, sortColumns);
}

template class PolyCollection<Dataset>;

} // namespace MLDB


