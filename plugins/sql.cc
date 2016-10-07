// This file is part of MLDB. Copyright 2015 Datacratic. All rights reserved.

/** sql.cc
    Jeremy Barnes, 24 December 2014
    Copyright (c) 2014 Datacratic Inc.  All rights reserved.

    sqlite plugin.
*/

#include "mldb/server/plugin_collection.h"
#include "mldb/server/dataset_collection.h"
#include "mldb/server/mldb_server.h"
#include "mldb/recoset_sqlite/sqlite3.h"
#include "mldb/recoset_sqlite/sqlite3pp.h"
#include "mldb/arch/timers.h"

using namespace std;



namespace MLDB {


void dumpQuery(sqlite3pp::database & db, const std::string & queryStr)
{
    cerr << "dumping query " << queryStr << endl;

    sqlite3pp::query query(db, queryStr.c_str());

    Json::Value allRecords;

    for (sqlite3pp::query::iterator i = query.begin(); i != query.end(); ++i) {
        Json::Value record;
        for (int j = 0; j < query.column_count(); ++j) {
            const char * v = (*i).get<char const*>(j);
            record[query.column_name(j)] = v ? Json::Value(v) : Json::Value();
        }

        allRecords.append(record);
    }

    cerr << allRecords;
}

Json::Value toJson(sqlite3_value * arg)
{
    switch (sqlite3_value_type(arg)) {
    case SQLITE_INTEGER:
        return sqlite3_value_int64(arg);
    case SQLITE_FLOAT:
        return sqlite3_value_double(arg);
    case SQLITE_NULL:
        return Json::Value();
    case SQLITE_TEXT: {
        int len = sqlite3_value_bytes(arg);
        auto bytes = sqlite3_value_text(arg);
        return string(bytes, bytes + len);
    }
    case SQLITE_BLOB:
        return "<<BLOB>>";
    default:
        throw ML::Exception("Unknown type for column");
    }
}

CellValue toCell(sqlite3_value * arg)
{
    switch (sqlite3_value_type(arg)) {
    case SQLITE_INTEGER:
        return (int64_t)sqlite3_value_int64(arg);
    case SQLITE_FLOAT:
        return sqlite3_value_double(arg);
    case SQLITE_NULL:
        return CellValue();
    case SQLITE_TEXT: {
        int len = sqlite3_value_bytes(arg);
        auto bytes = sqlite3_value_text(arg);
        return string(bytes, bytes + len);
    }
    default:
        throw ML::Exception("Unknown type for column");
    }
}

void dumpQueryArray(sqlite3pp::database & db, const std::string & queryStr)
{
    ML::Timer timer;
    cerr << "dumping query " << queryStr << endl;

    sqlite3pp::query query(db, queryStr.c_str());

    Json::Value allRecords;

    for (int j = 0; j < query.column_count(); ++j) {
        allRecords[0][j] = query.column_name(j);
    }

    for (sqlite3pp::query::iterator i = query.begin(); i != query.end(); ++i) {
        Json::Value record;
        for (int j = 0; j < query.column_count(); ++j) {
            switch ((*i).column_type(j)) {
            case SQLITE_INTEGER:
                record[j] = (*i).get<long long int>(j);
                break;
            case SQLITE_FLOAT:
                record[j] = (*i).get<double>(j);
                break;
            case SQLITE_NULL:
                break;
            case SQLITE_TEXT:
                record[j] = (*i).get<char const*>(j);;
                break;
            case SQLITE_BLOB:
                record[j] = "<<BLOB>>";
                break;
            default:
                throw ML::Exception("Unknown type for column");
            }
        }
        
        allRecords.append(record);
    }
    cerr << "query executed in " << timer.elapsed() << endl;

    cerr << allRecords;
}

void explainQuery(sqlite3pp::database & db, const std::string & queryStr)
{
    dumpQuery(db, "EXPLAIN QUERY PLAN " + queryStr);
}

std::string sqlEscape(const std::string & val)
{
    int numQuotes = 0;
    for (char c: val) {
        if (c == '\'')
            ++numQuotes;
        if (c < ' ' || c >= 127)
            throw ML::Exception("Non ASCII character in DB");
    }
    if (numQuotes == 0)
        return val;

    std::string result;
    result.reserve(val.size() + numQuotes);
    
    for (char c: val) {
        if (c == '\'')
            result += "\'\'";
        else result += c;
    }

    return result;
}

int sqlite3_result_ascii(sqlite3_context* context, const std::string & str)
{
    sqlite3_result_text(context, str.c_str(), str.length(), SQLITE_TRANSIENT);
    return SQLITE_OK;
}

int sqlite3_result_utf8(sqlite3_context* context, const Utf8String & str)
{
    sqlite3_result_text(context, str.rawData(), str.rawLength(), SQLITE_TRANSIENT);
    return SQLITE_OK;
}

namespace {
enum {
    TIMESTAMP_COLUMN = 0,
    ISO_TIMESTAMP_COLUMN = 1,
    ROW_HASH_COLUMN = 2,
    ROW_NAME_COLUMN = 3,
    NUM_FIXED_COLUMNS
};
} // file scope

static __thread MldbServer * currentMldb = nullptr;

struct BehaviourModule {

    struct OurVtab: public sqlite3_vtab {
        OurVtab(MldbServer * mldb)
            : mldb(mldb)
        {
        }

        MldbServer * mldb;
        std::shared_ptr<Dataset> dataset;
        std::vector<ColumnPath> columnNames;
        std::vector<ColumnHash> columnHashes;
    };

    static int Create(sqlite3 * db, void *pAux, int argc, const char * const * argv,
                      sqlite3_vtab ** ppVTab, char ** err)
    {
        try {
            cerr << "creating table" << endl;
            cerr << "argc = " << argc << endl;
            for (unsigned i = 0;  i < argc;  ++i)
                cerr << "  " << argv[i] << endl;

            ExcAssert(currentMldb);
            OurVtab * vtab;
            *ppVTab = vtab = new OurVtab(currentMldb);

            string datasetName = argv[2];

            // Get the dataset

            auto dataset = currentMldb->datasets->getExistingEntity(datasetName);

            vtab->dataset = dataset;

            auto index = dataset->getColumnIndex();

            string columnNameStr;
            vector<ColumnPath> columnNames;
            vector<ColumnHash> columnHashes;

            auto onColumnStats = [&] (ColumnHash ch,
                                      const ColumnPath & columnName,
                                      const ColumnStats & stats)
                {
                    columnNameStr += ", '" + sqlEscape(columnName.toString()) + "'";
                    columnNames.emplace_back(columnName);
                    columnHashes.emplace_back(columnName);
                    return true;
                };

            index->forEachColumnGetStats(onColumnStats);
            
            vtab->columnNames = columnNames;
            vtab->columnHashes = columnHashes;

            string statement = "CREATE TABLE " + sqlEscape(datasetName) + " ('Timestamp', 'TimestampIso8601', 'RowHash', 'RowName'" + columnNameStr + ")";

            cerr << "running statement " << statement << endl;

            int res = sqlite3_declare_vtab(db, statement.c_str());

            cerr << "res returned " << res << endl;

            cerr << "error " << sqlite3_errmsg(db) << endl;

            return res;
        } catch (const std::exception & exc) {
            *err = (char *)sqlite3_malloc(strlen(exc.what()) + 1);
            strcpy(*err, exc.what());
            return SQLITE_ERROR;
        } JML_CATCH_ALL {
            return SQLITE_ERROR;
        }
    }

    static int Connect(sqlite3 * db, void *pAux, int argc, const char * const * argv,
                       sqlite3_vtab ** ppVTab, char **)
    {
        cerr << "connecting to table" << endl;
        return SQLITE_OK;
    }

    static int BestIndex(sqlite3_vtab *pVTab, sqlite3_index_info* info)
    {
        OurVtab * vtab = static_cast<OurVtab *>(pVTab);

        cerr << "BestIndex" << endl;
        cerr << "info.nConstraint = " << info->nConstraint << endl;
        cerr << "info.nOrderBy = " << info->nOrderBy << endl;
        cerr << "info.aConstraintUsage = " << info->aConstraintUsage << endl;

        Json::Value indexPlan;

        for (unsigned i = 0;  i < info->nConstraint;  ++i) {
            auto & constraint = info->aConstraint[i];
            cerr << "constraint " << i
                 << " column " << info->aConstraint[i].iColumn
                 << " op " << (int)info->aConstraint[i].op
                 << " usable " << (int)info->aConstraint[i].usable
                 << endl;

            if (!constraint.usable)
                continue;

            int col = -1;
            if (constraint.iColumn >= NUM_FIXED_COLUMNS) {
                col = constraint.iColumn - NUM_FIXED_COLUMNS;
                cerr << "column is " << vtab->columnNames[col] << endl;
            }

            if (col == -1)
                continue;

            string constraintStr;

            switch (constraint.op) {
            case SQLITE_INDEX_CONSTRAINT_EQ:
                constraintStr = "==";  break;
            case SQLITE_INDEX_CONSTRAINT_GT:
                constraintStr = ">";  break;
            case SQLITE_INDEX_CONSTRAINT_LE:
                constraintStr = "<=";  break;
            case SQLITE_INDEX_CONSTRAINT_LT:
                constraintStr = "<";  break;
            case SQLITE_INDEX_CONSTRAINT_GE:
                constraintStr = ">=";  break;
            case SQLITE_INDEX_CONSTRAINT_MATCH:
                constraintStr = "MATCHES";  break;
            default:
                throw ML::Exception("unknown constraint type");
            }

            cerr << "constraintStr = " << constraintStr << endl;

            indexPlan[i]["cIdx"] = col;
            indexPlan[i]["cName"] = vtab->columnNames[col].toString();
            indexPlan[i]["op"] = constraintStr;
            indexPlan[i]["arg"] = i;

            // We can do this!
            
            //constraintsOut += " " + vtab->columnNames[col].toString() + "," + constraintStr;

            info->aConstraintUsage[i].argvIndex = i + 1;

            // IN clauses can't be optimized without this
            info->aConstraintUsage[i].omit = 1;
        }

        info->idxStr = sqlite3_mprintf("%s", indexPlan.toStringNoNewLine().c_str());
        info->needToFreeIdxStr = true;
        info->estimatedCost = indexPlan.isNull() ? 10000000 : 0;  // TODO: something sensible

        cerr << "bestIndex: plan " << indexPlan.toStringNoNewLine() << " cost "
             << info->estimatedCost << endl;

        return SQLITE_OK;
    }

    static int Disconnect(sqlite3_vtab *pVTab)
    {
        delete static_cast<OurVtab *>(pVTab);
        return SQLITE_OK;
    }

    static int Destroy(sqlite3_vtab *pVTab)
    {
        return Disconnect(pVTab);
    }

    static void Free(void *)
    {
        cerr << "destroying" << endl;
    }

    struct Cursor: public sqlite3_vtab_cursor {
        OurVtab * vtab;
        std::unique_ptr<MatrixEventIterator> it;
        std::map<ColumnHash, CellValue> knownValues;
    };

    static int Open(sqlite3_vtab *pVTab, sqlite3_vtab_cursor **ppCursor)
    {
        cerr << "open" << endl;
        OurVtab * vtab = static_cast<OurVtab *>(pVTab);
        Cursor * cursor;
        *ppCursor = cursor = new Cursor();

        cursor->it = vtab->dataset->getMatrixView()->allEvents();
        cursor->vtab = vtab;
        
        return SQLITE_OK;
    }

    static int Close(sqlite3_vtab_cursor* vcursor)
    {
        Cursor * cursor = static_cast<Cursor *>(vcursor);
        delete static_cast<Cursor *>(cursor);
        return SQLITE_OK;
    }

    static int Filter(sqlite3_vtab_cursor* vcursor,
                      int idxNum,
                      const char *idxStr,
                      int argc, sqlite3_value **argv)
    {
        Cursor * cursor = static_cast<Cursor *>(vcursor);
        OurVtab * vtab = cursor->vtab;
        cerr << "filter" << endl;
        cerr << "idxNum = " << idxNum << endl;
        cerr << "idxStr = " << idxStr << endl;
        cerr << "argc = " << argc << endl;

        for (unsigned i = 0;  i < argc;  ++i) {
            cerr << "arg " << argc << " = " << toJson(argv[i]);
        }

        Json::Value plan = Json::parse(idxStr);

        if (plan.isNull()) {
            cursor->it = vtab->dataset->getMatrixView()->allEvents();
            return SQLITE_OK;
        }

        vector<pair<RowHash, Date> > events;

        std::map<ColumnHash, CellValue> knownValues;

        for (unsigned i = 0;  i < plan.size();  ++i) {
            const Json::Value & planVal = plan[i];
            //int colIdx = planVal["cIdx"].asInt();
            ColumnPath colName(planVal["cName"].asString());
            int argNum = planVal["arg"].asInt();
            string op = planVal["op"].asString();
            CellValue arg = toCell(argv[argNum]);
            knownValues[colName] = arg;

            if (op != "==")
                throw ML::Exception("non-equality not implemented");

            cerr << "Filtering on " << colName << " " << op << " " << arg
                 << endl;
            
            auto clauseEvents = vtab->dataset->getColumnIndex()->rowsWithColumnValue(colName, arg);
            std::sort(clauseEvents.begin(), clauseEvents.end());

            if (i == 0)
                events = std::move(clauseEvents);
            else {
                vector<pair<RowHash, Date> > intersectedEvents;
                std::set_intersection(clauseEvents.begin(), clauseEvents.end(),
                                      events.begin(), events.end(),
                                      back_inserter(intersectedEvents));
                events = std::move(intersectedEvents);
            }

            cerr << events.size() << " events matching after clause " << i << endl;
        }

        cursor->it = vtab->dataset->getMatrixView()->events(events);
        cursor->knownValues = knownValues;

        return SQLITE_OK;
    }

    static int Next(sqlite3_vtab_cursor* vcursor)
    {
        Cursor * cursor = static_cast<Cursor *>(vcursor);
        //cerr << "next" << endl;
        cursor->it->next();
        return SQLITE_OK;
    }

    static int Eof(sqlite3_vtab_cursor* vcursor)
    {
        //cerr << "eof" << endl;
        Cursor * cursor = static_cast<Cursor *>(vcursor);
        return cursor->it->eof();
    }

    static int Column(sqlite3_vtab_cursor* vcursor,
                      sqlite3_context* context,
                      int colNum)
    {
        Cursor * cursor = static_cast<Cursor *>(vcursor);

        //cerr << "column " << colNum << endl;
        //cerr << "cursor->vtab->columnHashes.size() = "
        //     << cursor->vtab->columnHashes.size() << endl;

        switch (colNum) {
        case TIMESTAMP_COLUMN: 
            // timestamp column
            sqlite3_result_double(context, cursor->it->timestamp().secondsSinceEpoch());
            return SQLITE_OK;

        case ISO_TIMESTAMP_COLUMN:
            // timestamp utf column
            return sqlite3_result_ascii(context, cursor->it->timestamp().printIso8601());
        
        case ROW_HASH_COLUMN:
            // row ID column
            return sqlite3_result_ascii(context, cursor->it->rowName().toString());

        case ROW_NAME_COLUMN:
            // row ID column
            return sqlite3_result_ascii(context, cursor->it->rowHash().toString());

        default: {
            ColumnHash column = cursor->vtab->columnHashes[colNum - NUM_FIXED_COLUMNS];

            CellValue value;

            // If this is part of a constraint, then we know it without
            // needing to load the row
            auto it = cursor->knownValues.find(column);
            if (it != cursor->knownValues.end()) {
                value = it->second;
            }
            else {
                value = cursor->it->column(column);
            }

            switch (value.cellType()) {
            case CellValue::EMPTY:
                sqlite3_result_null(context);
                return SQLITE_OK;
            case CellValue::FLOAT:
                sqlite3_result_double(context, value.toDouble());
                return SQLITE_OK;
            case CellValue::INTEGER:
                sqlite3_result_int64(context, value.toInt());
                return SQLITE_OK;
            case CellValue::STRING:
                return sqlite3_result_ascii(context, value.toString());
            case CellValue::UTF8_STRING:
                return sqlite3_result_result_utf8(context, value.toUtf8String());
            default:
                throw ML::Exception("unknown cell type");
            }
        }
        }
                            
        return SQLITE_ERROR;
    }

    static int Rowid(sqlite3_vtab_cursor* vcursor, sqlite3_int64 *pRowid)
    {
        cerr << "rowid" << endl;
        return SQLITE_ERROR;
    }

    static int Update(sqlite3_vtab *, int, sqlite3_value **, sqlite3_int64 *)
    {
        cerr << "update" << endl;
        return SQLITE_ERROR;
    }

    static void implementFunc(sqlite3_context * context,
                              int arity,
                              sqlite3_value ** vals)
    {
        cerr << "implementing function with arity " << arity << endl;
        
    }

    static int FindFunction(sqlite3_vtab *pVtab, int nArg, const char *zName,
                            void (**pxFunc)(sqlite3_context*,int,sqlite3_value**),
                            void **ppArg)
    {
        cerr << "FindFunction " << zName << " arity " << nArg << endl;
        *pxFunc = implementFunc;
        return SQLITE_OK;
    }
};


struct SqlitePlugin: public Plugin {

    SqlitePlugin(ServicePeer * server,
                 PolyConfig config,
                 std::function<bool (const Json::Value & progress)> onProgress)
        : Plugin(static_cast<MldbServer *>(server)),
          db(":memory:")
    {
        init();

        watchDatasets = this->server->datasets->watchElements("*", true, string("Sqlite Datasets Watch"));
        watchDatasets.bind(std::bind(&SqlitePlugin::onNewDataset, this, std::placeholders::_1));
    }

    sqlite3pp::database db;
    sqlite3_module module;

    void init()
    {
        module = {
            1, // version
            &BehaviourModule::Create,
            &BehaviourModule::Connect,
            &BehaviourModule::BestIndex,
            &BehaviourModule::Disconnect,
            &BehaviourModule::Destroy,
            &BehaviourModule::Open,
            &BehaviourModule::Close,
            &BehaviourModule::Filter,
            &BehaviourModule::Next,
            &BehaviourModule::Eof,
            &BehaviourModule::Column,
            &BehaviourModule::Rowid,
            nullptr /* update */,
            nullptr /* begin */,
            nullptr /* sync */,
            nullptr /* commit */,
            nullptr /* rollback */,
            &BehaviourModule::FindFunction,
            nullptr /* rename */
        };

        int res = sqlite3_create_module_v2(db, "mldb", &module, nullptr,
                                           &BehaviourModule::Free);
        if (res != SQLITE_OK) {
            throw sqlite3pp::database_error(db);
        }
    }
    
    WatchT<DatasetCollection::ChildEvent> watchDatasets;

    void onNewDataset(const DatasetCollection::ChildEvent & dataset)
    {
        cerr << "SQL got new dataset " << dataset.key << endl;

        currentMldb = server;
        sqlite3pp::command command(db, ("CREATE VIRTUAL TABLE " + sqlEscape(dataset.key) + " USING mldb()").c_str());
        command.execute();
        currentMldb = nullptr;  // TODO: proper guard

        string query = "SELECT * FROM " + dataset.key + " WHERE aircrafttype IN ('RV6', 'RV4') ORDER BY Timestamp LIMIT 10";
        explainQuery(db, query);
        dumpQueryArray(db, query);

        //query = "SELECT status, COUNT(*) FROM " + dataset.key + " WHERE aircrafttype = 'A320' GROUP BY status";
        //query = "SELECT aircrafttype, status, COUNT(*) FROM " + dataset.key + " WHERE status = 'Approach' OR status = 'Landing' GROUP BY aircrafttype, status ORDER BY count(*) DESC";
        //query = "SELECT aircrafttype, COUNT(*) FROM " + dataset.key + " GROUP BY aircrafttype ORDER BY count(*) DESC";
        
        //query = "SELECT DISTINCT aircrafttype FROM " + dataset.key;

        //explainQuery(db, query);

        //for (unsigned i = 0;  i < 20;  ++i)
        //    dumpQueryArray(db, query);

    }

    Any getStatus() const
    {
        return Any();
    }

    RestRequestRouter router;

    virtual RestRequestMatchResult
    handleRequest(RestConnection & connection,
                  const RestRequest & request,
                  RestRequestParsingContext & context) const
    {
        cerr << "SqlitePlugin handling request " << request << endl;
        return router.processRequest(connection, request, context);
    }
};

namespace {

PluginCollection::RegisterType<SqlitePlugin> regSqlite("sql");

} // file scope


} // namespace MLDB

