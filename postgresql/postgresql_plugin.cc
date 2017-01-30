/* postgresql_plugin.cc
   Mathieu Marquis Bolduc, 16 July 2016
   Copyright (c) 2016 mldb.ai inc.  All rights reserved.
*/

#include "mldb/core/plugin.h"
#include "mldb/core/procedure.h"
#include "mldb/core/function.h"
#include "mldb/core/dataset.h"
#include "mldb/soa/credentials/credentials.h"
#include "mldb/types/structure_description.h"
#include "mldb/types/any_impl.h"

#include <postgresql/libpq-fe.h>

#include <unordered_set>

using namespace std;

MLDB::Plugin *
mldbPluginEnterV100(MLDB::MldbServer * server)
{
    return nullptr;
}


namespace MLDB {

const Package & postgresqlPackage()
{
    static const Package result("postgresql");
    return result;
}

const int postgresqlDefaultPort = 5432;

//#define POSTGRESQL_VERBOSE(x) x
#define POSTGRESQL_VERBOSE(x)

/*****************************************************************************/
/* POSTGRESQL UTILS                                                          */
/*****************************************************************************/
namespace {
CellValue getCellValueFromPostgres(const PGresult *res, int i, int j)
{
    int postgrestype = PQftype(res, j);
    if (postgrestype == 23) {
        return CellValue(atoi(PQgetvalue(res, i, j)));
    }
    else if (postgrestype == 701) {
        return CellValue(atof(PQgetvalue(res, i, j)));
    }
    else {
        return CellValue(PQgetvalue(res, i, j));
    }
}

pg_conn* startPostgresqlConnection(const string& databaseName, const string& host, int port)
{
    auto creds = getCredential("postgresql", host);

    string connString;
    connString += "dbname=" + databaseName
               + " host=" + host
               + " port=" + std::to_string(port)
               + " user=" + creds.id
               + " password=" + creds.secret;

    //POSTGRESQL_VERBOSE(cerr << connString << endl;) //careful about logging the username and password

    auto conn = PQconnectdb(connString.c_str());
    if (PQstatus(conn) != CONNECTION_OK)
    {
        string errorMsg(PQerrorMessage(conn));
        PQfinish(conn);
        throw HttpReturnException(400, "Could not connect to Postgresql database: ", errorMsg);
    }

    POSTGRESQL_VERBOSE(cerr << "connection successful!" << endl;)
    return conn;
}

}

/*****************************************************************************/
/* POSTGRESQL DATASET                                                        */
/*****************************************************************************/

struct PostgresqlDatasetConfig {
    string databaseName;
    int port;
    string host;
    string tableName;
    string primaryKey;

    PostgresqlDatasetConfig() {
        databaseName = "database";
        port = postgresqlDefaultPort;
        host = "localhost";
        tableName = "mytable";
        primaryKey = "key";
    }
};

DECLARE_STRUCTURE_DESCRIPTION(PostgresqlDatasetConfig);
DEFINE_STRUCTURE_DESCRIPTION(PostgresqlDatasetConfig);

PostgresqlDatasetConfigDescription::
PostgresqlDatasetConfigDescription()
{
    addField("databaseName", &PostgresqlDatasetConfig::databaseName, "Name of PostgreSQL the database to connect to.");
    addField("port", &PostgresqlDatasetConfig::port, "Port of the PostgreSQL database to connect to.", postgresqlDefaultPort);
    addField("host", &PostgresqlDatasetConfig::host, "Address of the PostgreSQL database to connect to.");
    addField("tableName", &PostgresqlDatasetConfig::tableName, "Name of the PostgreSQL table to be recorded into.");
    addField("primaryKey", &PostgresqlDatasetConfig::primaryKey, "Primary key used to access PostgreSQL table.");
}

struct PostgresqlDataset: public Dataset {

    PostgresqlDatasetConfig config_;

    PostgresqlDataset(MldbServer * owner,
                 PolyConfig config,
                 const ProgressFunc & onProgress)
        : Dataset(owner)
    {
        config_ = config.params.convert<PostgresqlDatasetConfig>();
    }
    
    virtual ~PostgresqlDataset()
    {
    }

    pg_conn* startConnection() const
    {
        return startPostgresqlConnection(config_.databaseName, config_.host, config_.port);
    }

    virtual Any getStatus() const override
    {
        return string("ok");
    }

    virtual void recordRowItl(const RowPath & rowName,
                              const std::vector<std::tuple<ColumnPath, CellValue, Date> > & vals) override
    {
        throw HttpReturnException(400, "PostgreSQL dataset is read-only");
    }
    
    virtual void recordRows(const std::vector<std::pair<RowPath, std::vector<std::tuple<ColumnPath, CellValue, Date> > > > & rows) override
    {
        throw HttpReturnException(400, "PostgreSQL dataset is read-only");
    }

    /** Commit changes to the database.  Default is a no-op. */
    virtual void commit() override
    {
        throw HttpReturnException(400, "PostgreSQL dataset is read-only");
    }

    virtual std::pair<Date, Date> getTimestampRange() const override
    {
        throw HttpReturnException(400, "PostgreSQL dataset is read-only");
    }

    virtual Date quantizeTimestamp(Date timestamp) const override
    {
        throw HttpReturnException(400, "PostgreSQL dataset is read-only");
    }

    virtual std::shared_ptr<MatrixView> getMatrixView() const override
    {
        throw HttpReturnException(400, "PostgreSQL dataset is read-only");
    }

    virtual std::shared_ptr<ColumnIndex> getColumnIndex() const override
    {
        throw HttpReturnException(400, "PostgreSQL dataset is read-only");
    }

    virtual std::shared_ptr<RowStream> getRowStream() const override
    {
        throw HttpReturnException(400, "PostgreSQL dataset is read-only");
    }

    virtual GenerateRowsWhereFunction
    generateRowsWhere(const SqlBindingScope & context,
                      const Utf8String& alias,
                      const SqlExpression & where,
                      ssize_t offset,
                      ssize_t limit) const override
    {

        auto conn = startConnection();
        string selectString = "SELECT ";
        selectString += config_.primaryKey + 
                        " FROM " + 
                        config_.tableName +
                        " WHERE " + 
                        where.surface.rawString();

        POSTGRESQL_VERBOSE(cerr << "postgress where select: " << endl);
        POSTGRESQL_VERBOSE(cerr << selectString << endl);

        auto res = PQexec(conn, selectString.c_str());
        if (PQresultStatus(res) != PGRES_TUPLES_OK) {
            string errorMsg(PQresultErrorMessage(res));
            PQclear(res);
            PQfinish(conn);
            throw HttpReturnException(400, "Could not select from postgreSQL: ", errorMsg);
        }
        
        POSTGRESQL_VERBOSE(cerr << "keys fetched from " << config_.tableName << " sucessfully!" << endl;)

        std::vector<RowPath> rowsToKeep;

        int nfields = PQnfields(res);
        int ntuples = PQntuples(res);

        POSTGRESQL_VERBOSE(cerr << ntuples << "," << nfields << endl;)

        if (nfields == 1) {
            for(int i = 0; i < ntuples; i++) {
                rowsToKeep.emplace_back(PQgetvalue(res, i, 0));        
            }
        }

        PQclear(res);
        PQfinish(conn);

        return {[=] (ssize_t numToGenerate, Any token,
                     const BoundParameters & params,
                     const ProgressFunc & onProgress)
        {
            ssize_t start = 0;
            ssize_t limit = numToGenerate;

            ExcAssertNotEqual(limit, 0);
            
            if (!token.empty())
                start = token.convert<size_t>();            

            start += rowsToKeep.size();
            Any newToken;
            if (rowsToKeep.size() == limit)
                newToken = start;

            return make_pair(std::move(rowsToKeep),
                             std::move(newToken));
        },
        "PostgresqlDataset row generation"};

    }

    /** Return a row as an expression value.  Default forwards to the matrix
    view's getRow() function.
    */
    virtual ExpressionValue getRowExpr(const RowPath & row) const override
    {
        auto conn = startConnection();
        string selectString = "SELECT * FROM ";
        selectString += config_.tableName +
                        " WHERE " + 
                        config_.primaryKey + 
                        " = '" + 
                        row.toUtf8String().rawString()
                        + "'";

        POSTGRESQL_VERBOSE(cerr << "postgress row select: " << endl;)
        POSTGRESQL_VERBOSE(cerr << selectString << endl;)

        auto res = PQexec(conn, selectString.c_str());
        if (PQresultStatus(res) != PGRES_TUPLES_OK) {
            string errorMsg(PQresultErrorMessage(res));
            PQclear(res);
            PQfinish(conn);
            throw HttpReturnException(400, "Could not select from postgreSQL: ", errorMsg);
        }

        POSTGRESQL_VERBOSE(cerr << "row fetched from " << config_.tableName << " successfully!" << endl;)

        int nfields = PQnfields(res);
        int ntuples = PQntuples(res);

        POSTGRESQL_VERBOSE(cerr << ntuples << "," << nfields << endl;)

        std::vector<std::tuple<ColumnPath, CellValue, Date> > rowValues;
        if (ntuples > 0) {
            std::vector<std::tuple<ColumnPath, CellValue, Date> > cols;
            for(int j = 0; j < nfields; j++) {
                rowValues.emplace_back(ColumnPath(PQfname(res, j)), getCellValueFromPostgres(res, 0, j), Date::Date::notADate());
                POSTGRESQL_VERBOSE(printf("[%d,%d] %s %s\n", 0, j, PQgetvalue(res, 0, j), PQfname(res, j));)
            }       
        }

        PQclear(res);
        PQfinish(conn);

        return ExpressionValue(rowValues);

    }

    /** Return whether or not all columns names and info are known.
    */
    virtual bool hasColumnNames() const override { return false; }

};

/*****************************************************************************/
/* POSTGRESQL RECORDER DATASET                                                */
/*****************************************************************************/

struct PostgresqlRecorderDatasetConfig {
    string databaseName;
    int port;
    string host;
    string tableName;
    bool createTable;
    bool dropTableIfExist;

    PostgresqlRecorderDatasetConfig() {
        databaseName = "database";
        port = postgresqlDefaultPort;
        host = "localhost";
        tableName = "mytable";
        createTable = false;
        dropTableIfExist = false;
    }
};

DECLARE_STRUCTURE_DESCRIPTION(PostgresqlRecorderDatasetConfig);
DEFINE_STRUCTURE_DESCRIPTION(PostgresqlRecorderDatasetConfig);

PostgresqlRecorderDatasetConfigDescription::
PostgresqlRecorderDatasetConfigDescription()
{
    addField("databaseName", &PostgresqlRecorderDatasetConfig::databaseName, "Name of the database to connect to.");
    addField("port", &PostgresqlRecorderDatasetConfig::port, "Port of the database to connect to.", postgresqlDefaultPort);
    addField("host", &PostgresqlRecorderDatasetConfig::host, "Address of the database to connect to ");
    addField("tableName", &PostgresqlRecorderDatasetConfig::tableName, "Name of the table to be recorded into");
    addField("createTable", &PostgresqlRecorderDatasetConfig::createTable, "Should we create the table when the dataset is created", false);
    addField("dropTableIfExist", &PostgresqlRecorderDatasetConfig::dropTableIfExist, "Should we drop an existing PostgreSQL table when creating it", false);
}

struct PostgresqlRecorderDataset: public Dataset {

    PostgresqlRecorderDatasetConfig config_;
    std::unordered_set<ColumnPath> insertedColumns;

    pg_conn* startConnection() 
    {
        return startPostgresqlConnection(config_.databaseName, config_.host, config_.port);
    }

    PostgresqlRecorderDataset(MldbServer * owner,
                 PolyConfig config,
                 const ProgressFunc & onProgress)
        : Dataset(owner)
    {
        config_ = config.params.convert<PostgresqlRecorderDatasetConfig>();

        if (config_.createTable) {            

            auto conn = startConnection();

            //Drop table if exist
            if (config_.dropTableIfExist) {
                string dropDescription = "DROP TABLE IF EXISTS " + config_.tableName;
                POSTGRESQL_VERBOSE(cerr << dropDescription << endl;)
                PGresult *res = PQexec(conn, dropDescription.c_str());
                if (PQresultStatus(res) != PGRES_COMMAND_OK) {
                     string errorMsg(PQresultErrorMessage(res));
                     PQclear(res);
                     PQfinish(conn);
                     throw HttpReturnException(400, "Could not drop PostgreSQL table:  ", errorMsg);
                }

                POSTGRESQL_VERBOSE(cerr << "table " << config_.tableName << " dropped!" << endl;)
                PQclear(res);
            }
            

            string tableDescription = "CREATE TABLE " + config_.tableName + " (" + ")";
            POSTGRESQL_VERBOSE(cerr << tableDescription << endl;)
            PGresult *res = PQexec(conn, tableDescription.c_str());
            if (PQresultStatus(res) != PGRES_COMMAND_OK) {
                string errorMsg(PQresultErrorMessage(res));
                PQclear(res);
                PQfinish(conn);
                throw HttpReturnException(400, "Could not create PostgreSQL table:  ", errorMsg);
            }

            POSTGRESQL_VERBOSE(cerr << "table " << config_.tableName << " created!" << endl;)
            PQclear(res);
            PQfinish(conn);
        }
    }
    
    virtual ~PostgresqlRecorderDataset()
    {
    }

    virtual Any getStatus() const override
    {
        return string("ok");
    }

    string getPostgresTypeString(CellValue v)
    {
        switch (v.cellType())
        {
            case CellValue::EMPTY:
            case CellValue::INTEGER:
                return "integer";
            case CellValue::FLOAT:
                return "float8";
            case CellValue::ASCII_STRING:
            case CellValue::UTF8_STRING:
            case CellValue::PATH:
                return "varchar";
            case CellValue::TIMESTAMP:
                return "timestamptz";
            case CellValue::TIMEINTERVAL:
                return "interval";
            case CellValue::BLOB:
                return "varbit";
            default:
                throw HttpReturnException(500, "Unknown cell type in getPostgresTypeString");
        }
    }

    void alterColumns(pg_conn* conn, const std::vector<std::tuple<ColumnPath, CellValue, Date> > & vals)
    {
        for (auto& p : vals) {
            ColumnPath column = std::get<0>(p);
            if (insertedColumns.count(column) == 0){
                insertedColumns.insert(column);
                string alterString = "ALTER TABLE " +
                                     config_.tableName +
                                     " ADD COLUMN " +
                                     column.toUtf8String().rawString() + 
                                     " "
                                     + getPostgresTypeString(std::get<1>(p));

                POSTGRESQL_VERBOSE(cerr << alterString << endl;)

                auto res = PQexec(conn, alterString.c_str());
                if (PQresultStatus(res) != PGRES_COMMAND_OK) {
                    string errorMsg(PQresultErrorMessage(res));
                    PQclear(res);
                    PQfinish(conn);
                    throw HttpReturnException(400, "Could not alter PostgreSQL table:  ", errorMsg);
                }

                PQclear(res);
            }
        }
    }

    void insertRowPostgresql(pg_conn* conn, const std::vector<std::tuple<ColumnPath, CellValue, Date> > & vals) 
    {
        alterColumns(conn, vals);

        string insertString = "INSERT INTO " + config_.tableName + " (";

        bool first = true;
        for (auto& p : vals) {                
            if (!first) {
                insertString += ",";                    
            }
            first = false;
            insertString += std::get<0>(p).toUtf8String().rawString();
        }

        insertString += ") VALUES (";

        first = true;
        for (auto& p : vals) {                
            if (!first) {
                insertString += ",";                    
            }
            first = false;
            auto& cell = std::get<1>(p);
            if (cell.isString())
                insertString += "'" + cell.toString() + "'";
            else
                insertString += cell.toString();
        }

        insertString += ")";

        POSTGRESQL_VERBOSE(cerr << insertString << endl;)

        auto res = PQexec(conn, insertString.c_str());
        if (PQresultStatus(res) != PGRES_COMMAND_OK) {
            string errorMsg(PQresultErrorMessage(res));
            PQclear(res);
            PQfinish(conn);
            throw HttpReturnException(400, "Could not insert data in PostgreSQL table:  ", errorMsg);
        }

        PQclear(res);
    }

    virtual void recordRowItl(const RowPath & rowName,
                              const std::vector<std::tuple<ColumnPath, CellValue, Date> > & vals) override
    {
        auto conn = startConnection();

        insertRowPostgresql(conn, vals);

        PQfinish(conn);
    }
    
    virtual void recordRows(const std::vector<std::pair<RowPath, std::vector<std::tuple<ColumnPath, CellValue, Date> > > > & rows) override
    {
        auto conn = startConnection();

        for (auto& vals : rows) {
            insertRowPostgresql(conn, vals.second);
        }        

        PQfinish(conn);
    }

    /** Commit changes to the database.  Default is a no-op. */
    virtual void commit() override
    {
    }

    virtual std::pair<Date, Date> getTimestampRange() const override
    {
        throw HttpReturnException(400, "PostgreSQL recorder dataset is record-only");
    }

    virtual Date quantizeTimestamp(Date timestamp) const override
    {
        throw HttpReturnException(400, "PostgreSQL recorder dataset is record-only");
    }

    virtual std::shared_ptr<MatrixView> getMatrixView() const override
    {
        throw HttpReturnException(400, "PostgreSQL recorder dataset is record-only");
    }

    virtual std::shared_ptr<ColumnIndex> getColumnIndex() const override
    {
        throw HttpReturnException(400, "PostgreSQL recorder dataset is record-only");
    }

    virtual std::shared_ptr<RowStream> getRowStream() const override
    {
        throw HttpReturnException(400, "PostgreSQL recorder dataset is record-only");
    }    

};


/*****************************************************************************/
/* POSTGRESQL IMPORT PROCEDURE                                               */
/*****************************************************************************/

struct PostgresqlImportConfig: public ProcedureConfig {

    static constexpr const char * name = "postgresql.import";

    string databaseName;
    int port;
    string host;
    string postgresqlQuery;

    /// The output dataset.  Rows will be dumped into here via insertRows.
    PolyConfigT<Dataset> outputDataset;

    PostgresqlImportConfig() {
        databaseName = "";
        port = postgresqlDefaultPort;
        host = "localhost";
        postgresqlQuery = "";

        outputDataset.withType("sparse.mutable");
    }

};

DECLARE_STRUCTURE_DESCRIPTION(PostgresqlImportConfig);
DEFINE_STRUCTURE_DESCRIPTION(PostgresqlImportConfig);

PostgresqlImportConfigDescription::
PostgresqlImportConfigDescription()
{
    addField("databaseName", &PostgresqlImportConfig::databaseName, "Name of the database to connect to.");
    addField("port", &PostgresqlImportConfig::port, "Port of the database to connect to.", postgresqlDefaultPort);
    addField("host", &PostgresqlImportConfig::host, "Address of the database to connect to ");
    addField("postgresqlQuery", &PostgresqlImportConfig::postgresqlQuery, "Query to run in postgresql to get rows");

    addField("outputDataset", &PostgresqlImportConfig::outputDataset,
             "Output dataset configuration.  This may refer either to an "
             "existing dataset, or a fully specified but non-existing dataset "
             "which will be created by the procedure.", PolyConfigT<Dataset>().withType("sparse.mutable"));

    addParent<ProcedureConfig>();
}

struct PostgresqlImportProcedure: public Procedure {

    PostgresqlImportConfig procedureConfig;

    PostgresqlImportProcedure(MldbServer * server,
                         const PolyConfig & config,
                         std::function<bool (Json::Value)> onProgress)
        : Procedure(server)
    {
        procedureConfig = config.params.convert<PostgresqlImportConfig>();
    }

    pg_conn* startConnection(const PostgresqlImportConfig & config_) const
    {
        return startPostgresqlConnection(config_.databaseName, config_.host, config_.port);
    }

    virtual Any getStatus() const override
    {
        Json::Value result;
        result["ok"] = true;
        return result;
    }

    virtual RunOutput run(const ProcedureRunConfig & run,
                          const std::function<bool (const Json::Value &)> & onProgress) const override
    {        
        RunOutput result;

        auto runProcConf = applyRunConfOverProcConf(procedureConfig, run);

        // Connect to Postgresl database
        auto conn = startConnection(runProcConf);

        // Create the output
        std::shared_ptr<Dataset> output =
        createDataset(server, runProcConf.outputDataset, nullptr, true ); //overwrite

        //query the rows
        auto res = PQexec(conn, runProcConf.postgresqlQuery.c_str());
        if (PQresultStatus(res) != PGRES_TUPLES_OK)
        {
            string errorMsg = PQresultErrorMessage(res);
            PQclear(res);
            PQfinish(conn);
            throw HttpReturnException(400, "Could not query PostgreSQL database", errorMsg);
        }

        int nfields = PQnfields(res);
        int ntuples = PQntuples(res);

        std::vector<std::pair<RowPath, std::vector<std::tuple<ColumnPath, CellValue, Date> > > > rows;
        for(int i = 0; i < ntuples; i++) {
            std::vector<std::tuple<ColumnPath, CellValue, Date> > cols;
            for(int j = 0; j < nfields; j++) {
                cols.emplace_back(ColumnPath(PQfname(res, j)), getCellValueFromPostgres(res, i, j), Date::Date::notADate());
            }
            rows.emplace_back(Path(string("row_" + std::to_string(i))), std::move(cols));            
        }

        output->recordRows(rows);

        // Save the dataset we created
        output->commit();

        result = output->getStatus();

        PQclear(res);
        PQfinish(conn);
        return result;
    }
};

/*****************************************************************************/
/* POSTGRES QUERY FUNCTION                                                   */
/*****************************************************************************/

struct PostgresqlQueryFunctionConfig
{
    string databaseName;
    int port;
    string host;

    string query;

    PostgresqlQueryFunctionConfig() {
        databaseName = "";
        port = postgresqlDefaultPort;
        host = "localhost";
        query = "";
    }
};

DEFINE_STRUCTURE_DESCRIPTION(PostgresqlQueryFunctionConfig);

PostgresqlQueryFunctionConfigDescription::
PostgresqlQueryFunctionConfigDescription()
{
    addField("databaseName", &PostgresqlQueryFunctionConfig::databaseName, "Name of the database to connect to.");
    addField("port", &PostgresqlQueryFunctionConfig::port, "Port of the database to connect to.", postgresqlDefaultPort);
    addField("host", &PostgresqlQueryFunctionConfig::host, "Address of the database to connect to ");    

    addField("query", &PostgresqlQueryFunctionConfig::query,
             "SQL query to run.  The values in the dataset, as "
             "well as the input values, will be available for the expression "
             "calculation");
}

struct PostgresqlQueryFunction : public Function
{
    PostgresqlQueryFunctionConfig functionConfig;

    PostgresqlQueryFunction(MldbServer * owner,
                     PolyConfig config,
                     const std::function<bool (const Json::Value &)> & onProgress)
        : Function(owner, config)
    {
        functionConfig = config.params.convert<PostgresqlQueryFunctionConfig>();
    }

    Any
    getStatus() const
    {
        Json::Value result;
        result["expression"]["query"] = functionConfig.query;
        return result;
    }

    /** Structure that does all the work of the SQL expression function. */
    struct PostgresqlQueryFunctionApplier: public FunctionApplier {

        const PostgresqlQueryFunction * function;
        PostgresqlQueryFunctionConfig config;

        PostgresqlQueryFunctionApplier(const PostgresqlQueryFunction * function,
                                       const PostgresqlQueryFunctionConfig & config)
            : FunctionApplier(function), function(function), config(config)
        {
           std::vector<KnownColumn> noColumns;

           this->info.input = { std::make_shared<RowValueInfo>(noColumns, SCHEMA_CLOSED) };
           
           this->info.output.reset(new RowValueInfo(std::move(noColumns),
                                                    SCHEMA_OPEN));
        }

        pg_conn* startConnection(const PostgresqlQueryFunctionConfig & config_) const
        {
            return startPostgresqlConnection(config_.databaseName, config_.host, config_.port);
        }

        virtual ~PostgresqlQueryFunctionApplier()
        {
        }

        ExpressionValue apply(const ExpressionValue & context) const
        {
            // Connect to Postgresl database
            auto conn = startConnection(config);

            //query the rows
            auto res = PQexec(conn, config.query.c_str());
            if (PQresultStatus(res) != PGRES_TUPLES_OK)
            {
                string errorMsg(PQresultErrorMessage(res));
                PQclear(res);
                PQfinish(conn);
                throw HttpReturnException(400, "Could not query PostgreSQL database", errorMsg);
            }

            int nfields = PQnfields(res);
            int ntuples = PQntuples(res);

            std::vector<std::tuple<ColumnPath, CellValue, Date> > row;
            if (ntuples > 0) {
                std::vector<std::tuple<ColumnPath, CellValue, Date> > cols;
                for(int j = 0; j < nfields; j++) {
                    row.emplace_back(ColumnPath(PQfname(res, j)), getCellValueFromPostgres(res, 0, j), Date::Date::notADate());
                }       
            }

            PQclear(res);
            PQfinish(conn);
            return ExpressionValue(row);
        }        
    };

    std::unique_ptr<FunctionApplier>
    bind(SqlBindingScope & outerContext,
         const std::vector<std::shared_ptr<ExpressionValueInfo> > & input) const
    {
        std::unique_ptr<PostgresqlQueryFunctionApplier> result
            (new PostgresqlQueryFunctionApplier(this, functionConfig));

        result->info.checkInputCompatibility(input);

        return std::move(result);
    }

    ExpressionValue
    apply(const FunctionApplier & applier,
          const ExpressionValue & context) const
    {
        return static_cast<const PostgresqlQueryFunctionApplier &>(applier)
            .apply(context);
    }

    FunctionInfo
    getFunctionInfo() const
    {
        PostgresqlQueryFunctionApplier applier(this, functionConfig);
        return applier.info;
    }
};

static RegisterDatasetType<PostgresqlRecorderDataset, PostgresqlRecorderDatasetConfig>
regPostgresqlRecorderDataset(postgresqlPackage(),
                 "postgresql.recorder",
                 "Dataset type that records to a PostgreSQL database",
                 "Postgresql.md.html",
                 nullptr,
                 { MldbEntity::INTERNAL_ENTITY });

static RegisterDatasetType<PostgresqlDataset, PostgresqlDatasetConfig>
regPostgresqlDataset(postgresqlPackage(),
                 "postgresql.dataset",
                 "Dataset type that reads from a PostgreSQL database",
                 "Postgresql.md.html",
                 nullptr,
                 { MldbEntity::INTERNAL_ENTITY });

static RegisterProcedureType<PostgresqlImportProcedure, PostgresqlImportConfig>
regPostgresqlImport(postgresqlPackage(),
                 "Import a dataset from PostgreSQL",
                 "Postgresql.md.html",
                 nullptr,
                 { MldbEntity::INTERNAL_ENTITY });

static RegisterFunctionType<PostgresqlQueryFunction, PostgresqlQueryFunctionConfig>
regSqlQueryFunction(postgresqlPackage(),
                    "postgresql.query",
                    "Run a single row SQL query against a PostgreSQL dataset",
                    "Postgresql.md.html",
                    nullptr,
                    { MldbEntity::INTERNAL_ENTITY });

} // namespace MLDB

