/* postgresql_plugin.cc
   Mathieu Marquis Bolduc, 16 July 2016
   Copyright (c) 2015 Datacratic Inc.  All rights reserved.
*/

#include "mldb/core/plugin.h"
#include "mldb/core/procedure.h"
#include "mldb/core/function.h"
#include "mldb/core/dataset.h"
#include "mldb/types/structure_description.h"
#include "mldb/types/any_impl.h"

#include "ext/postgresql/src/interfaces/libpq/libpq-fe.h"

using namespace std;

Datacratic::MLDB::Plugin *
mldbPluginEnterV100(Datacratic::MLDB::MldbServer * server)
{
    return nullptr;
}

namespace Datacratic {
namespace MLDB {

const Package & postgresqlPackage()
{
    static const Package result("postgresql");
    return result;
}

/*****************************************************************************/
/* POSTGRESQL DATASET                                                        */
/*****************************************************************************/

struct PostgresqlDatasetConfig {
};

DECLARE_STRUCTURE_DESCRIPTION(PostgresqlDatasetConfig);
DEFINE_STRUCTURE_DESCRIPTION(PostgresqlDatasetConfig);

PostgresqlDatasetConfigDescription::
PostgresqlDatasetConfigDescription()
{
}

struct PostgresqlDataset: public Dataset {

    PostgresqlDataset(MldbServer * owner,
                 PolyConfig config,
                 const std::function<bool (const Json::Value &)> & onProgress)
        : Dataset(owner)
    {
        //auto keywords = "";
        //auto values = "";
        //int expand_dbname = 0;

       // PGconn* conn = makeEmptyPGconn();
        //const char *conninfo = "hostaddr=spock.datacratic.com dbname = mldb port=5432 user=mldb password=mldb";
        const char *conninfo = "dbname = mldb port=5432 user=mldb password=mldb";

       // connectOptions1(conn, conninfo);

       /* if (conn->dbName == NULL)
            cerr << "no db name" << endl;
        else
            cerr << conn->dbName << endl;*/

        auto conn = PQconnectdb(conninfo);


        if (PQstatus(conn) != CONNECTION_OK)
        {
            cerr << "Connection to the database failed: " << PQerrorMessage(conn) << endl;
            PQfinish(conn);
            return;
        }
        else {
            cerr << "connection successful!" << endl;
        }

        PGresult *res = nullptr;

        //create a table
       /* PGresult *res = PQexec(conn, "CREATE TABLE hello (message VARCHAR(32))");
        if (PQresultStatus(res) != PGRES_COMMAND_OK)
            cerr << "could not create table" << PQresultErrorMessage(res);
        PQclear(res);*/

          //insert data
        /*res = PQexec(conn, "INSERT INTO hello VALUES ('Hello World!')");
        if (PQresultStatus(res) != PGRES_COMMAND_OK)
            cerr << "could not insert data" << PQresultErrorMessage(res);
        PQclear(res);*/

        //query the db
          res = PQexec(conn, "SELECT * FROM hello");
          if (PQresultStatus(res) != PGRES_TUPLES_OK)
             cerr << "could not query table: " << PQresultErrorMessage(res);
         else 
            cerr << "query successful" << endl;

          int nfields = PQnfields(res);
          int ntuples = PQntuples(res);

          for(int i = 0; i < ntuples; i++)
            for(int j = 0; j < nfields; j++)
              cerr << i << "," << j << ": " << PQgetvalue(res, i, j) << endl;
          PQclear(res);


        // close the connection to the database and cleanup
        PQfinish(conn);
    }
    
    virtual ~PostgresqlDataset()
    {
    }

    virtual Any getStatus() const
    {
        return string("ok");
    }

    virtual void recordRowItl(const RowName & rowName,
                              const std::vector<std::tuple<ColumnName, CellValue, Date> > & vals)
    {
    }
    
    virtual void recordRows(const std::vector<std::pair<RowName, std::vector<std::tuple<ColumnName, CellValue, Date> > > > & rows)
    {
    }

    /** Commit changes to the database.  Default is a no-op. */
    virtual void commit()
    {
    }

    virtual std::pair<Date, Date> getTimestampRange() const
    {
        throw HttpReturnException(400, "Postgresql dataset is record-only");
    }

    virtual Date quantizeTimestamp(Date timestamp) const
    {
        throw HttpReturnException(400, "Postgresql dataset is record-only");
    }

    virtual std::shared_ptr<MatrixView> getMatrixView() const
    {
        throw HttpReturnException(400, "Postgresql dataset is record-only");
    }

    virtual std::shared_ptr<ColumnIndex> getColumnIndex() const
    {
        throw HttpReturnException(400, "Postgresql dataset is record-only");
    }

    virtual std::shared_ptr<RowStream> getRowStream() const
    {
        throw HttpReturnException(400, "Postgresql dataset is record-only");
    }

};

/*****************************************************************************/
/* POSTGRESQL RECORDER DATASET                                                */
/*****************************************************************************/

struct PostgresqlRecorderDatasetConfig {
    string databaseName;
    string userName;
    int port;
    string host;
    string tableName;
    bool createTable;
    string createTableColumns;

    PostgresqlRecorderDatasetConfig() {
        databaseName = "database";
        userName = "user";
        port = 1234;
        host = "localhost";
        tableName = "mytable";
        createTable = false;
    }
};

DECLARE_STRUCTURE_DESCRIPTION(PostgresqlRecorderDatasetConfig);
DEFINE_STRUCTURE_DESCRIPTION(PostgresqlRecorderDatasetConfig);

PostgresqlRecorderDatasetConfigDescription::
PostgresqlRecorderDatasetConfigDescription()
{
    addField("databaseName", &PostgresqlRecorderDatasetConfig::databaseName, "Name of the database to connect to.");
    addField("userName", &PostgresqlRecorderDatasetConfig::userName, "User name in postgresql database");
    addField("port", &PostgresqlRecorderDatasetConfig::port, "Port of the database to connect to.", 1234);
    addField("host", &PostgresqlRecorderDatasetConfig::host, "Address of the database to connect to ");
    addField("tableName", &PostgresqlRecorderDatasetConfig::tableName, "Name of the table to be recorded into");
    addField("createTable", &PostgresqlRecorderDatasetConfig::createTable, "Should we create the table when the dataset is created", true);
    addField("createTableColumns", &PostgresqlRecorderDatasetConfig::createTableColumns, "Columns in the created table");
}

struct PostgresqlRecorderDataset: public Dataset {

    PostgresqlRecorderDatasetConfig config_;

    pg_conn* startConnection() 
    {
        string connString;
        connString += "dbname=" + config_.databaseName 
                   + " host=" + config_.host 
                   + " port=" + std::to_string(config_.port)
                   + " user=" + config_.userName
                   + " password=" + "mldb";

        cerr << connString << endl;

        auto conn = PQconnectdb(connString.c_str());
        if (PQstatus(conn) != CONNECTION_OK)
        {
            cerr << "Connection to the database failed: " << PQerrorMessage(conn) << endl;
            PQfinish(conn);
            throw HttpReturnException(400, "Could not connect to Postgresql database");
        }

        cerr << "connection successful!" << endl;
        return conn;
    }

    PostgresqlRecorderDataset(MldbServer * owner,
                 PolyConfig config,
                 const std::function<bool (const Json::Value &)> & onProgress)
        : Dataset(owner)
    {
        cerr << "TESSST" << endl;
        config_ = config.params.convert<PostgresqlRecorderDatasetConfig>();

        if (config_.createTable) {            

            auto conn = startConnection();
            if (!conn)
                return;

            //Drop table if exist
            string dropDescription = "DROP TABLE IF EXISTS " + config_.tableName;
            cerr << dropDescription << endl;
            PGresult *res = PQexec(conn, dropDescription.c_str());
            if (PQresultStatus(res) != PGRES_COMMAND_OK) {
                 cerr << "could not drop table: " << PQresultErrorMessage(res);
            }
            else {
                cerr << "table " << config_.tableName << " dropped!" << endl;
            }
            PQclear(res);
            //

            string tableDescription = "CREATE TABLE " + config_.tableName + " (" + config_.createTableColumns + ")";
            cerr << tableDescription << endl;
            res = PQexec(conn, tableDescription.c_str());
            if (PQresultStatus(res) != PGRES_COMMAND_OK) {
                cerr << "could not create table: " << PQresultErrorMessage(res);
            }
            else {
                cerr << "table " << config_.tableName << " created!" << endl;
            }
            PQclear(res);

            // close the connection to the database and cleanup
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

    void insertRowPostgresql(pg_conn* conn, const std::vector<std::tuple<ColumnName, CellValue, Date> > & vals) 
    {
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

        cerr << "insertion string: " << endl;
        cerr << insertString << endl;

        auto res = PQexec(conn, insertString.c_str());
        if (PQresultStatus(res) != PGRES_COMMAND_OK) {
            cerr << "could not insert data" << PQresultErrorMessage(res);
            throw HttpReturnException(400, "Could not insert data in Postgresql database");
        }

        PQclear(res);

        cerr << "insertion completed!" << endl;
    }

    virtual void recordRowItl(const RowName & rowName,
                              const std::vector<std::tuple<ColumnName, CellValue, Date> > & vals) override
    {
        auto conn = startConnection();

        insertRowPostgresql(conn, vals);

        PQfinish(conn);
    }
    
    virtual void recordRows(const std::vector<std::pair<RowName, std::vector<std::tuple<ColumnName, CellValue, Date> > > > & rows) override
    {
       // cerr << "recordRows" << endl;
       // ExcAssert(false); //unimplemented

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
        throw HttpReturnException(400, "Postgresql recorder dataset is record-only");
    }

    virtual Date quantizeTimestamp(Date timestamp) const override
    {
        throw HttpReturnException(400, "Postgresql recorder dataset is record-only");
    }

    virtual std::shared_ptr<MatrixView> getMatrixView() const override
    {
        throw HttpReturnException(400, "Postgresql recorder dataset is record-only");
    }

    virtual std::shared_ptr<ColumnIndex> getColumnIndex() const override
    {
        throw HttpReturnException(400, "Postgresql recorder dataset is record-only");
    }

    virtual std::shared_ptr<RowStream> getRowStream() const override
    {
        throw HttpReturnException(400, "Postgresql recorder dataset is record-only");
    }    

};


/*****************************************************************************/
/* POSTGRESQL IMPORT PROCEDURE                                               */
/*****************************************************************************/

struct PostgresqlImportConfig: public ProcedureConfig {

    static constexpr const char * name = "postgresql.import";

    string databaseName;
    string userName;
    int port;
    string host;
    string tableName;
    string postgresqlQuery;

    /// The output dataset.  Rows will be dumped into here via insertRows.
    PolyConfigT<Dataset> outputDataset;

    PostgresqlImportConfig() {
        databaseName = "database";
        userName = "user";
        port = 1234;
        host = "localhost";
        tableName = "mytable";
        postgresqlQuery = "";

        outputDataset.withType("sparse.mutable");
    }

};

DECLARE_STRUCTURE_DESCRIPTION(PostgresqlImportConfig);
DEFINE_STRUCTURE_DESCRIPTION(PostgresqlImportConfig);

PostgresqlImportConfigDescription::
PostgresqlImportConfigDescription()
{
    addParent<ProcedureConfig>();

    addField("databaseName", &PostgresqlImportConfig::databaseName, "Name of the database to connect to.");
    addField("userName", &PostgresqlImportConfig::userName, "User name in postgresql database");
    addField("port", &PostgresqlImportConfig::port, "Port of the database to connect to.", 1234);
    addField("host", &PostgresqlImportConfig::host, "Address of the database to connect to ");
    addField("postgresqlQuery", &PostgresqlImportConfig::postgresqlQuery, "Query to run in postgresql to get rows");

    addField("outputDataset", &PostgresqlImportConfig::outputDataset,
             "Output dataset configuration.  This may refer either to an "
             "existing dataset, or a fully specified but non-existing dataset "
             "which will be created by the procedure.", PolyConfigT<Dataset>().withType("sparse.mutable"));
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
        string connString;
        connString += "dbname=" + config_.databaseName 
                   + " host=" + config_.host 
                   + " port=" + std::to_string(config_.port)
                   + " user=" + config_.userName
                   + " password=" + "mldb";

        cerr << connString << endl;

        auto conn = PQconnectdb(connString.c_str());
        if (PQstatus(conn) != CONNECTION_OK)
        {
            cerr << "Connection to the database failed: " << PQerrorMessage(conn) << endl;
            PQfinish(conn);
            throw HttpReturnException(400, "Could not connect to Postgresql database");
        }

        cerr << "connection successful!" << endl;
        return conn;
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
        createDataset(server, runProcConf.outputDataset, nullptr, true /*overwrite*/);

        //query the rows
        auto res = PQexec(conn, runProcConf.postgresqlQuery.c_str());
        if (PQresultStatus(res) != PGRES_TUPLES_OK)
        {
            cerr << PQresultErrorMessage(res) << endl;
            PQclear(res);
            PQfinish(conn);
            throw HttpReturnException(400, "Could not query database");
        }

        int nfields = PQnfields(res);
        int ntuples = PQntuples(res);

        //cerr << nfields << " fields in " << ntuples << " tuples" << endl; 
        std::vector<std::pair<RowName, std::vector<std::tuple<ColumnName, CellValue, Date> > > > rows;
        for(int i = 0; i < ntuples; i++) {
            std::vector<std::tuple<ColumnName, CellValue, Date> > cols;
            for(int j = 0; j < nfields; j++) {
                cols.emplace_back(ColumnName(PQfname(res, j)), CellValue(PQgetvalue(res, i, j)), Date::Date::notADate());
                //printf("[%d,%d] %s\n", i, j, PQgetvalue(res, i, j));
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
    string userName;
    int port;
    string host;

    string query;

    PostgresqlQueryFunctionConfig() {
        databaseName = "database";
        userName = "user";
        port = 1234;
        host = "localhost";
        query = "";
    }
};

DEFINE_STRUCTURE_DESCRIPTION(PostgresqlQueryFunctionConfig);

PostgresqlQueryFunctionConfigDescription::
PostgresqlQueryFunctionConfigDescription()
{
    addField("databaseName", &PostgresqlQueryFunctionConfig::databaseName, "Name of the database to connect to.");
    addField("userName", &PostgresqlQueryFunctionConfig::userName, "User name in postgresql database");
    addField("port", &PostgresqlQueryFunctionConfig::port, "Port of the database to connect to.", 1234);
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
        : Function(owner)
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

           this->info.input = std::make_shared<RowValueInfo>(noColumns, SCHEMA_CLOSED);
           
           this->info.output.reset(new RowValueInfo(std::move(noColumns),
                                                    SCHEMA_OPEN));
        }

        pg_conn* startConnection(const PostgresqlQueryFunctionConfig & config_) const
        {
            string connString;
            connString += "dbname=" + config_.databaseName 
                       + " host=" + config_.host 
                       + " port=" + std::to_string(config_.port)
                       + " user=" + config_.userName
                       + " password=" + "mldb";

            cerr << connString << endl;

            auto conn = PQconnectdb(connString.c_str());
            if (PQstatus(conn) != CONNECTION_OK)
            {
                cerr << "Connection to the database failed: " << PQerrorMessage(conn) << endl;
                PQfinish(conn);
                throw HttpReturnException(400, "Could not connect to Postgresql database");
            }

            cerr << "connection successful!" << endl;
            return conn;
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
                cerr << PQresultErrorMessage(res) << endl;
                PQclear(res);
                PQfinish(conn);
                throw HttpReturnException(400, "Could not query database");
            }

            int nfields = PQnfields(res);
            int ntuples = PQntuples(res);

            //cerr << nfields << " fields in " << ntuples << " tuples" << endl; 
            std::vector<std::tuple<ColumnName, CellValue, Date> > row;
            if (ntuples > 0) {
                std::vector<std::tuple<ColumnName, CellValue, Date> > cols;
                for(int j = 0; j < nfields; j++) {
                    row.emplace_back(ColumnName(PQfname(res, j)), CellValue(PQgetvalue(res, 0, j)), Date::Date::notADate());
                    //printf("[%d,%d] %s\n", i, j, PQgetvalue(res, i, j));
                }       
            }

            PQclear(res);
            PQfinish(conn);
            return ExpressionValue(row);
        }        
    };

    std::unique_ptr<FunctionApplier>
    bind(SqlBindingScope & outerContext,
         const std::shared_ptr<RowValueInfo> & input) const
    {
        std::unique_ptr<PostgresqlQueryFunctionApplier> result
            (new PostgresqlQueryFunctionApplier(this, functionConfig));

        result->info.checkInputCompatibility(*input);

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
regPostgresqlRecorderDataset(//postgresqlPackage(),
                 builtinPackage(),
                 "postgresql.recorder",
                 "Dataset type that record to a Postgresql database",
                 "Postgresql.md.html");

static RegisterDatasetType<PostgresqlDataset, PostgresqlDatasetConfig>
regPostgresqlDataset(//postgresqlPackage(),
                 builtinPackage(),
                 "postgresql.dataset",
                 "Dataset type that reads from a Postgresql database",
                 "Postgresql.md.html");

static RegisterProcedureType<PostgresqlImportProcedure, PostgresqlImportConfig>
regPostgresqlImport(//postgresqlPackage(),
                builtinPackage(),
                 "Import a dataset from Postgresql",
                 "Postgresql.md.html");

static RegisterFunctionType<PostgresqlQueryFunction, PostgresqlQueryFunctionConfig>
regSqlQueryFunction(builtinPackage(),
                    "postgresql.query",
                    "Run a single row SQL query against a Postgresql dataset",
                    "Postgresql.md.html");

} // namespace MLDB
} // namespace Datacratic
