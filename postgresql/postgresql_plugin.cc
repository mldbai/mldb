/* postgresql_plugin.cc
   Mathieu Marquis Bolduc, 16 July 2016
   Copyright (c) 2015 Datacratic Inc.  All rights reserved.
*/

#include "mldb/core/plugin.h"
#include "mldb/core/procedure.h"
#include "mldb/core/function.h"
#include "mldb/core/dataset.h"
#include "mldb/types/structure_description.h"

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
        PQclear(res);**

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


        /* close the connection to the database and cleanup */
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
/* POSTGRESQL IMPORT PROCEDURE                                               */
/*****************************************************************************/

/*struct PostgresqlImportConfig: public ProcedureConfig {

    static constexpr const char * name = "postgresql.import";

};

DECLARE_STRUCTURE_DESCRIPTION(PostgresqlImportConfig);
DEFINE_STRUCTURE_DESCRIPTION(PostgresqlImportConfig);

PostgresqlImportConfigDescription::
PostgresqlImportConfigDescription()
{
    addParent<ProcedureConfig>();
}

struct PostgresqlImportProcedure: public Procedure {
    PostgresqlImportProcedure(MldbServer * server,
                         const PolyConfig & config,
                         std::function<bool (Json::Value)> onProgress)
        : Procedure(server)
    {
    }

    virtual Any getStatus() const
    {
        Json::Value result;
        result["ok"] = true;
        return result;
    }   

    virtual RunOutput run(const ProcedureRunConfig & run,
                          const std::function<bool (const Json::Value &)> & onProgress) const
    {        
        RunOutput result;
        return result;
    }
};
*/
static RegisterDatasetType<PostgresqlDataset, PostgresqlDatasetConfig>
regPostgresqlDataset(//postgresqlPackage(),
                 builtinPackage(),
                 "postgresql.dataset",
                 "Dataset type that reads from a Postgresql database",
                 "Postgresql.md.html");

/*static RegisterProcedureType<PostgresqlImportProcedure, PostgresqlImportConfig>
regPostgresqlImport(//postgresqlPackage(),
                builtinPackage(),
                 "Import a dataset from Postgresql",
                 "Postgresql.md.html");
*/
} // namespace MLDB
} // namespace Datacratic