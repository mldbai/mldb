// This file is part of MLDB. Copyright 2015 Datacratic. All rights reserved.

/** stats_table_procedure.h                                                   -*- C++ -*-
    Francois Maillet, 2 septembre 2015
    Copyright (c) 2015 Datacratic Inc.  All rights reserved.

    StatsTable procedure
*/

#pragma once

#include "types/value_description.h"
#include "server/plugin_resource.h"
#include "server/mldb_server.h"
#include "server/procedure.h"
#include "server/function.h"
#include "sql/sql_expression.h"
#include "mldb/jml/db/persistent_fwd.h"

namespace Datacratic {
namespace MLDB {


/*****************************************************************************/
/* STATS TABLE PROCEDURE CONFIG                                              */
/*****************************************************************************/

struct StatsTableProcedureConfig {
    StatsTableProcedureConfig() :
          select("*"),
          when(WhenExpression::TRUE),
          where(SqlExpression::TRUE)
    {
        output.withType("sparse.mutable");
    }

    std::shared_ptr<TableExpression> dataset;
    PolyConfigT<Dataset> output;

    /// The SELECT clause to tell us which features to keep
    SelectExpression select;

    /// The WHEN clause for the timespan tuples must belong to
    WhenExpression when;

    /// The WHERE clause for which rows to include from the dataset
    std::shared_ptr<SqlExpression> where;

    /// The expression to generate the outcomes
    std::vector<std::pair<std::string, std::shared_ptr<SqlExpression>>> outcomes;

    OrderByExpression orderBy;

    Url statsTableFileUrl;

    Utf8String functionName;
};

DECLARE_STRUCTURE_DESCRIPTION(StatsTableProcedureConfig);


/*****************************************************************************/
/* STATS TABLE                                                               */
/*****************************************************************************/

struct StatsTable {

    StatsTable(const ColumnName & colName=ColumnName("ND"),
            const std::vector<std::string> & outcome_names = {})
        : colName(colName), outcome_names(outcome_names),
          zeroCounts(std::make_pair(0, std::vector<int64_t>(outcome_names.size())))
    {
    }

    StatsTable(const std::string & filename);

    typedef std::pair<int64_t, std::vector<int64_t>> BucketCounts;
    const BucketCounts & increment(const CellValue & val,
                                   const std::vector<uint> & outcomes);
    const BucketCounts & getCounts(const CellValue & val) const;

    void save(const std::string & filename) const;
    void serialize(ML::DB::Store_Writer & store) const;
    void reconstitute(ML::DB::Store_Reader & store);


    ColumnName colName;

    std::vector<std::string> outcome_names;
    std::map<Utf8String, BucketCounts> counts;

    std::pair<int64_t, std::vector<int64_t>> zeroCounts;
};


/*****************************************************************************/
/* STATS TABLE PROCEDURE                                                     */
/*****************************************************************************/
    
typedef std::map<ColumnName, StatsTable> StatsTablesMap;

struct StatsTableProcedure: public Procedure {

    StatsTableProcedure(MldbServer * owner,
                PolyConfig config,
                const std::function<bool (const Json::Value &)> & onProgress);

    virtual RunOutput run(const ProcedureRunConfig & run,
                          const std::function<bool (const Json::Value &)> & onProgress) const;

    virtual Any getStatus() const;

    StatsTableProcedureConfig procConfig;
};

/*****************************************************************************/
/* STATS TABLE FUNCTION                                                      */
/*****************************************************************************/

struct StatsTableFunctionConfig {
    StatsTableFunctionConfig(const Url & statsTableFileUrl = Url())
        : statsTableFileUrl(statsTableFileUrl)
    {
    }

    Url statsTableFileUrl;
};

DECLARE_STRUCTURE_DESCRIPTION(StatsTableFunctionConfig);

struct StatsTableFunction: public Function {
    StatsTableFunction(MldbServer * owner,
                  PolyConfig config,
                  const std::function<bool (const Json::Value &)> & onProgress);

    ~StatsTableFunction();

    virtual Any getStatus() const;

    virtual Any getDetails() const;

    virtual FunctionOutput apply(const FunctionApplier & applier,
                              const FunctionContext & context) const;

    /** Describe what the input and output is for this function. */
    virtual FunctionInfo getFunctionInfo() const;

    StatsTableFunctionConfig functionConfig;

    StatsTablesMap statsTables;
};


/*****************************************************************************/
/* STATS TABLE DERIVED COLS FUNCTION                                         */
/*****************************************************************************/

struct StatsTableDerivedColumnsGeneratorProcedureConfig {
    StatsTableDerivedColumnsGeneratorProcedureConfig(
            const Url & statsTableFileUrl = Url())
        : statsTableFileUrl(statsTableFileUrl)
    {
    }

    std::string functionId;
    std::string expression;
    Url statsTableFileUrl;
};

DECLARE_STRUCTURE_DESCRIPTION(StatsTableDerivedColumnsGeneratorProcedureConfig);


struct StatsTableDerivedColumnsGeneratorProcedure: public Procedure {

    StatsTableDerivedColumnsGeneratorProcedure(MldbServer * owner,
                PolyConfig config,
                const std::function<bool (const Json::Value &)> & onProgress);

    virtual RunOutput run(const ProcedureRunConfig & run,
                          const std::function<bool (const Json::Value &)> & onProgress) const;

    virtual Any getStatus() const;

    StatsTableDerivedColumnsGeneratorProcedureConfig procConfig;
};


} // namespace MLDB
} // namespace Datacratic
