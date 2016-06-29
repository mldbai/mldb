// This file is part of MLDB. Copyright 2015 Datacratic. All rights reserved.

/** dist_table_procedure.h                                         -*- C++ -*-
    Simon Lemieux, June 2015
    Copyright (c) 2015 Datacratic Inc.  All rights reserved.

    distTable procedure

    Note that this was mainly copied from stats_table_procedure.h in a hurry.
    This will probably be revisited.
*/

#pragma once

#include "types/value_description.h"
#include "server/plugin_resource.h"
#include "server/mldb_server.h"
#include "mldb/core/procedure.h"
#include "mldb/core/function.h"
#include "sql/sql_expression.h"
#include "mldb/jml/db/persistent_fwd.h"
#include "mldb/types/optional.h"
#include "mldb/types/string.h"

namespace Datacratic {
namespace MLDB {


/*****************************************************************************/
/* DIST TABLE                                                               */
/*****************************************************************************/

struct DistTable {

    DistTable(const ColumnName & colName=ColumnName("ND"),
            const std::vector<Utf8String> & outcome_names = {})
        : colName(colName), outcome_names(outcome_names)
    {
    }

    DistTable(const std::string & filename);

    // Add the value of an outcome in our aggregations, for a given feature
    void increment(const Utf8String & featureValue, uint outcome,
                   double targetValue);

    // returns the stats (count, avg, std, min, max) for a given featureValue
    // and outcome
    std::tuple<uint64_t, double, double, double, double> getStats(
        const Utf8String & featureValue, int outcome) const;

    void save(const std::string & filename) const;
    void serialize(ML::DB::Store_Writer & store) const;
    void reconstitute(ML::DB::Store_Reader & store);

    ColumnName colName;

    std::vector<Utf8String> outcome_names;

    uint64_t getNbOutcomes() const { return outcome_names.size(); }

    // key: name of one of the values for our column
    // value: aggregation for each outcome. The map allows for NULL in one 
    // outcome while NOT NULL in another
    std::unordered_map<Utf8String,
                       std::unordered_map<int, uint64_t>> counts;
    std::unordered_map<Utf8String,
                       std::unordered_map<int, double>> avgs;
    // unbiased vars assuming we are on a sample and not the whole population
    std::unordered_map<Utf8String,
                       std::unordered_map<int, double>> vars;
    // this is only a helper for calculating the var
    std::unordered_map<Utf8String,
                       std::unordered_map<int, double>> M2s;
    std::unordered_map<Utf8String,
                       std::unordered_map<int, double>> mins;
    std::unordered_map<Utf8String,
                       std::unordered_map<int, double>> maxs;
};


/*****************************************************************************/
/* DIST TABLE PROCEDURE CONFIG                                              */
/*****************************************************************************/

struct DistTableProcedureConfig : public ProcedureConfig {
    static constexpr const char * name = "distTable.train";

    DistTableProcedureConfig()
    {
        output.withType("sparse.mutable");
    }

    InputQuery trainingData;
    PolyConfigT<Dataset> output;

    /// The expression to generate the outcomes
    std::vector<std::pair<std::string, std::shared_ptr<SqlExpression>>> outcomes;

    Url modelFileUrl;

    Utf8String functionName;
};

DECLARE_STRUCTURE_DESCRIPTION(DistTableProcedureConfig);


/*****************************************************************************/
/* DIST TABLE PROCEDURE                                                     */
/*****************************************************************************/

typedef std::map<ColumnName, DistTable> DistTablesMap;

struct DistTableProcedure: public Procedure {

    DistTableProcedure(MldbServer * owner,
                PolyConfig config,
                const std::function<bool (const Json::Value &)> & onProgress);

    virtual RunOutput run(const ProcedureRunConfig & run,
                          const std::function<bool (const Json::Value &)> & onProgress) const;

    virtual Any getStatus() const;

    DistTableProcedureConfig procConfig;
};

/*****************************************************************************/
/* DIST TABLE FUNCTION                                                      */
/*****************************************************************************/

struct DistTableFunctionConfig {
    DistTableFunctionConfig(const Url & modelFileUrl = Url())
        : modelFileUrl(modelFileUrl)
    {
    }

    Url modelFileUrl;
};

DECLARE_STRUCTURE_DESCRIPTION(DistTableFunctionConfig);

struct DistTableFunction: public Function {
    DistTableFunction(MldbServer * owner,
                  PolyConfig config,
                  const std::function<bool (const Json::Value &)> & onProgress);

    ~DistTableFunction();

    virtual Any getStatus() const;

    virtual Any getDetails() const;

    virtual ExpressionValue apply(const FunctionApplier & applier,
                              const ExpressionValue & context) const;

    /** Describe what the input and output is for this function. */
    virtual FunctionInfo getFunctionInfo() const;

    DistTableFunctionConfig functionConfig;

    DistTablesMap distTablesMap;
};

} // namespace MLDB
} // namespace Datacratic
