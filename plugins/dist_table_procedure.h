/** dist_table_procedure.h                                         -*- C++ -*-
    Simon Lemieux, June 2015
    This file is part of MLDB. Copyright 2015 mldb.ai inc. All rights reserved.

    distTable procedure
*/

#pragma once

#include "types/value_description_fwd.h"
#include "server/plugin_resource.h"
#include "server/mldb_server.h"
#include "mldb/core/procedure.h"
#include "mldb/core/function.h"
#include "sql/sql_expression.h"
#include "mldb/jml/db/persistent_fwd.h"
#include "mldb/types/optional.h"
#include "mldb/types/string.h"
#include <boost/thread/shared_mutex.hpp>


namespace MLDB {

/*****************************************************************************/
/* DIST TABLE STATS ENUM                                                     */
/*****************************************************************************/

enum DISTTABLE_STATISTICS {
    DT_COUNT,
    DT_AVG,
    DT_STD,
    DT_MIN,
    DT_MAX,
    DT_LAST,
    DT_SUM,
    DT_NUM_STATISTICS
};

inline DISTTABLE_STATISTICS parseDistTableStatistic(const Utf8String & st)
{
    if(st == "avg")   return DT_AVG;
    if(st == "std")   return DT_STD;
    if(st == "min")   return DT_MIN;
    if(st == "max")   return DT_MAX;
    if(st == "last")  return DT_LAST;
    if(st == "count") return DT_COUNT;
    if(st == "sum")   return DT_SUM;
    throw MLDB::Exception("Unknown distribution table statistic");
}

inline std::string print(DISTTABLE_STATISTICS stat)
{
    switch(stat) {
        case DT_COUNT:  return "count";
        case DT_AVG:    return "avg";
        case DT_STD:    return "std";
        case DT_MIN:    return "min";
        case DT_MAX:    return "max";
        case DT_LAST:   return "last";
        case DT_SUM:    return "sum";
        default:
            throw MLDB::Exception("Unknown DistTable_Stat");
    }
}


/*****************************************************************************/
/* DIST TABLE                                                               */
/*****************************************************************************/

struct DistTableStats {
    
    DistTableStats():
        count(0), last(NAN), avg(NAN), var(NAN), M2(NAN), min(NAN), max(NAN), sum(0)
    {}
                    
    uint64_t count;
    double last; // last value
    double avg;
    // unbiased vars assuming we are on a sample and not the whole population
    double var;
    // this is only a helper for calculating the var
    double M2;
    double min;
    double max;
    double sum;

    double getStd() const { return sqrt(var); }

    double getStat(DISTTABLE_STATISTICS stat) const;

    void increment(double value);
};


struct DistTable {

    DistTable(const ColumnPath & colName=ColumnPath("ND"),
            const std::vector<Utf8String> & outcome_names = {}):
        colName(colName), outcome_names(outcome_names),
        unknownStats(std::vector<DistTableStats>(outcome_names.size()))
    {
    }

    DistTable(const std::string & filename);

    // Add the value of an outcome in our aggregations, for a given feature
    void increment(const Utf8String & featureValue,
                   const std::vector<double> & targets);

    // returns the stats for a given featureValue
    // and outcome
    const std::vector<DistTableStats> & getStats(
            const Utf8String & featureValue) const;

    void save(const std::string & filename) const;
    void serialize(ML::DB::Store_Writer & store) const;
    void reconstitute(ML::DB::Store_Reader & store);

    ColumnPath colName;

    std::vector<Utf8String> outcome_names;

    // key: name of one of the values for our column
    // value: aggregated stats for each outcome.
    std::unordered_map<Utf8String, std::vector<DistTableStats>> stats;

    // this is the same as getStats(unseenBeforeFeatureValue), but faster
    std::vector<DistTableStats> unknownStats;
};


/*****************************************************************************/
/* DIST TABLE PROCEDURE CONFIG                                              */
/*****************************************************************************/

enum DistTableMode {
    DT_MODE_BAG_OF_WORDS,
    DT_MODE_FIXED_COLUMNS
};

struct DistTableProcedureConfig : public ProcedureConfig {
    static constexpr const char * name = "experimental.distTable.train";

    DistTableProcedureConfig() : mode(DT_MODE_FIXED_COLUMNS)
    {
    }

    InputQuery trainingData;

    Optional<PolyConfigT<Dataset> > output;
    static constexpr char const * defaultOutputDatasetType = "tabular";

    /// The expression to generate the outcomes
    std::vector<std::pair<std::string, std::shared_ptr<SqlExpression>>> outcomes;

    Url modelFileUrl;

    Utf8String functionName;

    std::vector<Utf8String> statistics;

    DistTableMode mode;
};

DECLARE_STRUCTURE_DESCRIPTION(DistTableProcedureConfig);


/*****************************************************************************/
/* DIST TABLE PROCEDURE                                                     */
/*****************************************************************************/

typedef std::map<ColumnPath, DistTable> DistTablesMap;

struct DistTableProcedure: public Procedure {

    DistTableProcedure(MldbServer * owner,
                PolyConfig config,
                const std::function<bool (const Json::Value &)> & onProgress);

    virtual RunOutput run(const ProcedureRunConfig & run,
                          const std::function<bool (const Json::Value &)> & onProgress) const;

    virtual Any getStatus() const;

    DistTableProcedureConfig procConfig;

    static void persist(const Url & modelFileUrl, DistTableMode mode,
                        const DistTablesMap & distTablesMap);

    enum { DIST_TABLE_PERSIST_VERSION=2 };

};


/*****************************************************************************/
/* DIST TABLE FUNCTION                                                       */
/*****************************************************************************/

struct DistTableFunctionConfig {
    DistTableFunctionConfig(const Url & modelFileUrl = Url(),
            std::vector<Utf8String> statistics = { "count", "avg", "std", "min", "max" })
        : statistics(statistics), modelFileUrl(modelFileUrl)
    {
    }

    std::vector<Utf8String> statistics;
    Url modelFileUrl;
};

DECLARE_STRUCTURE_DESCRIPTION(DistTableFunctionConfig);

struct DistTableFunction: public Function {
    DistTableFunction(MldbServer * owner,
                  PolyConfig config,
                  const std::function<bool (const Json::Value &)> & onProgress);

    ~DistTableFunction();

    virtual Any getStatus() const override;

    virtual Any getDetails() const override;

    ExpressionValue apply(const FunctionApplier & applier,
                              const ExpressionValue & context) const override;

    /** Describe what the input and output is for this function. */
    virtual FunctionInfo getFunctionInfo() const override;

    void increment(const std::vector<std::pair<Utf8String, Utf8String>> & keys,
                   const std::vector<double> & outcomes) const;
    
    void persist(const Url & modelFileUrl) const;

    virtual RestRequestMatchResult
    handleRequest(RestConnection & connection,
                  const RestRequest & request,
                  RestRequestParsingContext & context) const override;

    mutable boost::shared_mutex _access;

    DistTableFunctionConfig functionConfig;
    DistTableMode mode;

    std::string dtStatsNames[DT_NUM_STATISTICS];

    std::vector<DISTTABLE_STATISTICS> activeStats;
    mutable DistTablesMap distTablesMap;

    RestRequestRouter router;
};

} // namespace MLDB

