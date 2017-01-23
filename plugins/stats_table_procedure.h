/** stats_table_procedure.h                                                   -*- C++ -*-
    Francois Maillet, 2 septembre 2015
    Copyright (c) 2015 mldb.ai inc.  All rights reserved.
    This file is part of MLDB. Copyright 2015 mldb.ai inc. All rights reserved.

    StatsTable procedure
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


namespace MLDB {




/*****************************************************************************/
/* STATS TABLE                                                               */
/*****************************************************************************/

struct StatsTable {

    StatsTable(const ColumnPath & colName=ColumnPath("ND"),
            const std::vector<std::string> & outcome_names = {})
        : colName(colName), outcome_names(outcome_names),
          zeroCounts(std::make_pair(0, std::vector<int64_t>(outcome_names.size())))
    {
    }

    StatsTable(const std::string & filename);

    // .first : nb trial
    // .second : nb of occurence of each outcome
    typedef std::pair<int64_t, std::vector<int64_t>> BucketCounts;
    const BucketCounts & increment(const CellValue & val,
                                   const std::vector<uint> & outcomes);
    const BucketCounts & getCounts(const CellValue & val) const;

    void save(const std::string & filename) const;
    void serialize(ML::DB::Store_Writer & store) const;
    void reconstitute(ML::DB::Store_Reader & store);

    ColumnPath colName;

    std::vector<std::string> outcome_names;
    std::unordered_map<Utf8String, BucketCounts> counts;

    BucketCounts zeroCounts;
};




/*****************************************************************************/
/* STATS TABLE PROCEDURE CONFIG                                              */
/*****************************************************************************/

struct StatsTableProcedureConfig : public ProcedureConfig {
    static constexpr const char * name = "statsTable.train";

    StatsTableProcedureConfig()
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

DECLARE_STRUCTURE_DESCRIPTION(StatsTableProcedureConfig);


/*****************************************************************************/
/* STATS TABLE PROCEDURE                                                     */
/*****************************************************************************/

typedef std::map<ColumnPath, StatsTable> StatsTablesMap;

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
    StatsTableFunctionConfig(const Url & modelFileUrl = Url())
        : modelFileUrl(modelFileUrl)
    {
    }

    Url modelFileUrl;
};

DECLARE_STRUCTURE_DESCRIPTION(StatsTableFunctionConfig);

struct StatsTableFunction: public Function {
    StatsTableFunction(MldbServer * owner,
                  PolyConfig config,
                  const std::function<bool (const Json::Value &)> & onProgress);

    ~StatsTableFunction();

    virtual Any getStatus() const;

    virtual Any getDetails() const;

    virtual ExpressionValue apply(const FunctionApplier & applier,
                              const ExpressionValue & context) const;

    /** Describe what the input and output is for this function. */
    virtual FunctionInfo getFunctionInfo() const;

    StatsTableFunctionConfig functionConfig;

    StatsTablesMap statsTables;
};


/*****************************************************************************/
/* STATS TABLE DERIVED COLS FUNCTION                                         */
/*****************************************************************************/

struct StatsTableDerivedColumnsGeneratorProcedureConfig: public ProcedureConfig {
    static constexpr const char * name = "experimental.statsTable.derivedColumnsGenerator";

    StatsTableDerivedColumnsGeneratorProcedureConfig(
            const Url & modelFileUrl = Url())
        : modelFileUrl(modelFileUrl)
    {
    }

    std::string functionId;
    std::string expression;
    Url modelFileUrl;
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


/*****************************************************************************/
/* BOW STATS TABLE PROCEDURE CONFIG                                          */
/*****************************************************************************/

struct BagOfWordsStatsTableProcedureConfig : ProcedureConfig {

    static constexpr const char * name = "statsTable.bagOfWords.train";

    InputQuery trainingData;

    /// The expression to generate the outcomes
    std::vector<std::pair<std::string, std::shared_ptr<SqlExpression>>> outcomes;

    Url modelFileUrl;

    Utf8String functionName;
    std::string functionOutcomeToUse;

    Optional<PolyConfigT<Dataset>> outputDataset;

    static constexpr char const * defaultOutputDatasetType = "tabular";
};

DECLARE_STRUCTURE_DESCRIPTION(BagOfWordsStatsTableProcedureConfig);


/*****************************************************************************/
/* BOW STATS TABLE PROCEDURE                                                 */
/*****************************************************************************/

struct BagOfWordsStatsTableProcedure: public Procedure {

    BagOfWordsStatsTableProcedure(MldbServer * owner,
                PolyConfig config,
                const std::function<bool (const Json::Value &)> & onProgress);

    virtual RunOutput run(const ProcedureRunConfig & run,
                          const std::function<bool (const Json::Value &)> & onProgress) const;

    virtual Any getStatus() const;

    BagOfWordsStatsTableProcedureConfig procConfig;
};


/*****************************************************************************/
/* STATS TABLE POS NEG FUNCTION                                              */
/*****************************************************************************/

struct StatsTablePosNegFunctionConfig {
    StatsTablePosNegFunctionConfig(const Url & modelFileUrl = Url(),
            const std::string & outcomeToUse = "") :
        numPos(50), numNeg(50), minTrials(50),
        outcomeToUse(outcomeToUse),
        modelFileUrl(modelFileUrl)
    {
    }

    ssize_t numPos;
    ssize_t numNeg;
    ssize_t minTrials;

    std::string outcomeToUse;

    Url modelFileUrl;
};

DECLARE_STRUCTURE_DESCRIPTION(StatsTablePosNegFunctionConfig);

struct StatsTablePosNegFunction: public Function {
    StatsTablePosNegFunction(MldbServer * owner,
                  PolyConfig config,
                  const std::function<bool (const Json::Value &)> & onProgress);

    ~StatsTablePosNegFunction();

    virtual Any getStatus() const;

    virtual Any getDetails() const;

    virtual ExpressionValue apply(const FunctionApplier & applier,
                              const ExpressionValue & context) const;

    /** Describe what the input and output is for this function. */
    virtual FunctionInfo getFunctionInfo() const;

    StatsTablePosNegFunctionConfig functionConfig;


    std::map<Utf8String, float> p_outcomes;
};




} // namespace MLDB

