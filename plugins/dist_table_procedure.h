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

namespace Datacratic {
namespace MLDB {




/*****************************************************************************/
/* STATS TABLE                                                               */
/*****************************************************************************/

struct DistTable {

    DistTable(const ColumnName & colName=ColumnName("ND"),
            const std::vector<std::string> & outcome_names = {})
        : colName(colName), outcome_names(outcome_names),
          zeroCounts(std::make_pair(0, std::vector<int64_t>(outcome_names.size())))
    {
    }

    DistTable(const std::string & filename);

    // .first : nb trial
    // .second : nb of occurence of each outcome
    typedef std::pair<int64_t, std::vector<int64_t>> BucketCounts;
    const BucketCounts & increment(const CellValue & val,
                                   const std::vector<uint> & outcomes);
    const BucketCounts & getCounts(const CellValue & val) const;

    void save(const std::string & filename) const;
    void serialize(ML::DB::Store_Writer & store) const;
    void reconstitute(ML::DB::Store_Reader & store);

    ColumnName colName;

    std::vector<std::string> outcome_names;
    std::unordered_map<Utf8String, BucketCounts> counts;

    BucketCounts zeroCounts;
};




/*****************************************************************************/
/* STATS TABLE PROCEDURE CONFIG                                              */
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
/* STATS TABLE PROCEDURE                                                     */
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
/* STATS TABLE FUNCTION                                                      */
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

    DistTablesMap distTables;
};


/*****************************************************************************/
/* STATS TABLE DERIVED COLS FUNCTION                                         */
/*****************************************************************************/

struct DistTableDerivedColumnsGeneratorProcedureConfig: public ProcedureConfig {
    static constexpr const char * name = "experimental.distTable.derivedColumnsGenerator";

    DistTableDerivedColumnsGeneratorProcedureConfig(
            const Url & modelFileUrl = Url())
        : modelFileUrl(modelFileUrl)
    {
    }

    std::string functionId;
    std::string expression;
    Url modelFileUrl;
};

DECLARE_STRUCTURE_DESCRIPTION(DistTableDerivedColumnsGeneratorProcedureConfig);


struct DistTableDerivedColumnsGeneratorProcedure: public Procedure {

    DistTableDerivedColumnsGeneratorProcedure(MldbServer * owner,
                PolyConfig config,
                const std::function<bool (const Json::Value &)> & onProgress);

    virtual RunOutput run(const ProcedureRunConfig & run,
                          const std::function<bool (const Json::Value &)> & onProgress) const;

    virtual Any getStatus() const;

    DistTableDerivedColumnsGeneratorProcedureConfig procConfig;
};


/*****************************************************************************/
/* BOW STATS TABLE PROCEDURE CONFIG                                          */
/*****************************************************************************/

struct BagOfWordsDistTableProcedureConfig : ProcedureConfig {

    static constexpr const char * name = "distTable.bagOfWords.train";

    InputQuery trainingData;

    /// The expression to generate the outcomes
    std::vector<std::pair<std::string, std::shared_ptr<SqlExpression>>> outcomes;

    Url modelFileUrl;

    Utf8String functionName;
    std::string functionOutcomeToUse;

    Optional<PolyConfigT<Dataset>> outputDataset;

    static constexpr char const * defaultOutputDatasetType = "tabular";
};

DECLARE_STRUCTURE_DESCRIPTION(BagOfWordsDistTableProcedureConfig);


/*****************************************************************************/
/* BOW STATS TABLE PROCEDURE                                                 */
/*****************************************************************************/

struct BagOfWordsDistTableProcedure: public Procedure {

    BagOfWordsDistTableProcedure(MldbServer * owner,
                PolyConfig config,
                const std::function<bool (const Json::Value &)> & onProgress);

    virtual RunOutput run(const ProcedureRunConfig & run,
                          const std::function<bool (const Json::Value &)> & onProgress) const;

    virtual Any getStatus() const;

    BagOfWordsDistTableProcedureConfig procConfig;
};


/*****************************************************************************/
/* STATS TABLE POS NEG FUNCTION                                              */
/*****************************************************************************/

struct DistTablePosNegFunctionConfig {
    DistTablePosNegFunctionConfig(const Url & modelFileUrl = Url(),
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

DECLARE_STRUCTURE_DESCRIPTION(DistTablePosNegFunctionConfig);

struct DistTablePosNegFunction: public Function {
    DistTablePosNegFunction(MldbServer * owner,
                  PolyConfig config,
                  const std::function<bool (const Json::Value &)> & onProgress);

    ~DistTablePosNegFunction();

    virtual Any getStatus() const;

    virtual Any getDetails() const;

    virtual ExpressionValue apply(const FunctionApplier & applier,
                              const ExpressionValue & context) const;

    /** Describe what the input and output is for this function. */
    virtual FunctionInfo getFunctionInfo() const;

    DistTablePosNegFunctionConfig functionConfig;


    std::map<Utf8String, float> p_outcomes;
};




} // namespace MLDB
} // namespace Datacratic
