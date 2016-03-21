// This file is part of MLDB. Copyright 2015 Datacratic. All rights reserved.

/** experiment_procedure.h                                                   -*- C++ -*-
    Francois Maillet, 8 septembre 2015
    Copyright (c) 2015 Datacratic Inc.  All rights reserved.

    Experiment procedure. This is used to train and test a classifier in a
    single step. It abstracts away the need to create a classifier.train/test
    and function but uses those building blocks underneath
*/

#pragma once

#include "types/value_description.h"
#include "server/plugin_resource.h"
#include "server/mldb_server.h"
#include "mldb/core/procedure.h"
#include "sql/sql_expression.h"
#include "plugins/classifier.h"
#include "plugins/accuracy.h"
#include "types/optional.h"

namespace Datacratic {
namespace MLDB {


/*****************************************************************************/
/*  EXPERIMENT PROCEDURE CONFIG                                              */
/*****************************************************************************/

struct DatasetFoldConfig {
    DatasetFoldConfig(
            std::shared_ptr<SqlExpression> training_where = SqlExpression::parse("true"),
            std::shared_ptr<SqlExpression> testing_where = SqlExpression::parse("true"))
        : training_where(training_where),
          testing_where(testing_where),
          orderBy(OrderByExpression::parse("rowHash()")),
          training_offset(0), testing_offset(0),  
          training_limit(-1), testing_limit(-1)
    {
    }

    /// The WHERE clause for which rows to include from the dataset
    std::shared_ptr<SqlExpression> training_where;
    std::shared_ptr<SqlExpression> testing_where;

    /// How to order the rows when using an offset and a limit
    OrderByExpression orderBy;

    /// Where to start running
    ssize_t training_offset;
    ssize_t testing_offset;

    /// Maximum number of rows to use
    ssize_t training_limit;
    ssize_t testing_limit;
};

DECLARE_STRUCTURE_DESCRIPTION(DatasetFoldConfig);


struct ExperimentProcedureConfig : public ProcedureConfig {
    ExperimentProcedureConfig()
        : keepArtifacts(false),
          kfold(0),
          equalizationFactor(0.5),
          mode(CM_BOOLEAN),
          outputAccuracyDataset(true),
          evalTrain(false)
    {
    }

    std::string experimentName;

    bool keepArtifacts;

    /// SQL query to select the training data
    InputQuery trainingData;
    Optional<InputQuery> testingData;

    ssize_t kfold;
    std::vector<DatasetFoldConfig> datasetFolds;

    /// Folder where to save the experiment's result files
    Url modelFileUrlPattern;

    /// Configuration of the algorithm.  If empty, the configurationFile
    /// will be used instead.
    Json::Value configuration;

    /// Filename to load algorithm configuration from.  Default is an
    /// inbuilt file with a few basic configurations.
    std::string configurationFile;

    /// Classifier algorithm to use from configuration file.  Default is
    /// the empty string, ie the root object.
    std::string algorithm;

    /// Equalization factor for rare classes.  Affects the weighting.
    double equalizationFactor;

    /// What mode to run in
    ClassifierMode mode;

    bool outputAccuracyDataset;
    bool evalTrain;
};

DECLARE_STRUCTURE_DESCRIPTION(ExperimentProcedureConfig);


/*****************************************************************************/
/* JSON STATS STATISTICS GENERATOR
 *      Helper class that will be used to generate statistics over
 *      each of our evaluation metrics
 *****************************************************************************/
struct JsStatsStatsGenerator {

    void accumStats(const Json::Value & js, const std::string & path);

    Json::Value generateStatistics() const;

    void generateStatistics(const Json::Value & js,
                            Json::Value & result,
                            const std::string & path) const;

    Json::Value lastObj;
    std::map<std::string, ML::distribution<double>> outputStatsAccum;
};


/*****************************************************************************/
/* EXPERIMENT PROCEDURE                                                      */
/*****************************************************************************/

struct ExperimentProcedure: public Procedure {

    ExperimentProcedure(MldbServer * owner,
                PolyConfig config,
                const std::function<bool (const Json::Value &)> & onProgress);

    virtual RunOutput run(const ProcedureRunConfig & run,
                          const std::function<bool (const Json::Value &)> & onProgress) const;

    virtual Any getStatus() const;

    ExperimentProcedureConfig procConfig;
};

} // namespace MLDB
} // namespace Datacratic
