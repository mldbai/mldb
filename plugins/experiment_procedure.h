/** experiment_procedure.h                                                   -*- C++ -*-
    Francois Maillet, 8 septembre 2015
    Copyright (c) 2015 mldb.ai inc.  All rights reserved.
    This file is part of MLDB. Copyright 2015 mldb.ai inc. All rights reserved.

    Experiment procedure. This is used to train and test a classifier in a
    single step. It abstracts away the need to create a classifier.train/test
    and function but uses those building blocks underneath
*/

#pragma once

#include "types/value_description_fwd.h"
#include "server/plugin_resource.h"
#include "server/mldb_server.h"
#include "mldb/core/procedure.h"
#include "sql/sql_expression.h"
#include "plugins/classifier.h"
#include "plugins/accuracy.h"
#include "types/optional.h"


namespace MLDB {


/*****************************************************************************/
/*  EXPERIMENT PROCEDURE CONFIG                                              */
/*****************************************************************************/

struct DatasetFoldConfig {

    DatasetFoldConfig(
            std::shared_ptr<SqlExpression> trainingWhere = SqlExpression::parse("rowHash()"),
            std::shared_ptr<SqlExpression> testingWhere = SqlExpression::parse("rowHash()"))
        : trainingWhere(trainingWhere),
        testingWhere(testingWhere)
        {
        }

    /// The WHERE clause for which rows to include from the dataset
    std::shared_ptr<SqlExpression> trainingWhere;
    std::shared_ptr<SqlExpression> testingWhere;

    /// How to order the rows when using an offset and a limit
    OrderByExpression trainingOrderBy = OrderByExpression::parse("true");
    OrderByExpression testingOrderBy = OrderByExpression::parse("true");

    /// Where to start running
    ssize_t trainingOffset = 0;
    ssize_t testingOffset = 0;

    /// Maximum number of rows to use
    ssize_t trainingLimit = -1;
    ssize_t testingLimit = -1;
};

DECLARE_STRUCTURE_DESCRIPTION(DatasetFoldConfig);


struct ExperimentProcedureConfig : public ProcedureConfig {

    static constexpr const char * name = "classifier.experiment";

    std::string experimentName;

    bool keepArtifacts = false;

    /// SQL query to select the training data
    InputQuery inputData;
    Optional<InputQuery> testingDataOverride;

    ssize_t kfold = 0;
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
    double equalizationFactor = 0.5;

    /// What mode to run in
    ClassifierMode mode = CM_BOOLEAN;

    // Strategy to handle multilabel
    MultilabelStrategy multilabelStrategy = MULTILABEL_ONEVSALL;

    int accuracyOverN = 1;

    bool outputAccuracyDataset = true;
    bool uniqueScoresOnly = false;
    bool evalTrain = false;
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
    std::map<std::string, distribution<double>> outputStatsAccum;
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

