// This file is part of MLDB. Copyright 2015 Datacratic. All rights reserved.

/** tfidf.cc
    Mathieu Marquis Bolduc, November 27th, 2015
    Copyright (c) 2015 Datacratic Inc.  All rights reserved.

    Implementation of TF-IDF algorithm
*/

#include "tfidf.h"
#include "matrix.h"
#include "mldb/server/mldb_server.h"
#include "mldb/core/dataset.h"
#include "mldb/jml/stats/distribution.h"
#include <boost/multi_array.hpp>
#include "mldb/jml/utils/guard.h"
#include "mldb/jml/utils/worker_task.h"
#include "mldb/jml/utils/pair_utils.h"
#include "mldb/jml/utils/vector_utils.h"
#include "mldb/types/basic_value_descriptions.h"
#include "mldb/ml/value_descriptions.h"
#include "mldb/sql/sql_expression.h"
#include "mldb/server/analytics.h"
#include "mldb/types/any_impl.h"
#include "mldb/types/optional_description.h"

using namespace std;


namespace Datacratic {
namespace MLDB {

DEFINE_ENUM_DESCRIPTION(TFType);
DEFINE_ENUM_DESCRIPTION(IDFType);

TFTypeDescription::
TFTypeDescription()
{
    addValue("raw", TF_raw, "Use raw TF score, the frequency the term appears in the document.");
    addValue("log", TF_log, "Use logarithmic TF score, the logarithm of the frequency the term appears in the document.");
    addValue("augmented", TF_augmented, "Use augmented TF Score, the half-frequency the term appears in the document divided by the maximum frequency of any term in the document.");
}

IDFTypeDescription::
IDFTypeDescription()
{
    addValue("unary", IDF_unary, "Use unary IDF score, i.e. don't use IDF.");
    addValue("inverse", IDF_inverse, "Use inverse IDF, the logarithm of the number of document in the corpus divided by the number of documents the term appears in.");
    addValue("inverseSmooth", IDF_inverseSmooth, "Use inverse-smooth IDF. Similar to inverse but with logarithmic terms above 1.");
    addValue("inverseMax", IDF_inverseMax, "Use inverse-max IDF, similar as inverse-smooth but using the maximum term frequency.");
    addValue("probabilisticInverse", IDF_probabilistic_inverse, "Use probabilistic inverse IDF, similar to inverse but substracting the number of documents the term appears in from the total number of documents.");
}

DEFINE_STRUCTURE_DESCRIPTION(TfidfConfig);

TfidfConfigDescription::
TfidfConfigDescription()
{
    Optional<PolyConfigT<Dataset> > optional;
    optional.emplace(PolyConfigT<Dataset>().
                     withType(TfidfConfig::defaultOutputDatasetType));
    
    addFieldDesc("trainingDataset", &TfidfConfig::dataset,
                 "Dataset provided for input to the tfidf procedure."
                 "Contains the list of terms for each document.",
                 makeInputDatasetDescription());
    addField("outputDataset", &TfidfConfig::output,
             "Output dataset.  This dataset will contain a single row "
             "containing the number of documents each term appears in");
    addField("select", &TfidfConfig::select,
             "Columns to select from the input matrix for the coordinates to input "
             "into training.  The selected columns must be finite numbers "
             "and must not have missing values.",
             SelectExpression::STAR);
    addField("when", &TfidfConfig::when,
             "Boolean expression determining which tuples from the dataset "
             "to keep based on their timestamps",
             WhenExpression::TRUE);
    addField("where", &TfidfConfig::where,
             "Rows to select for training.  This expression allows a subset "
             "of the rows that were input to the training process to be selected.",
             SqlExpression::TRUE);
    addField("orderBy", &TfidfConfig::orderBy,
             "How to order the rows.  This only has an effect when OFFSET "
             "or LIMIT are used.  Default is to order by rowHash.",
             OrderByExpression::ROWHASH);
    addField("offset", &TfidfConfig::offset,
             "How many rows to skip before using data",
             ssize_t(0));
    addField("limit", &TfidfConfig::limit,
             "How many rows of data to use.  -1 (the default) means use all "
             "of the rows in the dataset.",
             ssize_t(-1));
    addField("functionName", &TfidfConfig::functionName,
             "If specified, a function of this name will be created using "
             "the training result.");
    addParent<ProcedureConfig>();
}


/*****************************************************************************/
/* TFIDF PROCEDURE                                                           */
/*****************************************************************************/

TfidfProcedure::
TfidfProcedure(MldbServer * owner,
               PolyConfig config,
               const std::function<bool (const Json::Value &)> & onProgress)
    : Procedure(owner)
{
    tfidfconfig = config.params.convert<TfidfConfig>();
}

Any
TfidfProcedure::
getStatus() const
{
    return Any();
}

RunOutput
TfidfProcedure::
run(const ProcedureRunConfig & run,
      const std::function<bool (const Json::Value &)> & onProgress) const
{
    auto runProcConf = applyRunConfOverProcConf(tfidfconfig, run);

    SqlExpressionMldbContext context(server);

    auto boundDataset = runProcConf.dataset->bind(context);

    //This will cummulate the number of documents each word is in 
    std::unordered_map<Utf8String, int> bagOfWords;

    auto aggregator = [&] (const MatrixNamedRow & row)
        {
            for (auto& col : row.columns) {                
                Utf8String word = get<0>(col).toUtf8String();
                bagOfWords[word] += 1;               
            }

            return true;
        };

    iterateDataset(runProcConf.select, *boundDataset.dataset, boundDataset.asName, 
                   runProcConf.when,
                   runProcConf.where,
                   aggregator,
                   runProcConf.orderBy,
                   runProcConf.offset,
                   runProcConf.limit,
                   onProgress);     

    PolyConfigT<Dataset> outputDataset = runProcConf.output;
    if (outputDataset.type.empty())
        outputDataset.type = TfidfConfig::defaultOutputDatasetType;

    auto output = createDataset(server, outputDataset, onProgress, true /*overwrite*/);

    Date applyDate = Date::now();

    std::vector<std::tuple<ColumnName, CellValue, Date> > row;
    row.reserve(bagOfWords.size());

    for (auto& val : bagOfWords) {
        row.emplace_back(ColumnName(val.first), val.second/* / (float)count*/, applyDate);
    }

    output->recordRow(RowName("Number of Documents with Word"), row);        
    output->commit();

    if(!runProcConf.functionName.empty()) {

        PolyConfig tfidfFuncPC;
        tfidfFuncPC.type = "tfidf";
        tfidfFuncPC.id = runProcConf.functionName;
        tfidfFuncPC.params = TfidfFunctionConfig(runProcConf.output, boundDataset.dataset->getMatrixView()->getRowCount());

        obtainFunction(server, tfidfFuncPC, onProgress);
    }

    return Any();
}

DEFINE_STRUCTURE_DESCRIPTION(TfidfFunctionConfig);

TfidfFunctionConfigDescription::
TfidfFunctionConfigDescription()
{
    addField("dataset", &TfidfFunctionConfig::dataset,
             "Dataset describing the number of document each term appears in.");    
    addField("sizeOfCorpus", &TfidfFunctionConfig::N,
             "Number of documents in the corpus");
    addField("tfType", &TfidfFunctionConfig::tf_type,
             "Type of TF scoring", TF_log);
    addField("idfType", &TfidfFunctionConfig::idf_type,
             "Type of IDF scoring", IDF_inverse);
}


/*****************************************************************************/
/* TFIDF FUNCTION                                                            */
/*****************************************************************************/

TfidfFunction::
TfidfFunction(MldbServer * owner,
            PolyConfig config,
            const std::function<bool (const Json::Value &)> & onProgress)
    : Function(owner)
{
    functionConfig = config.params.convert<TfidfFunctionConfig>();
    dataset = obtainDataset(server, functionConfig.dataset, onProgress);
}

Any
TfidfFunction::
getStatus() const
{
    return Any();
}

FunctionOutput
TfidfFunction::
apply(const FunctionApplier & applier,
      const FunctionContext & context) const
{
    FunctionOutput result;

    ExpressionValue storage;
    const ExpressionValue & inputVal = context.get("input", storage);

    SelectExpression select;
    size_t documentCount = 0;
    double maxFrequency = 0;

    select.clauses.reserve(inputVal.getRow().size());

    for (auto& col : inputVal.getRow() ) {

        Utf8String colName = std::get<0>(col).toUtf8String();
        int64_t value = std::get<1>(col).toInt();
        maxFrequency = std::max((double)value, maxFrequency);
        documentCount += value;
        auto expr = std::make_shared<ReadVariableExpression>("", colName);
        auto variableExpr = std::make_shared<ComputedVariable>(colName, expr);
        select.clauses.push_back(variableExpr);
    }

    maxFrequency /= documentCount;

     auto onProgress = [&] (const Json::Value & progress)
        {
            return true;
        };

    int64_t maxNt = 0;

    // Get values from the corpus dataset only for the words in the input
    // i.e. a subset of the dataset.
    std::unordered_map<Utf8String, int64_t> bagOfWords;

     //Check values in the corpus
     auto aggregator = [&] (const MatrixNamedRow & row)
        {
            for (auto& col : row.columns) {         

                Utf8String word = get<0>(col).toUtf8String();
                const CellValue& cell = get<1>(col);
                maxNt = std::max(maxNt, cell.toInt());
                bagOfWords[word] = cell.toInt();               
            }

            return true;
        };

    iterateDataset(select, *dataset, "", 
                   WhenExpression::TRUE,
                   SqlExpression::TRUE,
                   aggregator,
                   ORDER_BY_NOTHING,
                   0,
                   -1,
                   onProgress);     

    RowValue values;
    Date ts = inputVal.getEffectiveTimestamp();
    double N = functionConfig.N;

    // the different possible TF scores
    auto tf_raw = [=] (double frequency) {
        return frequency;
    };
    auto tf_log = [=] (double frequency) {
        return (std::log(1.0f + frequency));
    };
    auto tf_augmented = [=] (double frequency) {
        return 0.5f + (0.5f * frequency) / maxFrequency;
    };

    std::function<double(double)> tf_fct = tf_raw;

    switch (functionConfig.tf_type)
    {
        case TF_log:
            tf_fct = tf_log;
        break;
        case TF_augmented:
            tf_fct = tf_augmented;
        break;
        default:
        break;
    }

    // the different possible IDF scores
    auto idf_unary = [=] (double numberOfRelevantDoc) {
        return 1.0f;
    };
    auto idf_inverse = [=] (double numberOfRelevantDoc) {
        return std::log((N +1 )/ (1 + numberOfRelevantDoc));
    };
    auto idf_inverseSmooth = [=] (double numberOfRelevantDoc) {
        return std::log(1 + (N +1 )/ (1 + numberOfRelevantDoc));
    };
    auto idf_inverseMax = [=] (double numberOfRelevantDoc) {
        return std::log(1 + (maxNt)/ (1 + numberOfRelevantDoc));
    };
    auto idf_probabilistic_inverse = [=] (double numberOfRelevantDoc) {
        return std::log((N - numberOfRelevantDoc) / (1 + numberOfRelevantDoc));
    };

    std::function<double(double)> idf_fct = idf_unary;

    switch (functionConfig.idf_type)
    {
        case IDF_inverse:
            idf_fct = idf_inverse;
        break;
        case IDF_inverseSmooth:
            idf_fct = idf_inverseSmooth;
        break;
        case IDF_inverseMax:
            idf_fct = idf_inverseMax;
        break;
        case IDF_probabilistic_inverse:
            idf_fct = idf_probabilistic_inverse;
        break;
        default:
        break;
    }

    // Compute the score for every word in the input
    for (auto& col : inputVal.getRow() ) {
        Utf8String colName = std::get<0>(col).toUtf8String();

        double frequency = std::get<1>(col).toDouble() / documentCount;
        double tf = tf_fct(frequency);
        double idf = idf_fct(bagOfWords[colName]);
        values.emplace_back(std::get<0>(col),
                            tf*idf,
                            ts);
    }

    ExpressionValue outputRow(values);
    result.set("output", outputRow);
    
    return result;
}

FunctionInfo
TfidfFunction::
getFunctionInfo() const
{
    FunctionInfo result;

    result.input.addRowValue("input");
    result.output.addRowValue("output");

    return result;
}

namespace {

RegisterProcedureType<TfidfProcedure, TfidfConfig>
regTfidf(builtinPackage(),
          "tfidf.train",
          "Prepare data for a TF-IDF function",
          "procedures/TfidfProcedure.md.html");

RegisterFunctionType<TfidfFunction, TfidfFunctionConfig>
regTfidfFunction(builtinPackage(),
                  "tfidf",
                  "Apply a TF-IDF scoring to a bag of words",
                  "functions/Tfidf.md.html");

} // file scope

} // namespace MLDB
} // namespace Datacratic

