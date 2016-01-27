/** tfidf.cc
    Mathieu Marquis Bolduc, November 27th, 2015
    Copyright (c) 2015 Datacratic Inc.  All rights reserved.

    This file is part of MLDB. Copyright 2015 Datacratic. All rights reserved.
    
    Implementation of TF-IDF algorithm
*/

#include "tfidf.h"
#include "matrix.h"
#include "mldb/server/mldb_server.h"
#include "mldb/core/dataset.h"
#include "mldb/jml/stats/distribution.h"
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
#include "mldb/vfs/filter_streams.h"

using namespace std;

namespace {
void
serialize(ML::DB::Store_Writer & store, 
          uint64_t corpusSize, 
          const std::unordered_map<Datacratic::Utf8String, uint64_t> & dfs)
{
    std::string name = "tfidf";
    int version = 0;
    store << name << version;
    store << corpusSize;
    for (auto & df : dfs) {
        store << df.first.rawString();
        store << df.second;
    }
}

void
reconstitute(ML::DB::Store_Reader & store, 
             uint64_t & corpusSize,
             std::unordered_map<Datacratic::Utf8String, uint64_t> & dfs)
{
    std::string name;
    store >> name;
    if (name != "tfidf")
        throw ML::Exception("invalid name when loading a tf-idf object");

    int version;
    store >> version;
    if (version != 0)
        throw ML::Exception("invalid tf-idf version");

    store >> corpusSize;
    dfs.clear();
    dfs.reserve(corpusSize);
    for (int i=0; i < corpusSize; ++i) {
        std::string term;
        uint64_t df;
        store >> term;
        store >> df;
        dfs[term] = df;
    }
}

void
save(const std::string & filename,
     uint64_t corpusSize, 
     const std::unordered_map<Datacratic::Utf8String, uint64_t> & dfs)
{
    ML::filter_ostream stream(filename);
    ML::DB::Store_Writer store(stream);
    serialize(store, corpusSize, dfs);
}

void
load(const std::string & filename,
     uint64_t & corpusSize, 
     std::unordered_map<Datacratic::Utf8String, uint64_t> & dfs)
{
    ML::filter_istream stream(filename);
    ML::DB::Store_Reader store(stream);
    reconstitute(store, corpusSize, dfs);
}
}

namespace Datacratic {
namespace MLDB {

DEFINE_ENUM_DESCRIPTION(TFType);
DEFINE_ENUM_DESCRIPTION(IDFType);

TFTypeDescription::
TFTypeDescription()
{
    addValue("raw", TF_raw, "Use raw TF score, the frequency the term appears in the document.");
    addValue("log", TF_log, "Use logarithmic TF score, the logarithm of the frequency the term appears in the document.");
    addValue("augmented", TF_augmented, "Use augmented TF score, the half-frequency the term appears in the document divided by the maximum frequency of any term in the document.");
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
    
    addField("trainingData", &TfidfConfig::trainingData,
             "An SQL query to provide for input to the tfidf procedure."
             "Each row represents the terms in a given document.  Note that "
             "the procedure will not modify the terms in any ways. "
             "As an example, terms with different capitalization 'Montreal', "
             "'montreal' or with accented characters 'Montréal' "
             "will all be considered as different terms.");
    addField("modelFileUrl", &TfidfConfig::modelFileUrl,
             "URL where the model file (with extension '.idf') should be saved. "
             "This file can be loaded by a function of type 'tfidf' to compute "
             "the tf-idf of a given term. "
             "If someone is only interested in the number of documents the "
             "terms in the training input appear in then the parameter can be "
             "omitted and the outputDataset param can be provided instead.");
    addField("outputDataset", &TfidfConfig::output,
             "Output dataset.  This dataset will contain a single row "
             "containing the number of documents each term appears in.",
             optional);
    addField("functionName", &TfidfConfig::functionName,
             "If specified, a function of this name will be created using "
             "the training model.  Note that the 'modelFileUrl' must "
             "also be provided.");
    addParent<ProcedureConfig>();

    onPostValidate = [] (TfidfConfig * cfg, 
                         JsonParsingContext & context) {
        // to create a function we need access to a model
        if(!cfg->modelFileUrl.valid() && !cfg->functionName.empty()) {
            throw ML::Exception("modelFileUrl \"" + cfg->modelFileUrl.toString() 
                                + "\" is not valid.  A valid modelFileUrl parameter "
                                + "is required to create a function.");
        }
    };
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

    auto boundDataset = runProcConf.trainingData.stm->from->bind(context);

    //This will cummulate the number of documents each word is in 
    std::unordered_map<Utf8String, uint64_t> dfs;

    auto aggregator = [&] (const MatrixNamedRow & row)
        {
            for (auto& col : row.columns) {            
                Utf8String word = get<0>(col).toUtf8String();
                dfs[word] += 1;               
            }

            return true;
        };

    iterateDataset(runProcConf.trainingData.stm->select, *boundDataset.dataset, boundDataset.asName, 
                   runProcConf.trainingData.stm->when,
                   *runProcConf.trainingData.stm->where,
                   aggregator,
                   runProcConf.trainingData.stm->orderBy,
                   runProcConf.trainingData.stm->offset,
                   runProcConf.trainingData.stm->limit,
                   onProgress);     

    bool saved = false;
    if (!runProcConf.modelFileUrl.empty()) {
        save(runProcConf.modelFileUrl.toString(), dfs.size(), dfs);
        saved = true;
    }

    if (runProcConf.output) {
        PolyConfigT<Dataset> outputDataset = *runProcConf.output;
        if (outputDataset.type.empty())
            outputDataset.type = TfidfConfig::defaultOutputDatasetType;

        auto output = createDataset(server, outputDataset, onProgress, true /*overwrite*/);

        Date applyDate = Date::now();

        std::vector<std::tuple<ColumnName, CellValue, Date> > row;
        row.reserve(dfs.size());

        for (auto& df : dfs) {
            row.emplace_back(ColumnName(df.first), df.second/*(float)count*/, applyDate);
        }

        output->recordRow(RowName("Number of Documents with Word"), row);        
        output->commit();
    }

    if(!runProcConf.functionName.empty()) {
        if (saved) {
            TfidfFunctionConfig tfidfFunctionConf;
            tfidfFunctionConf.modelFileUrl = runProcConf.modelFileUrl;
            
            PolyConfig tfidfFuncPC;
            tfidfFuncPC.type = "tfidf";
            tfidfFuncPC.id = runProcConf.functionName;
            tfidfFuncPC.params = tfidfFunctionConf;

            obtainFunction(server, tfidfFuncPC, onProgress);
        } else {
            throw ML::Exception("Can't create tfidf function " +
                                runProcConf.functionName.rawString() + 
                                " Have you provided a valid modelFileUrl?");
        }
    }

    return Any();
}

DEFINE_STRUCTURE_DESCRIPTION(TfidfFunctionConfig);

TfidfFunctionConfigDescription::
TfidfFunctionConfigDescription()
{
    addField("modelFileUrl", &TfidfFunctionConfig::modelFileUrl,
             "An URL to a model file previously created with a 'tfidf.train' procedure.");
    addField("tfType", &TfidfFunctionConfig::tf_type,
             "Type of TF scoring", TF_raw);
    addField("idfType", &TfidfFunctionConfig::idf_type,
             "Type of IDF scoring", IDF_inverseSmooth);

    onPostValidate = [] (TfidfFunctionConfig * cfg, 
                         JsonParsingContext & context) {
        // this includes empty url
        if(!cfg->modelFileUrl.valid()) {
            throw ML::Exception("modelFileUrl \"" + cfg->modelFileUrl.toString() 
                                + "\" is not valid");
        }
    };
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
    load(functionConfig.modelFileUrl.toString(), corpusSize, dfs);
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

    uint64_t maxFrequency = 0; // max term frequency for the current document
    uint64_t maxNt = 0;        // max document frequency for terms in the current doc

    for (auto& col : inputVal.getRow() ) {
        Utf8String term = std::get<0>(col).toUtf8String();
        uint64_t value = std::get<1>(col).getAtom().toUInt();
        maxFrequency = std::max(value, maxFrequency);
        const auto termFrequency = dfs.find(term);
        if (termFrequency != dfs.end())
            maxNt = std::max(maxNt, termFrequency->second); 
    }

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
        return std::log(corpusSize / (1 + numberOfRelevantDoc));
    };
    auto idf_inverseSmooth = [=] (double numberOfRelevantDoc) {
        return std::log(1 + (corpusSize / (1 + numberOfRelevantDoc)));
    };
    auto idf_inverseMax = [=] (double numberOfRelevantDoc) {
        return std::log(1 + (maxNt) / (1 + numberOfRelevantDoc));
    };
    auto idf_probabilistic_inverse = [=] (double numberOfRelevantDoc) {
        return std::log((corpusSize - numberOfRelevantDoc) / (1 + numberOfRelevantDoc));
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

    RowValue values;
    Date ts = inputVal.getEffectiveTimestamp();

    // Compute the score for every word in the input
    for (auto& col : inputVal.getRow() ) {
        Utf8String term = std::get<0>(col).toUtf8String(); // the term is the columnName
        double frequency = (double) std::get<1>(col).getAtom().toUInt();

        double tf = tf_fct(frequency);
        const auto docFrequency = dfs.find(term);
        double idf = (docFrequency != dfs.end() 
                      ? idf_fct(docFrequency->second) 
                      : idf_fct(0)); 

        //cerr << term << " tf " << tf << " idf " << idf << endl;
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

