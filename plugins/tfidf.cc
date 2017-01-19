/** tfidf.cc
    Mathieu Marquis Bolduc, November 27th, 2015
    Copyright (c) 2015 mldb.ai inc.  All rights reserved.

    This file is part of MLDB. Copyright 2015 mldb.ai inc. All rights reserved.

    Implementation of TF-IDF algorithm
*/

#include "tfidf.h"
#include "matrix.h"
#include "mldb/server/mldb_server.h"
#include "mldb/core/dataset.h"
#include "mldb/jml/stats/distribution.h"
#include "mldb/jml/utils/guard.h"
#include "mldb/base/parallel.h"
#include "mldb/jml/utils/pair_utils.h"
#include "mldb/jml/utils/vector_utils.h"
#include "mldb/types/basic_value_descriptions.h"
#include "mldb/ml/value_descriptions.h"
#include "mldb/sql/sql_expression.h"
#include "mldb/server/analytics.h"
#include "mldb/types/any_impl.h"
#include "mldb/types/optional_description.h"
#include "mldb/vfs/filter_streams.h"
#include "mldb/vfs/fs_utils.h"
#include "mldb/plugins/sql_config_validator.h"
#include "mldb/utils/log.h"

using namespace std;

namespace {
void
serialize(ML::DB::Store_Writer & store,
          uint64_t corpusSize,
          const std::unordered_map<MLDB::Utf8String, uint64_t> & dfs)
{
    std::string name = "tfidf";
    int version = 0;
    store << name << version;
    store << corpusSize; // number of documents in corpus
    store << dfs.size(); // number of terms in corpus
    for (auto & df : dfs) {
        store << df.first.rawString();
        store << df.second;
    }
}

void
reconstitute(ML::DB::Store_Reader & store,
             uint64_t & corpusSize,
             std::unordered_map<MLDB::Utf8String, uint64_t> & dfs)
{
    std::string name;
    store >> name;
    if (name != "tfidf")
        throw MLDB::Exception("invalid name when loading a tf-idf object");

    int version;
    store >> version;
    if (version != 0)
        throw MLDB::Exception("invalid tf-idf version");

    uint64_t termCount = 0;

    store >> corpusSize; // number of documents in corpus
    store >> termCount; // number of terms in corpus
    dfs.clear();
    dfs.reserve(termCount);
    for (int i=0; i < termCount; ++i) {
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
     const std::unordered_map<MLDB::Utf8String, uint64_t> & dfs)
{
    MLDB::filter_ostream stream(filename);
    ML::DB::Store_Writer store(stream);
    serialize(store, corpusSize, dfs);
}

void
load(const std::string & filename,
     uint64_t & corpusSize,
     std::unordered_map<MLDB::Utf8String, uint64_t> & dfs)
{
    MLDB::filter_istream stream(filename);
    ML::DB::Store_Reader store(stream);
    reconstitute(store, corpusSize, dfs);
}
}


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
             "An SQL query to provide for input to the tfidf procedure. "
             "Rows represent documents, and column names are terms. "
             "If a cell contains anything other than `null` the document will be "
             "take to contain the term.  Note that "
             "this procedure will not normalize the terms in any ways: for example, "
             "terms with different capitalization 'Montreal', "
             "'montreal' or with accented characters 'Montréal' "
             "will all be considered to be different terms.");
    addField("outputDataset", &TfidfConfig::output,
             "This dataset will contain one row for each "
             "term in the input.  The row name will be the term "
             "and the column `count` will contain the number of documents "
             "containing the term.",
             optional);
    addField("modelFileUrl", &TfidfConfig::modelFileUrl,
             "URL where the model file (with extension '.idf') should be saved. "
             "This file can be loaded by the ![](%%doclink tfidf function). "
             "This parameter is optional unless the `functionName` parameter is used.");
    addField("functionName", &TfidfConfig::functionName,
             "If specified, an instance of the ![](%%doclink tfidf function) of this name will be created using "
             "the trained model. Note that to use this parameter, the `modelFileUrl` must "
             "also be provided.");
    addParent<ProcedureConfig>();

    onPostValidate = chain(validateQuery(&TfidfConfig::trainingData,
                                         MustContainFrom()),
                           validateFunction<TfidfConfig>());
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

    if (!runProcConf.modelFileUrl.empty()) {
        checkWritability(runProcConf.modelFileUrl.toDecodedString(),
                         "modelFileUrl");
    }

    SqlExpressionMldbScope context(server);

    ConvertProgressToJson convertProgressToJson(onProgress);
    auto boundDataset = runProcConf.trainingData.stm->from->bind(context, convertProgressToJson);

    //This will cummulate the number of documents each word is in
    std::unordered_map<Utf8String, uint64_t> dfs;
    std::atomic<uint64_t> corpusSize(0);

    auto processor = [&] (NamedRowValue & row_)
        {
            MatrixNamedRow row = row_.flattenDestructive();
            for (auto& col : row.columns) {
                Utf8String word = get<0>(col).toUtf8String();
                dfs[word] += 1;
            }
            ++corpusSize;

            return true;
        };

    iterateDataset(runProcConf.trainingData.stm->select, *boundDataset.dataset, boundDataset.asName,
                   runProcConf.trainingData.stm->when,
                   *runProcConf.trainingData.stm->where,
                   {processor,false/*processInParallel*/},
                   runProcConf.trainingData.stm->orderBy,
                   runProcConf.trainingData.stm->offset,
                   runProcConf.trainingData.stm->limit,
                   convertProgressToJson);

    bool saved = false;
    if (!runProcConf.modelFileUrl.empty()) {
        try {
            makeUriDirectory(
                runProcConf.modelFileUrl.toDecodedString());
            save(runProcConf.modelFileUrl.toDecodedString(), corpusSize, dfs);
            saved = true;
        }
        catch (const std::exception & exc) {
             throw HttpReturnException(400, "Error saving tfidf at location'" +
                                      runProcConf.modelFileUrl.toString() + "': " +
                                      exc.what());
        }
    }

    if (runProcConf.output) {
        PolyConfigT<Dataset> outputDataset = *runProcConf.output;
        if (outputDataset.type.empty())
            outputDataset.type = TfidfConfig::defaultOutputDatasetType;

        auto output = createDataset(server, outputDataset, onProgress, true /*overwrite*/);

        Date applyDate = Date::now();
        ColumnPath columnName(PathElement("count"));

        for (auto & df : dfs) {
            std::vector<std::tuple<ColumnPath, CellValue, Date> > columns;
            columns.emplace_back(columnName, df.second, applyDate);
            output->recordRow(PathElement(df.first), columns);
        }
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
            throw HttpReturnException(400, "Can't create tfidf function '" +
                                      runProcConf.functionName.rawString() +
                                      "'. Have you provided a valid modelFileUrl?",
                                      "modelFileUrl", runProcConf.modelFileUrl.toString());
        }
    }

    return Any();
}

DEFINE_STRUCTURE_DESCRIPTION(TfidfFunctionConfig);

TfidfFunctionConfigDescription::
TfidfFunctionConfigDescription()
{
    addField("modelFileUrl", &TfidfFunctionConfig::modelFileUrl,
             "URL of the model file (with extension '.idf') to load. "
             "This file is created by the ![](%%doclink tfidf.train procedure)."
             );
    addField("tfType", &TfidfFunctionConfig::tf_type,
             "Type of TF scoring", TF_raw);
    addField("idfType", &TfidfFunctionConfig::idf_type,
             "Type of IDF scoring", IDF_inverseSmooth);

    onPostValidate = [] (TfidfFunctionConfig * cfg,
                         JsonParsingContext & context) {
        // this includes empty url
        if(!cfg->modelFileUrl.valid()) {
            throw MLDB::Exception("modelFileUrl \"" + cfg->modelFileUrl.toString()
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
    : Function(owner, config)
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

ExpressionValue
TfidfFunction::
apply(const FunctionApplier & applier,
      const ExpressionValue & context) const
{
    ExpressionValue result;

    ExpressionValue inputVal = context.getColumn(PathElement("input"));
    
    uint64_t maxFrequency = 0; // max term frequency for the current document
    uint64_t maxNt = 0;        // max document frequency for terms in the current doc

    auto onColumn = [&] (const PathElement & name,
                         const ExpressionValue & val)
        {
            Utf8String term = name.toUtf8String();
            uint64_t value = val.getAtom().toUInt();
            maxFrequency = std::max(value, maxFrequency);
            const auto termFrequency = dfs.find(term);
            if (termFrequency != dfs.end())
                maxNt = std::max(maxNt, termFrequency->second); 
            return true;
        };

    inputVal.forEachColumn(onColumn);

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
    DEBUG_MSG(logger) << "corpus size: " << corpusSize;

    auto onColumn2 = [&] (const PathElement & name,
                          const ExpressionValue & val)
        {
            Utf8String term = name.toUtf8String();
            double frequency = val.getAtom().toDouble();

            double tf = tf_fct(frequency);
            const auto docFrequency = dfs.find(term);
            uint64_t docFrequencyInt = docFrequency != dfs.end() ? docFrequency->second : 0;
            double idf = idf_fct(docFrequencyInt);

            DEBUG_MSG(logger)
                << "term: '" << term << "', df: "
                << docFrequencyInt << ", tf: " << tf << ", idf: " << idf;

            values.emplace_back(name, tf*idf, ts);
            return true;
        };

    inputVal.forEachColumn(onColumn2);

    StructValue outputRow;
    outputRow.emplace_back("output", std::move(values));
    
    return std::move(outputRow);
}

FunctionInfo
TfidfFunction::
getFunctionInfo() const
{
    FunctionInfo result;

    std::vector<KnownColumn> inputColumns, outputColumns;
    inputColumns.emplace_back(PathElement("input"), std::make_shared<UnknownRowValueInfo>(),
                              COLUMN_IS_DENSE, 0);
    outputColumns.emplace_back(PathElement("output"), std::make_shared<UnknownRowValueInfo>(),
                               COLUMN_IS_DENSE, 0);
    
    result.input.emplace_back(new RowValueInfo(inputColumns, SCHEMA_CLOSED));
    result.output.reset(new RowValueInfo(outputColumns, SCHEMA_CLOSED));
    
    return result;
}

namespace {

RegisterProcedureType<TfidfProcedure, TfidfConfig>
regTfidf(builtinPackage(),
         "Prepare data for a TF-IDF function",
         "procedures/TfidfProcedure.md.html");

RegisterFunctionType<TfidfFunction, TfidfFunctionConfig>
regTfidfFunction(builtinPackage(),
                 "tfidf",
                 "Apply a TF-IDF scoring to a bag of words",
                 "functions/Tfidf.md.html");

} // file scope

} // namespace MLDB

