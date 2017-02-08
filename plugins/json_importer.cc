/* xlsx_importer.cc
   Francois Maillet, 19 janvier 2016

   This file is part of MLDB. Copyright 2016 mldb.ai inc. All rights reserved.

   Importer for text files containing a JSON per line
*/

#include "mldb/utils/progress.h"
#include "mldb/core/procedure.h"
#include "mldb/core/dataset.h"
#include "mldb/types/value_description.h"
#include "mldb/types/structure_description.h"
#include "mldb/vfs/filter_streams.h"
#include "mldb/types/any_impl.h"
#include "mldb/plugins/for_each_line.h"
#include "mldb/http/http_exception.h"
#include "mldb/vfs/filter_streams.h"
#include "mldb/vfs/fs_utils.h"
#include "mldb/sql/builtin_functions.h"
#include "mldb/server/per_thread_accumulator.h"
#include "mldb/base/parallel.h"
#include "mldb/arch/timers.h"
#include "mldb/base/parse_context.h"
#include "mldb/rest/cancellation_exception.h"
#include "mldb/server/dataset_context.h"
#include "mldb/utils/log.h"

using namespace std;



namespace MLDB {


/*****************************************************************************/
/* JSON IMPORTER                                                             */
/*****************************************************************************/

struct JSONImporterConfig : ProcedureConfig {

    static constexpr const char * name = "import.json";

    JSONImporterConfig() :
          limit(-1),
          offset(0),
          ignoreBadLines(false),
          select(SelectExpression::STAR),
          where(SqlExpression::TRUE),
          named(SqlExpression::TRUE), // Trick to ease comparison
          arrays(PARSE_ARRAYS)
    {
        outputDataset.withType("tabular");
    }

    Url dataFileUrl;
    PolyConfigT<Dataset> outputDataset;

    int64_t limit;
    int64_t offset;
    bool ignoreBadLines;
    SelectExpression select;
    std::shared_ptr<SqlExpression> where;
    std::shared_ptr<SqlExpression> named;
    JsonArrayHandling arrays;
};

DECLARE_STRUCTURE_DESCRIPTION(JSONImporterConfig);

DEFINE_STRUCTURE_DESCRIPTION(JSONImporterConfig);

JSONImporterConfigDescription::
JSONImporterConfigDescription()
{
    addField("dataFileUrl", &JSONImporterConfig::dataFileUrl,
             "URL to load text file from");
    addField("outputDataset", &JSONImporterConfig::outputDataset,
             "Configuration for output dataset",
             PolyConfigT<Dataset>().withType("tabular"));
    addField("limit", &JSONImporterConfig::limit,
             "Maximum number of lines to process");
    addField("offset", &JSONImporterConfig::offset,
             "Skip the first n lines.", int64_t(0));
    addField("ignoreBadLines", &JSONImporterConfig::ignoreBadLines,
             "If true, any line causing an error will be skipped. Any line "
             "with an invalid JSON object will cause an error.", false);
    addField("select", &JSONImporterConfig::select,
             "Which columns to use.",
             SelectExpression::STAR);
    addField("where", &JSONImporterConfig::where,
             "Which lines to use to create rows.",
             SqlExpression::TRUE);
    addField("named", &JSONImporterConfig::named,
             "Row name expression for output dataset. Note that each row "
             "must have a unique name and that names cannot be objects.",
             SqlExpression::parse("lineNumber()"));
    addField("arrays", &JSONImporterConfig::arrays,
            "Describes how arrays are encoded in the JSON output.  For "
            "''parse' (default), the arrays become structured values. "
            "For 'encode', "
            "arrays containing atoms are sparsified with the values "
            "representing one-hot "
            "keys and boolean true values", PARSE_ARRAYS);

    addParent<ProcedureConfig>();

    onPostValidate = [] (JSONImporterConfig * config,
                         JsonParsingContext & context)
    {
        if (config->dataFileUrl.empty()) {
            throw HttpReturnException(
                400,
                "dataFileUrl is a required property and must not be empty");
        }
    };
}

struct JsonRowScope : SqlRowScope {
    JsonRowScope(const ExpressionValue & expr, ssize_t lineNumber)
        : expr(expr), lineNumber(lineNumber) {}
    const ExpressionValue & expr;
    ssize_t lineNumber;
};

struct JsonScope : SqlExpressionMldbScope {


    JsonScope(MldbServer * server) : SqlExpressionMldbScope(server){}

    ColumnGetter doGetColumn(const Utf8String & tableName,
                                const ColumnPath & columnName) override
    {
        return {[=] (const SqlRowScope & scope, ExpressionValue & storage,
                     const VariableFilter & filter) -> const ExpressionValue &
            {
                const auto & row = scope.as<JsonRowScope>();
                const ExpressionValue * res =
                    row.expr.tryGetNestedColumn(columnName, storage, filter);
                if (res) {
                    return *res;
                }
                return storage = ExpressionValue();
            },
            std::make_shared<AtomValueInfo>()
        };
    }

    GetAllColumnsOutput
    doGetAllColumns(const Utf8String & tableName,
                    const ColumnFilter& keep) override
    {
        std::vector<KnownColumn> columnsWithInfo;

        auto exec = [=] (const SqlRowScope & scope, const VariableFilter & filter)
        {
            const auto & row = scope.as<JsonRowScope>();
            StructValue result;
            result.reserve(row.expr.rowLength());

            const auto onCol = [&] (const PathElement & columnName,
                                    const ExpressionValue & val)
            {
                const auto & newColName = keep(columnName);
                if (!newColName.empty()) {
                    result.emplace_back(newColName.front(), val);
                }
                return true;
            };
            row.expr.forEachColumnDestructive(onCol);
            result.shrink_to_fit();
            return result;
        };
        GetAllColumnsOutput result;
        result.exec = exec;
        result.info = std::make_shared<RowValueInfo>(std::move(columnsWithInfo),
                                                     SCHEMA_OPEN);
        return result;
    }

    BoundFunction
    doGetFunction(const Utf8String & tableName,
                  const Utf8String & functionName,
                  const std::vector<BoundSqlExpression> & args,
                  SqlBindingScope & argScope) override
    {
        if (functionName == "lineNumber") {
            return {[=] (const std::vector<ExpressionValue> & args,
                         const SqlRowScope & scope)
                {
                    const auto & row = scope.as<JsonRowScope>();
                    return ExpressionValue(row.lineNumber,
                                           Date::negativeInfinity());
                },
                std::make_shared<IntegerValueInfo>()
            };
        }
        return SqlBindingScope::doGetFunction(tableName, functionName, args,
                                              argScope);
    }

};

struct JSONImporter: public Procedure {

    JSONImporter(MldbServer * owner,
                 PolyConfig config_,
                 const std::function<bool (const Json::Value &)> & onProgress)
        : Procedure(owner)
    {
        config = config_.params.convert<JSONImporterConfig>();
    }

    JSONImporterConfig config;

    virtual RunOutput run(const ProcedureRunConfig & run,
                          const std::function<bool (const Json::Value &)> & onProgress) const
    {
        auto runProcConf = applyRunConfOverProcConf(config, run);
        Progress progress;

        std::shared_ptr<Step> iterationStep = progress.steps({
            make_pair("iterating", "lines")
        });

        // Create the output dataset
        std::shared_ptr<Dataset> outputDataset;

        if (runProcConf.outputDataset.type == "tabular") {
            if (runProcConf.outputDataset.params == nullptr) {
                 Json::Value params;
                 params["unknownColumns"] = "add";
                 runProcConf.outputDataset.params = params;
            }
            else {
                auto params =
                    runProcConf.outputDataset.params.as<Json::Value>();
                if (!params.isMember("unknownColumns")) {
                    params["unknownColumns"] = "add";
                    runProcConf.outputDataset.params = params;
                }
            }
        }
        outputDataset = createDataset(server, runProcConf.outputDataset,
                                      onProgress, true);

        if(!outputDataset) {
            throw MLDB::Exception("Unable to obtain output dataset");
        }

        Date zeroTs;

        std::atomic<int64_t> errors(0);
        std::atomic<int64_t> recordedLines(0);
        int64_t lineOffset = 1;
        std::string line;
        std::string filename = runProcConf.dataFileUrl.toDecodedString();

        filter_istream stream(filename);

        Date timestamp = stream.info().lastModified;

        Timer timer;

        // Skip those up to the offset
        for (size_t i = 0;  stream && i < config.offset;  ++i, ++lineOffset) {
            getline(stream, line);
        }

        auto handleError = [&](const std::string & message,
                               int64_t lineNumber,
                               const std::string& line) {
            if (config.ignoreBadLines) {
                ++errors;
                return true;
            }

            throw HttpReturnException(400, "Error parsing JSON row: "
                                      + message,
                                      "filename", filename,
                                      "lineNumber", lineNumber,
                                      "line", line);
        };

        Dataset::MultiChunkRecorder recorder
            = outputDataset->getChunkRecorder();

        struct ThreadAccum {
            /// Recorder object for this thread that the dataset gives us
            /// to record into the dataset.
            std::unique_ptr<Recorder> threadRecorder;
        };

        PerThreadAccumulator<ThreadAccum> accum;

        auto startChunk = [&] (int64_t chunkNumber, size_t lineNumber)
            {
                auto & threadAccum = accum.get();
                threadAccum.threadRecorder = recorder.newChunk(chunkNumber);
                return true;
            };

        auto doneChunk = [&] (int64_t chunkNumber, size_t lineNumber)
            {
                auto & threadAccum = accum.get();
                ExcAssert(threadAccum.threadRecorder.get());
                threadAccum.threadRecorder->finishedChunk();
                threadAccum.threadRecorder.reset(nullptr);
                return true;
            };

        bool useSelect = config.select != SelectExpression::STAR;
        bool useWhere = config.where != SqlExpression::TRUE;

        // using incorrect default value to ease check
        bool useNamed = config.named != SqlExpression::TRUE;

        JsonScope jsonScope(server);
        const auto whereBound = config.where->bind(jsonScope);
        const auto selectBound = config.select.bind(jsonScope);
        const auto namedBound = config.named->bind(jsonScope);
        bool keepGoing = true;
        mutex progressMutex;

        auto onLine = [&] (const char * line,
                           size_t lineLength,
                           int64_t blockNumber,
                           int64_t lineNumber)
        {
            auto & threadAccum = accum.get();

            uint64_t actualLineNum = lineNumber + lineOffset;

            // MLDB-1111 empty lines are treated as error
            if(lineLength == 0)
                return handleError("empty line", actualLineNum, "");

            StreamingJsonParsingContext parser(filename, line, lineLength,
                                               actualLineNum);

            skipJsonWhitespace(*parser.context);
            if (parser.context->eof()) {
                return handleError("empty line", actualLineNum, "");
            }

            ExpressionValue expr;
            try {
                expr = ExpressionValue::parseJson(parser, timestamp,
                                                  config.arrays);
            } catch (const std::exception & exc) {
                return handleError(exc.what(), actualLineNum, string(line, lineLength));
            }

            skipJsonWhitespace(*parser.context);
            if (!parser.context->eof()) {
                return handleError("extra characters at end of line", actualLineNum, "");
            }

            RowPath rowName(actualLineNum);
            if (useWhere || useSelect || useNamed) {
                JsonRowScope row(expr, actualLineNum);
                ExpressionValue storage;
                if (useWhere) {
                    if (!whereBound(row, storage, GET_ALL).isTrue()) {
                        return true;
                    }
                }

                if (useNamed) {
                    rowName = RowPath(
                        namedBound(row, storage, GET_ALL).toUtf8String());
                }

                if (useSelect) {
                    expr = selectBound(row, storage, GET_ALL);
                    storage = expr;
                }

            }

            int numLines = recordedLines.fetch_add(1);
            if (numLines % PROGRESS_RATE_LOW == 0) {
                lock_guard<mutex> l(progressMutex);
                if (numLines > iterationStep->value) {
                    iterationStep->value = numLines;
                }
                keepGoing = onProgress(jsonEncode(progress));
            }

            threadAccum.threadRecorder->recordRowExprDestructive(
                std::move(rowName), std::move(expr));

            return keepGoing;
        };

        forEachLineBlock(stream, onLine, runProcConf.limit, 32,
                         startChunk, doneChunk);
        if (!keepGoing) {
            throw MLDB::CancellationException("Procedure import.json cancelled");
        }

        DEBUG_MSG(logger) << timer.elapsed();
        timer.restart();

        DEBUG_MSG(logger) << "committing dataset";

        recorder.commit();

        DEBUG_MSG(logger) << timer.elapsed();

        DEBUG_MSG(logger) << "done";

        Json::Value result;
        result["rowCount"] = (int64_t)recordedLines;
        result["numLineErrors"] = (int64_t)errors;
        return RunOutput(result);
    }

    virtual Any getStatus() const
    {
        return Any();
    }

    JSONImporterConfig procConfig;
};

static RegisterProcedureType<JSONImporter, JSONImporterConfig>
regJSON(builtinPackage(),
        "Import a text file with one JSON per line into MLDB",
        "procedures/JSONImporter.md.html");


} // namespace MLDB

