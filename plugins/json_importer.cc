/* xlsx_importer.cc
   Francois Maillet, 19 janvier 2016

   This file is part of MLDB. Copyright 2016 Datacratic. All rights reserved.

   Importer for text files containing a JSON per line
*/

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
#include "mldb/server/dataset_context.h"

using namespace std;


namespace Datacratic {
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
          where(SqlExpression::TRUE)
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

    addParent<ProcedureConfig>();
}

struct JsonRowScope : SqlRowScope {
    JsonRowScope(const ExpressionValue & expr) : expr(expr) {}
    const ExpressionValue & expr;
};

struct JsonScope : SqlExpressionMldbScope {


    JsonScope(MldbServer * server) : SqlExpressionMldbScope(server){}

    ColumnGetter doGetColumn(const Utf8String & tableName,
                                const ColumnName & columnName) override
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
                    std::function<ColumnName (const ColumnName &)> keep)
    {
        std::vector<KnownColumn> columnsWithInfo;

        auto exec = [=] (const SqlRowScope & scope, const VariableFilter & filter)
        {
            const auto & row = scope.as<JsonRowScope>();
            return row.expr;
        };
        GetAllColumnsOutput result;
        result.exec = exec;
        result.info = std::make_shared<RowValueInfo>(std::move(columnsWithInfo),
                                                     SCHEMA_CLOSED);
        return result;
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
            throw ML::Exception("Unable to obtain output dataset");
        }

        Date zeroTs;

        std::atomic<int64_t> errors(0);
        std::atomic<int64_t> recordedLines(0);
        int64_t lineOffset = 1;
        std::string line;
        std::string filename = runProcConf.dataFileUrl.toString();

        filter_istream stream(filename);

        Date timestamp = stream.info().lastModified;

        ML::Timer timer;

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
        JsonScope jsonScope(server);
        ExpressionValue storage;
        const auto whereBound = config.where->bind(jsonScope);
        const auto selectBound = config.select.bind(jsonScope);

        auto onLine = [&] (const char * line,
                           size_t lineLength,
                           int64_t blockNumber,
                           int64_t lineNumber)
        {
            auto & threadAccum = accum.get();

            int64_t actualLineNum = lineNumber + lineOffset;

            // MLDB-1111 empty lines are treated as error
            if(lineLength == 0)
                return handleError("empty line", actualLineNum, "");

            StreamingJsonParsingContext parser(filename, line, lineLength,
                                               actualLineNum);

            skipJsonWhitespace(*parser.context);
            if (parser.context->eof()) {
                return handleError("empty line", actualLineNum, "");
            }

            // TODO: in the configuration
            JsonArrayHandling arrays = ENCODE_ARRAYS;

            ExpressionValue expr;
            try {
                expr = ExpressionValue::parseJson(parser, timestamp, arrays);
            } catch (const std::exception & exc) {
                return handleError(exc.what(), actualLineNum, string(line, lineLength));
            }

            if (useWhere || useSelect) {
                JsonRowScope row(expr);
                if (!whereBound(row, storage, GET_ALL).isTrue()) {
                    return true;

                }

                if (useSelect) {
                    expr = selectBound(row, storage, GET_ALL);
                }
            }


            skipJsonWhitespace(*parser.context);
            if (!parser.context->eof()) {
                return handleError("extra characters at end of line", actualLineNum, "");
            }

            recordedLines++;

            RowName rowName(actualLineNum);
            threadAccum.threadRecorder->recordRowExprDestructive(RowName(actualLineNum), std::move(expr));

            return true;
        };

        forEachLineBlock(stream, onLine, runProcConf.limit, 32,
                         startChunk, doneChunk);

        cerr << timer.elapsed() << endl;
        timer.restart();

        cerr << "committing dataset" << endl;

        recorder.commit();

        cerr << timer.elapsed() << endl;

        cerr << "done" << endl;

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
} // namespace Datacratic
