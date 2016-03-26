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

using namespace std;


namespace Datacratic {
namespace MLDB {


/*****************************************************************************/
/* JSON IMPORTER                                                             */
/*****************************************************************************/

struct JSONImporterConfig : ProcedureConfig {

    JSONImporterConfig() :
          limit(-1),
          offset(0),
          ignoreBadLines(false)
    {}

    Url dataFileUrl;
    PolyConfigT<Dataset> outputDataset;
    
    int64_t limit;
    int64_t offset;
    bool ignoreBadLines;
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
             PolyConfigT<Dataset>().withType("sparse.mutable"));
    addField("limit", &JSONImporterConfig::limit,
             "Maximum number of lines to process");
    addField("offset", &JSONImporterConfig::offset,
            "Skip the first n lines.", int64_t(0));
    addField("ignoreBadLines", &JSONImporterConfig::ignoreBadLines,
             "If true, any line causing an error will be skipped. Any line "
             "with an invalid JSON object will cause an error.", false);
    
    addParent<ProcedureConfig>();
}


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
 
        if (!runProcConf.outputDataset.type.empty()
            || !runProcConf.outputDataset.id.empty()) {
            outputDataset = createDataset(server, runProcConf.outputDataset, nullptr, true);
        }

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
            /// Rows we've accumulated up to here for the chunl
            std::vector<std::pair<RowName, ExpressionValue> > rows;

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
                auto & rows = threadAccum.rows;
                ExcAssert(threadAccum.threadRecorder.get());
                if (!rows.empty())
                    threadAccum.threadRecorder->recordRowsExprDestructive
                        (std::move(rows));
                rows.clear();
                threadAccum.threadRecorder->finishedChunk();
                threadAccum.threadRecorder.reset(nullptr);
                return true;
            };

        auto onLine = [&] (const char * line,
                           size_t lineLength,
                           int64_t blockNumber,
                           int64_t lineNumber)
        {
            auto & threadAccum = accum.get();
            auto & rows = threadAccum.rows;

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

            skipJsonWhitespace(*parser.context);
            if (!parser.context->eof()) {
                return handleError("extra characters at end of line", actualLineNum, "");
            }

            recordedLines++;

            rows.emplace_back(RowName(actualLineNum), std::move(expr));

            if (rows.size() > 1000) {
                threadAccum.threadRecorder->recordRowsExprDestructive(std::move(rows));
                rows.clear();
            }
            return true;
        };

        forEachLineBlock(stream, onLine, runProcConf.limit, 32,
                         startChunk, doneChunk);

        cerr << "cleaning up" << endl;
        
        auto doLeftoverChunk = [&] (int threadNum)
            {
                ExcAssertEqual(accum.threads.at(threadNum)->rows.size(), 0);
                //auto rows = *accum.threads.at(threadNum).get();
                //outputDataset->recordRowsExpr(rows);
            };

        parallelMap(0, accum.threads.size(), doLeftoverChunk);

        cerr << timer.elapsed() << endl;
        timer.restart();

        cerr << "committing dataset" << endl;

        outputDataset->commit();

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
        "import.json",
        "Import a text file with one JSON per line into MLDB",
        "procedures/JSONImporter.md.html");


} // namespace MLDB
} // namespace Datacratic
