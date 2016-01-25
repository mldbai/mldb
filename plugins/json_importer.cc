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
#include "mldb/sql/builtin_functions.h"

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
            outputDataset = obtainDataset(server, runProcConf.outputDataset);
        }

        if(!outputDataset) {
            throw ML::Exception("Unable to obtain output dataset");
        }

        Date zeroTs;

        std::mutex recordMutex;

        std::atomic<int64_t> errors(0);
        std::atomic<int64_t> recordedLines(0);
        int64_t lineOffset = 1;
        std::string line;

        ML::filter_istream stream(runProcConf.dataFileUrl.toString());

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
            
            throw HttpReturnException(400, "Error parsing CSV row: "
                                      + message,
                                      "lineNumber", lineNumber,
                                      "line", line);
        };

        auto onLine = [& ](const char * line,
                           size_t lineLength,
                           int64_t blockNumber,
                           int64_t lineNumber)
        {
            int64_t actualLineNum = lineNumber + lineOffset;
                          
            // MLDB-1111 empty lines are treated as error
            if(lineLength == 0)
                return handleError("empty line", actualLineNum, "");

            Json::Reader reader;
            Json::Value root;

            if (!reader.parse(line, line+lineLength, root))
                return handleError("Unable to parse line %d to JSON", 
                                   actualLineNum,
                                   string(line, lineLength));
            
            if(!root.isObject())
                return handleError("JSON is not an object",
                                   actualLineNum,
                                   string(line, lineLength));

            MatrixNamedRow outputRow;
            outputRow.rowName = RowName(ML::format("row%d", actualLineNum));

            for (const std::string & id : root.getMemberNames()) {
                Builtins::unpackJson(outputRow.columns, id, root[id], zeroTs);
            }

            recordedLines++;

            std::unique_lock<std::mutex> guard(recordMutex);
            outputDataset->recordRow(outputRow.rowName, outputRow.columns);
            return true;
        };

        forEachLineBlock(stream, onLine, runProcConf.limit);
        outputDataset->commit();

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
