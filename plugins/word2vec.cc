// This file is part of MLDB. Copyright 2015 Datacratic. All rights reserved.

/** word2vec.cc
    Jeremy Barnes, 14 October 2015
    Copyright (c) Datacratic Inc.  All rights reserved.
*/

#include "mldb/core/procedure.h"
#include "mldb/core/dataset.h"
#include "mldb/types/url.h"
#include "mldb/types/structure_description.h"
#include "mldb/types/any_impl.h"
#include "mldb/vfs/fs_utils.h"
#include "mldb/vfs/filter_streams.h"
#include "mldb/jml/stats/distribution.h"
#include <boost/algorithm/string.hpp>

using namespace std;



namespace MLDB {


/*****************************************************************************/
/* WORD2VEC IMPORTER                                                         */
/*****************************************************************************/

struct Word2VecImporterConfig : ProcedureConfig {
    static constexpr const char * name = "import.word2vec";

    Word2VecImporterConfig()
        : offset(0), limit(-1)
    {
        output.withType("embedding");
    }

    Url dataFileUrl;
    PolyConfigT<Dataset> output;
    uint64_t offset;
    int64_t limit;
};

DECLARE_STRUCTURE_DESCRIPTION(Word2VecImporterConfig);

DEFINE_STRUCTURE_DESCRIPTION(Word2VecImporterConfig);

Word2VecImporterConfigDescription::
Word2VecImporterConfigDescription()
{
    addField("dataFileUrl", &Word2VecImporterConfig::dataFileUrl,
             "URL to load Excel workbook from");
    addField("outputDataset", &Word2VecImporterConfig::output,
             "Output dataset for result",
             PolyConfigT<Dataset>().withType("embedding"));
    addField("offset", &Word2VecImporterConfig::offset,
             "Start at word number (0 = start)", (uint64_t)0);
    addField("limit", &Word2VecImporterConfig::limit,
             "Limit of number of rows to record (-1 = all)", (int64_t)-1);
    addParent<ProcedureConfig>();
}

struct Word2VecImporter: public Procedure {

    Word2VecImporter(MldbServer * owner,
                 PolyConfig config_,
                 const std::function<bool (const Json::Value &)> & onProgress)
        : Procedure(owner)
    {
        config = config_.params.convert<Word2VecImporterConfig>();
    }

    Word2VecImporterConfig config;

    virtual RunOutput run(const ProcedureRunConfig & run,
                          const std::function<bool (const Json::Value &)> & onProgress) const
    {
        auto runProcConf = applyRunConfOverProcConf(config, run);
        auto info = getUriObjectInfo(
            runProcConf.dataFileUrl.toDecodedString());

        filter_istream stream(runProcConf.dataFileUrl);

        std::string header;
        getline(stream, header);

        vector<string> fields;
        boost::split(fields, header, boost::is_any_of(" "));

        ExcAssertEqual(fields.size(), 2);

        int numWords = std::stoi(fields[0]);
        int numDims  = std::stoi(fields[1]);

        std::shared_ptr<Dataset> output;
        if (!runProcConf.output.type.empty() || !runProcConf.output.id.empty()) {
            output = createDataset(server, runProcConf.output, nullptr, true /*overwrite*/);
        }

        vector<ColumnName> columnNames;
        for (unsigned i = 0;  i < numDims;  ++i) {
            columnNames.emplace_back(ML::format("%06d", i));
        }

        vector<tuple<RowPath, vector<float>, Date> > rows;
        int64_t numRecorded = 0;

        for (unsigned i = 0;  i < numWords;  ++i) {
            std::string word;
            getline(stream, word, ' ');

            std::vector<float> vec(numDims);
            stream.read((char *)&vec[0], numDims * sizeof(float));

            if (i < runProcConf.offset)
                continue;
            if (runProcConf.limit != -1 && numRecorded >= runProcConf.limit)
                break;

            rows.emplace_back(RowPath(word), std::move(vec), info.lastModified);
            ++numRecorded;

            if (rows.size() == 10000) {
                if (output)
                    output->recordEmbedding(columnNames, rows);
                rows.clear();
                cerr << "recorded " << (i+1) << " of " << numWords << " words"
                     << endl;
            }

            //cerr << "got word " << word << endl;
        }

        if (output) {
            output->recordEmbedding(columnNames, rows);
            output->commit();
        }

        RunOutput result;
        return result;
    }

    virtual Any getStatus() const
    {
        return Any();
    }
};

RegisterProcedureType<Word2VecImporter, Word2VecImporterConfig>
regScript(builtinPackage(),
          "Import a word2vec file into MLDB",
          "procedures/Word2VecImporter.md.html");


} // namespace MLDB

