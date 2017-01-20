// This file is part of MLDB. Copyright 2015 mldb.ai inc. All rights reserved.

/** sentiwordnet.cc
    Francois Maillet, 4 novembre 2015
    Copyright (c) mldb.ai inc.  All rights reserved.

    Importer class for SentiWordNet: http://sentiwordnet.isti.cnr.it
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
#include <boost/lexical_cast.hpp>

using namespace std;



namespace MLDB {


/*****************************************************************************/
/* SENTIWORDNET IMPORTER                                                     */
/*****************************************************************************/

struct SentiWordNetImporterConfig : ProcedureConfig {
    static constexpr const char * name = "import.sentiwordnet";

    SentiWordNetImporterConfig()
    {
        outputDataset.withType("sparse.mutable");
    }

    Url dataFileUrl;
    PolyConfigT<Dataset> outputDataset;
};

DECLARE_STRUCTURE_DESCRIPTION(SentiWordNetImporterConfig);

DEFINE_STRUCTURE_DESCRIPTION(SentiWordNetImporterConfig);

SentiWordNetImporterConfigDescription::
SentiWordNetImporterConfigDescription()
{
    addField("dataFileUrl", &SentiWordNetImporterConfig::dataFileUrl,
             "Path to SentiWordNet 3.0 data file");
    addField("outputDataset", &SentiWordNetImporterConfig::outputDataset,
             "Output dataset for result",
             PolyConfigT<Dataset>().withType("sparse.mutable"));
    addParent<ProcedureConfig>();
}

struct SentiWordNetImporter: public Procedure {

    SentiWordNetImporter(MldbServer * owner,
                 PolyConfig config_,
                 const std::function<bool (const Json::Value &)> & onProgress)
        : Procedure(owner)
    {
        config = config_.params.convert<SentiWordNetImporterConfig>();
    }

    SentiWordNetImporterConfig config;

    virtual RunOutput run(const ProcedureRunConfig & run,
                          const std::function<bool (const Json::Value &)> & onProgress) const
    {
        auto runProcConf = applyRunConfOverProcConf(config, run);

        cout << jsonEncode(runProcConf) << endl;

        enum SynsetScoreNames { POS, NEG, OBJ };
        typedef std::array<double, 3> SynsetScores;

        auto info = getUriObjectInfo(
            runProcConf.dataFileUrl.toDecodedString());

        filter_istream stream(runProcConf.dataFileUrl);

        std::shared_ptr<Dataset> outputDataset;
        if (!runProcConf.outputDataset.type.empty() || !runProcConf.outputDataset.id.empty()) {
            outputDataset = createDataset(server, runProcConf.outputDataset, nullptr, true /*overwrite*/);
        }


        // an example line for ref:
        // a<tab>00002956<tab>0<tab>0<tab>abducting#1 abducent#1<tab>especially of muscles; drawing away from the midline of the body or from an adjacent part

        map<string, vector<pair<int, SynsetScores>>> accumulator;

        std::string line;
        while(getline(stream, line)) {
            // skip comments
            if(line.substr(0, 1) == "#") continue;

            vector<string> fields;
            boost::split(fields, line, boost::is_any_of("\t"));

            ExcAssertEqual(fields.size(), 6);

            const string & wordType = fields[0];
            SynsetScores scores;
            try {
                scores[POS] = boost::lexical_cast<float>(fields[2]);
                scores[NEG] = boost::lexical_cast<float>(fields[3]);
            }
            catch(boost::bad_lexical_cast & blc) {
                cout << "skipping: " << line << endl;
                continue;
            }
            // for a given synset s the scores (Objective(s)+Positive(s)+Negative(s)) sum to 1
            scores[OBJ] = 1 - scores[POS] - scores[NEG];


            // going over all: abducting#1 abducent#1
            vector<string> synsetTerms;
            boost::split(synsetTerms, fields[4], boost::is_any_of(" "));

            for(const string & synsetTerm : synsetTerms) {
                size_t sep_pos = synsetTerm.find('#');
                string synTerm = synsetTerm.substr(0, sep_pos) + '#' + wordType;
                int synTermRank = boost::lexical_cast<int>(synsetTerm.substr(sep_pos + 1));

                // now add it to our accumulator. we're looking to build the following:
                // {synTerm: [ (rank, score), (rank, score), ... ] }
                auto it = accumulator.find(synTerm);
                if(it == accumulator.end()) {
                    accumulator.emplace(std::make_pair(std::move(synTerm),
                                vector<pair<int, SynsetScores>>{std::make_pair(synTermRank, scores)}));
                }
                else {
                    it->second.push_back(std::make_pair(synTermRank, scores));
                }
            }
        }

        Date d = Date::now();
        vector<ColumnPath> columnNames = {PathElement("SentiPos"), PathElement("SentiNeg"), PathElement("SentiObj")};

        // We now go through our accumulator to compute the final scores
        vector<pair<RowPath, vector<tuple<ColumnPath, CellValue, Date> > > > rows;
        int64_t numRecorded = 0;
        for(const auto & it : accumulator) {
            double sum = 0;
            std::vector<float> scoreAccum(3);
            for(const pair<int, SynsetScores> & scores : it.second) {
                for(int i=0; i<3; i++) {
                    scoreAccum[i] += scores.second[i] / scores.first;
                }
                sum += 1.0 / scores.first;
            }

            vector<tuple<ColumnPath, CellValue, Date> > cols;
            for(int i=0; i<3; i++) {
                cols.emplace_back(columnNames[i], (scoreAccum[i] / sum), d);
            }
            cols.emplace_back(PathElement("POS"), it.first.substr(it.first.size() - 1), d);
            cols.emplace_back(PathElement("baseWord"), it.first.substr(0, it.first.size() - 2), d);

            rows.emplace_back(RowPath(it.first), std::move(cols));
            ++numRecorded;
        }

        if (outputDataset) {
            outputDataset->recordRows(rows);
            outputDataset->commit();
        }

        RunOutput result;
        return result;
    }

    virtual Any getStatus() const
    {
        return Any();
    }

};

RegisterProcedureType<SentiWordNetImporter, SentiWordNetImporterConfig>
regSentiWordNet(builtinPackage(),
                "Import a SentiWordNet file into MLDB",
                "procedures/SentiWordNetImporter.md.html");


} // namespace MLDB

