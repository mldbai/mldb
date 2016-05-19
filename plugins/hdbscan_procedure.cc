/** hdbscan_procedure.h                                            -*- C++ -*-
    Mathieu Marquis Bolduc, May 19th, 2016
    Copyright (c) 2016 Datacratic Inc.  All rights reserved.

    This file is part of MLDB. Copyright 2016 Datacratic. All rights reserved.

    HDBSCAN clustering procedure.
    Based on 'Density-Based Clustering Based on Hierarchical Density Estimates' 
    by Ricardo J. G. B. Campello, Davoud Moulavi, Joerg Sander
*/

#include "hdbscan_procedure.h"
#include "matrix.h"
#include "mldb/server/mldb_server.h"
#include "mldb/server/procedure_collection.h"
#include "mldb/server/function_collection.h"
#include "jml/stats/distribution.h"
#include <boost/multi_array.hpp>
#include "jml/utils/guard.h"
#include "base/parallel.h"
#include "jml/utils/pair_utils.h"
#include "mldb/arch/timers.h"
#include "mldb/types/optional_description.h"
#include "jml/utils/vector_utils.h"
#include "mldb/types/basic_value_descriptions.h"
#include "mldb/sql/sql_expression.h"
#include "mldb/server/analytics.h"
#include "mldb/types/any_impl.h"
#include "jml/utils/smart_ptr_utils.h"
#include "mldb/vfs/fs_utils.h"
#include "mldb/plugins/sql_config_validator.h"


using namespace std;


namespace Datacratic {
namespace MLDB {

DEFINE_STRUCTURE_DESCRIPTION(HDBSCANConfig);

HDBSCANConfigDescription::
HDBSCANConfigDescription()
{
    Optional<PolyConfigT<Dataset> > optional;
    optional.emplace(PolyConfigT<Dataset>().
                     withType(HDBSCANConfig::defaultOutputDatasetType));

    addField("trainingData", &HDBSCANConfig::trainingData,
             "Specification of the data for input to the procedure.  This should be "
             "organized as an embedding, with each selected row containing the same "
             "set of columns with numeric values to be used as coordinates.  The select statement "
             "does not support groupby and having clauses.");
    addField("outputDataset", &HDBSCANConfig::output,
             "Dataset for cluster assignment.  This dataset will contain the same "
             "row names as the input dataset, but the coordinates will be replaced "
             "by a single column giving the cluster number that the row was assigned to.",
              optional);
    addField("numInputDimensions", &HDBSCANConfig::numInputDimensions,
             "Number of dimensions from the input to use (-1 = all).  This limits "
             "the number of columns used.  Columns will be ordered alphabetically "
             "and the lowest ones kept.",
             -1);
    addField("coreDistance", &HDBSCANConfig::coreDistance," ", 5);
    addParent<ProcedureConfig>();

  //  onPostValidate = validateFunction<HDBSCANConfig>();
}

/*****************************************************************************/
/* HDBSCAN PROCEDURE                                                         */
/*****************************************************************************/

HDBSCANProcedure::
HDBSCANProcedure(MldbServer * owner,
               PolyConfig config,
               const std::function<bool (const Json::Value &)> & onProgress)
    : Procedure(owner)
{
    this->hdbscanConfig = config.params.convert<HDBSCANConfig>();


}

Any
HDBSCANProcedure::
getStatus() const
{
    return Any();

}

RunOutput
HDBSCANProcedure::
run(const ProcedureRunConfig & run,
    const std::function<bool (const Json::Value &)> & onProgress) const
{
    auto runProcConf = applyRunConfOverProcConf(hdbscanConfig, run);

    auto onProgress2 = [&] (const Json::Value & progress)
        {
            Json::Value value;
            value["dataset"] = progress;
            return onProgress(value);
        };

    SqlExpressionMldbScope context(server);

    auto embeddingOutput = getEmbedding(*runProcConf.trainingData.stm,
                                        context,
                                        runProcConf.numInputDimensions,
                                        onProgress2);

    auto rows = embeddingOutput.first;
    //std::vector<KnownColumn> & vars = embeddingOutput.second;

    std::vector<ML::distribution<double> > vecs;

    for (unsigned i = 0;  i < rows.size();  ++i) {
        vecs.emplace_back(ML::distribution<double>(std::get<2>(rows[i]).begin(),
                                                   std::get<2>(rows[i]).end()));
    }

    if (vecs.size() == 0)
        throw HttpReturnException(400, "HDBSCAN clustering training requires at least 1 datapoint. "
                                  "Make sure your dataset is not empty and that your WHERE expression "
                                  "does not filter all the rows");

    /////* ACTUAL ML STUFF STARTS HERE *///////////

    //1.0 find core distance. We should optimize with space partitioning

    int coreDistance = runProcConf.coreDistance;
    std::vector<double> coreDistances;
    std::vector<std::vector<std::pair<int, double>>> reachability;

    for (unsigned i = 0;  i < vecs.size();  ++i) {
        ML::distribution<double> x = vecs[i];
        std::vector<std::pair<int, double>> kclosest;

        for (unsigned j = 0;  j < rows.size();  ++j) {

            if (i == j)
                continue;

            ML::distribution<double> y = vecs[j];

            double distance = (x - y).two_norm();

            if (kclosest.empty()) {
                kclosest.emplace_back(j, distance);
            }
            else {
                int p = 0;
                for (; p < kclosest.size(); ++p) {
                    if (distance < kclosest[p].second) {
                        kclosest.insert(kclosest.begin() + p, {j, distance});
                        break;
                    }
                }

                if (p == kclosest.size() && kclosest.size() < coreDistance) {
                    kclosest.emplace_back(j, distance);
                }
                else if (kclosest.size() > coreDistance) {
                    kclosest.pop_back();
                }
            }
        }

        ExcAssert(!kclosest.empty());

        coreDistances.push_back(kclosest.back().second);
        reachability.push_back(std::move(kclosest));
    }

    //2.0 mutual reachability. max(core(x), core(y), d(x,y))

    for (unsigned i = 0;  i < vecs.size();  ++i) {
        double myCoreDistance = coreDistances[i];
        for (auto& pair : reachability[i]) {
            int j = pair.first;
            double otherCoreDistance = coreDistances[j];
            pair.second = std::max(pair.second, std::max(myCoreDistance, otherCoreDistance));
        }
    }

    //3.0 build the minimum spanning tree via Prim's algorithm.
    //    should do a more efficient implementation
    std::vector<std::pair<int, int>> edges;
    std::vector<int> used;
    used.push_back(0);

    while (used.size() < vecs.size()) {
        //cerr << "NEW ITERATION" << endl;
        int minI = 0;
        int minJ = 0;
        int minDistance = 0.0f;

        for (int i : used) {       
          //  cerr << "from " <<  i << " to " << endl; 
            for (const auto& pair : reachability[i]) {
                int j = pair.first;
            //    cerr << j << endl; 
                if (std::find(used.begin(), used.end(), j) == used.end()) {
                    double mutualReachability = pair.second;
              //      cerr << "distance " << mutualReachability << endl; 
                    if (minJ == 0 || mutualReachability < minDistance) {
                //        cerr << "new minimum" << endl;
                        minI = i;
                        minJ = j;
                        minDistance = mutualReachability;
                    }
                }
            }
        }

        ExcAssert(minJ != 0);

        used.push_back(minJ);
        edges.emplace_back(minI, minJ);
    }

  //  for (auto& edge : edges) {
  //      cerr << edge.first << " -> " << edge.second << endl;
  //  }

    return Any();
}

namespace {

RegisterProcedureType<HDBSCANProcedure, HDBSCANConfig>
regEM(builtinPackage(),
      "clustering algorithm",
      "procedures/EMProcedure.md.html",
      nullptr /* static route */,
      { MldbEntity::INTERNAL_ENTITY });

} // file scope

}
}
