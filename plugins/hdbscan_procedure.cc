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
    addField("minClusterSize", &HDBSCANConfig::minClusterSize," ", 5);
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
   // std::vector<std::vector<std::pair<int, double>>> reachability;

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
       // reachability.push_back(std::move(kclosest));
    }

    //2.0 mutual reachability. max(core(x), core(y), d(x,y))

 /*   for (unsigned i = 0;  i < vecs.size();  ++i) {
        double myCoreDistance = coreDistances[i];
        for (auto& pair : reachability[i]) {
            int j = pair.first;
            double otherCoreDistance = coreDistances[j];
            pair.second = std::max(pair.second, std::max(myCoreDistance, otherCoreDistance));
        }
    }*/

    auto getMutualReachability = [&] (int i, int j) {
        double myCoreDistance = coreDistances[i];
        double otherCoreDistance = coreDistances[j];
        ML::distribution<double> x = vecs[i];
        ML::distribution<double> y = vecs[j];
        double distance = (x - y).two_norm();
        return std::max(distance, std::max(myCoreDistance, otherCoreDistance));
    };

    //3.0 build the minimum spanning tree via Prim's algorithm.
    //    should do a more efficient implementation
    std::vector<std::tuple<int, int, double> > edges;
    std::vector<int> used;
    used.push_back(0);

    while (used.size() < vecs.size()) {
    //    cerr << "NEW ITERATION" << endl;
        int minI = 0;
        int minJ = 0;
        double minDistance = 0.0f;

        for (int i : used) {       
           // cerr << "from " <<  i << " to " << endl; 
            //for (const auto& pair : reachability[i]) {
              //  int j = pair.first;
            for (int j = 0; j < vecs.size(); ++j) {

                if (i == j )
                    continue;

               // cerr << j << endl; 
                if (std::find(used.begin(), used.end(), j) == used.end()) {
                    double mutualReachability = getMutualReachability(i, j);//pair.second ;
                 //   cerr << "distance " << mutualReachability << " vs " << minDistance << endl; 
                    if (minJ == 0 || mutualReachability < minDistance) {
                   //     cerr << "new minimum" << endl;
                        minI = i;
                        minJ = j;
                        minDistance = mutualReachability;
                    }
                }
            }
        }

        ExcAssert(minJ != 0);

        used.push_back(minJ);
        edges.emplace_back(minI, minJ, minDistance);
    }

    //4.0 Build the cluster hierarchy. Not optimized, etc, etc.
    std::sort(edges.begin(), edges.end(), [] (const std::tuple<int, int, double>& a, const std::tuple<int, int, double>& b) {
        return std::get<2>(a) < std::get<2>(b);
    });

    cerr << "sorted edges" << endl;
    for (auto& edge : edges) {
        cerr << std::get<0>(edge) << " -> " << std::get<1>(edge) << "   , " << std::get<2>(edge) << endl;
    }

    std::vector<int> clusterSizes;
    clusterSizes.reserve(edges.size());
    std::vector<double> clusterDistances; //length of the edge that merges two clusters into this one
    clusterDistances.reserve(edges.size());
  //  std::vector<int> clusterIndex(vecs.size(), -1);
    std::vector<int> clusterParent(vecs.size(), -1);
    std::vector<std::pair<int, int> > clusterChilds;
    clusterChilds.reserve(edges.size());

    //init the leaf clusters
    for (int index = 0; index < vecs.size(); ++index) {
  //      clusterIndex[index] = index;
        clusterParent[index] = index;
        clusterSizes.push_back(1);
        clusterChilds.emplace_back(-1, -1);
        clusterDistances.push_back(0);
    }

    for (auto& edge : edges) {

        int newCluster = clusterSizes.size();

     //   int& cluster0 = clusterIndex[std::get<0>(edge)];
     //   int& cluster1 = clusterIndex[std::get<1>(edge)];

        int clusterParent0 = clusterParent[std::get<0>(edge)];
        int clusterParent1 = clusterParent[std::get<1>(edge)];

        clusterChilds.emplace_back(clusterParent0, clusterParent1);

        int size0 = clusterParent0 >= 0 ? clusterSizes[clusterParent0] : 1;
        int size1 = clusterParent1 >= 0 ? clusterSizes[clusterParent1] : 1;

        for (auto& p : clusterParent) {
            if ((p == clusterParent0 && clusterParent0 != -1) || (p == clusterParent1 && clusterParent1 != -1)) {
                p = newCluster;
            }
        }

        clusterParent[std::get<0>(edge)] = newCluster;
        clusterParent[std::get<1>(edge)] = newCluster;

        

     /*   if (cluster0 < 0)
            cluster0 = newCluster;
        if (cluster1 < 0)
            cluster1 = newCluster;*/

        clusterSizes.push_back(size0 + size1);
        clusterDistances.push_back(std::get<2>(edge));
    }   

    cerr << "cluster sizes" << endl;
    for (auto& size : clusterSizes) {
        cerr << size << endl;
    }

    //5.0 Condense the clusters

    int minClusterSize = runProcConf.minClusterSize;

    struct MetaCluster {

        MetaCluster(double lambda_) {parent = -1; lambda = lambda_; stability = 0; /*leftoversCount = 0;*/ selected = false;}

        std::vector<ssize_t> childs;
        //std::std::vector<ssize_t> parents;
        ssize_t parent;
        double lambda;

        double stability;
       // size_t leftoversCount;
        bool selected;
    };

    std::vector<MetaCluster> metaClusters;

    cerr << "CONDENSING THE CLUSTERS" << endl;

    std::function<ssize_t(ssize_t, ssize_t, double)> condenseClusterRecursive = [&] (ssize_t i, ssize_t metaIndex, double lambda) {
        auto childs = clusterChilds[i];        

        int childIndex1 = childs.first;
        ssize_t childSize1 = clusterSizes[childIndex1];
        int childIndex2 = childs.second;
        ssize_t childSize2 = clusterSizes[childIndex2];

        if (metaIndex < 0) {
            metaClusters.emplace_back(lambda);
            metaIndex = metaClusters.size() - 1;
        }

    //    MetaCluster & metacluster =  metaClusters[metaIndex];

        if (childSize1 < minClusterSize && childSize2 < minClusterSize) {
            //Add the leftovers from both side
            MetaCluster & metacluster =  metaClusters[metaIndex];
            double lambda1 = (1.0/clusterDistances[childIndex1]) - lambda;
            double lambda2 = (1.0/clusterDistances[childIndex2]) - lambda;
            metacluster.stability += lambda1*childSize1;
            metacluster.stability += lambda2*childSize2;
        }
        else if (childSize1 < minClusterSize) {
            //need to merge with child2
            condenseClusterRecursive(childIndex2, metaIndex, lambda);
            //and add the "leftovers" from child1
            MetaCluster & metacluster =  metaClusters[metaIndex];
            double lambda1 = (1.0/clusterDistances[childIndex1]) - lambda;
            metacluster.stability += lambda1*childSize1;
        }
        else if (childSize2 < minClusterSize) {
            //need to merge with child1
            condenseClusterRecursive(childIndex1, metaIndex, lambda);
            //and add the "leftovers" from child2
            MetaCluster & metacluster =  metaClusters[metaIndex];
            double lambda2 = (1.0/clusterDistances[childIndex2]) - lambda;
            metacluster.stability += lambda2*childSize2;
        }
        else {
            //True split

            //need the lambda of the split
            double childLambda = 1.0 / clusterDistances[i];

            auto child1 = condenseClusterRecursive(childIndex1, -1, childLambda);
            auto child2 = condenseClusterRecursive(childIndex2, -1, childLambda);

            cerr << metaIndex << ",";
            cerr << child1 << ",";
            cerr << child2 << endl;

            MetaCluster & metacluster =  metaClusters[metaIndex];

            metacluster.childs.push_back(child1);
            metacluster.childs.push_back(child2);

            metaClusters[child1].parent = metaIndex;
            metaClusters[child2].parent = metaIndex;
        } 

        cerr << "returning meta index: " << metaIndex << endl;
        return metaIndex;

    };

    condenseClusterRecursive(clusterSizes.size() -1, -1, 0.0);

    cerr << "number of meta clusters: " << metaClusters.size() << endl;

    //5.0 Extract the clusters

    cerr << "EXTRACTING CLUSTERS" << endl;

    std::function<double(ssize_t)> getStabilityRecursive = [&] (ssize_t i) {
        MetaCluster & metacluster =  metaClusters[i];        

        if (metacluster.childs.size() > 0) {

            cerr << "Parent Meta Cluster: " << i << endl;

            double childStability1 = getStabilityRecursive(metacluster.childs[0]);
            double childStability2 = getStabilityRecursive(metacluster.childs[1]);

            
            cerr << "child stabilities: " << childStability1 << " , " << childStability2 << endl;
            cerr << "vs own: " << metacluster.stability << endl;

            metacluster.selected = metacluster.stability > childStability1 + childStability2;
            return metacluster.stability;
        }
        else
        {
            cerr << "Meta Cluster: " << i << endl;
            cerr << "   is a leaf " << endl;
            metacluster.selected = true;
            return metacluster.stability;
        }

    };

    //this will flag as true clusters or not
    getStabilityRecursive(0);

    //6.0 Finalize

    std::function<void(ssize_t)> finalizeRecursive = [&] (ssize_t i) {

        MetaCluster & metacluster =  metaClusters[i];
        if (metacluster.selected) {
            cerr << "MetaCluster " << i << " is a true cluster" << endl;
        }
        else if (metacluster.childs.size() > 0) {
            finalizeRecursive(metacluster.childs[0]);
            finalizeRecursive(metacluster.childs[1]);
        }
    };

    finalizeRecursive(0);

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
