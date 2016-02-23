/*


*/

#include "prototype.h"
#include "mldb/types/basic_value_descriptions.h"
#include "mldb/ml/value_descriptions.h"
#include "mldb/plugins/sql_expression_extractors.h"
#include "mldb/plugins/dataset_feature_space.h"
#include "mldb/server/bound_queries.h"
#include "mldb/types/any_impl.h"
#include "mldb/jml/utils/worker_task.h"
#include "mldb/arch/timers.h"
#include "mldb/jml/utils/profile.h"

#include <boost/random/mersenne_twister.hpp>
#include <boost/random/uniform_int.hpp>
#include <boost/random/uniform_01.hpp>

using namespace std;
using namespace ML;

namespace Datacratic {
namespace MLDB {

DEFINE_STRUCTURE_DESCRIPTION(PrototypeConfig);

PrototypeConfigDescription::
PrototypeConfigDescription()
{
    addField("trainingData", &PrototypeConfig::trainingData,
             "Specification of the data for input to the classifier procedure. "
             "The select expression must contain these two sub-expressions: one row expression "
             "to identify the features on which to train and one scalar expression "
             "to identify the label.  The type of the label expression must match "
             "that of the classifier mode: a boolean (0 or 1) for `boolean` mode; "
             "a real for regression mode, and any combination of numbers and strings "
             "for `categorical` mode.  Labels with a null value will have their row skipped. "
             "The select expression can contain an optional weigth expression.  The weight "
             "allows the relative importance of examples to be set.  It must "
             "be a real number.  If the expression is not specified each example will have "
             "a weight of one.  Rows with a null weight will cause a training error. "
             "The select statement does not support groupby and having clauses. "
             "Also, unlike most select expressions, this one can only select whole columns, "
             "not expressions involving columns. So X will work, but not X + 1. "
             "If you need derived values in the select expression, create a dataset with "
             "the derived columns as a previous step and run the classifier over that dataset instead.");
    //addField("modelFileUrl", &PrototypeConfig::modelFileUrl,
    //         "URL where the model file (with extension '.cls') should be saved. "
    //         "This file can be loaded by a function of type 'classifier'.");
    addField("configuration", &PrototypeConfig::configuration,
             "Configuration object to use for the classifier.  Each one has "
             "its own parameters.  If none is passed, then the configuration "
             "will be loaded from the ConfigurationFile parameter",
             Json::Value());
    addParent<ProcedureConfig>();    
}

struct PrototypeRNG {

    PrototypeRNG()
    {
    }

    boost::mt19937 rng;
    
    template<class T>
    T operator () (T val)
    {
        return rng() % val;
    }
};

/*****************************************************************************/
/* CLASSIFIER PROCEDURE                                                       */
/*****************************************************************************/

PrototypeProcedure::
PrototypeProcedure(MldbServer * owner,
            PolyConfig config,
            const std::function<bool (const Json::Value &)> & onProgress)
    : Procedure(owner)
{
    this->procedureConfig = config.params.convert<PrototypeConfig>();
}

Any
PrototypeProcedure::
getStatus() const
{
    return Any();
}

/* WE WILL ONLY DO BOOLEAN CLASSIFICATION FOR NOW */

RunOutput
PrototypeProcedure::
run(const ProcedureRunConfig & run,
      const std::function<bool (const Json::Value &)> & onProgress) const
{
	const int numBags = 1;

	PrototypeConfig runProcConf =
        applyRunConfOverProcConf(procedureConfig, run);

    // this includes being empty
   // if(!runProcConf.modelFileUrl.valid()) {
   //     throw ML::Exception("modelFileUrl is not valid");
   // }

    // 1.  Get the input dataset
    SqlExpressionMldbContext context(server);

    auto boundDataset = runProcConf.trainingData.stm->from->bind(context);

    ML::Mutable_Feature_Info labelInfo = ML::Mutable_Feature_Info(ML::BOOLEAN);
    labelInfo.set_biased(true);

        auto extractWithinExpression = [](std::shared_ptr<SqlExpression> expr) 
        -> std::shared_ptr<SqlRowExpression>
        {
            auto withinExpression = std::dynamic_pointer_cast<const SelectWithinExpression>(expr);
            if (withinExpression)
                return withinExpression->select;
            
            return nullptr;
        };

    auto label = extractNamedSubSelect("label", runProcConf.trainingData.stm->select)->expression;
    auto features = extractNamedSubSelect("features", runProcConf.trainingData.stm->select)->expression;
    //auto weightSubSelect = extractNamedSubSelect("weight", runProcConf.trainingData.stm->select);
    shared_ptr<SqlExpression> weight = /*weightSubSelect ? weightSubSelect->expression :*/ SqlExpression::ONE;
    shared_ptr<SqlRowExpression> subSelect = extractWithinExpression(features);

    if (!label || !subSelect)
        throw HttpReturnException(400, "trainingData must return a 'features' row and a 'label'");

    SelectExpression select({subSelect});

    std::set<ColumnName> knownInputColumns;
    {
        // Find only those variables used
        SqlExpressionDatasetContext context(boundDataset);
        
        auto selectBound = select.bind(context);

        for (auto & c : selectBound.info->getKnownColumns()) {
            knownInputColumns.insert(c.columnName);
        }
    }

    // THIS ACTUALL MIGHT DO A LOTTTTTTT OF WORK
    // "GET COLUMN STATS" -> list of all possible values per column ?!? 
    // Profile this!
        auto featureSpace = std::make_shared<DatasetFeatureSpace>
        (boundDataset.dataset, labelInfo, knownInputColumns);

    for (auto& c : knownInputColumns) {
      cerr << c.toString() << " feature " << featureSpace->getFeature(c) << endl;
    }

    //NEED A TABLE WITH N ROW CONTAINING FOR EACH DATA POINT:
    // FEATURES LIST  (k * sizeof)
    // WEIGHT FOR EACH BAG (float*bag)
    // LEAF/PARTITION PER BAG
    // LABEL

    //TODO: Need to pack this into 1 memory buffer

    //optimize when we want every row
    size_t numRow = boundDataset.dataset->getMatrixView()->getRowCount();

    struct DataLine
    {
    	std::vector<std::pair<ML::Feature, float>> features;
    	std::vector<float> weightsperbag;
    	std::vector<int> partitionperbag;
    	bool label;
    };

  //  struct ThreadAccum {
    	std::vector<DataLine> lines;
  //  };

    lines.resize(numRow);

    //std::atomic<int> numRows(0);

    distribution<float> weightPerRow(numRow);

    int numFeatures = knownInputColumns.size();
    cerr << "NUM FEATURES : " << numFeatures << endl;

    //PerThreadAccumulator<ThreadAccum> accum;

    int numBuckets = 32*8; //whatever
    size_t numPerBucket = std::max((size_t)std::floor((float)numRow / numBuckets), (size_t)1);

    std::vector<size_t> groupCount(numBuckets, 0);

    auto aggregator = [&] (NamedRowValue & row_,
                           const std::vector<ExpressionValue> & extraVals,
                           int bucket)
        {
        	 MatrixNamedRow row = row_.flattenDestructive();
           CellValue label = extraVals.at(0).getAtom();
           if (label.empty())
                return true; //should we support that?

          //  ThreadAccum & thr = accum.get();

            bool encodedLabel = label.isTrue();

            float weight = extraVals.at(1).toDouble();

            //++numRows;

            std::vector<std::pair<ML::Feature, float> > features(numFeatures)   ;
          //  = { { labelFeature, encodedLabel }, { weightFeature, weight } };
                
            for (auto & c: row.columns) {
                featureSpace->encodeFeature(std::get<0>(c), std::get<1>(c), features);
            }

            size_t rowIndex = groupCount[bucket] + bucket*numPerBucket;
            groupCount[bucket] += 1;

            DataLine & line = lines[rowIndex];

            line.features = std::move(features);
            line.weightsperbag.resize(numBags, weight);
            line.partitionperbag.resize(numBags, 0);
            line.label = encodedLabel;

            weightPerRow[rowIndex] = weight;

            //thr.fvs.emplace_back(row.rowName, std::move(features));
            return true;
        };

    //we should use buckets so that we dont need to merge stuff

    std::vector<std::shared_ptr<SqlExpression> > extra
        = { label, weight };

    BoundSelectQuery(select, *boundDataset.dataset,
                     boundDataset.asName, runProcConf.trainingData.stm->when,
                     *runProcConf.trainingData.stm->where,
                     runProcConf.trainingData.stm->orderBy, extra,
                     false /* implicit order by row hash */,
                     numBuckets)
        .execute(aggregator, 
                 runProcConf.trainingData.stm->offset, 
                 runProcConf.trainingData.stm->limit, 
                 nullptr /* progress */);


    /// WE GOT THE INPUT DATA NOW BUILD THE BAGS

    const float trainprop = 1.0f;
    
    PrototypeRNG myrng;
    
    auto bagPrepare = [&] (int bag)
        {
        	boost::mt19937 rng;
        	distribution<float> in_training(numRow);
  		    vector<int> tr_ex_nums(numRow);
  		    std::iota(tr_ex_nums.begin(), tr_ex_nums.end(), 0);  // 0, 1, 2, 3, 4, 5, 6, 7... N
  		    std::random_shuffle(tr_ex_nums.begin(), tr_ex_nums.end(), myrng);  //5, 1, 14, N...
  		    for (unsigned i = 0;  i < numRow * trainprop;  ++i)
  		        in_training[tr_ex_nums[i]] = 1.0;                      //0, 0, 0, 1, 0, 1, 0, 1, 1, ....
  		    //distribution<float> not_training(nx, 1.0);                 //1, 1, 1, 0, 1, 0, 1, 0, 0, ...
  		    //not_training -= in_training;

  		    distribution<float> example_weights(numRow);

          	// Generate our example weights. 
  		    for (unsigned i = 0;  i < numRow;  ++i)
  		        example_weights[myrng(numRow)] += 1.0;   // MBOLDUC random numbers between 0 and N - lots of 0. Several samples in neither training nor validation?

  		    distribution<float> training_weights
  		        = in_training * example_weights * weightPerRow;
  		    training_weights.normalize();          // MBOLDUC can't we know the norm? Is this using SIMD?

  		    for (unsigned i = 0;  i < numRow;  ++i)
  		    {
  		    	DataLine & line = lines[i];
  		    	line.weightsperbag[bag];
  		    }

  		    //distribution<float> validate_weights
  		    //    = not_training * example_weights * info.training_ex_weights;
  		    //validate_weights.normalize();          // MBOLDUC can't we know the norm? Is this using SIMD?

        };

    run_in_parallel(0, numBags, bagPrepare);

  	weightPerRow.resize(0);    // dont need it anymore
  	weightPerRow.shrink_to_fit();

  	//NEED TO FIND THE SPLIT POINTS


	//ACTUAL LEARNING
	//START WITH SOMETHING NAIVE

/*	Worker_Task & worker =  Worker_Task::instance();

	auto groupId = worker.get_group(NO_JOB, "");

	auto bagExec = [&] (int bag)
        {
        //	cerr << bag << endl;
        };

    ML::Timer timer;
    for (int i = 0; i < numBags; ++i)
    {
    	worker.add(std::bind<void>(bagExec, i), "", groupId);
    }
     cerr << "adding tasks in " << timer.elapsed() << endl;*/

    struct BagW
    {
      	struct PerPartition
      	{
          PerPartition() : totalLeft(0), totalRight(0) {}
      		//std::vector< std::tuple< Feature, float > score;
      		std::map< std::pair<ML::Feature, float>, float > score;
          std::map< std::pair<ML::Feature, float>, float > leftscore; //merge with above?

          std::pair<ML::Feature, float> bestSplit; //put this in a another array?
          float totalLeft;
          float totalRight;
          bool isPureLeft;
          bool isPureRight;

      	};

      	std::vector< PerPartition > perPartitionW;

        void clear(int iter)
        {          
            int numLeaf = (1 << iter); //+1 is for root

            //TODO: not super efficient because we lose the memory buffers in the score map in PerPartition
            perPartitionW.clear();
            perPartitionW.resize(numLeaf);
        }

        int lastNewPartition;
    };

    int right = 1;
    int doneMask = 1 << 31;
    int iteration = 0;

    std::vector<BagW> wPerBag(numBags);

    struct TreeNode
    {
        Feature feature;
        float value;
        int childIndexLeft;
        int childIndexRight;
    };   

    struct Tree
    {
        std::vector<TreeNode> nodes;
    };

    std::function<void(Tree& , TreeNode& , int )> printNode = [&] (Tree& tree, TreeNode& node, int depth)
    {
        for (int i = 0; i < depth; ++i)
          cerr << "-";

        cerr << node.feature << "," << node.value << endl;

        if (node.childIndexLeft > 0)
          printNode(tree, tree.nodes[node.childIndexLeft], depth+1);
        else
        { 
            for (int i = 0; i < depth + 1; ++i)
            cerr << "-";
            cerr << "null" << endl;
        }

        if (node.childIndexRight > 0)
          printNode(tree, tree.nodes[node.childIndexRight], depth+1);
        else 
        { 
            for (int i = 0; i < depth + 1; ++i)
            cerr << "-";
            cerr << "null" << endl;
        }
    };

    std::vector<Tree> treesPerBag(numBags);

    auto bagIter = [&] (int bag)
    {
        STACK_PROFILE(BagIter);
        cerr << "starting bag " << bag << endl;

        //Todo: init this and dont realloc it all the time
      	BagW& w = wPerBag[bag];
        Tree& tree = treesPerBag[bag];

        if (iteration > 0 && w.lastNewPartition == 0)
          return true;

        w.clear(iteration);
        
        {
            STACK_PROFILE(BagIter_scanlines);
       //     STACK_PROFILE_SEED(BagIter_scanlines_a);
            for (auto& line : lines)
            {
                float weight = line.weightsperbag[bag];
                if (weight == 0)
                  continue;

                int partition = line.partitionperbag[bag];

                if ( partition < 0)
                {
                //   cerr << "line is part of uniform partition" << endl;
                   continue; //already reached uniformity
                }

                bool label = line.label;
                
                
                //weight = label ? weight : -weight;

                BagW::PerPartition& partitionScore = w.perPartitionW[partition];

                //TODO: check if partition is dead-end
              //  if (iteration == 0)
               //       cerr << line.label;

                {
                  //  STACK_PROFILE_ADD(BagIter_scanlines_a);
                    partitionScore.totalLeft += label ? weight : 0;
                    partitionScore.totalRight += label ? 0 : weight;
                    for (auto& f : line.features)
                    {
                  //      if (iteration == 0)
                    //      cerr << " (" << f.first.arg1() << "," << f.second << ")";

                        partitionScore.score[f] += weight;
                        partitionScore.leftscore[f] += label ? weight : 0;
                    }

                    //if (iteration == 0)
                    //  cerr << endl;
                }
                
            }
        }      	

        w.lastNewPartition = 0;
        int numNewPartition = 0;

        //ok, now for every partition, find the best split point
        int maxpartition = w.perPartitionW.size();
        cerr << "max partition: " << maxpartition << endl;
        for (int partition = 0; partition < maxpartition; partition++)
        {
            BagW::PerPartition& partitionScore = w.perPartitionW[partition];

            //partition is uniform
            if (partitionScore.totalLeft == 0 || partitionScore.totalRight == 0)
            {
          //     cerr << "partition " << partition << " is uniform" << endl;
               continue;
            }

            numNewPartition++;

            float bestScore = 0.0f;
            float bestLeft = 0.0f;
            float bestRight = 0.0f;
         //   float bestScoreSide = 0.0f;
            std::pair<ML::Feature, float> bestSplit;

            auto iter = partitionScore.score.begin();
            auto trueiter = partitionScore.leftscore.begin();
            auto iterEnd = partitionScore.score.end();

            ML::Feature currentType;
            //float currentTypeSum = 0.0f;

            float totalTrue = 0.0f;
            float total = 0.0f;
            float bigTotalScore = partitionScore.totalLeft + partitionScore.totalRight;

            while (iter != iterEnd)
            {
                const std::pair<ML::Feature, float>& f = iter->first;
                if (f.first != currentType )
                {
                    totalTrue = 0.0f;
                    total = 0.0f;
                    currentType = f.first;
                 //   cerr << "next feature" << endl;
                }

               /* currentTypeSum += iter->second;
                //TODO: This is not like really correct
                //Need to take into account the total weight on each side
                float absoluteScore = fabs(currentTypeSum);
                if ( absoluteScore > bestScore)
                {
                    bestScoreSide = currentTypeSum;
                    bestScore = absoluteScore;
                    bestSplit = f;
                } */

                totalTrue += trueiter->second;
                total += iter->second;

                float totalRight = bigTotalScore - total;
                if (totalRight > 0.01f)
                {
                    float probTrueOnLeft = totalTrue / total;
                    float probTrueOnRight = (partitionScore.totalLeft - totalTrue) / (totalRight);

                    //cerr << "probTrueOnLeft: " << probTrueOnLeft << " ,probTrueOnRight: " << probTrueOnRight << endl;

                    float purityLeft = probTrueOnLeft > 0.5f ? probTrueOnLeft : 1.0f - probTrueOnLeft;
                    float purityRight = probTrueOnRight > 0.5f ? probTrueOnRight : 1.0f - probTrueOnRight;

                    //float totalPurity = purityLeft * purityRight;
                    float totalPurity = total*purityLeft + totalRight*purityRight;

                    if ( totalPurity > bestScore)
                    {
                        //bestScoreSide = totalPurity;
                        bestScore = totalPurity;
                        bestSplit = f;
                        bestLeft = purityLeft;
                        bestRight = purityRight;
                    }
                }                

                ++iter;    
                ++trueiter;           
            }

            partitionScore.bestSplit = bestSplit;
            partitionScore.isPureLeft =  bestLeft > 0.999f;
            partitionScore.isPureRight =  bestRight > 0.999f;

            //float uniformity = bestScoreSide > 0 ? (bestScoreSide / partitionScore.totalLeft) : (bestScoreSide / partitionScore.totalRight);

        //    cerr << "splitting bag " << bag << " on partition " << partition 
         //        << " with feature " << bestSplit.first << " value " << bestSplit.second 
            //     << " and purity " << bestScore /*<< ", uniformity: " << uniformity */<< endl;

            if (iteration == 0)
            {
                tree.nodes.emplace_back(TreeNode{ bestSplit.first, bestSplit.second, -1, -1 });                
            }
            else
            {
                //find the parent node.
                //todo: keep this somewhere? In a hash map of partition? an array? still would be log(n)...  
                TreeNode* parent = &(tree.nodes[0]);
                for (int i = 0; i < iteration - 1; ++i)
                {
                   bool right = (partition & (1 << i));                   
                   int index = (right ? parent->childIndexRight : parent->childIndexLeft);
                   parent = &(tree.nodes[index]);
                }

                bool right = (partition & (1 << (iteration - 1)));

                if (right)
                  parent->childIndexRight = tree.nodes.size();
                else
                  parent->childIndexLeft = tree.nodes.size();

                tree.nodes.emplace_back(TreeNode{ bestSplit.first, bestSplit.second, -1, -1 });
            }

           // printNode(tree, tree.nodes[0], 0);
        }

        cerr << "bag " << bag << " has " << 2*numNewPartition << "new partition leafs" << endl; 

        w.lastNewPartition = numNewPartition;
        if (numNewPartition == 0)
          return true;

        int iterMaskRight = (1 << iteration);

        //Re-partition lines
        for (auto& line : lines)
        {
            float weight = line.weightsperbag[bag];
            if (weight == 0)
              continue;

            //bool label = line.label;
            int& partition = line.partitionperbag[bag];

            if (partition < 0)
              continue; //part of a uniform partition

            BagW::PerPartition& partitionScore = w.perPartitionW[partition];

         //   if (partitionScore.)
        ///    {
          //      partition |= doneMask;
          //      continue;
          //  }

            std::pair<ML::Feature, float> bestSplit = partitionScore.bestSplit;

            bool isLeft = line.features[bestSplit.first.arg1()].second <= bestSplit.second;
            int mask = isLeft? 0 : iterMaskRight;            

            if ((partitionScore.isPureLeft && isLeft) || (!isLeft && partitionScore.isPureRight))
            {
                //cerr << "uniform" << endl;
                mask |= doneMask;
            } 

            partition |= mask; 
        }

        return true;
    };
	
    int test = 10;
  	while (test > 0)
  	{
  		/*for (auto& line : lines)
  		{
  			worker.run_until_finished(groupId, false);
  			line.label += 0;
  			//worker.unlock_group(groupId);

  			//TODO: dont delete the jobs, make them resetable
  			groupId = worker.get_group(NO_JOB, "");
  			for (int i = 0; i < numBags; ++i)
  		    {
  		    	worker.add(std::bind<void>(bagExec, i), "", groupId);
  		    }
  		} */
  		--test;

  		run_in_parallel(0, numBags, bagIter);

      right = right << 1; 

  		cerr << "FINISHED ITER" << endl;
      iteration++;
  	} 	

    //print the bags
   /* for (int bag = 0; bag < numBags; ++bag)
    {
        cerr << "Printing tree for bag " << bag << endl;
        Tree& tree = treesPerBag[bag];
        if (tree.nodes.size() > 0)
        {
            printNode(tree, tree.nodes[0], 0);
        }
    }*/

    return RunOutput();

}

namespace{
	static RegisterProcedureType<PrototypeProcedure, PrototypeConfig>
	regPrototypeClassifier(builtinPackage(),
	              "prototype",
	              "Train a supervised classifier",
	              "procedures/Classifier.md.html");

}












}
}