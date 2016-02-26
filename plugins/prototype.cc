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

#include <boost/dynamic_bitset.hpp>

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
    //	std::vector<std::pair<ML::Feature, float>> features;
      std::vector<std::pair<int, int>> features; //<Feature index, value index>
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

    std::vector<size_t> ranges = featureSpace->getRanges();

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

          //  std::vector<std::pair<ML::Feature, float> > features(numFeatures)   ;
          //  = { { labelFeature, encodedLabel }, { weightFeature, weight } };
            std::vector<std::pair<int, int>> features(numFeatures);
                
            for (auto & c: row.columns) {
                //featureSpace->encodeFeature(std::get<0>(c), std::get<1>(c), features);
                featureSpace->encodeFeatureInt(std::get<0>(c), std::get<1>(c), features);
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
         /* struct FeatureValues
          {
              typedef std::pair<float, float> values;//score, leftscore
              std::map< float, values> scores;
          };*/

          PerPartition() : totalLeft(0), totalRight(0) {}
      		//std::vector< std::tuple< Feature, float > score;
      		//std::map< std::pair<ML::Feature, float>, float > score;
          //std::map< std::pair<ML::Feature, float>, float > leftscore; //merge with above?
         // std::vector<FeatureValues> scorePerFeature;
          std::vector<std::vector< std::pair<float, float> > > scorePerFeature; //value, score, left score

          std::pair<ML::Feature, int> bestSplit; //put this in a another array?

          boost::dynamic_bitset<> relevantFeatures;

          float totalLeft;
          float totalRight;
          bool isPureLeft;
          bool isPureRight;

          void init (std::vector<size_t>& ranges)
          {
              scorePerFeature.clear();
              scorePerFeature.resize(ranges.size());
              size_t count = 0;
              for (auto& f : scorePerFeature)
              {
                 f.resize(ranges[count]);
                 count++;
              }
          }

          void add(const std::pair<int, int>& point, float weight, float leftWeight)
          {
              //FeatureValues::values& v = scorePerFeature[point.first.arg1()].scores[point.second];
              //v.first += weight;
              //v.second += leftWeight; 

              auto& featureArray = scorePerFeature[point.first];
          //    cerr << "feat " << point.first.arg1() << endl;
         //     std::tuple<float, float, float> testTuple = std::make_tuple(point.second, 0.0f, 0.0f);
          //    auto insertIter = std::lower_bound(featureArray.begin(), featureArray.end(), testTuple);
           //   if (insertIter == featureArray.end() || std::get<0>(*insertIter) != point.second)
           //   {
              //    cerr << "adding new tuple " << point.second << "," << weight << "," << leftWeight << endl;
            //      testTuple = std::make_tuple(point.second, weight, leftWeight);
             //     featureArray.insert(insertIter, testTuple);
             // }
             // else
             // {
                //  cerr << "adding to existing tuple " << point.second << "," << weight << "," << leftWeight << endl;
              //    *insertIter = std::make_tuple(point.second, std::get<1>(*insertIter) + weight, std::get<2>(*insertIter) + leftWeight);
             // }
              auto& tuple = featureArray[point.second];
              std::get<0>(tuple) += weight;
              std::get<1>(tuple) += leftWeight;
          }

      	};

      	std::vector< PerPartition > perPartitionW[2];

        void clear(int iter, std::vector<size_t>& ranges)
        {          
            int numLeaf = (1 << (iter+1));

            currentFrame = iter%2;
            int nextFrame = (iter+1) %2;

            if (iter == 0)
            {
               // ExcAssert(nextFrame == 1);
                auto& pArray = perPartitionW[0];
                pArray.resize(1);  
                pArray[0].init(ranges);         
            }

            //TODO: not super efficient because we lose the memory buffers in the score map in PerPartition
            auto& pArray = perPartitionW[nextFrame];
            pArray.clear();
            pArray.resize(numLeaf);

            for (auto& p : pArray)
            {              
               p.init(ranges);
            }
        }

        int currentFrame; //we use double buffering on the PerPartition arrays
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
        cerr << "num features " << numFeatures << endl;

        //Todo: init this and dont realloc it all the time
      	BagW& w = wPerBag[bag];
        Tree& tree = treesPerBag[bag];
        int currentFrame = iteration%2;
        int nextFrame = (iteration+1) %2;
        auto& currentPartitions = w.perPartitionW[currentFrame];
        auto& nextPartitions = w.perPartitionW[nextFrame];

        if (iteration > 0 && w.lastNewPartition == 0)
          return true;

        w.clear(iteration, ranges);

        if (iteration == 0)
        {
            //ExcAssert(w.perPartitionW[0].size() == 1);
            w.perPartitionW[0][0].relevantFeatures.resize(numFeatures);
        }
        
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
            //    ExcAssert(partition < currentPartitions.size());
            //    ExcAssert(partition < currentPartitions.size());
                BagW::PerPartition& partitionScore = currentPartitions[partition];

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

                        if (partitionScore.relevantFeatures.test(f.first))
                          continue;

                        //partitionScore.score[f] += weight;
                        //partitionScore.leftscore[f] += label ? weight : 0;
                      //  partitionScore.add(f, weight, label ? weight : 0);

                        float leftWeight = label ? weight : 0;

                        ///////////

                        auto& featureArray = partitionScore.scorePerFeature[f.first];
                      //  cerr << "feat " << f.first << endl;

                        auto& tuple = featureArray[f.second];
                        std::get<0>(tuple) += weight;
                        std::get<1>(tuple) += leftWeight;

                        ////////////

                    }

                    //if (iteration == 0)
                    //  cerr << endl;
                }
                
            }
        }      	

        w.lastNewPartition = 0;
        int numNewPartition = 0;

        //ok, now for every partition, find the best split point
        int maxpartition = currentPartitions.size();
        cerr << "max partition: " << maxpartition << endl;
        for (int partition = 0; partition < maxpartition; partition++)
        {
            BagW::PerPartition& partitionScore = currentPartitions[partition];

            //partition is uniform
            if (partitionScore.totalLeft == 0 || partitionScore.totalRight == 0)
            {
             //  cerr << "partition " << partition << " is uniform" << endl;
               continue;
            }

            numNewPartition++;

            float bestScore = 0.0f;
            float bestLeft = 0.0f;
            float bestRight = 0.0f;
         //   float bestScoreSide = 0.0f;
            //std::pair<ML::Feature, float> bestSplit;
            std::pair<int, float> bestSplit;
            float bigTotalScore = partitionScore.totalLeft + partitionScore.totalRight;

          //  cerr << "bigTotalScore " << bigTotalScore << "," << partitionScore.totalLeft << "," << partitionScore.totalRight << endl;

            for (int fIndex = 0; fIndex < numFeatures; ++fIndex)
            {
             //   cerr << "feature index " << fIndex << endl;
                auto& fmap = partitionScore.scorePerFeature[fIndex];



              /*  if (fmap.size() == 1)
                {
               //     cerr << "feature has only 1 value " << endl;
                    partitionScore.relevantFeatures.set(fIndex);
                }
                else if (!fmap.empty())*/
                {
                //    cerr << "feature not empty " << endl;
                    float totalTrue = 0.0f;
                    float total = 0.0f;
                    int count = 0;

                    int currentValue = -1;
                    for (auto& value : fmap)
                    {
                        currentValue++;
                        float weight = std::get<0>(value);
                        if (weight < 0.001f)
                          continue;

                        totalTrue += std::get<1>(value);
                        total += weight;

                        count++;

                        float totalRight = bigTotalScore - total;
                        if (totalRight > 0.01f)
                        {
                           // if (totalTrue > total + 0.01f)
                           // {
                            //   cerr << "bad totals " << totalTrue << ", " << total << endl;
                           //    ExcAssert(false);
                           // }
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
                              //  cerr << "NEW BEST TOTAL TRUE: " << totalTrue << endl;
                                bestScore = totalPurity;
                                bestSplit = std::pair<int, float>(fIndex, currentValue);
                                bestLeft = purityLeft;
                                bestRight = purityRight;
                            }
                            
                        }  
                    }

                    if (count <= 1)
                    {
                        partitionScore.relevantFeatures.set(fIndex);
                    }
                }
            }

         //   cerr << "test partition: " << partition << endl;

            partitionScore.bestSplit = { Feature(1, bestSplit.first, 0), bestSplit.second };
            partitionScore.isPureLeft =  bestLeft > 0.999f;
            partitionScore.isPureRight =  bestRight > 0.999f;

        //    cerr << "PURES " << bestLeft << " " << bestRight << endl;
        //    cerr << "PURES " << partitionScore.isPureLeft << " " << partitionScore.isPureRight << endl;

            int rightNextPartition = partition | (1 << iteration);
            nextPartitions[partition].relevantFeatures = partitionScore.relevantFeatures;
            nextPartitions[rightNextPartition].relevantFeatures = std::move(partitionScore.relevantFeatures);

            //float uniformity = bestScoreSide > 0 ? (bestScoreSide / partitionScore.totalLeft) : (bestScoreSide / partitionScore.totalRight);

        //    cerr << "splitting bag " << bag << " on partition " << partition 
         //        << " with feature " << bestSplit.first << " value " << bestSplit.second 
            //     << " and purity " << bestScore /*<< ", uniformity: " << uniformity */<< endl;

            if (iteration == 0)
            {
                tree.nodes.emplace_back(TreeNode{ partitionScore.bestSplit.first, bestSplit.second, -1, -1 });                
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

                tree.nodes.emplace_back(TreeNode{ partitionScore.bestSplit.first, bestSplit.second, -1, -1 });
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

            BagW::PerPartition& partitionScore = currentPartitions[partition];

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
           //     cerr << "uniform" << endl;
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
    if (false)
    {
        for (int bag = 0; bag < numBags; ++bag)
        {
            cerr << "Printing tree for bag " << bag << endl;
            Tree& tree = treesPerBag[bag];
            if (tree.nodes.size() > 0)
            {
                printNode(tree, tree.nodes[0], 0);
            }
        }
    }
   
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