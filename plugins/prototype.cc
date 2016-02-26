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
#include "mldb/ml/jml/tree.h"
#include "mldb/ml/jml/stump_training_bin.h"
#include "mldb/ml/jml/decision_tree.h"

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

/** Holds the set of data for a partition of a decision tree. */
struct PartitionData {

    PartitionData()
        : fs(nullptr)
    {
    }

    PartitionData(const DatasetFeatureSpace & fs)
        : fs(&fs), features(fs.columnInfo.size())
    {
        for (auto & c: fs.columnInfo) {
            Feature & f = features.at(c.second.index);
            f.active = c.second.distinctValues > 1;
            f.ordinal = !c.second.buckets.splits.empty();
            f.numBuckets = c.second.distinctValues;
            if (c.second.buckets.splits.size())
                f.numBuckets = c.second.buckets.splits.size() + 1;
            f.info = &c.second;
        }
    }

    const DatasetFeatureSpace * fs;

    /// Entry for an individual row
    struct Row {
        bool label;                 ///< Label associated with
        float weight;               ///< Weight of the example 
        const int * features;       ///< Value of each feature (dense)
    };
    
    // Entry for an individual feature
    struct Feature {
        Feature()
            : active(false), ordinal(false), numBuckets(0),
              info(nullptr)
        {
        }

        bool active;  ///< If true, the feature can be split on
        bool ordinal; ///< If true, it's continuous valued; otherwise categ.
        int numBuckets;
        const DatasetFeatureSpace::ColumnInfo * info;
    };

    // All rows of data in this partition
    std::vector<Row> rows;

    // All features that are active
    std::vector<Feature> features;

    /** Add the given row. */
    void addRow(const Row & row)
    {
        rows.push_back(row);
    }

    void addRow(bool label, float weight, const int * features)
    {
        rows.emplace_back(Row{label, weight, features});
    }

    /** Split the partition here. */
    std::pair<PartitionData, PartitionData>
    split(int featureToSplitOn, int splitValue)
    {
        PartitionData left, right;

        left.fs = fs;
        right.fs = fs;
        left.features = features;
        right.features = features;

        // For each example, it goes either in left or right, depending
        // upon the value of the chosen feature.

        if (features[featureToSplitOn].ordinal) {
            // Ordinal feature
            for (auto & r: rows) {
                int bucket = r.features[featureToSplitOn];
                (bucket <= splitValue ? left : right).addRow(r);
            }
        }
        else {
            // Categorical feature
            for (auto & r: rows) {
                int bucket = r.features[featureToSplitOn];
                (bucket == splitValue ? left : right).addRow(r);
            }
        }

        return { std::move(left), std::move(right) };
    }

    template<typename Float>
    struct WT {
        WT()
            : v { 0, 0 }
        {
        }

        Float v[2];

        Float & operator [] (bool i)
        {
            return v[i];
        }

        const Float & operator [] (bool i) const
        {
            return v[i];
        }

        WT & operator += (const WT & other)
        {
            v[0] += other.v[0];
            v[1] += other.v[1];
            return *this;
        }

        WT & operator -= (const WT & other)
        {
            v[0] -= other.v[0];
            v[1] -= other.v[1];
            return *this;
        }
    };

    //typedef WT<double> W;
    typedef WT<ML::FixedPointAccum64> W;

    /** Test all features for a split.  Returns the feature number,
        the bucket number and the goodness of the split.
    */
    std::tuple<double, int, int, W>
    testAll(int depth)
    {
        bool debug = false;

        int nf = features.size();

        // For each feature, for each bucket, for each label
        std::vector<std::vector<W> > w(nf);

        size_t totalNumBuckets = 0;
        size_t activeFeatures = 0;

        for (unsigned i = 0;  i < nf;  ++i) {
            if (!features[i].active)
                continue;
            ++activeFeatures;
            w[i].resize(features[i].numBuckets);
            totalNumBuckets += features[i].numBuckets;
        }

        if (debug || depth == 0) {
            cerr << "total of " << totalNumBuckets << " buckets" << endl;
            cerr << activeFeatures << " of " << nf << " features active"
                 << endl;
        }

        W wAll;
        for (auto & r: rows) {
            wAll[r.label] += r.weight;
            for (unsigned i = 0;  i < nf;  ++i) {
                if (!features[i].active)
                    continue;
                int bucket = r.features[i];
                w[i][bucket][r.label] += r.weight;
            }
        }

        if (wAll[0] == 0 || wAll[1] == 0)
            return std::make_tuple(1.0, -1, -1, wAll);

        double bestScore = INFINITY;
        int bestFeature = -1;
        int bestSplit = -1;

        int bucketsEmpty = 0;
        int bucketsOne = 0;
        int bucketsBoth = 0;

        // Score each feature
        for (unsigned i = 0;  i < nf;  ++i) {
            if (!features[i].active)
                continue;

            W wAll; // TODO: do we need this?
            for (auto & wt: w[i]) {
                wAll += wt;
                bucketsEmpty += wt[0] == 0 && wt[1] == 0;
                bucketsBoth += wt[0] != 0 && wt[1] != 0;
                bucketsOne += (wt[0] == 0) ^ (wt[1] == 0);
            }

            auto score = [] (const W & wFalse, const W & wTrue) -> double
                {
                    double score
                    = 2.0 * (  sqrt(wFalse[0] * wFalse[1])
                             + sqrt(wTrue[0] * wTrue[1]));
                    return score;
                };
            
            if (debug) {
                cerr << "feature " << i << " " << features[i].info->columnName
                     << endl;
                cerr << "    all: " << wAll[0] << " " << wAll[1] << endl;
            }

            if (features[i].ordinal) {
                // Calculate best split point for ordered values
                W wFalse = wAll, wTrue;
                
                // Now test split points one by one
                for (unsigned j = 0;  j < w[i].size() - 1;  ++j) {
                    double s = score(wFalse, wTrue);

                    if (debug) {
                        cerr << "  ord split " << j << " "
                             << features[i].info->getBucketValue(j)
                             << " had score " << s << endl;
                        cerr << "    false: " << wFalse[0] << " " << wFalse[1] << endl;
                        cerr << "    true:  " << wTrue[0] << " " << wTrue[1] << endl;
                    }

                    wFalse -= w[i][j];
                    wTrue += w[i][j];

                    if (s < bestScore) {
                        bestScore = s;
                        bestFeature = i;
                        bestSplit = j;
                    }
                }
            }
            else {
                // Calculate best split point for non-ordered values
                // Now test split points one by one
                for (unsigned j = 0;  j < w[i].size();  ++j) {
                    W wFalse = wAll;
                    wFalse -= w[i][j];

                    double s = score(wFalse, w[i][j]);

                    if (debug) {
                        cerr << "  non ord split " << j << " "
                             << features[i].info->getBucketValue(j)
                             << " had score " << s << endl;
                        cerr << "    false: " << wFalse[0] << " " << wFalse[1] << endl;
                        cerr << "    true:  " << w[i][j][0] << " " << w[i][j][1] << endl;
                    }
             
                    if (s < bestScore) {
                        bestScore = s;
                        bestFeature = i;
                        bestSplit = j;
                    }
                }

            }
        }
        
        if (debug) {
            cerr << "buckets: empty " << bucketsEmpty << " one " << bucketsOne
                 << " both " << bucketsBoth << endl;
            cerr << "bestScore " << bestScore << endl;
            cerr << "bestFeature " << bestFeature << " "
                 << features[bestFeature].info->columnName << endl;
            cerr << "bestSplit " << bestSplit << " "
                 << features[bestFeature].info->getBucketValue(bestSplit)
                 << endl;
        }

        return std::make_tuple(bestScore, bestFeature, bestSplit, wAll);
    }

    static void fillinBase(ML::Tree::Base * node, const W & wAll)
    {
        node->examples = wAll[0] + wAll[1];
        node->pred = {
            float(wAll[0]) / node->examples,
            float(wAll[1]) / node->examples };
    }

    ML::Tree::Ptr getLeaf(ML::Tree & tree)
    {
        W wAll;
        for (auto & r: rows) {
            wAll[r.label] += r.weight;
        }
        
        ML::Tree::Node * node = tree.new_node();
        fillinBase(node, wAll);
        return node;
    }

    ML::Tree::Ptr train(int depth, int maxDepth,
                        ML::Tree & tree)
    {
        if (rows.empty())
            return ML::Tree::Ptr();
        if (rows.size() < 2)
            return getLeaf(tree);

        if (depth >= maxDepth)
            return getLeaf(tree);

        //cerr << "training with " << rows.size() << " rows" << endl;

        ML::Timer timer;

        double bestScore;
        int bestFeature;
        int bestSplit;
        W wAll;
        
        std::tie(bestScore, bestFeature, bestSplit, wAll) = testAll(depth);

        std::pair<PartitionData, PartitionData> splits
            = split(bestFeature, bestSplit);

        //cerr << "done split in " << timer.elapsed() << endl;

        //cerr << "left had " << splits.first.rows.size() << " rows" << endl;
        //cerr << "right had " << splits.second.rows.size() << " rows" << endl;

        ML::Tree::Ptr left = splits.first.train(depth + 1, maxDepth, tree);
        ML::Tree::Ptr right = splits.second.train(depth + 1, maxDepth, tree);

        if (bestFeature != -1 && left && right) {
            Tree::Node * node = tree.new_node();
            ML::Feature feature = fs->getFeature(features[bestFeature].info->columnName);
            float splitVal;
            if (features[bestFeature].ordinal) {
                splitVal = (bestSplit == features[bestFeature].info->buckets.splits.size()
                            ? INFINITY : features[bestFeature].info->buckets.splits.at(bestSplit));
            }
            else {
                splitVal = bestSplit;
            }

            ML::Split split(feature, splitVal,
                            features[bestFeature].ordinal
                            ? ML::Split::LESS : ML::Split::EQUAL);
            
            node->split = split;
            node->child_true = left;
            node->child_false = right;
            node->z = bestScore;
            fillinBase(node, wAll);

            return node;
        }
        else {
            Tree::Leaf * leaf = tree.new_leaf();
            fillinBase(leaf, wAll);

            return leaf;
        }
    }
};


/* WE WILL ONLY DO BOOLEAN CLASSIFICATION FOR NOW */

RunOutput
PrototypeProcedure::
run(const ProcedureRunConfig & run,
      const std::function<bool (const Json::Value &)> & onProgress) const
{
    const int numBags = 100;
    int maxDepth = 20;

    //numBags = 1;
    //maxDepth = 4;

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
        (boundDataset.dataset, labelInfo, knownInputColumns, true /* bucketize */);

    for (auto& c : knownInputColumns) {
        cerr << c.toString() << " feature " << featureSpace->getFeature(c) << endl;
    }

    // Get the feature buckets per row


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
        std::vector<int> features;
    	std::vector<float> weightsperbag;
    	bool label;
    };

    std::vector<DataLine> lines;

    lines.resize(numRow);

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

            std::vector<int> features(numFeatures);
            //  = { { labelFeature, encodedLabel }, { weightFeature, weight } };
                
            for (auto & c: row.columns) {
                int featureNum;
                int featureBucketNum;
                std::tie(featureNum, featureBucketNum)
                    = featureSpace->getFeatureBucket(std::get<0>(c), std::get<1>(c));
                features[featureNum] = featureBucketNum;
            }

            size_t rowIndex = groupCount[bucket] + bucket*numPerBucket;
            groupCount[bucket] += 1;

            DataLine & line = lines[rowIndex];

            line.features = std::move(features);
            line.weightsperbag.resize(numBags, weight);
            line.label = encodedLabel;

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
            boost::mt19937 rng(bag + 245);
            distribution<float> in_training(numRow);
            vector<int> tr_ex_nums(numRow);
            std::iota(tr_ex_nums.begin(), tr_ex_nums.end(), 0);  // 0, 1, 2, 3, 4, 5, 6, 7... N
            std::random_shuffle(tr_ex_nums.begin(), tr_ex_nums.end(), myrng);  //5, 1, 14, N...
            for (unsigned i = 0;  i < numRow * trainprop;  ++i)
                in_training[tr_ex_nums[i]] = 1.0;                      //0, 0, 0, 1, 0, 1, 0, 1, 1, ....

            distribution<float> example_weights(numRow);

            // Generate our example weights. 
            for (unsigned i = 0;  i < numRow;  ++i)
                example_weights[myrng(numRow)] += 1.0;   // MBOLDUC random numbers between 0 and N - lots of 0. Several samples in neither training nor validation?

            distribution<float> training_weights
                = in_training * example_weights;

            training_weights.normalize();          // MBOLDUC can't we know the norm? Is this using SIMD?

            


            PartitionData data(*featureSpace);

            for (unsigned i = 0;  i < data.features.size();  ++i)
                if (data.features[i].active
                    && rng() % 2 != 0)
                    data.features[i].active = false;
            
            vector<vector<int> > bucketColumns;

            for (size_t i = 0;  i < lines.size();  ++i) {
                if (training_weights[i] == 0)
                    continue;

                DataLine & line = lines[i];

                data.addRow(line.label, training_weights[i], &line.features[0]);
            }



            ML::Timer timer;
            ML::Tree tree;
            tree.root = data.train(0 /* depth */, maxDepth, tree);
            cerr << "bag " << bag << " took " << timer.elapsed() << endl;

            ML::Decision_Tree dtree(featureSpace, labelFeature);
            dtree.tree = std::move(tree);
            
            //cerr << dtree.print() << endl;

        };

    run_in_parallel(0, numBags, bagPrepare);


#if 0

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
          std::vector<std::vector< std::tuple<float, float, float> > > scorePerFeature; //value, score, left score

          std::pair<ML::Feature, float> bestSplit; //put this in a another array?

          boost::dynamic_bitset<> relevantFeatures;

          float totalLeft;
          float totalRight;
          bool isPureLeft;
          bool isPureRight;

          void init (int numFeatures)
          {
              scorePerFeature.clear();
              scorePerFeature.resize(numFeatures);
              for (auto& f : scorePerFeature)
              {
                 f.reserve(1000);
              }
          }

          void add(const std::pair<ML::Feature, float>& point, float weight, float leftWeight)
          {
              //FeatureValues::values& v = scorePerFeature[point.first.arg1()].scores[point.second];
              //v.first += weight;
              //v.second += leftWeight; 

              auto& featureArray = scorePerFeature[point.first.arg1()];
          //    cerr << "feat " << point.first.arg1() << endl;
              std::tuple<float, float, float> testTuple = std::make_tuple(point.second, 0.0f, 0.0f);
              auto insertIter = std::lower_bound(featureArray.begin(), featureArray.end(), testTuple);
              if (insertIter == featureArray.end() || std::get<0>(*insertIter) != point.second)
              {
              //    cerr << "adding new tuple " << point.second << "," << weight << "," << leftWeight << endl;
                  testTuple = std::make_tuple(point.second, weight, leftWeight);
                  featureArray.insert(insertIter, testTuple);
              }
              else
              {
                //  cerr << "adding to existing tuple " << point.second << "," << weight << "," << leftWeight << endl;
                  *insertIter = std::make_tuple(point.second, std::get<1>(*insertIter) + weight, std::get<2>(*insertIter) + leftWeight);
              }
          }

      	};

      	std::vector< PerPartition > perPartitionW[2];

        void clear(int iter, int numFeatures)
        {          
            int numLeaf = (1 << (iter+1));

            currentFrame = iter%2;
            int nextFrame = (iter+1) %2;

            if (iter == 0)
            {
               // ExcAssert(nextFrame == 1);
                auto& pArray = perPartitionW[0];
                pArray.resize(1);  
                pArray[0].init(numFeatures);         
            }

            //TODO: not super efficient because we lose the memory buffers in the score map in PerPartition
            auto& pArray = perPartitionW[nextFrame];
            pArray.clear();
            pArray.resize(numLeaf);

            for (auto& p : pArray)
            {              
               p.init(numFeatures);
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

        w.clear(iteration, numFeatures);

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

                        if (partitionScore.relevantFeatures.test(f.first.arg1()))
                          continue;

                        //partitionScore.score[f] += weight;
                        //partitionScore.leftscore[f] += label ? weight : 0;
                      //  partitionScore.add(f, weight, label ? weight : 0);

                        float leftWeight = label ? weight : 0;

                        ///////////

                        auto& featureArray = partitionScore.scorePerFeature[f.first.arg1()];
                    //    cerr << "feat " << point.first.arg1() << endl;
                        std::tuple<float, float, float> testTuple = std::make_tuple(f.second, 0.0f, 0.0f);
                        auto insertIter = std::lower_bound(featureArray.begin(), featureArray.end(), testTuple);
                        if (insertIter == featureArray.end() || std::get<0>(*insertIter) != f.second)
                        {
                        //    cerr << "adding new tuple " << point.second << "," << weight << "," << leftWeight << endl;
                            testTuple = std::make_tuple(f.second, weight, leftWeight);
                            featureArray.insert(insertIter, testTuple);
                        }
                        else
                        {
                          //  cerr << "adding to existing tuple " << point.second << "," << weight << "," << leftWeight << endl;
                            *insertIter = std::make_tuple(f.second, std::get<1>(*insertIter) + weight, std::get<2>(*insertIter) + leftWeight);
                        }

                        ////////////

                          // auto& v = partitionScore.scorePerFeature[f.first.arg1()].scores[f.second];
                          //v.first += weight;
                          // v.second += label ? weight : 0; 
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
            cerr << "partition " << partition << endl;
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

                if (fmap.size() == 1)
                {
               //     cerr << "feature has only 1 value " << endl;
                    partitionScore.relevantFeatures.set(fIndex);
                }
                else if (!fmap.empty())
                {
                    //cerr << "feature " << fIndex << " has " << fmap.size() << " values" << endl;
                //    cerr << "feature not empty " << endl;
                    float totalTrue = 0.0f;
                    float total = 0.0f;

                    for (auto& value : fmap)
                    {
                        totalTrue += std::get<2>(value);
                        total += std::get<1>(value);

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
                                bestSplit = std::pair<int, float>(fIndex, std::get<0>(value));
                                bestLeft = purityLeft;
                                bestRight = purityRight;
                            }
                            
                        }  
                    }
                }
            }


/////////
          //  auto iter = partitionScore.score.begin();
          //  auto trueiter = partitionScore.leftscore.begin();
          //  auto iterEnd = partitionScore.score.end();

         /*   ML::Feature currentType;
            
            float totalTrue = 0.0f;
            float total = -1.0f;
            int currentCount = 0;
            float bigTotalScore = partitionScore.totalLeft + partitionScore.totalRight;

            while (iter != iterEnd)
            {
                const std::pair<ML::Feature, float>& f = iter->first;
                if (f.first != currentType )
                {
                    //was there a unique value for this feature in this partition?
                    if (currentCount == 1)
                    {
                        //partitionScore.removetype(currentType);
                       // if (currentType.arg1() >= partitionScore.relevantFeatures.size())
                      //  {
                       //     cerr << currentFrame << "," << nextFrame << endl;
                        //    cerr << currentType.arg1() << ", " << partitionScore.relevantFeatures.size() << endl;
                         //   ExcAssert(false);
                       // }
                     //   cerr << "excluding feature type " << currentType.arg1() << " from partition " << partition << endl;
                        partitionScore.relevantFeatures.set(currentType.arg1());
                    }

                    totalTrue = 0.0f;
                    total = 0.0f;
                    currentType = f.first;
                    currentCount = 1;
                 //   cerr << "next feature" << endl;
                }
                else
                {
                  ++currentCount;
                }*/

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

             /*   totalTrue += trueiter->second;
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
            }*/
///////////////
           /* if (currentCount == 1)
            {
                //partitionScore.removetype(currentType);
                partitionScore.relevantFeatures.set(currentType.arg1());
            }*/

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
    if (true)
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

#endif
   
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
