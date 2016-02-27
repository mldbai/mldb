/*


*/

#include "prototype.h"
#include "mldb/types/basic_value_descriptions.h"
#include "mldb/ml/value_descriptions.h"
#include "mldb/plugins/sql_expression_extractors.h"
#include "mldb/plugins/dataset_feature_space.h"
#include "mldb/server/bound_queries.h"
#include "mldb/types/any_impl.h"
#include "mldb/base/parallel.h"
#include "mldb/arch/timers.h"
#include "mldb/jml/utils/profile.h"
#include "mldb/ml/jml/tree.h"
#include "mldb/ml/jml/stump_training_bin.h"
#include "mldb/ml/jml/decision_tree.h"
#include "mldb/base/thread_pool.h"

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
        int exampleNum;             ///< index into feature array
    };
    
    // Entry for an individual feature
    struct Feature {
        Feature()
            : active(false), ordinal(false), numBuckets(0),
              info(nullptr), buckets(nullptr)
        {
        }

        bool active;  ///< If true, the feature can be split on
        bool ordinal; ///< If true, it's continuous valued; otherwise categ.
        int numBuckets;
        const DatasetFeatureSpace::ColumnInfo * info;
        const int * buckets;  ///< Bucket number, per example
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

    void addRow(bool label, float weight, int exampleNum)
    {
        rows.emplace_back(Row{label, weight, exampleNum});
    }

    /** Split the partition here. */
    std::pair<PartitionData, PartitionData>
    split(int featureToSplitOn, int splitValue)
    {
        ExcAssertGreaterEqual(featureToSplitOn, 0);
        ExcAssertLess(featureToSplitOn, features.size());
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
                int bucket = features[featureToSplitOn].buckets[r.exampleNum];
                (bucket <= splitValue ? left : right).addRow(r);
            }
        }
        else {
            // Categorical feature
            for (auto & r: rows) {
                int bucket = features[featureToSplitOn].buckets[r.exampleNum];
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

        bool empty() const { return v[0] == 0 && v[1] == 0; };

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
#if 0
        for (auto & r: rows) {
            wAll[r.label] += r.weight;
            for (unsigned i = 0;  i < nf;  ++i) {
                if (!features[i].active)
                    continue;
                int bucket = features[i].buckets[r.exampleNum];
                ExcAssertLess(bucket, w[i].size());
                w[i][bucket][r.label] += r.weight;
            }
        }
#else
        for (auto & r: rows) {
            wAll[r.label] += r.weight;
        }

        for (unsigned i = 0;  i < nf;  ++i) {
            if (!features[i].active)
                continue;
            bool twoBuckets = false;
            int lastBucket = -1;
            for (auto & r: rows) {
                int bucket = features[i].buckets[r.exampleNum];

                if (!twoBuckets) {
                    if (lastBucket != -1 && bucket != lastBucket)
                        twoBuckets = true;
                    lastBucket = bucket;
                }

                ExcAssertLess(bucket, w[i].size());
                w[i][bucket][r.label] += r.weight;
            }

            // If all examples were in a single bucket, then the
            // feature is no longer active.
            if (!twoBuckets)
                features[i].active = false;
        }
#endif

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
                    if (w[i][j].empty())
                        continue;

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

        if (bestFeature == -1) {
            Tree::Leaf * leaf = tree.new_leaf();
            fillinBase(leaf, wAll);

            return leaf;
        }

        std::pair<PartitionData, PartitionData> splits
            = split(bestFeature, bestSplit);

        //cerr << "done split in " << timer.elapsed() << endl;

        //cerr << "left had " << splits.first.rows.size() << " rows" << endl;
        //cerr << "right had " << splits.second.rows.size() << " rows" << endl;

        ML::Tree::Ptr left, right;
        auto runLeft = [&] () { left = splits.first.train(depth + 1, maxDepth, tree); };
        auto runRight = [&] () { right = splits.second.train(depth + 1, maxDepth, tree); };

#if 1
        ThreadPool tp;
        if (splits.first.rows.size() > 10000) {
            tp.add(runLeft);
        }
        if (splits.second.rows.size() > 10000) {
            tp.add(runRight);
        }
        if (splits.first.rows.size() <= 10000) {
            runLeft();
        }
        if (splits.second.rows.size() <= 10000) {
            runRight();
        }

        tp.waitForAll();
#else
        runLeft();
        runRight();
#endif

        if (left && right) {
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
    const int numBags = 10;
    const int featurePartitionsPerBag = 10;
    const int maxDepth = 20;

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

            vector<vector<int> > featureBuckets(data.features.size());

            int n = 0;
            for (size_t i = 0;  i < lines.size();  ++i) {
                if (training_weights[i] == 0)
                    continue;

                DataLine & line = lines[i];

                for (unsigned i = 0;  i < data.features.size();  ++i) {
                    if (data.features[i].active)
                        featureBuckets[i].push_back(line.features[i]);
                }
                data.addRow(line.label, training_weights[i], n++);
            }

            for (unsigned i = 0;  i < data.features.size();  ++i) {
                if (data.features[i].active) {
                    data.features[i].buckets = &featureBuckets[i][0];
                }
            }


            auto trainFeaturePartition = [&] (int partitionNum)
            {
                boost::mt19937 rng(bag + 245 + partitionNum);

                PartitionData mydata(data);
                for (unsigned i = 0;  i < data.features.size();  ++i) {
                    if (mydata.features[i].active
                        && rng() % 3 != 0)
                        mydata.features[i].active = false;
                }

                ML::Timer timer;
                ML::Tree tree;
                tree.root = mydata.train(0 /* depth */, maxDepth, tree);
                cerr << "bag " << bag << " partition " << partitionNum << " took "
                     << timer.elapsed() << endl;

                ML::Decision_Tree dtree(featureSpace, labelFeature);
                dtree.tree = std::move(tree);
            };

            parallelMap(0, featurePartitionsPerBag, trainFeaturePartition);

            //cerr << dtree.print() << endl;

        };

    for (unsigned i = 0;  i < numBags;  ++i)
        bagPrepare(i);
    //parallelMap(0, numBags, bagPrepare);

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
