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
#include "mldb/arch/bit_range_ops.h"
#include "mldb/arch/bitops.h"

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

template<typename T>
static std::shared_ptr<T>
makeSharedArray(size_t len)
{
    return std::shared_ptr<T>(new T[len],
                              [] (T * p) { delete[] p; });
}

#if 1
/** Holds an array of bucket indexes, efficiently. */
struct BucketList {

    BucketList()
        : entryBits(0), numEntries(0)
    {
    }

    int operator [] (uint32_t i) const
    {
        //ExcAssertLess(i, numEntries);
        size_t wordNum = (i * entryBits) / 64;
        size_t bitNum = (i * entryBits) % 64;
        return (storage.get()[wordNum] >> bitNum) & ((1ULL << entryBits) - 1);
    }

    std::shared_ptr<const uint64_t> storage;
    int entryBits;
    size_t numEntries;
};

/** Writable version of the above.  OK to slice. */
struct WritableBucketList: public BucketList {
    WritableBucketList()
        : current(0), bitsWritten(0)
    {
    }

    void init(size_t numElements, uint32_t numBuckets)
    {
        entryBits = ML::highest_bit(numBuckets) + 1;

        // Take a number of bits per entry that evenly divides into
        // 64 bits.
        if (entryBits == 0) ;
        else if (entryBits == 1) ;
        else if (entryBits == 2) ;
        else if (entryBits <= 4)
            entryBits = 4;
        else if (entryBits <= 8)
            entryBits = 8;
        else if (entryBits <= 16)
            entryBits = 16;
        else entryBits = 32;

        //cerr << "using " << entryBits << " bits for " << numBuckets
        //     << " buckets" << endl;

        size_t numWords = (entryBits * numElements + 63) / 64;
        auto writableStorage = makeSharedArray<uint64_t>(numWords);
        this->current = writableStorage.get();
        this->storage = writableStorage;
        this->bitsWritten = 0;
        this->numEntries = numElements;
        this->numWritten = 0;
    }

    void write(uint64_t value)
    {
        uint64_t already = bitsWritten ? *current : 0;
        *current = already | (value << bitsWritten);
        bitsWritten += entryBits;
        current += (bitsWritten >= 64);
        bitsWritten *= (bitsWritten < 64);

        //ExcAssertEqual(this->operator [] (numWritten), value);
        //ExcAssertLess(numWritten, numEntries);
        numWritten += 1;
    }

    uint64_t * current;
    int bitsWritten;
    size_t numWritten;
};
#else
/** Holds an array of bucket indexes, efficiently. */
struct BucketList {

    BucketList()
        : entryBits(0)
    {
    }

    int operator [] (uint32_t i) const
    {
        ML::Bit_Extractor<uint64_t> bits(storage.get());
        bits.advance(i * entryBits);
        return bits.extract<uint64_t>(entryBits);
    }

    template<typename Fn>
    void forEach(Fn fn) const
    {
        ML::Bit_Extractor<uint64_t> bits(storage.get());

        for (size_t i = 0;  i < numBuckets;  ++i) {
            fn(bits.extract<uint64_t>(entryBits));
            bits.advance(entryBits);
        }
    }

    std::shared_ptr<const uint64_t> storage;
    int entryBits;
    size_t numBuckets;
};

/** Writable version of the above.  OK to slice. */
struct WritableBucketList: public BucketList {
    WritableBucketList()
        : writer(nullptr)
    {
    }

    void init(size_t numElements, uint32_t numBuckets)
    {
        entryBits = ML::highest_bit(numBuckets) + 1;
        size_t numWords = (entryBits * numElements + 63) / 64;
        auto writableStorage = makeSharedArray<uint64_t>(numWords);
        writer = ML::Bit_Writer<uint64_t>(writableStorage.get());
        storage = writableStorage;
        this->numBuckets = numBuckets;
    }

    void write(uint32_t value)
    {
        writer.write(value, entryBits);
    }

    ML::Bit_Writer<uint64_t> writer;
};

#endif

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
              info(nullptr)
        {
        }

        bool active;  ///< If true, the feature can be split on
        bool ordinal; ///< If true, it's continuous valued; otherwise categ.
        int numBuckets;
        const DatasetFeatureSpace::ColumnInfo * info;
        BucketList buckets;  ///< List of bucket numbers, per example
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

        bool empty() const { return total() == 0; }

        Float total() const { return v[0] + v[1]; }

        WT & operator += (const WT & other)
        {
            v[0] += other.v[0];
            v[1] += other.v[1];
            return *this;
        }

        WT operator + (const WT & other) const
        {
            WT result = *this;
            result += other;
            return result;
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

    /** Split the partition here. */
    std::pair<PartitionData, PartitionData>
    split(int featureToSplitOn, int splitValue, const W & wLeft, const W & wRight)
    {
        ExcAssertGreaterEqual(featureToSplitOn, 0);
        ExcAssertLess(featureToSplitOn, features.size());

        PartitionData sides[2];
        PartitionData & left = sides[0];
        PartitionData & right = sides[1];

        left.fs = fs;
        right.fs = fs;
        left.features = features;
        right.features = features;

        bool ordinal = features[featureToSplitOn].ordinal;

        double useRatio = 1.0 * rows.size() / rows.back().exampleNum;

        bool reIndex = useRatio < 0.1;
        //reIndex = false;
        //cerr << "useRatio = " << useRatio << endl;

        if (!reIndex) {

            sides[0].rows.reserve(rows.size());
            sides[1].rows.reserve(rows.size());

            for (size_t i = 0;  i < rows.size();  ++i) {
                int bucket = features[featureToSplitOn].buckets[rows[i].exampleNum];
                int side = ordinal ? bucket > splitValue : bucket != splitValue;
                sides[side].addRow(rows[i]);
            }

            rows.clear();
            rows.shrink_to_fit();
            features.clear();
        }
        else {
            int nf = features.size();

            // For each example, it goes either in left or right, depending
            // upon the value of the chosen feature.

            std::vector<uint8_t> lr(rows.size());
            bool ordinal = features[featureToSplitOn].ordinal;
            size_t numOnSide[2] = { 0, 0 };

            // TODO: could reserve less than this...
            sides[0].rows.reserve(rows.size());
            sides[1].rows.reserve(rows.size());

            for (size_t i = 0;  i < rows.size();  ++i) {
                int bucket = features[featureToSplitOn].buckets[rows[i].exampleNum];
                int side = ordinal ? bucket > splitValue : bucket != splitValue;
                lr[i] = side;
                sides[side].addRow(rows[i].label, rows[i].weight, numOnSide[side]++);
            }

#if 0
            cerr << "left " << numOnSide[0] << " " << wLeft.total()
                 << " " << (100.0 * numOnSide[0] / (rows.back().exampleNum + 1))
                 << "%" << endl;
            cerr << "right " << numOnSide[1] << " " << wRight.total()
                 << " " << (100.0 * numOnSide[1] / (rows.back().exampleNum + 1))
                 << "%" << endl;
#endif

            for (unsigned i = 0;  i < nf;  ++i) {
                if (!features[i].active)
                    continue;

                WritableBucketList newFeatures[2];
                newFeatures[0].init(numOnSide[0], features[i].info->distinctValues);
                newFeatures[1].init(numOnSide[1], features[i].info->distinctValues);
                size_t index[2] = { 0, 0 };

                for (size_t j = 0;  j < rows.size();  ++j) {
                    int side = lr[j];
                    newFeatures[side].write(features[i].buckets[rows[j].exampleNum]);
                    ++index[side];
                }

                sides[0].features[i].buckets = newFeatures[0];
                sides[1].features[i].buckets = newFeatures[1];
            }

            rows.clear();
            rows.shrink_to_fit();
            features.clear();
        }

        return { std::move(left), std::move(right) };
    }

    /** Test all features for a split.  Returns the feature number,
        the bucket number and the goodness of the split.

        Outputs
        - Z score of split
        - Feature number
        - Split point
        - W for the left side of the split
        - W from the right side of the split
    */
    std::tuple<double, int, int, W, W>
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

        if (debug) {
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

        // We have no impurity in our bucket.  Time to stop
        if (wAll[0] == 0 || wAll[1] == 0)
            return std::make_tuple(1.0, -1, -1, wAll, W());

        double bestScore = INFINITY;
        int bestFeature = -1;
        int bestSplit = -1;
        
        W bestLeft;
        W bestRight;

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

                    if (s < bestScore) {
                        bestScore = s;
                        bestFeature = i;
                        bestSplit = j;
                        bestRight = wFalse;
                        bestLeft = wTrue;
                    }

                    wFalse -= w[i][j];
                    wTrue += w[i][j];

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
                        bestRight = wFalse;
                        bestLeft = w[i][j];
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

        return std::make_tuple(bestScore, bestFeature, bestSplit, bestLeft, bestRight);
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

        double bestScore;
        int bestFeature;
        int bestSplit;
        W wLeft;
        W wRight;
        
        std::tie(bestScore, bestFeature, bestSplit, wLeft, wRight)
            = testAll(depth);

        if (bestFeature == -1) {
            Tree::Leaf * leaf = tree.new_leaf();
            fillinBase(leaf, wLeft + wRight);
            
            return leaf;
        }

        std::pair<PartitionData, PartitionData> splits
            = split(bestFeature, bestSplit, wLeft, wRight);

        //cerr << "done split in " << timer.elapsed() << endl;

        //cerr << "left had " << splits.first.rows.size() << " rows" << endl;
        //cerr << "right had " << splits.second.rows.size() << " rows" << endl;

        ML::Tree::Ptr left, right;
        auto runLeft = [&] () { left = splits.first.train(depth + 1, maxDepth, tree); };
        auto runRight = [&] () { right = splits.second.train(depth + 1, maxDepth, tree); };

#if 1
        ThreadPool tp;
        size_t leftRows = splits.first.rows.size();
        size_t rightRows = splits.second.rows.size();

        // Put the smallest one on the thread pool, so that we have the highest
        // probability of running both on our thread in case of lots of work.
        if (leftRows < rightRows) {
            tp.add(runLeft);
            runRight();
        }
        else {
            tp.add(runRight);
            runLeft();
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
            fillinBase(node, wLeft + wRight);

            return node;
        }
        else {
            Tree::Leaf * leaf = tree.new_leaf();
            fillinBase(leaf, wLeft + wRight);

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
    int numBags = 5;
    int featurePartitionsPerBag = 20;
    int maxDepth = 20;
    int maxBagsAtOnce = 5;
    int maxTreesAtOnce = 8;

    //numBags = 1;
    //maxDepth = 4;
    //featurePartitionsPerBag = 1;

    PrototypeConfig runProcConf =
        applyRunConfOverProcConf(procedureConfig, run);

    ML::Timer timer;

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

    cerr << "feature space construction took " << timer.elapsed() << endl;
    timer.restart();

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
        float weight;
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
            line.weight = weight;
            line.label = encodedLabel;

            //thr.fvs.emplace_back(row.rowName, std::move(features));
            return true;
        };

    //we should use buckets so that we dont need to merge stuff

    std::vector<std::shared_ptr<SqlExpression> > extra
        = { label, weight };

    BoundSelectQuery(select,
                     *boundDataset.dataset,
                     boundDataset.asName,
                     runProcConf.trainingData.stm->when,
                     *runProcConf.trainingData.stm->where,
                     runProcConf.trainingData.stm->orderBy, extra,
                     false /* implicit order by row hash */,
                     numBuckets)
        .execute(aggregator, 
                 runProcConf.trainingData.stm->offset, 
                 runProcConf.trainingData.stm->limit, 
                 nullptr /* progress */);


    cerr << "select took " << timer.elapsed() << endl;

    /// WE GOT THE INPUT DATA NOW BUILD THE BAGS

    const float trainprop = 1.0f;
    
    PrototypeRNG myrng;
    
    auto bagPrepare = [&] (int bag)
        {
            ML::Timer bagTimer;

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

            
            size_t numNonZero = (training_weights != 0).count();
            cerr << "numNonZero = " << numNonZero << endl;

            PartitionData data(*featureSpace);

            vector<WritableBucketList>
            featureBuckets(data.features.size());

            for (unsigned i = 0;  i < data.features.size();  ++i) {
                if (data.features[i].active) {
                    featureBuckets[i].init(numNonZero,
                                           data.features[i].info->distinctValues);
                    data.features[i].buckets = featureBuckets[i];
                }
            }

            data.rows.reserve(numNonZero);

            int n = 0;
            for (size_t i = 0;  i < lines.size();  ++i) {
                if (training_weights[i] == 0)
                    continue;

                ExcAssert(n < numNonZero);

                DataLine & line = lines[i];

                for (unsigned i = 0;  i < data.features.size();  ++i) {
                    if (data.features[i].active)
                        featureBuckets[i].write(line.features[i]);
                }
                data.addRow(line.label, training_weights[i], n++);
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
                //cerr << dtree.print() << endl;
            };

            parallelMap(0, featurePartitionsPerBag, trainFeaturePartition,
                        maxTreesAtOnce);

            cerr << "bag " << bag << " took " << bagTimer.elapsed() << endl;
        };

    //for (unsigned i = 0;  i < numBags;  ++i)
    //    bagPrepare(i);
    parallelMap(0, numBags, bagPrepare, maxBagsAtOnce);

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
