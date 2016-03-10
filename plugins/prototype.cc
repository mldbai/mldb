/*


*/

#include "prototype.h"
#include "mldb/types/basic_value_descriptions.h"
#include "mldb/types/set_description.h"
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
            cerr << "column " << c.first << " index " << c.second.index
                 << endl;
            Feature & f = features.at(c.second.index);
            f.active = c.second.distinctValues > 1;
            f.buckets = c.second.buckets;
            f.info = &c.second;
        }
    }

    /** Create a new dataset with the same labels, different weights
        (by element-wise multiplication), and with
        zero weights filtered out such that example numbers are strictly
        increasing.
    */
    PartitionData reweightAndCompact(const std::vector<float> & weights) const
    {
        size_t numNonZero = 0;
        for (auto & w: weights)
            numNonZero += (w != 0);
        
        PartitionData data;
        data.features = this->features;
        data.fs = this->fs;
        data.reserve(numNonZero);

        vector<WritableBucketList>
            featureBuckets(features.size());

        for (unsigned i = 0;  i < data.features.size();  ++i) {
            if (data.features[i].active) {
                featureBuckets[i].init(numNonZero,
                                       data.features[i].info->distinctValues);
                //cerr << "initializing with " << numNonZero << " slots of "
                //     << data.features[i].info->distinctValues << " values"
                //     << endl;
                data.features[i].buckets = featureBuckets[i];
            }
        }

        auto doFeature = [&] (size_t f)
            {
                if (f == data.features.size()) {
                    // Do the row index
                    size_t n = 0;
                    for (size_t i = 0;  i < rows.size();  ++i) {
                        if (weights[i] == 0)
                            continue;
                        data.addRow(rows[i].label, rows[i].weight * weights[i],
                                    n++);
                    }
                    ExcAssertEqual(n, numNonZero);
                    return;
                }

                if (!data.features[f].active)
                    return;

                size_t n = 0;
                for (size_t i = 0;  i < rows.size();  ++i) {
                    if (weights[i] == 0)
                        continue;

                    uint32_t bucket = features[f].buckets[rows[i].exampleNum];
                    featureBuckets[f].write(bucket);
                    ++n;
                }

                ExcAssertEqual(n, numNonZero);
            };

        Datacratic::parallelMap(0, data.features.size() + 1, doFeature);

        return data;
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
            : active(false), ordinal(true),
              info(nullptr)
        {
        }

        bool active;  ///< If true, the feature can be split on
        bool ordinal; ///< If true, it's continuous valued; otherwise categ.
        const DatasetFeatureSpace::ColumnInfo * info;
        BucketList buckets;  ///< List of bucket numbers, per example
    };

    // All rows of data in this partition
    std::vector<Row> rows;

    // All features that are active
    std::vector<Feature> features;

    /** Reserve enough space for the given number of rows. */
    void reserve(size_t n)
    {
        rows.reserve(n);
    }

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

        std::unique_ptr<ML::Timer> timer;
        if (depth <= 4)
            timer.reset(new ML::Timer);

        // For each feature, for each bucket, for each label
        std::vector<std::vector<W> > w(nf);
        std::vector<int> maxSplits(nf);

        size_t totalNumBuckets = 0;
        size_t activeFeatures = 0;

        for (unsigned i = 0;  i < nf;  ++i) {
            if (!features[i].active)
                continue;
            ++activeFeatures;
            w[i].resize(features[i].buckets.numBuckets);
            totalNumBuckets += features[i].buckets.numBuckets;
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

        auto doFeature = [&] (int i)
            {
                if (i == nf) {
                    for (auto & r: rows) {
                        wAll[r.label] += r.weight;
                    }
                    return;
                }

                if (!features[i].active)
                    return;
                bool twoBuckets = false;
                int lastBucket = -1;
                int maxBucket = 0;

                for (size_t j = 0;  j < rows.size();  ++j) {
                    auto & r = rows[j];
                    int bucket = features[i].buckets[r.exampleNum];

                    twoBuckets = twoBuckets
                        || (lastBucket != -1 && bucket != lastBucket);
                    lastBucket = bucket;

                    if (bucket >= w[i].size()) {
                        cerr << "depth " << depth << " row " << j << " of "
                             << rows.size() << " bucket " << bucket
                             << " weight " << rows[j].weight
                             << " exampleNum " << r.exampleNum
                             << " num buckets " << w[i].size()
                             << " featureName " << features[i].info->columnName
                             << endl;
                    }

                    ExcAssertLess(bucket, w[i].size());
                    //if (bucket >= w[i].size())
                    //    continue; // HACK HACK HACK
                    w[i][bucket][r.label] += r.weight;
                    maxBucket = std::max(maxBucket, bucket);
                }

                // If all examples were in a single bucket, then the
                // feature is no longer active.
                if (!twoBuckets)
                    features[i].active = false;

                maxSplits[i] = maxBucket;
            };

        if (depth < 4 || true) {
            parallelMap(0, nf + 1, doFeature);
        }
        else {
            for (unsigned i = 0;  i <= nf;  ++i)
                doFeature(i);
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

            int maxBucket = maxSplits[i];

            if (features[i].ordinal) {
                // Calculate best split point for ordered values
                W wFalse = wAll, wTrue;
                
                // Now test split points one by one
                for (unsigned j = 0;  j < maxBucket/*w[i].size() - 1*/;  ++j) {
                    if (w[i][j].empty())
                        continue;

                    double s = score(wFalse, wTrue);

                    if (debug) {
                        cerr << "  ord split " << j << " "
                             << features[i].info->bucketDescriptions.getValue(j)
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
                for (unsigned j = 0;  j <= maxBucket/*w[i].size()*/;  ++j) {
                    W wFalse = wAll;
                    wFalse -= w[i][j];

                    double s = score(wFalse, w[i][j]);

                    if (debug) {
                        cerr << "  non ord split " << j << " "
                             << features[i].info->bucketDescriptions.getValue(j)
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
                 << features[bestFeature].info->bucketDescriptions.getValue(bestSplit)
                 << endl;
        }

        if (timer)
            cerr << "chunk at depth " << depth << " with " << rows.size()
                 << " rows took " << timer->elapsed()
                 << endl;

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
        size_t leftRows = splits.first.rows.size();
        size_t rightRows = splits.second.rows.size();

       /* if (leftRows == 0 || rightRows == 0) {
            //cerr << "no split found" << endl;
            // NOTE: this is a bug, and we should assert on it
            // only keeping without an assert forbenchmarking
            Tree::Leaf * leaf = tree.new_leaf();
            fillinBase(leaf, wLeft + wRight);

            return leaf;
        }*/
        //ExcAssert(leftRows > 0 && rightRows > 0);

        ThreadPool tp;
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
                auto splitCell = features[bestFeature].info->bucketDescriptions
                    .getSplit(bestFeature);
                if (splitCell.isNumeric())
                    splitVal = splitCell.toDouble();
                else splitVal = bestSplit;
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

struct ColumnScope: public SqlExpressionMldbContext {
    ColumnScope(MldbServer * server, std::shared_ptr<Dataset> dataset)
        : SqlExpressionMldbContext(server), dataset(dataset)
    {
    }

    std::shared_ptr<Dataset> dataset;

    std::map<ColumnName, size_t> requiredColumnIndexes;
    std::vector<ColumnName> requiredColumns;

    struct RowScope: public SqlRowScope {
        RowScope(size_t rowIndex,
                 const std::vector<std::vector<CellValue> > & inputs)
            : rowIndex(rowIndex), inputs(inputs)
        {
        }

        size_t rowIndex;
        const std::vector<std::vector<CellValue> > & inputs;
    };

    virtual VariableGetter
    doGetVariable(const Utf8String & tableName,
                  const Utf8String & variableName)
    {
        ColumnName columnName(variableName);
        if (!requiredColumnIndexes.count(columnName)) {
            size_t index = requiredColumns.size();
            requiredColumnIndexes[columnName] = index;
            requiredColumns.push_back(columnName);
        }

        size_t index = requiredColumnIndexes[columnName];
        
        return {[=] (const SqlRowScope & scope,
                     ExpressionValue & storage,
                     const VariableFilter & filter) -> const ExpressionValue &
                {
                    auto & row = scope.as<RowScope>();
                    return storage
                        = ExpressionValue(row.inputs.at(index).at(row.rowIndex),
                                          Date::notADate());
                },
                std::make_shared<AtomValueInfo>()};
    }

    virtual GetAllColumnsOutput
    doGetAllColumns(const Utf8String & tableName,
                    std::function<Utf8String (const Utf8String &)> keep)
    {
        throw HttpReturnException(400, "Attempt to bind expression with wildcard");
    }

    virtual BoundFunction
    doGetFunction(const Utf8String & tableName,
                  const Utf8String & functionName,
                  const std::vector<BoundSqlExpression> & args,
                  SqlBindingScope & argScope)
    {
        return SqlBindingScope::doGetFunction(tableName, functionName, args, argScope);
    }

    std::vector<std::vector<CellValue> >
    run(const std::vector<BoundSqlExpression> & exprs) const
    {
        size_t numRows = dataset->getMatrixView()->getRowCount();

        std::vector<std::vector<CellValue> > inputs(requiredColumns.size());
        for (size_t i = 0;  i < inputs.size();  ++i) {
            inputs[i] = dataset->getColumnIndex()
                ->getColumnDense(requiredColumns[i]);
        }

        std::vector<std::vector<CellValue> > results(exprs.size());
        for (auto & r: results)
            r.resize(numRows);

        // Apply the expression to everything
        auto doRow = [&] (size_t first, size_t last)
            {
                for (size_t i = first;  i < last;  ++i) {
                    RowScope scope(i, inputs);
                    for (unsigned j = 0;  j < exprs.size();  ++j) {
                        ExpressionValue storage;
                        const ExpressionValue & result
                            = exprs[j](scope, storage, GET_LATEST);
                
                        // Currently, only atoms are supported as results
                        results[j][i] = result.getAtom();
                    }
                }
            };
        
        parallelMapChunked(0, numRows, 1024 /* rows at once */,
                           doRow);

        return std::move(results);
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
    int maxTreesAtOnce = 20;

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

    ColumnScope colScope(server, boundDataset.dataset);
    auto boundLabel = label->bind(colScope);

    cerr << "label uses columns " << jsonEncode(colScope.requiredColumns)
         << endl;

    ML::Timer labelsTimer;

    std::vector<CellValue> labels(std::move(colScope.run({boundLabel})[0]));
    
    cerr << "got " << labels.size() << " labels in " << labelsTimer.elapsed()
         << endl;

    SelectExpression select({subSelect});

    auto getColumnsInExpression = [&] (const SqlExpression & expr)
        -> std::set<ColumnName>
        {
            std::set<ColumnName> knownInputColumns;
            
            // Find only those variables used
            SqlExpressionDatasetContext scope(boundDataset);
            
            auto selectBound = select.bind(scope);
            
            for (auto & c : selectBound.info->getKnownColumns()) {
                knownInputColumns.insert(c.columnName);
            }

            return knownInputColumns;
        };
    
    std::set<ColumnName> knownInputColumns
        = getColumnsInExpression(select);


    // THIS ACTUALL MIGHT DO A LOTTTTTTT OF WORK
    // "GET COLUMN STATS" -> list of all possible values per column ?!? 
    // Profile this!
    auto featureSpace = std::make_shared<DatasetFeatureSpace>
        (boundDataset.dataset, labelInfo, knownInputColumns, true /* bucketize */);

    cerr << "feature space construction took " << timer.elapsed() << endl;
    timer.restart();

    for (auto& c : knownInputColumns) {
        cerr << c.toString() << " feature " << featureSpace->getFeature(c)
             << " had " << featureSpace->columnInfo[c].buckets.numBuckets
             << " buckets" << endl;
    }

    // Get the feature buckets per row


    //NEED A TABLE WITH N ROW CONTAINING FOR EACH DATA POINT:
    // FEATURES LIST  (k * sizeof)
    // WEIGHT FOR EACH BAG (float*bag)
    // LEAF/PARTITION PER BAG
    // LABEL

    //TODO: Need to pack this into 1 memory buffer

    //optimize when we want every row
    size_t numRows = boundDataset.dataset->getMatrixView()->getRowCount();

    int numFeatures = knownInputColumns.size();
    cerr << "NUM FEATURES : " << numFeatures << endl;

    PartitionData allData(*featureSpace);

    allData.reserve(numRows);
    for (size_t i = 0;  i < numRows;  ++i) {
        allData.addRow(labels[i].isTrue(), 1.0 /* weight */, i);
    }

#if 0
    //PerThreadAccumulator<ThreadAccum> accum;

    int numBuckets = 32*8; //whatever
    size_t numPerBucket = std::max((size_t)std::floor((float)numRows / numBuckets), (size_t)1);

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

#if 0
            //++numRows;

            std::vector<int> features(numFeatures);
                
            for (auto & c: row.columns) {
                int featureNum;
                int featureBucketNum;
                std::tie(featureNum, featureBucketNum)
                    = featureSpace->getFeatureBucket(std::get<0>(c), std::get<1>(c));
                features[featureNum] = featureBucketNum;
            }
#endif

            size_t rowIndex = groupCount[bucket] + bucket*numPerBucket;
            groupCount[bucket] += 1;

            data.rows[rowIndex].weight = weight;
            data.rows[rowIndex].label = label;
            data.rows[rowIndex].exampleNum = rowIndex;

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

#endif

    const float trainprop = 1.0f;
    
    PrototypeRNG myrng;

    vector<FeatureBuckets> result;
    
    auto bagPrepare = [&] (int bag)
        {
            ML::Timer bagTimer;

            boost::mt19937 rng(bag + 245);
            distribution<float> in_training(numRows);
            vector<int> tr_ex_nums(numRows);
            std::iota(tr_ex_nums.begin(), tr_ex_nums.end(), 0);  // 0, 1, 2, 3, 4, 5, 6, 7... N
            std::random_shuffle(tr_ex_nums.begin(), tr_ex_nums.end(), myrng);  //5, 1, 14, N...
            for (unsigned i = 0;  i < numRows * trainprop;  ++i)
                in_training[tr_ex_nums[i]] = 1.0;                      //0, 0, 0, 1, 0, 1, 0, 1, 1, ....

            distribution<float> example_weights(numRows);

            // Generate our example weights. 
            for (unsigned i = 0;  i < numRows;  ++i)
                example_weights[myrng(numRows)] += 1.0;   // MBOLDUC random numbers between 0 and N - lots of 0. Several samples in neither training nor validation?

            distribution<float> training_weights
                = in_training * example_weights;

            training_weights.normalize();          // MBOLDUC can't we know the norm? Is this using SIMD?

            


            size_t numNonZero = (training_weights != 0).count();
            cerr << "numNonZero = " << numNonZero << endl;

            auto data = allData.reweightAndCompact(training_weights);

            cerr << "bag " << bag << " setup took " << bagTimer.elapsed() << endl;

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
