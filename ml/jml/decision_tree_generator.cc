// This file is part of MLDB. Copyright 2015 mldb.ai inc. All rights reserved.

/* decision_tree_generator.cc
   Jeremy Barnes, 15 March 2006
   Copyright (c) 2006 Jeremy Barnes  All rights reserved.
   $Source$

   Generator for decision trees.
*/

#include "decision_tree_generator.h"
#include "mldb/ml/jml/registry.h"
#include <boost/timer.hpp>
#include <boost/progress.hpp>
#include "training_index.h"
#include "weighted_training.h"
#include "stump_training_core.h"
#include "stump_training.h"
#include "stump_training_bin.h"
#include "stump_regress.h"
#include "binary_symmetric.h"
#include "mldb/jml/utils/smart_ptr_utils.h"
#include "mldb/base/thread_pool.h"

#include <random>
#include <mutex>


using namespace std;


namespace ML {

// Tuning parameters for the compacting code.  This controls how frequently
// we compact.  Compacting makes operations faster, but takes time and
// memory.

/// If there are less than 1 in COMPACT_SPARSENESS_RATIO non-zero entries,
/// then we compact the dataset.
static constexpr int COMPACT_SPARSENESS_RATIO=8;

/// On entry, if we have a density less than COMPACT_INITIAL_DENSITY,
/// we compact the dataset.
static constexpr float COMPACT_INITIAL_DENSITY=0.5;



/// Glue code to allow the templated version of compactDataset to find
/// the first weight, no matter what types are passed in.
float getFirstWeight(const float * f)
{
    return *f;
}

float getFirstWeight(float f)
{
    return f;
}

template<class Weights>
void compact_dataset(const Training_Data & data,
                     const vector<float> & in_class,
                     const Weights & weights,
                     const distribution<float> & binary_weights,
                     int num_non_zero,
                     Training_Data & new_data,
                     vector<float> & new_in_class,
                     Weights & new_weights,
                     distribution<float> & new_binary_weights,
                     const vector<Feature> & features,
                     const Feature & predicted)
{
    /* Compact the dataset, since we only need a fraction of it.  We
       simply recreate all of the strucutres, but with the entries that
       would have been zero not there anymore.
       
       Note that this can lead to high memory usage, and is not necessary.
       If there are problems with memory, we could disable this code and
       the algorithm would continue to function.
    */

    new_in_class.reserve(num_non_zero);

    if (binary_weights.empty())
        new_weights.reserve(num_non_zero);
    else new_binary_weights.reserve(num_non_zero);

    for (unsigned i = 0;  i < in_class.size();  ++i) {
        // NOTE: here we assume that either all class weights are zero, or
        // none are zero.  That way we only need to look at the first weight.
        if (binary_weights.empty()) {
            if (in_class[i] <= 0.0f || getFirstWeight(weights[i]) == 0.0f) continue;
            new_in_class.push_back(in_class[i]);
            new_weights.push_back(weights[i]);
        }
        else {
            if (in_class[i] <= 0.0f || binary_weights[i] == 0.0f) continue;
            new_in_class.push_back(in_class[i]);
            new_binary_weights.push_back(binary_weights[i]);
        }
    }

    // Create a filtered version of the dataset
    new_data.initFiltered(data, in_class, predicted, features);
}


/*****************************************************************************/
/* DECISION_TREE_GENERATOR                                                   */
/*****************************************************************************/

Decision_Tree_Generator::
Decision_Tree_Generator()
{
    defaults();
}

Decision_Tree_Generator::~Decision_Tree_Generator()
{
}

void
Decision_Tree_Generator::
configure(const Configuration & config, vector<string> & unparsedKeys)
{
    Classifier_Generator::configure(config, unparsedKeys);

    config.findAndRemove(trace, "trace", unparsedKeys);
    config.findAndRemove(max_depth, "max_depth", unparsedKeys);
    config.findAndRemove(update_alg, "update_alg", unparsedKeys);
    config.findAndRemove(random_feature_propn, "random_feature_propn", unparsedKeys);
    config.findAndRemove(verbosity, "verbosity", unparsedKeys);
}

void
Decision_Tree_Generator::
defaults()
{
    Classifier_Generator::defaults();
    trace = 0;
    max_depth = -1;
    update_alg = Stump::PROB;
    random_feature_propn = 1.0;
}

Config_Options
Decision_Tree_Generator::
options() const
{
    Config_Options result = Classifier_Generator::options();
    result
        .add("trace", trace, "0-",
             "trace execution of training in a very fine-grained fashion")
        .add("max_depth", max_depth, "0- or -1",
             "give maximum tree depth.  -1 means go until data separated")
        .add("update_alg", update_alg,
             "select the type of output that the tree gives")
        .add("random_feature_propn", random_feature_propn, "0.0-1.0",
             "proportion of the features to enable (for random forests)");
    
    return result;
}

void
Decision_Tree_Generator::
init(std::shared_ptr<const Feature_Space> fs, Feature predicted)
{
    Classifier_Generator::init(fs, predicted);
    model = Decision_Tree(fs, predicted);
}

std::shared_ptr<Classifier_Impl>
Decision_Tree_Generator::
generate(Thread_Context & context,
         const Training_Data & training_set,
         const Training_Data & validation_set,
         const distribution<float> & training_ex_weights,
         const distribution<float> & validate_ex_weights,
         const std::vector<Feature> & features, int) const
{
    boost::timer timer;

    Feature predicted = model.predicted();

    boost::multi_array<float, 2> weights
        = expand_weights(training_set, training_ex_weights, predicted);

    Decision_Tree current
        = train_weighted(context, training_set, weights, features, max_depth);
    
    if (verbosity > 2)
        cerr << current.print() << endl;
    
    return std::make_shared<Decision_Tree>(std::move(current));
}

std::shared_ptr<Classifier_Impl>
Decision_Tree_Generator::
generate(Thread_Context & context,
         const Training_Data & training_set,
         const boost::multi_array<float, 2> & weights,
         const std::vector<Feature> & features,
         float & Z,
         int recursion) const
{
    //boost::timer timer;

    //Feature predicted = model.predicted();

    Decision_Tree current
        = train_weighted(context, training_set, weights, features, max_depth);
    
    if (verbosity > 2) cerr << current.print() << endl;
    
    return make_sp(current.make_copy());
}

Decision_Tree
Decision_Tree_Generator::
train_weighted(Thread_Context & context,
               const Training_Data & data,
               const boost::multi_array<float, 2> & weights,
               const std::vector<Feature> & features,
               int max_depth) const
{
    Decision_Tree result = model;

    Feature predicted = model.predicted();

    /* Record which examples are in our class and with what weight they
       are there. */
    distribution<float> in_class(data.example_count(), 1.0);

    int nlw = weights.shape()[1];

    for (unsigned i = 0;  i < data.example_count();  ++i) {
        bool nonZero = false;
        for (unsigned j = 0;  j < nlw && !nonZero;  ++j) {
            nonZero = weights[i][j] != 0;
        }

        in_class[i] = nonZero;
    }
    
    bool regression_problem
        = result.feature_space()->info(predicted).type() == REAL;

    if (random_feature_propn < 0.0 || random_feature_propn > 1.0)
        throw Exception("random_feature_propn is not between 0.0 and 1.0");


    vector<Feature> filtered_features;
    if (random_feature_propn < 1.0) {

        int iter = 0;
        while (filtered_features.empty() && iter < 50) {
            typedef mt19937 engine_type;
            engine_type engine(context.random());
            std::uniform_real_distribution<> rng(0, 1);
            
            for (unsigned i = 0;  i < features.size();  ++i) {
                if (rng(engine) < random_feature_propn)
                    filtered_features.push_back(features[i]);
            }
        }
        
        if (filtered_features.empty())
            throw Exception("random_feature_propn is too low");
    }
    else filtered_features.insert(filtered_features.end(),
                                  features.begin(), features.end());

    if (max_depth == -1)
        max_depth = 50;

    if (regression_problem) {
        vector<float> weights_vec(data.example_count());
        for (unsigned x = 0;  x < weights_vec.size();  ++x)
            weights_vec[x] = weights[x][0];

        result.tree.root = train_recursive_regression
            (context, data, weights_vec, filtered_features, in_class,
             0, max_depth, result.tree);
    }
    else {

        convert_bin_sym(const_cast<boost::multi_array<float, 2> &>(weights),
                        data, predicted, features);

        int advance = get_advance(weights);
        vector<const float *> weights_vec(data.example_count());

        for (unsigned x = 0;  x < weights_vec.size();  ++x)
            weights_vec[x] = &weights[x][0];

        int numNonZero = 0;

        for (unsigned i = 0;  i < weights_vec.size();  ++i) {
            if (weights_vec[i][0] != 0.0)
                ++numNonZero;
        }
        
        cerr << "numNonZero = " << numNonZero << endl;

        if (numNonZero < COMPACT_INITIAL_DENSITY * weights_vec.size()) {
            Training_Data new_data(data.feature_space());
            distribution<float> new_in_class;
            vector<const float *> new_weights;
            distribution<float> new_binary_weights;

            compact_dataset(data, in_class, weights_vec, {} /* binary weights */, numNonZero,
                            new_data, new_in_class, new_weights, new_binary_weights,
                            features, model.predicted());

            result.tree.root = train_recursive
                (context, new_data, new_weights, advance, filtered_features,
                 new_in_class, 0, max_depth, result.tree);
        }
        else {
            result.tree.root = train_recursive
                (context, data, weights_vec, advance, filtered_features, in_class,
                 0, max_depth, result.tree);
        }

        result.encoding = Stump::update_to_encoding(update_alg);

        /* Validate that the examples in the training set are indeed split
           the way it says */
        // TODO
    }
    
    return result;
}

namespace {

/** Structure in which we hold the results of the line search over potential
    split points. */
template<class W, class Z, class Tracer = No_Trace>
struct Tree_Accum {

    Tree_Accum(const Feature_Space & fs, int nl,
               const Tracer & tracer = Tracer())
        : tracer(tracer), best_w(nl),
          best_arg(numeric_limits<float>::quiet_NaN()),
          best_z(Z::worst),
          best_feature(MISSING_FEATURE), fs(fs)
    {
    }

    Tracer tracer;
    
    Z calc_z;

    W best_w;
    float best_arg;
    float best_z;
    Feature best_feature;

    bool has_result() const { return best_feature != MISSING_FEATURE; }

    Lock lock;

    const Feature_Space & fs;

    /** Method that gets called when we start a new feature.  We use it to
        pre-cache part of the work from the Z calculation, as we are
        assured that the MISSING buckets of W will never change after this
        method is called.

        Return value is used to allow an early exit from the training process,
        due to it being impossible for this feature to have a high enough
        value to be included.
    */
    bool start(const Feature & feature, const W & w, double & missing)
    {
        bool optional = fs.info(feature).optional();
        missing = calc_z.missing(w, optional);
        bool keep_going = calc_z.can_beat(w, missing, best_z);

        return keep_going;
    }

    /** Method that gets called when we have found a potential split point. */
    float add_z(const Feature & feature, const W & w, float arg, float z)
    {
        if (false) {
        // Check that the dataset was split evenly enough, ie that at least
        // 10% of the data is in one bucket

            double w_true = w(0,true,0) + w(0,true,1);
            double w_false = w(0,false,0) + w(0,false,1);
            double w_missing = w(0,MISSING,0) + w(0,MISSING,1);
            double w_total = (w_true + w_false + w_missing);
            w_true /= w_total;  w_false /= w_total;  w_missing /= w_total;
            double threshold = 0.2;

            int n = (w_true > threshold)
                + (w_false > threshold)
                + (w_missing > threshold);
            
            if (n < 2) { z += (1 - z) * 0.9; };
        }


        bool print_feat = false;
        //print_feat = fs.print(feature) == "language_cosine";
        if (tracer || print_feat)
            tracer("tree accum", 3)
                << "  accum: feature " << feature << " arg " << arg
                << " (" << fs.print(feature, arg)
                << "; 0x" << format("%08x", reinterpret_as_int(arg))
                << ") z " << z << "  " << fs.print(feature)
                << (z < best_z ? " ****" : "")
                << endl;

        if (tracer || print_feat)
            tracer("tree accum", 4) << w.print() << endl;
        
        if (z < best_z) {
            Guard guard(lock);

            if (z < best_z) {

#if 0 // MLDB-784... sparse features can have a non-finite split                
                if (!isfinite(arg)) {  // will never happen
                    static std::mutex mutex;
                    std::unique_lock<std::mutex> guard(mutex);

                    cerr << "Best arg had non-finite split" << endl;
                    cerr << "feature = " << fs.print(feature) << endl;
                    cerr << "info = " << fs.info(feature) << endl;
                    cerr << "z = " << z << endl;
                    cerr << "arg = " << arg << endl;

                    cerr << "Beating previous best" << endl;
                    cerr << "feature = " << fs.print(best_feature) << endl;
                    cerr << "info = " << fs.info(best_feature) << endl;
                    cerr << "z = " << best_z << endl;
                    cerr << "arg = " << best_arg << endl;
                }
#endif // MLDB-784

                if (tracer || print_feat)
                    tracer("tree accum", 4) << w.print() << endl;
                // A better one.  This replaces whatever we had accumulated so
                // far.
                best_z = z;
                best_w = w;
                best_arg = arg;
                best_feature = feature;
            }
        }
        
        return z;
    }

    float add(const Feature & feature, const W & w, float arg, double missing)
    {
        // If the decision tree generator is having a really tough time
        // separating the classes, and it's a bucketed feature, than it
        // may send back a -INF for arg (which means split on missing or
        // not missing), even if there is no missing feature, due to
        // numerical issues.  Since the decision tree can't handle a split
        // point of -INIFINITY, we return that we don't want this split
        // point so that it will continue looking for something better.

        //if (!isfinite(arg)) {
        //    return Z::none;
        //}

        float z = calc_z.non_missing(w, missing);
        return add_z(feature, w, arg, z);
    }

    float add_presence(const Feature & feature, const W & w, float arg,
                       double missing)
    {
        float z = calc_z.non_missing_presence(w, missing);
        return add_z(feature, w, arg, z);
    }

    void finish(const Feature & feature)
    {
        // nothing to do here, at the moment
    }

    Split split()
    {
        if (!has_result()) return Split();
        Split result(best_feature, best_arg, fs);
        return result;
    }

    double z() const
    {
        return best_z;
    }
};

template<class W>
void
get_probs(distribution<float> & probs, const W & w_,
          Stump::Update update, float epsilon = 0.0)
{
#if 1
    W w = w_;
    for (unsigned j = 1;  j < 3;  ++j) {
        for (unsigned l = 0;  l < w.nl();  ++l) {
            w(l, 0, true) += w(l, j, true);
            w(l, 0, false) += w(l, j, false);
        }
    }

    probs.resize(w.nl());
    
    C_any c(update);
    return c(&probs[0], 0, w, epsilon, false);

#else
    distribution<float> result(w.nl());

    for (unsigned j = 0;  j < 3;  ++j)
        for (unsigned l = 0;  l < w.nl();  ++l)
            result[l] += w(l, j, true);

    result.normalize();
    return result;
#endif
}

void
fillin_leaf(Tree::Leaf & leaf,
            const Training_Data & data,
            const Feature & predicted,
            const vector<const float *> & weights,
            int advance,
            const distribution<float> & in_class,
            Stump::Update update_alg,
            float examples = -1.0)
{
    /* Use the stump trainer to accumulate for us. */

    if (examples == -1.0) examples = in_class.total();

    //cerr << "new_leaf: advance = " << advance << endl;
    
    if (advance < 0)
        throw Exception("invalid advance");

    leaf.examples = examples;

    if (advance != 0) {
        typedef W_normal W;
        typedef Z_normal Z;
        typedef Stump_Trainer<W, Z> Trainer;
        Trainer trainer;

        W w = trainer.calc_default_w(data, predicted, in_class, weights, advance);

        double epsilon = xdiv<double>(1.0, examples);
        get_probs(leaf.pred, w, update_alg, epsilon);

        //cerr << "new_leaf norm: dist = " << dist << " W = " << endl
        //     << w.print() << endl;
    }
    else {
        typedef W_binsym W;
        typedef Z_binsym Z;
        typedef Stump_Trainer<W, Z> Trainer;
        Trainer trainer;
        
        //cerr << "getting default W" << endl;

        W w = trainer.calc_default_w(data, predicted, in_class, weights, advance);

        double epsilon = xdiv<double>(1.0, examples);
        get_probs(leaf.pred, w, update_alg, epsilon);

        //cerr << "new_leaf binsym: dist = " << dist << " W = " << endl
        //     << w.print() << endl;
    }
}

void
fillin_leaf_regression(Tree::Leaf & leaf,
                       const Training_Data & data,
                       const Feature & predicted,
                       const vector<float> & weights,
                       const distribution<float> & in_class,
                       float examples = -1.0)
{
    /* Calculate the weighted mean over the examples in this class. */
    int nx = data.example_count();

    double total_weight = 0.0;
    double total_val = 0.0;

    const vector<Label> & labels = data.index().labels(predicted);

    for (unsigned x = 0;  x < nx;  ++x) {
        float w = weights[x] * in_class[x];
        float val = labels[x].value();
        total_weight += w;
        total_val += w * val;
    }

    distribution<float> dist(1, total_val / total_weight);

    if (examples == -1.0) examples = in_class.total();

    leaf.pred = dist;
    leaf.examples = examples;
}

void split_dataset(const Training_Data & data,
                   const Split & split,
                   const distribution<float> & in_class,
                   distribution<float> & class_true,
                   distribution<float> & class_false,
                   distribution<float> & class_missing,
                   double & total_true,
                   double & total_false,
                   double & total_missing,
                   bool validate)
{
    // TODO: we could use the index for the feature instead?

    /* Split these examples based upon what the split said. */
    int nx = data.example_count();

    class_true = distribution<float>(nx);
    class_false = distribution<float>(nx);
    class_missing = distribution<float>(nx);

    total_true = 0.0;
    total_false = 0.0;
    total_missing = 0.0;

    Joint_Index index = data.index().dist(split.feature(), BY_EXAMPLE,
                                          IC_VALUE | IC_DIVISOR | IC_EXAMPLE);

    int last_example = -1;
    for (unsigned i = 0;  i < index.size();  ++i) {
        int example = index[i].example();

        // Any which we skipped over are missing
        for (++last_example; last_example < example; ++last_example) {
            float w = in_class[last_example];
            if (w == 0.0) continue;
            //cerr << "missing: example = " << example << " last_example = "
            //     << last_example << endl;
            class_missing[last_example] += w;
            total_missing += w;
        }
        
        last_example = example;

        float w = in_class[example];
        if (w == 0.0) continue;

        float val = index[i].value();
        float divisor = index[i].divisor();
        w *= divisor;
        
        if (MLDB_UNLIKELY(isnanf(val))) {
            // We only have NaN values explicitly represented if there is a
            // feature that is both present and missing in the same example.
            // In that case, we need to deal with the missing part here.
            class_missing[example] += w;
            total_missing += w;
            continue;
        }

        int decision;
        try {
            decision = split.apply(val);
        } catch (...) {
            cerr << "exception on split: " << split.print(*data.feature_space())
                 << endl;
            throw;
        }

        switch(decision) {
        case false:
            class_false[example] += w;
            total_false += w;
            break;
        case true:
            class_true[example] += w;
            total_true += w;
            break;
        case MISSING:
            class_missing[example] += w;
            total_missing += w;
            break;
        default:
            throw Exception("split_dataset: bad decision");
        };
    }

    // Any examples we never touched are also missing
    for (++last_example; last_example < nx;  ++last_example) {
        float w = in_class[last_example];
        if (w == 0.0) continue;
        class_missing[last_example] += w;
        total_missing += w;
    }

    /* For validation: make sure that each is in exactly one */
    if (validate) {
        for (unsigned x = 0;  x < nx;  ++x) {
            double w_total = class_true[x] + class_false[x] + class_missing[x];
            double error = in_class[x] - w_total;
            if (abs(error) > 0.000001) {
                cerr << "x = " << x << endl;
                cerr << "orig  = " << in_class[x] << endl;
                cerr << "false = " << class_false[x] << endl;
                cerr << "true  = " << class_true[x] << endl;
                cerr << "miss  = " << class_missing[x] << endl;
                cerr << "total = " << w_total << endl;
                cerr << "error = " << error << endl;
                throw Exception("split_weights: weights don't add up");
            }
        }
    }
}

/// The training code expects weights to be double-indexed, by first example
/// number and second label, so you can do weights[example][label] to extract
/// the label.  In the optimization for the binary symmetric case (where we
/// always have 2 identical weights, one for each label) we have all of the
/// weights in a single array.  This code adds an extra dimension to the
/// weights, allowing us to call AddDimension(weights)[example][label] and
/// have it return weights[example].
struct AddDimension {
    AddDimension(const distribution<float> & vals)
        : vals(vals)
    {
    }

    const distribution<float> & vals;

    const float * operator [] (int n) const { return &vals[n]; };
};

int get_advance(const AddDimension &)
{
    return 0;
}

template<typename W, typename Z>
struct TreeTrainer {
    Tree & tree;
    int max_depth;
    const vector<Feature> & features;
    int advance;
    int nl;
    Feature predicted;
    int trace;
    Stump::Update update_alg;
    std::shared_ptr<const Feature_Space> feature_space;
    bool validate;

    TreeTrainer(Tree & tree,
                int max_depth,
                const vector<Feature> & features,
                int advance,
                Feature predicted,
                int trace,
                Stump::Update update_alg,
                std::shared_ptr<const Feature_Space> feature_space,
                bool validate)
        : tree(tree), max_depth(max_depth),
          features(features), advance(advance),
          predicted(predicted), trace(trace),
          update_alg(update_alg),
          feature_space(feature_space),
          validate(validate)
    {
        nl = feature_space->info(predicted).value_count();
    }
        
    
    typedef Stump_Trainer<W, Z> WeightTrainer;

    typedef No_Trace TrainerTracer;
    typedef Tree_Accum<W, Z, Stream_Tracer> Accum;
    typedef Stump_Trainer<W, Z, TrainerTracer> SplitTrainer;
        
    void do_branch(Tree::Ptr & ptr,
                   Thread_Context & context,
                   const Training_Data & data,
                   const vector<const float *> & weights,
                   const distribution<float> & binary_weights,
                   int advance,
                   const vector<Feature> & features,
                   const distribution<float> & new_in_class,
                   double total_in_class,
                   int new_depth, int max_depth,
                   Tree & tree,
                   MLDB::ThreadPool & tp) const
    {
#if 0
        if (total_in_class > 1024) {
            // Worth multithreading... do it
            if (group_to_wait_on == -1) {
                // Create a new group
                group_to_wait_on = context.worker().get_group(NO_JOB,
                                                              "decision tree",
                                                              context.group());
            }
            Thread_Context child_context = context.child(group_to_wait_on);

            Train_Recursive_Job job(ptr, this, child_context, data, weights,
                                    advance,
                                    features, new_in_class, new_depth, max_depth,
                                    tree);

            context.worker().add(job, "train decision tree branch",
                                 child_context.group());
        }
#else
        if (false) ;
#endif
        else if (total_in_class > 0.0)
            ptr = this->train(context, data, weights, new_in_class, binary_weights,
                              new_depth);
        else {
            // Leaf only
            ptr = tree.new_leaf();
            fillin_leaf(*ptr.leaf(), data, predicted, weights,
                        advance, new_in_class, update_alg, 0.0);
        }
    }

    // This structure needs to be defererenceable and incrementable, and always
    // return 1 when dereferenced.  It's like an iterator into an infinite array
    // of ones.
    struct AlwaysOne {
        float operator * () const { return 1.0f; }
        void operator ++ () {}
    };

    Tree::Ptr
    train(Thread_Context & context,
          const Training_Data & data,
          const std::vector<const float *> & weights,
          const distribution<float> & in_class,
          const distribution<float> & binary_weights,
          int depth) const
    {
        bool debug = false;

#if 0
        int numNonZero = 0;
        for (unsigned i = 0;  i <  in_class.size();  ++i) {
            if (weights[i][0] * in_class[i] != 0)
                ++numNonZero;
            if (i < 10 && false) {
                cerr << i << " p " << weights[i] << " weight " << weights[i][0]
                     << " in_class " << in_class[i] << " total " << weights[i][0] * in_class[i] << endl;
            }
        }
        //cerr << "in class: " << numNonZero << " of " << in_class.size() << endl;
#endif    

#if 0
        if (numNonZero * 20 < in_class.size()) {
            cerr << "warning: extremely sparse examples: " << numNonZero << " of "
                 << in_class.size() << endl;
        }
#endif    

        if (depth > 100 && max_depth == -1)
            throw Exception("Decision_Tree_Generator::train_recursive(): "
                            "depth of 100 reached");
        if (debug)
            cerr << "train_recursive: depth " << depth << endl;

        double total_weight = in_class.total();


#if 0
        if (debug) {
            cerr << "predicted = " << predicted << endl;
            cerr << "fs = " << data.feature_space()->print() << endl;
            cerr << "data[0] = " << data.feature_space()->print(data[0]) << endl;
            cerr << "data.example_count() = " << data.example_count() << endl;

            cerr << "data.label_count(predicted) = " << data.label_count(predicted)
                 << endl;
            cerr << "data.label_count(model.predicted()) = "
                 << data.label_count(model.predicted())
                 << endl;
        }
#endif

        W default_w(nl);

        /* Check for zero impurity, and return a leaf if we have it. */
        double class_weights[nl];

        // What would we have as a leaf if we were to stop splitting here?
        Tree::Leaf leaf;
        leaf.examples = total_weight;
        double epsilon = xdiv<double>(1.0, total_weight);

        /* Use the stump trainer to accumulate for us. */
        WeightTrainer weightTrainer;
        
        if (binary_weights.empty())
            default_w = weightTrainer.calc_default_w(data, predicted, in_class, weights, advance);
        else default_w = weightTrainer.calc_default_w(data, predicted, in_class, AddDimension(binary_weights),
                                                      advance);

        // Calculate how many classes (labels) have non-zero weight.  We need at least
        // 2 distinct labels for training to make sense; otherwise we bail out

        double maxClassWeight = 0.0;
        double totalClassWeight = 0.0;
        int numNonZeroClasses = 0;
        for (unsigned l = 0;  l < nl;  ++l) {
            double w = 0.0;  // weight for the label class
            for (unsigned j = 0;  j < 3;  ++j)
                w += default_w(l, j, true);
            class_weights[l] = w;
            maxClassWeight = std::max(maxClassWeight, w);
            totalClassWeight += w;
            if (w != 0)
                numNonZeroClasses += 1;
        }
        
        // Fill in the prediction that this would have made if we stopped
        // training here.  This is used, notably, if we prune the tree and
        // for explanations.
        get_probs(leaf.pred, default_w, update_alg, epsilon);

        // Look for early stopping conditions:
        if (maxClassWeight == 1.0       // minimum impurity; one per class
            || depth == max_depth       // reached maximum depth
            || numNonZeroClasses <= 1   // only one non-zero weighted label left
            || total_weight < 1.0       // split up finer than one example
            || totalClassWeight == 0.0  // weights too small to count
            || in_class.size() == 1     // only one example
            || false) {
            Tree::Leaf * result = tree.new_leaf();
            *result = leaf;
            return result;
        }
    
        int num_non_zero = std::count_if(in_class.begin(), in_class.end(),
                                         std::bind2nd(std::greater<float>(), 0.0));
    
        if (debug) {
            cerr << "in_class.size() = " << in_class.size() << " num_non_zero = "
                 << num_non_zero << " total_weight = " << total_weight
                 << endl;
        }

        if (num_non_zero * COMPACT_SPARSENESS_RATIO < in_class.size()) {
            Training_Data new_data(data.feature_space());
            distribution<float> new_in_class;
            distribution<float> new_binary_weights;
            vector<const float *> new_weights;

            compact_dataset(data, in_class, weights, binary_weights, num_non_zero,
                            new_data, new_in_class,
                            new_weights,
                            new_binary_weights,
                            features, predicted);

            /* Restart, with the new training data. */
            return train(context, new_data, new_weights,
                         new_in_class, new_binary_weights, depth);
        }
        
        Split split;
        float best_z = 0.0;

        Accum accum(*feature_space, nl, trace);
        SplitTrainer splitTrainer;
    
        if (binary_weights.empty())
            splitTrainer.test_all
                (context, default_w, features, data, predicted,
                 weights, in_class, accum, advance);
        else 
            splitTrainer.test_all
                (context, default_w, features, data, predicted,
                 AddDimension(binary_weights), in_class, accum, advance);
        
        split = accum.split();
        best_z = accum.z();

        if (split.feature() == MISSING_FEATURE) {
            Tree::Leaf * result = tree.new_leaf();
            *result = leaf;
            return result;
        }

        if (debug) {
            cerr << " decision tree training: best split is "
                 << split.print(*feature_space) << endl;
            cerr << "z = " << best_z << endl;
        }

        // We used to not allow the decision tree to learn a perfect split.  Now we allow it
        // but we make sure that the next level down only leaf nodes will be created as there
        // will be only one label.
        if (best_z == 0.0 && false) {
            // No impurity at all
            Tree::Leaf * result = tree.new_leaf();
            *result = leaf;
            return result;
        }
    
        /* Split these examples based upon what the split said. */
        distribution<float> class_true;
        distribution<float> class_false;
        distribution<float> class_missing;
        double total_true;
        double total_false;
        double total_missing;

        //boost::timer timer;
        split_dataset(data, split, in_class,
                      class_true, class_false, class_missing,
                      total_true, total_false, total_missing,
                      validate);

        int numClasses = (total_true != 0) + (total_false != 0) + (total_missing != 0);

        // If we classify everything into one class, then we can't split any
        // further.
        if (numClasses < 2) {
            Tree::Leaf * result = tree.new_leaf();
            *result = leaf;
            return result;
        }

        if (debug) {
            //cerr << timer.elapsed() << "s split" << endl;
        
            cerr << " totals: true " << total_true << " false " << total_false
                 << " missing " << total_missing << endl;
        
            cerr << " totals2: true " << class_true.total()
                 << " false " << class_false.total()
                 << " missing " << class_missing.total() << endl;
        }

        Tree::Node * node = tree.new_node();
        node->split = split;
        node->z = best_z;
        node->examples = total_weight;
        node->pred = leaf.pred;

        MLDB::ThreadPool tp;
        
        do_branch(node->child_true,
                  context, data, weights, binary_weights, advance, features,
                  class_true, total_true, depth + 1, max_depth,
                  tree, tp);

        do_branch(node->child_false,
                  context, data, weights, binary_weights, advance, features,
                  class_false, total_false, depth + 1, max_depth,
                  tree, tp);
    
        do_branch(node->child_missing,
                  context, data, weights, binary_weights, advance, features,
                  class_missing, total_missing, depth + 1, max_depth,
                  tree, tp);

        tp.waitForAll();
        
        return node;
    }
};

} // file scope

struct Decision_Tree_Generator::Train_Recursive_Job {

    Tree::Ptr & ptr;
    const Decision_Tree_Generator * generator;
    Thread_Context context;
    const Training_Data & data;
    const vector<const float *> & weights;
    int advance;
    const vector<Feature> & features;
    const distribution<float> & in_class;
    int depth;
    int max_depth;
    Tree & tree;

    Train_Recursive_Job(Tree::Ptr & ptr,
                        const Decision_Tree_Generator * generator,
                        const Thread_Context & context,
                        const Training_Data & data,
                        const vector<const float *> & weights,
                        int advance,
                        const vector<Feature> & features,
                        const distribution<float> & in_class,
                        int depth, int max_depth,
                        Tree & tree)
        : ptr(ptr), generator(generator), context(context), data(data),
          weights(weights), advance(advance), features(features),
          in_class(in_class), depth(depth), max_depth(max_depth),
          tree(tree)
    {
    }

    void operator () ()
    {
        ptr = generator->train_recursive(context, data, weights, advance,
                                         features, in_class, depth,
                                         max_depth, tree);
    }
};

Tree::Ptr
Decision_Tree_Generator::
train_recursive(Thread_Context & context,
                const Training_Data & data,
                const vector<const float *> & weights,
                int advance,
                const vector<Feature> & features,
                const distribution<float> & in_class,
                int depth, int max_depth,
                Tree & tree) const
{
    if (advance == 0) {
        // binary symmetric, we can use an optimized version
        typedef W_binsym W;
        typedef Z_binsym Z;

        // Pre-calculate a more memory friendly version of in_class
        distribution<float> binary_weights(in_class.size(), 0.0);
        for (unsigned i = 0;  i < in_class.size();  ++i) {
            binary_weights[i] = weights[i][0];
        }

        TreeTrainer<W, Z> trainer(tree, max_depth, features, advance,
                                  predicted, trace, update_alg, feature_space,
                                  validate);
        return trainer.train(context, data, weights, in_class, binary_weights, depth);
    }
    else {
        // generalized version
        typedef W_normal W;
        typedef Z_normal Z;
        TreeTrainer<W, Z> trainer(tree, max_depth, features, advance,
                                  predicted, trace, update_alg, feature_space,
                                  validate);
        return trainer.train(context, data, weights, in_class, {} /* binary weights */, depth);
    }
}

Tree::Ptr
Decision_Tree_Generator::
train_recursive_regression(Thread_Context & context,
                           const Training_Data & data,
                           const vector<float> & weights,
                           const vector<Feature> & features_,
                           const distribution<float> & in_class,
                           int depth, int max_depth,
                           Tree & tree) const
{
    if (depth > 100 && max_depth == -1)
        throw Exception("Decision_Tree_Generator::train_recursive_regression(): "
                        "depth of 100 reached");

    size_t nx = data.example_count();

    Tree::Leaf leaf;
    fillin_leaf_regression(leaf, data, model.predicted(), weights, in_class);

    const vector<Label> & labels = data.index().labels(model.predicted());

    /* Check for all of the labels in the class having the same value. */
    {
        float val_found = NAN;
        bool all_same = true;
        for (unsigned x = 0;  x < nx;  ++x) {
            if (in_class[x] == 0.0) continue;  // not in our class
            if (weights[x] == 0.0) continue;   // no weight; doesn't count
            if (isnanf(val_found)) val_found = labels[x].value();
            else if (val_found != labels[x].value()) {
                all_same = false;
                break;
            }
        }

        if (all_same) {
            Tree::Leaf * result = tree.new_leaf();
            *result = leaf;
            return result;
        }
    }
        
    double total_weight = in_class.total();

    if (depth == max_depth || total_weight < 1.0) {
        Tree::Leaf * result = tree.new_leaf();
        *result = leaf;
        return result;
    }
    
    int num_non_zero = std::count_if(in_class.begin(), in_class.end(),
                                     std::bind2nd(std::greater<float>(), 0.0));

    //cerr << "in_class.size() = " << in_class.size() << " num_non_zero = "
    //     << num_non_zero << " total_weight = " << total_weight
    //     << endl;

    if (num_non_zero * 16 < in_class.size()) {
        Training_Data new_data(data.feature_space());
        distribution<float> new_in_class;
        vector<float> new_weights;
        distribution<float> new_binary_weights;

        compact_dataset(data, in_class, weights, {} /* binary weights */, num_non_zero,
                        new_data, new_in_class, new_weights, new_binary_weights, features_,
                        model.predicted());

        /* Restart, with the new training data. */
        return train_recursive_regression
            (context, new_data, new_weights, features_, new_in_class, depth,
             max_depth, tree);
    }
    
    //cerr << "training decision tree with total weight "
    //     << total_weight << " at depth " << depth << endl;
    
    typedef W_regress W;
    typedef Z_regress Z;
    
    typedef Tree_Accum<W, Z, Stream_Tracer> Accum;
    //typedef Tree_Accum<W, Z> Accum;
    typedef Stump_Trainer<W, Z> Trainer;
    
    Accum accum(*model.feature_space(), nl, trace);
    Trainer trainer;
    
    vector<Feature> features = features_;
    
    /* We need it in a fixed array like this. */
    boost::multi_array<float, 2> weights2(boost::extents[weights.size()][1]);
    std::copy(weights.begin(), weights.end(), weights2.data());
    trainer.test_all_and_sort(features, data, model.predicted(), weights2,
                              in_class, accum);
    
    //cerr << "z = " << accum.best_z << endl;
    //cerr << "w = " << endl << accum.best_w.print() << endl;

    if (accum.best_z == 0.0 || accum.best_feature == MISSING_FEATURE) {
        Tree::Leaf * result = tree.new_leaf();
        *result = leaf;
        return result;
    }

    distribution<float> class_true;
    distribution<float> class_false;
    distribution<float> class_missing;

    double total_true;
    double total_false;
    double total_missing;

    //boost::timer timer;
    split_dataset(data, accum.split(), in_class,
                  class_true, class_false, class_missing,
                  total_true, total_false, total_missing,
                  validate);

    //cerr << timer.elapsed() << "s split" << endl;

    //cerr << " totals: true " << total_true << " false " << total_false
    //     << " missing " << total_missing << endl;

    Tree::Node * node = tree.new_node();
    node->split = accum.split();
    node->z = accum.z();
    node->examples = total_weight;
    node->pred = leaf.pred;

    node->child_true
        = train_recursive_regression(context, data, weights, features,
                                     class_true, depth + 1, max_depth,
                                     tree);
    node->child_false
        = train_recursive_regression(context, data, weights, features,
                                     class_false, depth + 1, max_depth,
                                     tree); 
    node->child_missing
        = train_recursive_regression(context, data, weights, features,
                                     class_missing, depth + 1, max_depth,
                                     tree);
    
    return node;
}


/*****************************************************************************/
/* REGISTRATION                                                              */
/*****************************************************************************/

namespace {

Register_Factory<Classifier_Generator, Decision_Tree_Generator>
    DECISION_TREE_REGISTER("decision_tree");

} // file scope

} // namespace ML
