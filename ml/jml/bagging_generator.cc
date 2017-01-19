// This file is part of MLDB. Copyright 2015 mldb.ai inc. All rights reserved.

/* bagging_generator.cc
   Jeremy Barnes, 15 March 2006
   Copyright (c) 2006 Jeremy Barnes  All rights reserved.
   $Source$

   Generator for boosted stumps.
*/

#include "bagging_generator.h"
#include "mldb/ml/jml/registry.h"
#include <boost/timer.hpp>
#include <boost/progress.hpp>
#include "training_index.h"
#include "weighted_training.h"
#include "mldb/ml/jml/committee.h"
#include "mldb/jml/utils/sgi_numeric.h"
#include <random>
#include "mldb/base/parallel.h"
#include "mldb/jml/utils/guard.h"
#include "mldb/jml/utils/smart_ptr_utils.h"


using namespace std;


namespace ML {

/*****************************************************************************/
/* BAGGING_GENERATOR                                                         */
/*****************************************************************************/

Bagging_Generator::
Bagging_Generator()
{
    defaults();
}

Bagging_Generator::~Bagging_Generator()
{
}

void
Bagging_Generator::
configure(const Configuration & config, vector<string> & unparsedKeys)
{
    Classifier_Generator::configure(config, unparsedKeys);
    config.findAndRemove(num_bags, "num_bags", unparsedKeys);
    config.findAndRemove(validation_split, "validation_split", unparsedKeys);

    weak_learner = get_trainer("weak_learner", config);

}

void
Bagging_Generator::
defaults()
{
    num_bags = 10;
    validation_split = 0.35;
    weak_learner.reset();
}

Config_Options
Bagging_Generator::
options() const
{
    Config_Options result = Classifier_Generator::options();
    result
        .add("num_bags", num_bags, "N>=1",
             "number of bags to divide classifier into")
        .add("validation_split", validation_split, "0<N<=1",
             "how much of training data to hold off as validation data")
        .subconfig("weak_leaner", weak_learner,
                   "weak learner that produces each bag");
    
    return result;
}

void
Bagging_Generator::
init(std::shared_ptr<const Feature_Space> fs, Feature predicted)
{
    Classifier_Generator::init(fs, predicted);
    weak_learner->init(fs, predicted);
}

namespace {

struct Bag_Job_Info {
    const Training_Data & training_set;
    const distribution<float> & training_ex_weights;
    const vector<Feature> & features;
    vector<std::shared_ptr<Classifier_Impl> > & results;
    float train_prop;
    std::shared_ptr<Classifier_Generator> weak_learner;
    boost::progress_display * progress;
    int num_bags;
    
    Bag_Job_Info(const Training_Data & training_set,
                 const distribution<float> & training_ex_weights,
                 const vector<Feature> & features,
                 vector<std::shared_ptr<Classifier_Impl> > & results,
                 float train_prop,
                 std::shared_ptr<Classifier_Generator> weak_learner,
                 int num_bags)
        : training_set(training_set), training_ex_weights(training_ex_weights),
          features(features), results(results),
          train_prop(train_prop), weak_learner(weak_learner),
          progress(0), num_bags(num_bags)
    {
    }
};

struct Bag_Job {
    Bag_Job(Bag_Job_Info & info,
            Thread_Context & context,
            int bag_num, int verbosity)
        : info(info), context(context), bag_num(bag_num),
          verbosity(verbosity)
    {
    }

    Bag_Job_Info & info;
    Thread_Context & context;
    int bag_num;
    int verbosity;

    typedef mt19937 engine_type;

    void operator () () const
    {
        Thread_Context::RNG_Type rng = context.rng();

        int nx = info.training_set.example_count();
        /* Partition the dataset. */

#if 0    
        distribution<float> test_eq_weights, test_uniform_weights;
        
        if (test) {
            test_eq_weights = apply_weight_spec(*test, weight_spec);
            test_uniform_weights = distribution<float>(test->example_count(), 1.0);
        }
#endif
        
        distribution<float> in_training(nx);
        vector<int> tr_ex_nums(nx);
        std::iota(tr_ex_nums.begin(), tr_ex_nums.end(), 0);
        std::random_shuffle(tr_ex_nums.begin(), tr_ex_nums.end(), rng);
        for (unsigned i = 0;  i < nx * info.train_prop;  ++i)
            in_training[tr_ex_nums[i]] = 1.0;
        distribution<float> not_training(nx, 1.0);
        not_training -= in_training;

        distribution<float> example_weights(nx);
        
        /* Generate our example weights. */
        for (unsigned i = 0;  i < nx;  ++i)
            example_weights[rng(nx)] += 1.0;

        distribution<float> training_weights
            = in_training * example_weights * info.training_ex_weights;
        training_weights.normalize();

        distribution<float> validate_weights
            = not_training * example_weights * info.training_ex_weights;
        validate_weights.normalize();

        if (verbosity > 0)
            cerr << "bag " << bag_num << " of " << info.num_bags << endl;

#if 0
        cerr << "train_prop = " << info.train_prop << endl;
        cerr << "in_training = " << in_training << endl;
        cerr << "example_weights = " << example_weights << endl;
        cerr << "info.training_ex_weights = " << info.training_ex_weights
             << endl;
        cerr << "training_weights = " << training_weights << endl;
        cerr << "validate_weights = " << validate_weights << endl;
#endif

        /* Train me! */
        std::shared_ptr<Classifier_Impl> bag
            = info.weak_learner
            ->generate(context,
                       info.training_set, info.training_set,
                       training_weights, validate_weights,
                       info.features);

        /* No need to lock since we're the only one accessing this part of
           the array. */
        info.results[bag_num] = bag;

        if (info.progress)
            ++(*info.progress);

#if 0
        if (test) {
            cerr << "testing results: "
                 << bag->accuracy(*test, test_eq_weights) * 100.0
                 << "% eq this bag, "
                 << result.accuracy(*test, test_eq_weights) * 100.0
                 << "% eq overall, "
                 << result.accuracy(*test, test_uniform_weights) * 100
                 << "% uniform overall" << endl;
        }
#endif
        //cerr << endl;
    }
};

} // file scope

std::shared_ptr<Classifier_Impl>
Bagging_Generator::
generate(Thread_Context & context,
         const Training_Data & training_data,
         const distribution<float> & ex_weights,
         const std::vector<Feature> & features,
         int recursion) const
{
    boost::timer timer;

    cerr << "(ex_weights != 0).count() = "
         << (ex_weights != 0).count()
         << endl;

    float train_prop = 1.0 - validation_split;

    if (train_prop <= 0.0 || train_prop > 1.00001)
        throw Exception("Training proportion out of range");

    if (validation_split < 0.0 || validation_split > 1.00001)
        throw Exception("Validation proportion out of range");

    bool local_thread_only = true;  //(num_bags > num_threads() * 2);

    vector<std::shared_ptr<Classifier_Impl> > results(num_bags);
    vector<Thread_Context> contexts(num_bags);
    for (unsigned i = 0;  i < num_bags;  ++i)
        contexts[i] = context.child(local_thread_only);

    Bag_Job_Info info(training_data, ex_weights,
                      features, results,
                      train_prop, weak_learner, num_bags);

    auto onBag = [&] (size_t i)
        {
            Bag_Job(info, contexts[i], i, verbosity)();
        };

    MLDB::parallelMap(0, num_bags, onBag);
    
    Committee result(feature_space, predicted);
    
    for (unsigned i = 0;  i < num_bags;  ++i)
        result.add(results[i], 1.0 / num_bags);
    
    if (profile)
        cerr << "training time: " << timer.elapsed() << "s" << endl;
    
    return make_sp(result.make_copy());
}


/*****************************************************************************/
/* REGISTRATION                                                              */
/*****************************************************************************/

namespace {

Register_Factory<Classifier_Generator, Bagging_Generator>
    BAGGING_REGISTER("bagging");

} // file scope

} // namespace ML
