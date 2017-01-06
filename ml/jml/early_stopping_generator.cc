// This file is part of MLDB. Copyright 2015 Datacratic. All rights reserved.

/* early_stopping_generator.cc
   Jeremy Barnes, 17 March 2006
   Copyright (c) 2006 Jeremy Barnes  All rights reserved.
   $Source$

   Sets up separate training and validation sets for early stopping.
*/

#include "mldb/ml/jml/early_stopping_generator.h"
#include "mldb/arch/demangle.h"
#include "mldb/jml/utils/sgi_numeric.h"
#include "mldb/types/structure_description.h"
#include "mldb/server/mldb_server.h"

using namespace std;


namespace ML {

/*****************************************************************************/
/* EARLY_STOPPING_GENERATOR_CONFIG                                           */
/*****************************************************************************/
Early_Stopping_Generator_Config::
Early_Stopping_Generator_Config() : validate_split(0.5)
{
}

void
Early_Stopping_Generator_Config::
defaults()
{
    Classifier_Generator_Config::defaults();
    validate_split = 0.5;
}

DEFINE_STRUCTURE_DESCRIPTION(Early_Stopping_Generator_Config);

Early_Stopping_Generator_ConfigDescription::
Early_Stopping_Generator_ConfigDescription()
{
    addField("validate_split",
             &Early_Stopping_Generator_Config::validate_split,
             "", (float)0.5);
    addParent<Classifier_Generator_Config>();
}

/*****************************************************************************/
/* EARLY_STOPPING_GENERATOR                                                  */
/*****************************************************************************/
Early_Stopping_Generator::
Early_Stopping_Generator()
    : Classifier_Generator(static_cast<shared_ptr<Classifier_Generator_Config>>(make_shared<Early_Stopping_Generator_Config>()))
{
}

Early_Stopping_Generator::
Early_Stopping_Generator(shared_ptr<Classifier_Generator_Config> config)
    : Classifier_Generator(config)
{
}

Early_Stopping_Generator::
~Early_Stopping_Generator()
{
}

std::shared_ptr<Classifier_Impl>
Early_Stopping_Generator::
generate(Thread_Context & context,
         const Training_Data & training_data,
         const distribution<float> & ex_weights,
         const std::vector<Feature> & features,
         int recursion) const
{
    const auto * cfg =
        static_cast<const Early_Stopping_Generator_Config*>(config.get());
    if (recursion > 10)
        throw Exception("Early_Stopping_Generator::generate(): recursion");

    if (cfg->validate_split <= 0.0 || cfg->validate_split >= 1.0)
        throw Exception("invalid validate split value");

    float train_prop = 1.0 - cfg->validate_split;

    int nx = ex_weights.size();

    Thread_Context::RNG_Type rng = context.rng();

    distribution<float> in_training(nx);
    vector<int> tr_ex_nums(nx);
    std::iota(tr_ex_nums.begin(), tr_ex_nums.end(), 0);
    std::random_shuffle(tr_ex_nums.begin(), tr_ex_nums.end(), rng);
    for (unsigned i = 0;  i < nx * train_prop;  ++i)
        in_training[tr_ex_nums[i]] = 1.0;
    distribution<float> not_training(nx, 1.0);
    not_training -= in_training;
    
    distribution<float> example_weights(nx);
    
    /* Generate our example weights. */
    for (unsigned i = 0;  i < nx;  ++i)
        example_weights[rng(nx)] += 1.0;
    
    distribution<float> training_weights
        = in_training * example_weights * ex_weights;

    //cerr << "in_training.total() = " << in_training.total() << endl;
    //cerr << "example_weights.total() = " << example_weights.total()
    //     << endl;
    //cerr << "ex_weights.total() = " << ex_weights.total() << endl;
        

    if (training_weights.total() == 0.0)
        throw Exception("training weights were empty");

    training_weights.normalize();
    
    distribution<float> validate_weights
        = not_training * example_weights * ex_weights;

    if (validate_weights.total() == 0.0)
        throw Exception("validate weights were empty");

    validate_weights.normalize();

    return generate(context, training_data, training_data,
                    training_weights, validate_weights,
                    features, recursion + 1);
}

} // namespace ML
