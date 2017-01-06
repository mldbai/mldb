// This file is part of MLDB. Copyright 2015 Datacratic. All rights reserved.

/* classifier_generator.cc
   Jeremy Barnes, 15 March 2006
   Copyright (c) 2006 Jeremy Barnes  All rights reserved.
   $Source$

*/

#include "classifier_generator.h"
#include "mldb/ml/jml/registry.h"
#include "mldb/vfs/filter_streams.h"
#include "mldb/types/basic_value_descriptions.h"


using namespace std;


namespace ML {

/*****************************************************************************/
/* CLASSIFIER_GENERATOR_CONFIG                                               */
/*****************************************************************************/
Classifier_Generator_Config::
Classifier_Generator_Config() :
    verbosity(2), profile(false), validate(false)
{
}

void
Classifier_Generator_Config::
defaults()
{
    verbosity = 2;
    profile = false;
    validate = false;
}

void
Classifier_Generator_Config::
validateFct()
{
    if (verbosity < 0 || verbosity > 5) {
        throw Exception("Verbosity must be between 0 and 5.");
    }
}

DEFINE_STRUCTURE_DESCRIPTION(Classifier_Generator_Config);

Classifier_Generator_ConfigDescription::
Classifier_Generator_ConfigDescription()
{
    addField("verbosity", &Classifier_Generator_Config::verbosity,
             "verbosity of information from training (0-5)", 2);
    addField("profile", &Classifier_Generator_Config::profile,
             "whether or not to profile", false);
    addField("validate", &Classifier_Generator_Config::validate,
             "perform expensive internal validation", false);

    onUnknownField = [] (Classifier_Generator_Config * config,
                         JsonParsingContext & context)
    {
        config->unparsedKeys.push_back(context.fieldName());
    };

    // If redefined in children classes, do not forget to call back the base
    // class validate.
    onPostValidate = [&] (Classifier_Generator_Config * cfg,
                          JsonParsingContext & context)
    {
        cfg->validateFct();
    };
}

/*****************************************************************************/
/* CLASSIFIER_GENERATOR                                                      */
/*****************************************************************************/
Classifier_Generator::
Classifier_Generator() : config(make_shared<Classifier_Generator_Config>())
{
    config->defaults();
}

Classifier_Generator::
Classifier_Generator(shared_ptr<Classifier_Generator_Config> config)
    : config(config)
{
    config->defaults();
}

Classifier_Generator::
~Classifier_Generator()
{
}

void
Classifier_Generator::
init(std::shared_ptr<const Feature_Space> fs, Feature predicted)
{
    this->feature_space = fs;
    this->predicted = predicted;
    nl = feature_space->info(predicted).value_count();
}

void
Classifier_Generator::
configure(const shared_ptr<Classifier_Generator_Config> & config)
{
    this->config = config;
}

void
Classifier_Generator::
configure(const Json::Value & config)
{
    configure(make_shared<Classifier_Generator_Config>(
                jsonDecode<Classifier_Generator_Config>(config)));
}

void
Classifier_Generator::
defaults()
{
    config->defaults();
}

std::shared_ptr<Classifier_Impl>
Classifier_Generator::
generate(Thread_Context & context,
         const Training_Data & training_data,
         const Training_Data & validation_data,
         const std::vector<Feature> & features,
         int recursion) const
{
    if (recursion > 3)
        throw Exception("Classifier_Generator::generate(): recursion! "
                        "at least one generate method must be overridden");

    distribution<float> training_weights(training_data.example_count(), 1.0);
    distribution<float> validation_weights(validation_data.example_count(), 1.0);
    return generate(context, training_data, validation_data, training_weights,
                    validation_weights, features, recursion + 1);
}

std::shared_ptr<Classifier_Impl>
Classifier_Generator::
generate(Thread_Context & context,
         const Training_Data & training_data,
         const Training_Data & validation_data,
         const distribution<float> & training_weights,
         const distribution<float> & validation_weights,
         const std::vector<Feature> & features,
         int recursion) const
{
    if (recursion > 3)
        throw Exception("Classifier_Generator::generate(): recursion! "
                        "at least one generate method must be overridden");

    return generate(context, training_data, training_weights, features,
                    recursion + 1);
}

std::shared_ptr<Classifier_Impl>
Classifier_Generator::
generate(Thread_Context & context,
         const Training_Data & training_data,
         const distribution<float> & ex_weights,
         const std::vector<Feature> & features,
         int recursion) const
{
    if (recursion > 3)
        throw Exception("Classifier_Generator::generate(): recursion! "
                        "at least one generate method must be overridden");

    size_t nx = training_data.example_count();

    /* Expand the weights */
    boost::multi_array<float, 2> weights(boost::extents[nx][nl]);

    if (ex_weights.empty())
        std::fill(weights.data(), weights.data() + nx * nl, 1.0 / nl * nx);
    else {
        double tot = ex_weights.total();
        if (ex_weights.size() != nx)
            throw Exception("Classifier_Generator::generate(): "
                            "wrong sized example weights");
        else if (abs(tot) < 1e-10)
            throw Exception("Classifier_Generator::generate(): "
                            "zero or nearly zero example weights total");
        
        float norm = 1.0 / (ex_weights.total() * nl);

        for (unsigned x = 0;  x < nx;  ++x) {
            if ((ex_weights[x] < 0.0) || ex_weights[x] > 1e10)
                throw Exception("Classifier_Generator::generate(): weight "
                                "out of range");
            double val = ex_weights[x] * norm;
            std::fill(&weights[x][0], &weights[x][0] + nl, val);
        }
    }

    float Z = 0.0;
    return generate(context, training_data, weights, features, Z,
                    recursion + 1);
}

std::shared_ptr<Classifier_Impl>
Classifier_Generator::
generate(Thread_Context & context,
         const Training_Data & training_data,
         const boost::multi_array<float, 2> & weights,
         const std::vector<Feature> & features,
         float & Z,
         int recursion) const
{
    if (recursion > 3)
        throw Exception("Classifier_Generator::generate(): recursion! "
                        "at least one generate method must be overridden");

    size_t nx = training_data.example_count();

    if ((weights.shape()[0] != nx)
        || (weights.shape()[1] != nl && nl != 2 && weights.shape()[1] != 1))
        throw Exception("Classifier_Generator::generate(): "
                        "weights array has the wrong dimensions"
                        + format("(%dx%x), should be (%dx%d)",
                                 (int)weights.shape()[0],
                                 (int)weights.shape()[1],
                                 (int)nx,
                                 (int)nl));
    
    /* Generate some normal example weights as the average of those here. */
    distribution<float> ex_weights(nx);

    double total = 0.0;
    for (unsigned x = 0;  x < nx;  ++x) {
        double ex_total = 0.0;
        for (unsigned l = 0;  l < weights.shape()[1];  ++l)
            ex_total += weights[x][l];
        if (nl == 2 && weights.shape()[1] == 1)
            ex_total *= 2.0;
        ex_weights[x] = ex_total;
        total += ex_total;
    }
    
    ex_weights *= nx / total;

    return generate(context, training_data, ex_weights, features,
                    recursion + 1);
}

std::ostream &
Classifier_Generator::
log(const std::string & module, int level) const
{
    //cerr << "level " << level << " verbosity " << verbosity << endl;

    static MLDB::filter_ostream cnull("");

    if (level <= config->verbosity)
        return cerr;
    else return cnull;
}

std::string
Classifier_Generator::
type() const
{
    return demangle(typeid(*this).name());
}

/*****************************************************************************/
/* FACTORIES                                                                 */
/*****************************************************************************/

std::shared_ptr<Classifier_Generator>
get_trainer(const std::string & name,
            const Json::Value & config)
{
    const auto type = config["type"].asString();
    if (type == "") {
        //cerr << config << endl;
        throw Exception("Object with name \"" + name + "\" has no type "
                        "in configuration file with prefix ");
                        // TODO + config.prefix());
    }
    std::shared_ptr<Classifier_Generator> result
        = Registry<Classifier_Generator>::singleton().create(type);

    // TODO throw unparseable
    result->configure(config);

    if (!result->config->unparsedKeys.empty()) {
        //TODO more verbose than that please
        throw Exception("unpased keys found");
    }

    return result;
}

} // namespace ML

