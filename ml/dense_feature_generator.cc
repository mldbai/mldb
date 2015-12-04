// This file is part of MLDB. Copyright 2015 Datacratic. All rights reserved.

/* dense_feature_generator.cc                                      -*- C++ -*-
   Jeremy Barnes, 20 June 2012
   Copyright (c) 2012 Datacratic.  All rights reserved.

*/

#include "dense_feature_generator.h"
#include <boost/thread/thread.hpp>
#include <unordered_map>
#include "mldb/arch/demangle.h"
#include <boost/thread/recursive_mutex.hpp>


using namespace std;


namespace Datacratic {


std::string
FeatureExplanation::
explain(int numFeatures) const
{
    std::vector<std::pair<std::string, float> > sorted(vals.begin(), vals.end());

    std::sort(sorted.begin(), sorted.end(),
              [] (const pair<string, float> & v1,
                  const pair<string, float> & v2)
              {
                  return abs(v1.second) > abs(v2.second);
              });

    std::string result;

    for (auto & v: sorted)
        result += ML::format("%12.6f ", v.second) + v.first + "\n";

    return result;
}


/*****************************************************************************/
/* DENSE FEATURE GENERATOR                                                   */
/*****************************************************************************/

namespace {

struct FGInfo {
    typedef DenseFeatureGenerator::Factory Factory;
    Factory factory;
};

struct FactoryRegistry {
    boost::recursive_mutex featureGeneratorsLock;
    std::unordered_map<std::string, FGInfo> featureGenerators;
};

FactoryRegistry & getRegistry()
{
    static FactoryRegistry registry;

    return registry;
}

} // file scope

void
DenseFeatureGenerator::
serialize(ML::DB::Store_Writer & store) const
{
    throw ML::Exception("DenseFeatureGenerator " + ML::type_name(*this)
                        + " doesn't support serialization/reconstitution");
}

void
DenseFeatureGenerator::
reconstitute(ML::DB::Store_Reader & store)
{
    throw ML::Exception("DenseFeatureGenerator " + ML::type_name(*this)
                        + " doesn't support serialization/reconstitution");
}

std::pair<std::string, std::string>
DenseFeatureGenerator::
className() const
{
    throw ML::Exception("DenseFeatureGenerator " + ML::type_name(*this)
                        + " doesn't support serialization/reconstitution");
}

std::shared_ptr<DenseFeatureGenerator>
DenseFeatureGenerator::
polyReconstitute(ML::DB::Store_Reader & store)
{
    unsigned char version;
    store >> version;
    if (version != 0)
        throw ML::Exception("unexpected poly reconstitute version");
    std::string tag;
    store >> tag;
    if (tag != "DenseFeatureGeneratorPoly")
        throw ML::Exception("unexpected poly reconstitute tag "
                            + tag);

    std::string type, args;
    store >> type >> args;

    FactoryRegistry & registry = getRegistry();
    boost::unique_lock<boost::recursive_mutex> guard(registry.featureGeneratorsLock);
    auto it = registry.featureGenerators.find(type);
    if (it == registry.featureGenerators.end())
        throw ML::Exception("couldn't reconstitute DenseFeatureGenerator "
                            "of type " + type + " with args '" + args
                            + "'");
    auto result = it->second.factory(args);
    result->reconstitute(store);

    store >> tag;
    if (tag != "DenseFeatureGeneratorPolyEnd")
        throw ML::Exception("unexpected poly reconstitute ending tag "
                            + tag);

    return result;
}

void
DenseFeatureGenerator::
polySerialize(const DenseFeatureGenerator & fgen,
              ML::DB::Store_Writer & store)
{
    unsigned char version = 0;
    store << version;
    string tag = "DenseFeatureGeneratorPoly";
    store << tag;

    pair<string, string> args = fgen.className();
    store << args.first << args.second;

    fgen.serialize(store);

    tag = "DenseFeatureGeneratorPolyEnd";
    store << tag;
}

void
DenseFeatureGenerator::
registerFactory(const std::string & className,
                const Factory & factory)
{
    //cerr << "registerFactory " << className << endl;
    FactoryRegistry & registry = getRegistry();
    boost::unique_lock<boost::recursive_mutex> guard(registry.featureGeneratorsLock);
    if (registry.featureGenerators.count(className))
        throw ML::Exception("attempt to double register feature generator "
                            + className);
    registry.featureGenerators[className].factory = factory;
}

/*****************************************************************************/
/* COMBINED FEATURE GENERATOR                                                */
/*****************************************************************************/

const std::type_info &
CombinedFeatureGenerator::
paramType() const
{
    if (generators.empty())
        return typeid(void);
    return generators[0].generator->paramType();
}

} // namespace Datacratic
