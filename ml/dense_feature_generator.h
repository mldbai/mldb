/** dense_feature_generator.h                                      -*- C++ -*-
    Jeremy Barnes, 13 May 2012
    Copyright (c) 2012 mldb.ai inc.  All rights reserved.

    This file is part of MLDB. Copyright 2015 mldb.ai inc. All rights reserved.

    Feature generator for dense feature sets.
*/

#pragma once

#include "mldb/jml/db/persistent.h"
#include "mldb/base/exc_assert.h"
#include "mldb/ml/dense_classifier.h"
#include "mldb/ml/tuple_encoder.h"
#include <boost/any.hpp>
#include <tuple>
#include "pipeline_execution_context.h"


namespace MLDB {


/*****************************************************************************/
/* FEATURE EXPLANATION                                                       */
/*****************************************************************************/

/** Contains the explanation of multiple feature's effect on an outcome. */

struct FeatureExplanation {

    std::map<std::string, double> vals;

    void add(const FeatureExplanation & other, double weight = 1.0)
    {
        for (auto & v: other.vals)
            vals[v.first] += weight * v.second;
    }

    std::string explain(int numFeatures = -1) const;
};


/*****************************************************************************/
/* DENSE FEATURE GENERATOR                                                   */
/*****************************************************************************/

/** Turns something into a list of features. */

struct DenseFeatureGenerator {

    virtual ~DenseFeatureGenerator()
    {
    }

    /** What is the type info node for the concrete type that our
        any is expecting?
    */
    virtual const std::type_info & paramType() const = 0;

    /** What features does this feature generator produce? */
    virtual std::shared_ptr<ML::Dense_Feature_Space>
    featureSpace() const = 0;

    /** Generate the features for the given user. */
    virtual distribution<float>
    featuresGeneric(const boost::any & args) const = 0;

    /** Generate a weighted average of the contribution of input
        features to the generation of the given feature set.
    */
    virtual FeatureExplanation
    explainGeneric(const distribution<float> & featureWeights,
                   const boost::any & args) const = 0;

    /** Generate features for the given context.  Default will call
        featuresGeneric.
    */
    virtual distribution<float>
    featuresContext(const PipelineExecutionContext & context) const
    {
        return featuresGeneric(context.seed);
    }

    /** Generate a weighted average of the contribution of input
        features to the generation of the given feature set.
    */
    virtual FeatureExplanation
    explainContext(const distribution<float> & featureWeights,
                   const PipelineExecutionContext & context) const
    {
        return explainGeneric(featureWeights, context.seed);
    }

    /** Serialize the feature generator's parameters to the given
        store.

        Default implementation throws exception.
    */
    virtual void serialize(ML::DB::Store_Writer & store) const;

    /** Reconstitute the feature generator.
        
        Default implementation throws exception.
     */
    virtual void reconstitute(ML::DB::Store_Reader & store);

    /** Return the class name and arguments used to deal with
        serialization/reconstitution.

        Default implementation throws exception.
    */
    virtual std::pair<std::string, std::string> className() const;
    
    static std::shared_ptr<DenseFeatureGenerator>
    polyReconstitute(ML::DB::Store_Reader & store);

    static void polySerialize(const DenseFeatureGenerator & fgen,
                              ML::DB::Store_Writer & store);

    /** Register a factory to create one of these from a className
        and a store.
    */

    typedef std::function<std::shared_ptr<DenseFeatureGenerator>
                          (std::string)> Factory;

    static void registerFactory(const std::string & className,
                                const Factory & factor);

    virtual bool isCallableFromMultipleThreads() const
    {
        return true;  // default
    }
};


/*****************************************************************************/
/* DENSE FEATURE GENERATOR TEMPLATE                                          */
/*****************************************************************************/

/** Generates a set of features from some set of parameters. */

template<typename... Args>
struct DenseFeatureGeneratorT
    : virtual public DenseFeatureGenerator,
      virtual public TupleEncoder<Args...> {

    virtual ~DenseFeatureGeneratorT()
    {
    }

    virtual const std::type_info & paramType() const
    {
        return TupleEncoder<Args...>::paramType();
    }

    /** Generate the features for the given user. */
    virtual distribution<float>
    featuresGeneric(const boost::any & args) const
    {
        return callPmfWithTuple(&DenseFeatureGeneratorT::features,
                                *this,
                                this->decodeStatic(args));
    }

    virtual FeatureExplanation
    explainGeneric(const distribution<float> & featureWeights,
                   const boost::any & args) const
    {
        return callPmfWithTuple(&DenseFeatureGeneratorT::explain,
                                *this,
                                std::tuple_cat(std::make_tuple(featureWeights),
                                               this->decodeStatic(args)));
    }

    /** Generate the features for the given user. */
    virtual distribution<float>
    features(Args... args) const = 0;

    virtual FeatureExplanation
    explain(const distribution<float> & featureWeights,
            Args... args) const
    {
        // By default... nothing
        return FeatureExplanation();
    }

    static std::shared_ptr<DenseFeatureGeneratorT>
    polyReconstitute(ML::DB::Store_Reader & store)
    {
        return std::dynamic_pointer_cast<DenseFeatureGeneratorT>
            (DenseFeatureGenerator::polyReconstitute(store));
    }

    static void polySerialize(const DenseFeatureGeneratorT & fgen,
                              ML::DB::Store_Writer & store)
    {
        DenseFeatureGenerator::polySerialize(fgen, store);
    }
};


/*****************************************************************************/
/* CUSTOM DENSE FEATURE GENERATOR                                            */
/*****************************************************************************/

/** DenseFeatureGenerator customized with a bunch of std::functions to
    implement all of the require methods.
*/

struct CustomDenseFeatureGenerator : public DenseFeatureGenerator {

    typedef std::function<std::shared_ptr<ML::Dense_Feature_Space> ()>
    FeatureSpaceFn;
    typedef std::function<distribution<float> (const boost::any &)>
    FeaturesFn;
    typedef std::function<FeatureExplanation (const distribution<float> &,
                                              const boost::any &)> ExplainFn;

    FeatureSpaceFn onGetFeatureSpace;
    FeaturesFn onGetFeatures;
    ExplainFn onExplain;


    CustomDenseFeatureGenerator(const FeatureSpaceFn & onGetFeatureSpace
                                    = FeatureSpaceFn(),
                                const FeaturesFn & onGetFeatures
                                    = FeaturesFn(),
                                const ExplainFn & onExplain
                                    = ExplainFn())
        : onGetFeatureSpace(onGetFeatureSpace),
          onGetFeatures(onGetFeatures),
          onExplain(onExplain)
    {
    }

    virtual ~CustomDenseFeatureGenerator()
    {
    }

    /** What features does this feature generator produce? */
    virtual std::shared_ptr<ML::Dense_Feature_Space>
    featureSpace() const
    {
        return onGetFeatureSpace();
    }

    /** Generate the features for the given user. */
    virtual distribution<float>
    featuresGeneric(const boost::any & args) const
    {
        return onGetFeatures(args);
    }

    virtual FeatureExplanation
    explainGeneric(const distribution<float> & featureWeights,
                   const boost::any & args) const
    {
        return onExplain(featureWeights, args);
    }

    /** Serialize the feature generator's parameters to the given
        store. */
    virtual void serialize(ML::DB::Store_Writer & store) const
    {
        throw MLDB::Exception("CustomDenseFeatureGenerator can't be serialized");
    }

    /** Reconstitute the feature generator. */
    virtual void reconstitute(ML::DB::Store_Reader & store)
    {
        throw MLDB::Exception("CustomDenseFeatureGenerator can't be "
                            "reconstituted");
    }

    virtual std::pair<std::string, std::string> className() const
    {
        throw MLDB::Exception("CustomDenseFeatureGenerator can't be "
                            "serialized or reconstituted");
    }
};


/*****************************************************************************/
/* CUSTOM DENSE FEATURE GENERATOR                                            */
/*****************************************************************************/

/** DenseFeatureGenerator customized with a bunch of std::functions to
    implement all of the require methods.
*/

template<typename... Args>
struct CustomDenseFeatureGeneratorT : public DenseFeatureGeneratorT<Args...> {

    typedef std::function<std::shared_ptr<ML::Dense_Feature_Space> ()>
    FeatureSpaceFn;
    typedef std::function<distribution<float> (Args... args)>
    FeaturesFn;
    typedef std::function<FeatureExplanation (const distribution<float> &,
                                              Args... args)>
    ExplainFn;

    FeatureSpaceFn onGetFeatureSpace;
    FeaturesFn onGetFeatures;

    CustomDenseFeatureGeneratorT(const FeatureSpaceFn & onGetFeatureSpace
                                     = FeatureSpaceFn(),
                                 const FeaturesFn & onGetFeatures
                                     = FeaturesFn(),
                                 const ExplainFn & onExplain
                                     = ExplainFn())
        : onGetFeatureSpace(onGetFeatureSpace),
          onGetFeatures(onGetFeatures)
    {
    }

    virtual ~CustomDenseFeatureGeneratorT()
    {
    }

    /** What features does this feature generator produce? */
    virtual std::shared_ptr<ML::Dense_Feature_Space>
    featureSpace() const
    {
        return onGetFeatureSpace();
    }

    /** Generate the features for the given user. */
    virtual distribution<float>
    features(Args... args) const
    {
        return onGetFeatures(args...);
    }

    virtual FeatureExplanation
    explain(const distribution<float> & featureWeights,
            Args... args) const
    {
        return onExplain(featureWeights, args...);
    }
};


template<void (*Fn) ()>
struct DoInitialize {
    DoInitialize()
    {
        Fn();
    }

    void doMe() __attribute__((__noinline__))
    {
        asm ("");
    }
};


/*****************************************************************************/
/* COMBINED FEATURE GENERATOR                                                */
/*****************************************************************************/

/** DenseFeatureGenerator that gets its features from multiple other
    feature sources.
*/

struct CombinedFeatureGenerator: virtual public DenseFeatureGenerator {

    CombinedFeatureGenerator()
        : featureSpace_(new ML::Dense_Feature_Space())
    {
    }

    virtual ~CombinedFeatureGenerator()
    {
    }

    virtual const std::type_info & paramType() const;

    /** What features does this feature generator produce? */
    virtual std::shared_ptr<ML::Dense_Feature_Space>
    featureSpace() const
    {
        return featureSpace_;
    }

    /** Generate the features for the given user. */
    virtual distribution<float>
    featuresGeneric(const boost::any & args) const
    {
        distribution<float> result;
        for (unsigned i = 0;  i < generators.size();  ++i)
            result.extend(generators[i].generator->featuresGeneric(args));
        return result;
    }

    /** Generate the features for the given user. */
    virtual FeatureExplanation
    explainGeneric(const distribution<float> & featureWeights,
                   const boost::any & args) const
    {
        FeatureExplanation result;

        int numFeaturesDone = 0;

        //using namespace std;
        
        //cerr << "generators.size() = " << generators.size() << endl;

        for (unsigned i = 0;  i < generators.size();  ++i) {
            ExcAssertLessEqual(numFeaturesDone + generators[i].numFeatures,
                               featureWeights.size());

            distribution<float> genWeights
                (featureWeights.begin() + numFeaturesDone,
                 featureWeights.begin() + numFeaturesDone + generators[i].numFeatures);

            //cerr << "explaining " << MLDB::type_name(*generators[i].generator)
            //     << " with " << genWeights.size() << " weights" << endl;

            result.add(generators[i].generator->explainGeneric(genWeights, args));
            numFeaturesDone += generators[i].numFeatures;
        }

        return result;
    }

    /** Add the generator. */
    virtual void add(std::shared_ptr<DenseFeatureGenerator> generator,
                     const std::string & prefix = "")
    {
        // TODO: check if it's compatible...
        GeneratorEntry entry;
        entry.generator = generator;
        entry.featureSpace = generator->featureSpace();
        entry.numFeatures = entry.featureSpace->variable_count();
        entry.prefix = prefix;

        featureSpace_->add(*entry.featureSpace, prefix);
        generators.emplace_back(std::move(entry));
    }

    /** Serialize the feature generator's parameters to the given
        store. */
    virtual void serialize(ML::DB::Store_Writer & store) const
    {
        unsigned char version = 0;
        store << version;
        store << ML::DB::compact_size_t(generators.size());
        for (unsigned i = 0;  i < generators.size();  ++i) {
            store << generators[i].prefix;
            DenseFeatureGenerator::polySerialize(*generators[i].generator, store);
        }
    }

    /** Reconstitute the feature generator. */
    virtual void reconstitute(ML::DB::Store_Reader & store)
    {
        unsigned char version;
        store >> version;
        if (version != 0)
            throw MLDB::Exception("unknown CombinedFeatureGeneratorT version %d",
                                (int)version);
        ML::DB::compact_size_t ngen(store);

        //using namespace std;
        //cerr << "reconstituting " << ngen << " feature generators" << endl;

        featureSpace_.reset(new ML::Dense_Feature_Space());
        generators.clear();

        for (unsigned i = 0;  i < ngen;  ++i) {
            GeneratorEntry entry;

            std::string prefix;
            store >> prefix;
            //cerr << "featureSpace_ size is " << featureSpace_->variable_count()
            //<< endl;
            auto fgen = DenseFeatureGenerator::polyReconstitute(store);

            add(fgen, prefix);
        }

        //cerr << "featureSpace_ size is " << featureSpace_->variable_count()
        //     << endl;
    }

    virtual std::pair<std::string, std::string> className() const
    {
        return std::make_pair("Combined", "");
    }

    virtual bool isCallableFromMultipleThreads() const
    {
        for (auto& gen: generators)
            if (!gen.generator->isCallableFromMultipleThreads())
                return false;
        return true;
    }
    
    /** Information about a single generator */
    struct GeneratorEntry {
        std::shared_ptr<DenseFeatureGenerator> generator;
        std::shared_ptr<ML::Dense_Feature_Space> featureSpace;
        int numFeatures;
        std::string prefix;
    };

    /** List of feature generators and associated info. */
    std::vector<GeneratorEntry> generators;

    /** Current feature space. */
    std::shared_ptr<ML::Dense_Feature_Space> featureSpace_;
};


/*****************************************************************************/
/* COMBINED FEATURE GENERATOR TEMPLATE                                       */
/*****************************************************************************/

/** DenseFeatureGenerator that gets its features from multiple other
    features.
*/

template<typename... Args>
struct CombinedFeatureGeneratorT
    : virtual public DenseFeatureGeneratorT<Args...>,
      virtual public CombinedFeatureGenerator {

    typedef DenseFeatureGeneratorT<Args...> DenseGenerator;
    typedef typename DenseGenerator::tuple_type tuple_type;

    CombinedFeatureGeneratorT()
    {
        init.doMe();
    }

    virtual ~CombinedFeatureGeneratorT()
    {
    }

    virtual const std::type_info & paramType() const
    {
        return DenseFeatureGeneratorT<Args...>::paramType();
    }

    /** Generate the features for the given user. */
    virtual distribution<float>
    features(Args... args) const
    {
        boost::any encoded = this->encode(args...);
        return featuresGeneric(encoded);
    }

    virtual distribution<float>
    featuresGeneric(const boost::any & args) const
    {
        return CombinedFeatureGenerator::featuresGeneric(args);
    }

    /** Generate the features for the given user. */
    virtual FeatureExplanation
    explain(const distribution<float> & featureWeights,
            Args... args) const
    {
        boost::any encoded = this->encode(args...);
        return explainGeneric(featureWeights, encoded);
    }

    virtual FeatureExplanation
    explainGeneric(const distribution<float> & featureWeights,
                   const boost::any & args) const
    {
        return CombinedFeatureGenerator::explainGeneric(featureWeights, args);
    }

    virtual std::pair<std::string, std::string> className() const
    {
        return make_pair("Combined<" + MLDB::type_name<tuple_type>() + ">", "");
    }

    static std::shared_ptr<DenseFeatureGenerator>
    factory(const std::string & name2)
    {
        auto result = std::make_shared<CombinedFeatureGeneratorT>();
        return result;
    }

    typedef std::function<std::shared_ptr<DenseFeatureGenerator>
                          (std::string)> Factory;

    static void doInitialize()
    {
        DenseFeatureGenerator::registerFactory
            ("Combined<" + MLDB::type_name<tuple_type>() + ">", factory);
    }

    static DoInitialize<&CombinedFeatureGeneratorT::doInitialize> init;
};

template<typename... Args>
DoInitialize<&CombinedFeatureGeneratorT<Args...>::doInitialize>
CombinedFeatureGeneratorT<Args...>::init;


} // namespace MLDB
