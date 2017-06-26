/* info_override_feature_space.h                                 -*- C++ -*-
   Mathieu Marquis Bolduc, 22 March 2017
   This file is part of MLDB. Copyright 2017 mldb.ai inc. All rights reserved.

   Feature space that wraps a const feature place and allow feature info to be 
   overriden
*/

#pragma once

#include "feature_space.h"
#include "feature_set.h"
#include "mldb/ml/jml/feature_info.h"
#include <map>

namespace ML {

class Info_Override_Feature_Space : public ML::Feature_Space {
public:

    Info_Override_Feature_Space(std::shared_ptr<const Feature_Space> inner) : 
        inner(inner) 
    {

    }

    virtual ~Info_Override_Feature_Space() {

    }

    /** Return information on how we use a particular feature.
        \param feature       the Feature that we are interested in.  The
                             interpretation of this is up to the feature
                             space.
        \returns             a Feature_Info object for the given feature.
        
        This is an abstract method; subclasses must override it.  In most
        cases, choosing a REAL value should be sufficient.

        Note also that this method is called frequently, and thus shouldn't
        do any major computation.
     */
    virtual Feature_Info info(const Feature & feature) const override;

    void set_info(const ML::Feature & feature, const ML::Feature_Info & info);
    
    virtual void serialize(DB::Store_Writer & store) const override;
    virtual void reconstitute(DB::Store_Reader & store,
                              const std::shared_ptr<const Feature_Space> & fs) override;
    virtual std::string class_id() const override;    
    virtual Feature_Space * make_copy() const override;

    //All other methods redirect to the inner
    virtual Type type() const override
        { return inner->type();}
    virtual void serialize(DB::Store_Writer & store,
                           const Feature & feature) const override
        { return inner->serialize(store, feature);}
    virtual void reconstitute(DB::Store_Reader & store,
                              Feature & feature) const override
        { return inner->reconstitute(store, feature);}
    virtual void serialize(DB::Store_Writer & store,
                           const Feature & feature,
                           float value) const override
         { return inner->serialize(store, feature, value);}
    virtual void reconstitute(DB::Store_Reader & store,
                              const Feature & feature,
                              float & value) const override
        { return inner->reconstitute(store, feature, value);}
    virtual void serialize(DB::Store_Writer & store,
                           const Feature_Set & fs) const override
        { return inner->serialize(store, fs);}
    virtual void reconstitute(DB::Store_Reader & store,
                              std::shared_ptr<Feature_Set> & fs) const override
        { return inner->reconstitute(store, fs);}
    virtual std::string print() const override
        { return inner->print(); }
    virtual std::string print(const Feature & feature) const override 
        { return inner->print(feature); }
    virtual std::string print(const Feature & feature, float value) const override 
        { return inner->print(feature, value); }
    virtual bool parse(ParseContext & context, Feature & feature) const override 
        { return inner->parse(context, feature); }
    virtual void parse(const std::string & name, Feature & feature) const override
        { return inner->parse(name, feature); }
    virtual void expect(ParseContext & context, Feature & feature) const override
        { return inner->expect(context, feature); }
    virtual std::string print(const Feature_Set & fs) const override
        { return inner->print(fs); }
    virtual const std::vector<Feature> & dense_features() const override
        { return inner->dense_features(); }
    virtual void freeze() override
        { throw MLDB::Exception("Cannot freeze Info_Override_Feature_Space"); }
    virtual std::shared_ptr<Training_Data> 
        training_data(const std::shared_ptr<const Feature_Space> & fs) const override
        { return inner->training_data(fs); }

private:
    std::shared_ptr<const Feature_Space> inner;
    std::map<Feature, Feature_Info> overridenInfo;
};

}
