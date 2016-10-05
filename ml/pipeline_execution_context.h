// This file is part of MLDB. Copyright 2015 Datacratic. All rights reserved.

/** pipeline_execution_context.h                                   -*- C++ -*-
    Jeremy Barnes, 30 October 2013
    Copyright (c) 2013 Datacratic Inc.  All rights reserved.

*/

#pragma once

#include <boost/any.hpp>
#include <map>
#include "mldb/jml/stats/distribution.h"
#include "mldb/ml/jml/classifier.h"

namespace MLDB {

/*****************************************************************************/
/* PIPELINE EXECUTION CONTEXT                                                */
/*****************************************************************************/

/** Context in which a pipeline executes. */

struct PipelineExecutionContext: public ML::PredictionContext {

    template<typename T>
    void set(const std::string & name, T value)
    {
        entries[name] = value;
    }

    void setFeatures(const std::string & name,
                     const ML::distribution<float> & feats)
    {
        set<ML::distribution<float> >(name, feats);
    }

    template<typename T>
    T get(const std::string & name) const
    {
        auto it = entries.find(name);
        if (it == entries.end())
            throw ML::Exception("couldn't find required entry " + name);
        try {
            return boost::any_cast<T>(it->second);
        } catch (const boost::bad_any_cast & exc) {
            throw ML::Exception("attempt to access feature " + name + " as "
                                + ML::type_name<T>() + " but it is recorded as "
                                + ML::demangle(it->second.type().name()));
        }
    }

    ML::distribution<float> getFeatures(const std::string & name) const
    {
        return get<ML::distribution<float> >(name);
    }
};

} // namespace MLDB

