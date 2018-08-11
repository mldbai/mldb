/** pipeline_execution_context.h                                   -*- C++ -*-
    Jeremy Barnes, 30 October 2013
    Copyright (c) 2013 mldb.ai inc.  All rights reserved.
    This file is part of MLDB. Copyright 2015 mldb.ai inc. All rights reserved.

*/

#pragma once

#include <any>
#include <map>
#include "mldb/utils/distribution.h"
#include "mldb/plugins/jml/jml/classifier.h"

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
                     const distribution<float> & feats)
    {
        set<distribution<float> >(name, feats);
    }

    template<typename T>
    T get(const std::string & name) const
    {
        auto it = entries.find(name);
        if (it == entries.end())
            throw MLDB::Exception("couldn't find required entry " + name);
        try {
            return std::any_cast<T>(it->second);
        } catch (const std::bad_any_cast & exc) {
            throw MLDB::Exception("attempt to access feature " + name + " as "
                                + MLDB::type_name<T>() + " but it is recorded as "
                                + demangle(it->second.type().name()));
        }
    }

    distribution<float> getFeatures(const std::string & name) const
    {
        return get<distribution<float> >(name);
    }
};

} // namespace MLDB

