// This file is part of MLDB. Copyright 2015 Datacratic. All rights reserved.

/*  -*- C++ -*- */
#pragma once

#include "mldb/core/function.h"
#include "mldb/types/value_description.h"

namespace Datacratic {
namespace MLDB {


struct TestFunctionConfig {
    TestFunctionConfig()
    {
    }

};

DECLARE_STRUCTURE_DESCRIPTION(TestFunctionConfig);

struct TestFunction: public Function {
    TestFunction(MldbServer * owner,
              PolyConfig config,
              const std::function<bool (const Json::Value &)> & onProgress);
    
    ~TestFunction();
    
    virtual Any getStatus() const;
    
    virtual FunctionOutput apply(const FunctionApplier & applier,
                              const FunctionContext & context) const;

    /** Describe what the input and output is for this function. */
    virtual FunctionInfo getFunctionInfo() const;

    //Classifier classifier;
    TestFunctionConfig functionConfig;

    static std::atomic<int> cnt;

};


}
}
