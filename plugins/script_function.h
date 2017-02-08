// This file is part of MLDB. Copyright 2015 mldb.ai inc. All rights reserved.

/** script_functions.h                                               -*- C++ -*-
    Francois Maillet, 14 juillet 2015
    Copyright (c) 2015 mldb.ai inc.  All rights reserved.

    Functions to deal with js/py scripts
*/

#pragma once

#include "mldb/core/function.h"
#include "mldb/server/plugin_resource.h"


namespace MLDB {


/*****************************************************************************/
/* SCRIPT FUNCTION CONFIG                                                    */
/*****************************************************************************/

struct ScriptFunctionConfig {
    std::string language;
    ScriptResource scriptConfig;
};

DECLARE_STRUCTURE_DESCRIPTION(ScriptFunctionConfig);


/*****************************************************************************/
/* SCRIPT FUNCTION                                                           */
/** Function that runs a js/py script */
/*****************************************************************************/

struct ScriptFunction: public Function {
    ScriptFunction(MldbServer * owner,
                   PolyConfig config,
                   const std::function<bool (const Json::Value &)> & onProgress);
    
    virtual Any getStatus() const;

    virtual ExpressionValue apply(const FunctionApplier & applier,
                              const ExpressionValue & context) const;

    virtual FunctionInfo getFunctionInfo() const;

    ScriptFunctionConfig functionConfig;

    std::string runner;
    ScriptResource cachedResource;
};




} // namespace MLDB


