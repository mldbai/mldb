/** tokensplit.h                                      -*- C++ -*-
    Mathieu Marquis Bolduc, November 24th 2015
    Copyright (c) 2015 Datacratic Inc.  All rights reserved.

    Function to parse strings for tokens and insert separators
*/

#pragma once

#include "mldb/server/function.h"
#include "mldb/types/value_description_fwd.h"
#include "mldb/types/optional.h"

namespace Datacratic {
namespace MLDB {

struct ParseHtmlConfig {
    ParseHtmlConfig()
    {
    }
};

DECLARE_STRUCTURE_DESCRIPTION(ParseHtmlConfig);


/*****************************************************************************/
/* PARSE HTML FUNCTION                                                       */
/*****************************************************************************/

struct ParseHtml: public Function {
    ParseHtml(MldbServer * owner,
                PolyConfig config,
                const std::function<bool (const Json::Value &)> & onProgress);
    
    virtual Any getStatus() const;
    
    virtual FunctionOutput apply(const FunctionApplier & applier,
                                 const FunctionContext & context) const;
    
    /** Describe what the input and output is for this function. */
    virtual FunctionInfo getFunctionInfo() const;    
   
    ParseHtmlConfig functionConfig;
};


} // namespace MLDB
} // namespace Datacratic
