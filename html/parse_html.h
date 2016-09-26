/** parse_html.h                                                   -*- C++ -*-
    Jeremy Barnes, 29 November 2015
    Copyright (c) 2015 Datacratic Inc.  All rights reserved.

    HTML parser function.
*/

#pragma once

#include "mldb/core/function.h"
#include "mldb/types/value_description_fwd.h"

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
    
    virtual ExpressionValue apply(const FunctionApplier & applier,
                                  const ExpressionValue & context) const;
    
    /** Describe what the input and output is for this function. */
    virtual FunctionInfo
    getFunctionInfo() const;    
   
    ParseHtmlConfig functionConfig;
};


/*****************************************************************************/
/* EXTRACT LINKS FUNCTION                                                    */
/*****************************************************************************/

struct ExtractLinksConfig {
    ExtractLinksConfig()
    {
    }
};

DECLARE_STRUCTURE_DESCRIPTION(ExtractLinksConfig);

struct ExtractLinks: public Function {
    ExtractLinks(MldbServer * owner,
                 PolyConfig config,
                 const std::function<bool (const Json::Value &)> & onProgress);
    
    virtual Any getStatus() const;
    
    virtual ExpressionValue
    apply(const FunctionApplier & applier,
          const ExpressionValue & context) const;
    
    /** Describe what the input and output is for this function. */
    virtual FunctionInfo
    getFunctionInfo() const;    

    ExtractLinksConfig functionConfig;
};


} // namespace MLDB
