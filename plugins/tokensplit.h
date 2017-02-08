/** tokensplit.h                                      -*- C++ -*-
    Mathieu Marquis Bolduc, November 24th 2015
    Copyright (c) 2015 mldb.ai inc.  All rights reserved.

    This file is part of MLDB. Copyright 2015 mldb.ai inc. All rights reserved.

    Function to parse strings for tokens and insert separators
*/

#pragma once

#include "mldb/core/dataset.h"
#include "mldb/core/function.h"
#include "mldb/types/value_description_fwd.h"
#include "mldb/types/optional.h"


namespace MLDB {

struct TokenSplitConfig {
    TokenSplitConfig() : 
        splitchars(" ,"),      //<space><comma>
        splitcharToInsert(" ") // <space>
    {
    }

    InputQuery tokens;
    Utf8String splitchars;
    Utf8String splitcharToInsert;
};

DECLARE_STRUCTURE_DESCRIPTION(TokenSplitConfig);


/*****************************************************************************/
/* TOKEN SPLIT FUNCTION                                                       */
/*****************************************************************************/

struct TokenSplit: public Function {
    TokenSplit(MldbServer * owner,
                PolyConfig config,
                const std::function<bool (const Json::Value &)> & onProgress);
    
    virtual Any getStatus() const;
    
    virtual ExpressionValue apply(const FunctionApplier & applier,
                                 const ExpressionValue & context) const;
    
    /** Describe what the input and output is for this function. */
    virtual FunctionInfo getFunctionInfo() const;    
   
    TokenSplitConfig functionConfig;

    std::vector<Utf8String> dictionary;
};


} // namespace MLDB

