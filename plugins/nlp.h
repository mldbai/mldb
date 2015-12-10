// This file is part of MLDB. Copyright 2015 Datacratic. All rights reserved.

/** nlp.h                                               -*- C++ -*-
    Francois Maillet, 20 octobre 2015
    Copyright (c) 2015 Datacratic Inc.  All rights reserved.

*/

#pragma once

#include "mldb/core/function.h"
#include "mldb/ext/libstemmer/libstemmer.h"
#include <mutex>

namespace Datacratic {
namespace MLDB {


/*****************************************************************************/
/* APPLY STOP WORDS FUNCTION CONFIG                                          */
/*****************************************************************************/

struct ApplyStopWordsFunctionConfig {
    ApplyStopWordsFunctionConfig()
        : language("english")
    {}

    std::string language;
};

DECLARE_STRUCTURE_DESCRIPTION(ApplyStopWordsFunctionConfig);


/*****************************************************************************/
/* APPLY STOP WORDS FUNCTION                                                 */
/*****************************************************************************/

struct ApplyStopWordsFunction: public Function {
    ApplyStopWordsFunction(MldbServer * owner,
                   PolyConfig config,
                   const std::function<bool (const Json::Value &)> & onProgress);
    
    virtual Any getStatus() const;

    virtual FunctionOutput apply(const FunctionApplier & applier,
                              const FunctionContext & context) const;

    virtual FunctionInfo getFunctionInfo() const;

    std::map<std::string, std::set<std::string>> stopwords;
    std::set<std::string> * selected_stopwords;

    ApplyStopWordsFunctionConfig functionConfig;
};


/*****************************************************************************/
/* STEMMER FUNCTION CONFIG                                                   */
/*****************************************************************************/

struct StemmerFunctionConfig {
    StemmerFunctionConfig()
        : language("english")
    {}

    std::string language;
};

DECLARE_STRUCTURE_DESCRIPTION(StemmerFunctionConfig);


/*****************************************************************************/
/* APPLY STOP WORDS FUNCTION                                                 */
/*****************************************************************************/

struct StemmerFunction: public Function {
    StemmerFunction(MldbServer * owner,
                   PolyConfig config,
                   const std::function<bool (const Json::Value &)> & onProgress);
   
    virtual Any getStatus() const;

    virtual FunctionOutput apply(const FunctionApplier & applier,
                              const FunctionContext & context) const;

    virtual FunctionInfo getFunctionInfo() const;

    std::unique_ptr<sb_stemmer> stemmer;

    mutable std::mutex apply_mutex; 

    StemmerFunctionConfig functionConfig;
};

struct StemmerOnDocumentFunction: public Function {
    StemmerOnDocumentFunction(MldbServer * owner,
                   PolyConfig config,
                   const std::function<bool (const Json::Value &)> & onProgress);
   
    virtual Any getStatus() const;

    virtual FunctionOutput apply(const FunctionApplier & applier,
                              const FunctionContext & context) const;

    virtual FunctionInfo getFunctionInfo() const;

    std::unique_ptr<sb_stemmer> stemmer;

    mutable std::mutex apply_mutex; 

    StemmerFunctionConfig functionConfig;
};

} // namespace MLDB
} // namespace Datacratic

