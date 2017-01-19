/** nlp.h                                               -*- C++ -*-
    Francois Maillet, 20 octobre 2015
    Copyright (c) 2015 mldb.ai inc.  All rights reserved.

    This file is part of MLDB. Copyright 2015 mldb.ai inc. All rights reserved.
*/

#pragma once

#include "mldb/core/value_function.h"
#include "mldb/ext/libstemmer/libstemmer.h"
#include <mutex>


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

struct Words {
    ExpressionValue words;  // row 
};

DECLARE_STRUCTURE_DESCRIPTION(Words);

struct Document {
    ExpressionValue document; // string
};

DECLARE_STRUCTURE_DESCRIPTION(Document);


/*****************************************************************************/
/* APPLY STOP WORDS FUNCTION                                                 */
/*****************************************************************************/

struct ApplyStopWordsFunction: public ValueFunctionT<Words, Words> {
    ApplyStopWordsFunction(MldbServer * owner,
                           PolyConfig config,
                           const std::function<bool (const Json::Value &)> & onProgress);
    
    virtual Words call(Words input) const override;

    std::map<std::string, std::set<std::string> > stopwords;
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
/* STEMMER FUNCTION                                                          */
/*****************************************************************************/

struct StemmerFunction: public ValueFunctionT<Words, Words> {
    StemmerFunction(MldbServer * owner,
                    PolyConfig config,
                    const std::function<bool (const Json::Value &)> & onProgress);
   
    virtual Words call(Words input) const override;

    StemmerFunctionConfig functionConfig;
};

struct StemmerOnDocumentFunction: public ValueFunctionT<Document, Document> {
    StemmerOnDocumentFunction(MldbServer * owner,
                              PolyConfig config,
                              const std::function<bool (const Json::Value &)> & onProgress);
    
    virtual Document call(Document input) const override;
    
    StemmerFunctionConfig functionConfig;
};

} // namespace MLDB


