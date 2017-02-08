/** useragent_function.h                                               -*- C++ -*-
    Francois Maillet, 27 juin 2016
    This file is part of MLDB. Copyright 2016 mldb.ai inc. All rights reserved.
*/

#pragma once

#include "mldb/core/value_function.h"
#include "mldb/ext/uap-cpp/UaParser.h"



namespace MLDB {


/*****************************************************************************/
/* USER AGENT PARSER FUNCTION CONFIG                                         */
/*****************************************************************************/

struct ParseUserAgentFunctionConfig {
    ParseUserAgentFunctionConfig()
        : regexFile("/opt/bin/useragent-regexes.yaml")
    {}

    std::string regexFile;
};

DECLARE_STRUCTURE_DESCRIPTION(ParseUserAgentFunctionConfig);


/*****************************************************************************/
/* USER AGENT PARSER FUNCTION                                                */
/*****************************************************************************/

struct UserAgentParserArgs  {
    ExpressionValue ua;
};


DECLARE_STRUCTURE_DESCRIPTION(UserAgentParserArgs);


struct UaDevice {
    ExpressionValue brand;
    ExpressionValue model;
};

struct UaSoftware {
    ExpressionValue family;
    ExpressionValue version;
};

struct ParsedUserAgent {
    UaDevice device;
    UaSoftware browser;
    UaSoftware os;
    ExpressionValue isSpider;
};

DECLARE_STRUCTURE_DESCRIPTION(ParsedUserAgent);


struct ParseUserAgentFunction: public ValueFunctionT<UserAgentParserArgs, ParsedUserAgent> {
    ParseUserAgentFunction(MldbServer * owner,
                           PolyConfig config,
                           const std::function<bool (const Json::Value &)> & onProgress);

    virtual ParsedUserAgent call(UserAgentParserArgs input) const override;

    std::shared_ptr<UaParser::UserAgentParser> parser;

    ParseUserAgentFunctionConfig functionConfig;
};

} // namespace MLDB
