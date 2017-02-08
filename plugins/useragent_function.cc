/** useragent_function.cc
    Francois Maillet, 26 juin 2016
    This file is part of MLDB. Copyright 2016 mldb.ai inc. All rights reserved.

*/

#include "useragent_function.h"
#include "mldb/server/mldb_server.h"
#include "mldb/types/basic_value_descriptions.h"
#include "mldb/types/any_impl.h"

using namespace std;



namespace MLDB {

    
DEFINE_STRUCTURE_DESCRIPTION(ParseUserAgentFunctionConfig);

ParseUserAgentFunctionConfigDescription::
ParseUserAgentFunctionConfigDescription()
{
    addField("regexFile", &ParseUserAgentFunctionConfig::regexFile,
        "User agent string parser YAML configuration file",
        string("/opt/bin/useragent-regexes.yaml"));
}


DEFINE_STRUCTURE_DESCRIPTION(UserAgentParserArgs);

UserAgentParserArgsDescription::
UserAgentParserArgsDescription()
{
    addField("ua", &UserAgentParserArgs::ua, "User agent string");
}


DEFINE_STRUCTURE_DESCRIPTION(UaDevice);
UaDeviceDescription::UaDeviceDescription()
{
    addField("brand", &UaDevice::brand, "Device brand");
    addField("model", &UaDevice::model, "Device model");
}

DEFINE_STRUCTURE_DESCRIPTION(UaSoftware);
UaSoftwareDescription::UaSoftwareDescription()
{
    addField("family", &UaSoftware::family, "Software family");
    addField("version", &UaSoftware::version, "Software version");
}


DEFINE_STRUCTURE_DESCRIPTION(ParsedUserAgent);
ParsedUserAgentDescription::ParsedUserAgentDescription()
{
    addField("os", &ParsedUserAgent::os, "Operating system");
    addField("browser", &ParsedUserAgent::browser, "Browser");
    addField("device", &ParsedUserAgent::device, "Device");
    addField("isSpider", &ParsedUserAgent::isSpider, "Does this user agent represent a spider");
}


/*****************************************************************************/
/* USER AGENT PARSER FUNCTION                                                */
/*****************************************************************************/
                      
ParseUserAgentFunction::
ParseUserAgentFunction(MldbServer * owner,
                       PolyConfig config,
                       const std::function<bool (const Json::Value &)> & onProgress)
    : BaseT(owner, config)
{
    functionConfig = config.params.convert<ParseUserAgentFunctionConfig>();

    parser = make_shared<UaParser::UserAgentParser>(functionConfig.regexFile);
}

    
ParsedUserAgent 
ParseUserAgentFunction::
call(UserAgentParserArgs input) const
{
    if(input.ua.empty())
        return ParsedUserAgent();

    auto parsedResults = parser->parse(input.ua.toUtf8String().rawString());

    Date ts = input.ua.getEffectiveTimestamp();

    ParsedUserAgent result;
    result.os.family = ExpressionValue(std::move(Utf8String(parsedResults.os.family)), ts);
    result.os.version = ExpressionValue(std::move(Utf8String(parsedResults.os.toVersionString())), ts);

    result.browser.family = ExpressionValue(std::move(Utf8String(parsedResults.browser.family)), ts);
    result.browser.version = ExpressionValue(std::move(Utf8String(parsedResults.browser.toVersionString())), ts);

    result.device.model = ExpressionValue(std::move(Utf8String(parsedResults.device.model)), ts);
    result.device.brand = ExpressionValue(std::move(Utf8String(parsedResults.device.brand)), ts);

    result.isSpider = ExpressionValue(parsedResults.isSpider(), ts);

    return result;
}

static RegisterFunctionType<ParseUserAgentFunction, ParseUserAgentFunctionConfig>
regParseUserAgentFunction(builtinPackage(),
                          "http.useragent",
                          "Parse user agent strings into their components",
                          "functions/ParseUserAgent.md.html");

} // namespace MLDB

