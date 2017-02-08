// This file is part of MLDB. Copyright 2015 mldb.ai inc. All rights reserved.

/* rest_request_binding.cc                                         -*- C++ -*-
   Jeremy Barnes, 21 May 2014
   Copyright (c) 2014 mldb.ai inc.  All rights reserved.

*/

#include "mldb/rest/rest_request_binding.h"
#include "mldb/http/http_exception.h"

using namespace std;


namespace MLDB {

/** These functions turn an argument to the request binding into a function
    that can generate the value required by the handler function.

*/

std::function<std::string
              (RestConnection & connection,
               const RestRequest & request,
               const RestRequestParsingContext & context)>
createParameterExtractor(Json::Value & argHelp,
                         const StringPayload & p, void *)
{
    Json::Value & v = argHelp["payload"];
    v["description"] = p.description;

    return [=] (RestConnection & connection,
                const RestRequest & request,
                const RestRequestParsingContext & context)
        {
            return request.payload;
        };
}

/** Pass the connection on */
std::function<RestConnection &
                     (RestConnection & connection,
                      const RestRequest & request,
                      const RestRequestParsingContext & context)>
createParameterExtractor(Json::Value & argHelp,
                         const PassConnectionId &, void *)
{
    return [] (RestConnection & connection,
                const RestRequest & request,
                const RestRequestParsingContext & context)
        -> RestConnection &
        {
            return connection;
        };
}

/** Pass the connection on */
std::function<const RestRequestParsingContext &
                     (RestConnection & connection,
                      const RestRequest & request,
                      const RestRequestParsingContext & context)>
createParameterExtractor(Json::Value & argHelp,
                         const PassParsingContext &, void *)
{
    return [] (RestConnection & connection,
                const RestRequest & request,
                const RestRequestParsingContext & context)
        -> const RestRequestParsingContext &
        {
            return context;
        };
}

/** Pass the connection on */
std::function<const RestRequest &
                     (RestConnection & connection,
                      const RestRequest & request,
                      const RestRequestParsingContext & context)>
createParameterExtractor(Json::Value & argHelp,
                         const PassRequest &, void *)
{
    return [] (RestConnection & connection,
               const RestRequest & request,
               const RestRequestParsingContext & context)
        -> const RestRequest &
        {
            return request;
        };
}

std::function<bool
              (RestConnection & connection,
               const RestRequest & request,
               const RestRequestParsingContext & context)>
createRequestValidater(const Json::Value & argHelp,
                       std::set<Utf8String> ignored)
{
    std::set<Utf8String> acceptedParams = std::move(ignored);

    if (!argHelp.isNull() && argHelp.isMember("requestParams")) {
        for (const auto & p: argHelp["requestParams"]) {
            Utf8String s = p["name"].asStringUtf8();
            acceptedParams.insert(std::move(s));
        }
    }

    auto result = [=] (RestConnection & connection,
                       const RestRequest & request,
                       const RestRequestParsingContext & context)
        {
            vector<Json::Value> unknowns;
            
            for (auto & s: request.params) {
                if (!acceptedParams.count(s.first)) {
                    Json::Value detail;
                    detail["paramName"] = s.first;
                    detail["paramValue"] = s.second;

                    unknowns.emplace_back(std::move(detail));
                }
            }

            if (unknowns.empty()) {
                return true;  // pass the request
            }
            
            Json::Value exc;
            exc["error"] = "Unknown parameter(s) in REST call";
            exc["httpCode"] = 400;
            Json::Value & details = exc["details"];
            details["help"] = argHelp;
            details["verb"] = request.verb;
            details["resource"] = request.resource;
            for (auto & detail: unknowns) {
                details["unknownParameters"].append(std::move(detail));
            }

            connection.sendErrorResponse(400, exc);
            return false;
        };

    return result;
}

std::set<Utf8String> getIgnoredArgs(const RequestFilter & filter)
{
    return filter.getIgnoredQueryParameters();
}



} // namespace MLDB
