// This file is part of MLDB. Copyright 2015 mldb.ai inc. All rights reserved.

/* rest_request_router.cc
   Jeremy Barnes, 15 November 2012
   Copyright (c) 2012 mldb.ai inc.  All rights reserved.

*/

#include "mldb/types/url.h"
#include "mldb/rest/rest_request_router.h"
#include "mldb/jml/utils/vector_utils.h"
#include "mldb/arch/exception_handler.h"
#include "mldb/jml/utils/set_utils.h"
#include "mldb/jml/utils/environment.h"
#include "mldb/jml/utils/file_functions.h"
#include "mldb/jml/utils/string_functions.h"
#include "mldb/jml/utils/less.h"
#include "mldb/types/value_description.h"


using namespace std;


namespace MLDB {


/*****************************************************************************/
/* PATH SPEC                                                                 */
/*****************************************************************************/

PathSpec::
PathSpec()
    : type(NONE)
{
}
        
PathSpec::
PathSpec(const std::string & fullPath)
    : type(STRING), path(fullPath)
{
}

PathSpec::
PathSpec(const Utf8String & fullPath)
    : type(STRING), path(fullPath)
{
}

PathSpec::
PathSpec(const char * fullPath)
    : type(STRING), path(fullPath)
{
}

PathSpec::
PathSpec(Regex rex)
    : type(REGEX),
      path(rex.surface()),
      rex(std::move(rex))
{
}

void
PathSpec::
getHelp(Json::Value & result) const
{
    switch (type) {
    case STRING:
        result["path"] = path;
        break;
    case REGEX: {
        Json::Value & v = result["path"];
        v["regex"] = path;
        v["desc"] = desc;
        break;
    }
    default:
        throw HttpReturnException(400, "unknown path parameter");
    }
}
    
Utf8String
PathSpec::
getPathDesc() const
{
    if (!desc.empty())
        return desc;
    return path;
}

int
PathSpec::
numCapturedElements() const
{
    switch (type) {
    case NONE: return 0;
    case STRING: return 1;
    case REGEX: return rex.mark_count() + 1;
    default:
        throw HttpReturnException(400, "unknown mark count");
    }
}

bool
PathSpec::
operator == (const PathSpec & other) const
{
    return path == other.path;
}

bool
PathSpec::
operator != (const PathSpec & other) const
{
    return ! operator == (other);
}

bool
PathSpec::
operator < (const PathSpec & other) const
{
    return path < other.path;
}

PathSpec
Rx(const Utf8String & regexString, const Utf8String & desc)
{
    Regex rex(regexString);
    PathSpec result(rex);
    result.desc = desc;
    return result;
}

std::ostream & operator << (std::ostream & stream, const PathSpec & path)
{
    switch (path.type) {
    case PathSpec::STRING: return stream << path.path;
    case PathSpec::REGEX: return stream << path.rex.surface();
    case PathSpec::NONE:
    default:
        return stream << "none";
    }
}
               
bool
RequestParamFilter::
operator < (const RequestParamFilter & other) const
{
    return  ML::less_all(location, other.location,
                 param, other.param,
                 value, other.value);
}


/*****************************************************************************/
/* REQUEST FILTER                                                            */
/*****************************************************************************/

/** Filter for a REST request by method, etc. */

RequestFilter::
RequestFilter()
{
}

RequestFilter::
RequestFilter(const std::string & verb)
{
    verbs.insert(verb);
    parseVerbs();
}

RequestFilter::
RequestFilter(const char * verb)
{
    verbs.insert(verb);
    parseVerbs();
}

RequestFilter::
RequestFilter(std::set<std::string> verbs)
    : verbs(std::move(verbs))
{
    parseVerbs();
}

RequestFilter::
RequestFilter(const std::initializer_list<std::string> & verbs)
    : verbs(verbs)
{
    parseVerbs();
}

void
RequestFilter::
parseVerbs()
{
    std::set<std::string> newVerbs;
        
    for (auto & v: verbs) {
        auto i = v.find('=');

        if (i == std::string::npos) {
            newVerbs.insert(v);
            continue;
        }
            
        std::string key(v, 0, i);
        std::string value(v, i + 1);

        RequestParamFilter::Location location
            = RequestParamFilter::QUERY;

        if (key.find("header:") == 0) {
            location = RequestParamFilter::HEADER;
            key = string(key, 7);  // strip off header:
        }
        
        filters.emplace_back(location, key, value);
    }

    verbs = newVerbs;
}

void
RequestFilter::
getHelp(Json::Value & result) const
{
    if (!verbs.empty()) {
        int i = 0;
        for (auto it = verbs.begin(), end = verbs.end(); it != end;  ++it, ++i) {
            result["verbs"][i] = *it;
        }
    }
    if (!filters.empty()) {
        for (auto & f: filters) {
            string loc = (f.location == RequestParamFilter::HEADER ? "header:" : "");
            result["filters"].append(loc + f.param + "=" + f.value);
        }
    }
}

std::set<Utf8String>
RequestFilter::
getIgnoredQueryParameters() const
{
    std::set<Utf8String> result;
    for (auto & f: filters) {
        if (f.location == RequestParamFilter::QUERY)
            result.insert(f.param);
    }
    return result;
}

std::ostream & operator << (std::ostream & stream, const RequestParamFilter & filter)
{
    return stream << (filter.location == RequestParamFilter::QUERY ? "query:" : "header:") 
                  << filter.param << "=" << filter.value;
}

std::ostream & operator << (std::ostream & stream, const RequestFilter & filter)
{
    return stream << "verbs " << filter.verbs <<  " filters " << filter.filters; 
}


/*****************************************************************************/
/* REST REQUEST PARSING CONTEXT                                              */
/*****************************************************************************/

std::ostream & operator << (std::ostream & stream,
                            const RestRequestParsingContext & context)
{
    return stream << context.resources << " " << context.remaining;
}


/*****************************************************************************/
/* REST REQUEST ROUTER                                                       */
/*****************************************************************************/

RestRequestRouter::
RestRequestRouter()
    : terminal(false)
{
    notFoundHandler = defaultNotFoundHandler;
}

RestRequestRouter::
RestRequestRouter(const OnProcessRequest & processRequest,
                  const OnNotFoundRequest & notFoundHandler,
                  const Utf8String & description,
                  bool terminal,
                  const Json::Value & argHelp)
    : rootHandler(processRequest),
      notFoundHandler(notFoundHandler),
      description(description),
      terminal(terminal),
      argHelp(argHelp)
{
}

RestRequestRouter::
~RestRequestRouter()
{
}
    
RestRequestRouter::OnHandleRequest
RestRequestRouter::
requestHandler() const
{
    return std::bind(&RestRequestRouter::handleRequest,
                     this,
                     std::placeholders::_1,
                     std::placeholders::_2);
}

void
RestRequestRouter::
handleRequest(RestConnection & connection,
              const RestRequest & request) const
{
    //MLDB_TRACE_EXCEPTIONS(false);

    RestRequestParsingContext context(request);
    RestRequestMatchResult res = processRequest(connection, request, context);
    if (res == MR_NO) {
        notFoundHandler(connection, request);
    }
}

static std::string getVerbsStr(const std::set<std::string> & verbs)
{
    string verbsStr;
    for (auto v: verbs) {
        if (!verbsStr.empty())
            verbsStr += ",";
        verbsStr += v;
    }
            
    return verbsStr;
}

namespace {

EnvOption<bool, true> TRACE_REST_REQUESTS("TRACE_REST_REQUESTS", false);

} // file scope

RestRequestMatchResult
RestRequestRouter::
processRequest(RestConnection & connection,
               const RestRequest & request,
               RestRequestParsingContext & context) const
{
    bool debug = TRACE_REST_REQUESTS;

    if (debug) {
        cerr << "processing request " << request
             << " with context " << context
             << " against route " << description 
             << " with " << subRoutes.size() << " subroutes" << endl;
    }

    if (request.verb == "OPTIONS") {
        Json::Value help;
        std::set<std::string> verbs;

        this->options(verbs, help, request, context);

        RestParams headers = { { "Allow", getVerbsStr(verbs) } };
        
        if (verbs.empty())
            connection.sendHttpResponse(400, "", "", headers);
        else
            connection.sendHttpResponse(200, help.toStyledString(),
                                        "application/json",
                                        headers);
        return MR_YES;
    }

    if (rootHandler && (!terminal || context.remaining.empty())) {
        if (debug) {
            cerr << "invoked root handler for request " << request << endl;
        }
        return rootHandler(connection, request, context);
    }

    for (auto & sr: subRoutes) {
        if (debug)
            cerr << "  trying subroute " << sr.router->description << endl;
        try {
            RestRequestMatchResult mr = sr.process(request, context, connection);
            //cerr << "returned " << mr << endl;
            if (mr == MR_YES || mr == MR_ASYNC || mr == MR_ERROR) {
                if (debug) {
                    cerr << "invoked subroute "
                         << " for request " << request << endl;
                } 
                return mr;
            }
        } catch (const std::exception & exc) {
            return sendExceptionResponse(connection, exc);
        } catch (...) {
            connection.sendErrorResponse(500, "unknown exception");
            return MR_YES;
        }
    }

    return MR_NO;
    //connection.sendErrorResponse(404, "invalid route for "
    //                             + request.resource);
}

void
RestRequestRouter::
options(std::set<std::string> & verbsAccepted,
        Json::Value & help,
        const RestRequest & request,
        RestRequestParsingContext & context) const
{
    for (auto & sr: subRoutes) {
        sr.options(verbsAccepted, help, request, context);
    }
}

bool
RestRequestRouter::Route::
matchPath(RestRequestParsingContext & context) const
{
    switch (path.type) {
    case PathSpec::STRING: {
        if (context.remaining.removePrefix(path.path)) {
            context.resources.push_back(path.path);
            break;
        }
        else return false;
    }
    case PathSpec::REGEX: {
        MatchResults results;
        bool found
            = regex_search(context.remaining, results, path.rex,
                           std::regex_constants::match_continuous)
            && !results.prefix().matched;  // matches from the start

        if (!found)
            return false;
        for (unsigned i = 0;  i < results.size();  ++i) {
            // decode URI prior to pushing it to context.resources
            Utf8String in(results[i].first, results[i].second);
            context.resources.push_back(Url::decodeUri(in));
        }
        context.remaining.replace(0, results[0].length(), "");
        break;
    }
    case PathSpec::NONE:
    default:
        throw HttpReturnException(400, "unknown rest request type");
    }

    return true;
}

RestRequestMatchResult
RestRequestRouter::Route::
process(const RestRequest & request,
        RestRequestParsingContext & context,
        RestConnection & connection) const
{
    using namespace std;

    bool debug = TRACE_REST_REQUESTS;

    if (debug) {
        cerr << "verb = " << request.verb << " filter.verbs = " << filter.verbs
             << endl;
    }
    if (!filter.verbs.empty()
        && !filter.verbs.count(request.verb))
        return MR_NO;

    // Check that the parameter filters match
    for (auto & f: filter.filters) {
        bool matched = false;

        if (f.location == RequestParamFilter::QUERY) {
            for (auto & p: request.params) {
                if (p.first == f.param && (f.value == "*" || p.second == f.value)) {
                    matched = true;
                    break;
                }
            }
        }
        else if (f.location == RequestParamFilter::HEADER) {
            if (debug) {
                cerr << "matching header " << f.param << " with value "
                     << request.header.tryGetHeader(f.param)
                     << " against " << f.value << endl;
            }
            if (request.header.tryGetHeader(f.param) == f.value) {
                matched = true;
            }
        }

        if (!matched)
            return MR_NO;
    }

    // At the end, make sure we put the context back to how it was
    RestRequestParsingContext::StateGuard guard(&context);

    if (!matchPath(context))
        return MR_NO;

    if (extractObject)
        extractObject(connection, request, context);

    if (connection.responseSent())
        return MR_YES;

    return router->processRequest(connection, request, context);
}

void
RestRequestRouter::Route::
options(std::set<std::string> & verbsAccepted,
        Json::Value & help,
        const RestRequest & request,
        RestRequestParsingContext & context) const
{
    RestRequestParsingContext::StateGuard guard(&context);

    if (!matchPath(context))
        return;

    if (context.remaining.empty()) {
        verbsAccepted.insert(filter.verbs.begin(), filter.verbs.end());

        string path = "";//this->path.getPathDesc();
        Json::Value & sri = help[path + getVerbsStr(filter.verbs)];
        this->path.getHelp(sri);
        filter.getHelp(sri);
        router->getHelp(help, path, filter.verbs);
    }
    router->options(verbsAccepted, help, request, context);
}

void
RestRequestRouter::
addRoute(PathSpec path, RequestFilter filter,
         const std::shared_ptr<RestRequestRouter> & handler,
         ExtractObject extractObject)
{
    if (rootHandler)
        throw HttpReturnException(500, "can't add a sub-route to a terminal route");

    Route route;
    route.path = path;
    route.filter = filter;
    route.router = handler;
    route.extractObject = extractObject;

    // see explanation in header as why this is necessary
    std::vector<RequestParamFilter> sortedNewFilters(route.filter.filters);
    std::sort(sortedNewFilters.begin(), sortedNewFilters.end());

    auto hideRoute = [&](const Route& existingRoute) {
        std::vector<RequestParamFilter> sortedExistingFilters(existingRoute.filter.filters);
        std::sort(sortedExistingFilters.begin(), sortedExistingFilters.end());

        // check if verbs and filters are already part of an existing route
        if ( !std::includes(existingRoute.filter.verbs.begin(), 
                            existingRoute.filter.verbs.end(),
                            route.filter.verbs.begin(), 
                            route.filter.verbs.end()) ||
             !std::includes(sortedNewFilters.begin(), 
                            sortedNewFilters.end(),
                            sortedExistingFilters.begin(), 
                            sortedExistingFilters.end()) )
            return false;

        // check if the path match one of an existing route
        switch (route.path.type) {
        case PathSpec::STRING:
            switch (existingRoute.path.type) {
            case PathSpec::STRING:
                return route.path.path == existingRoute.path.path;
            case PathSpec::REGEX: {
                MatchResults results;
                //cerr << "path " << route.path.path.rawString() << " regex " << existingRoute.path.rex.surface().rawString() << endl;
                if (route.path.path.empty())
                    return existingRoute.path.rex.surface().empty();

                return regex_match(route.path.path, results, existingRoute.path.rex);
            }
            case PathSpec::NONE:
            default:
                throw HttpReturnException(400, "unknown rest request type");
            }
        case PathSpec::REGEX:
            switch (existingRoute.path.type) {
            case PathSpec::STRING: {
                //cerr << "regex " << route.path.rex.surface() << " path " << existingRoute.path.path << endl;
                return false; // we assume here that a regex path cannot be hidden by a string path
            }
            case PathSpec::REGEX:
                return route.path.rex.surface() == existingRoute.path.rex.surface();
            case PathSpec::NONE:
            default:
                throw HttpReturnException(400, "unknown rest request type");
            }
        case PathSpec::NONE:
        default:
            throw HttpReturnException(400, "unknown rest request type");
        }
    };

    /* do not allow route to be hidden by a previously registered one */
    auto hidingRoute = std::find_if(subRoutes.begin(), subRoutes.end(), hideRoute);
    if (hidingRoute != subRoutes.end()) {
        std::ostringstream message;
        message <<  "route [" << hidingRoute->path << " " << hidingRoute->filter << "]"
                << " is hiding newly added route [" << route.path << " " << route.filter << "]";

        throw HttpReturnException(500, message.str());
    }
    subRoutes.emplace_back(std::move(route));
}

void
RestRequestRouter::
addRoute(PathSpec path, RequestFilter filter,
         const Utf8String & description,
         const OnProcessRequest & cb,
         const Json::Value & argHelp,
         ExtractObject extractObject)
{
    addRoute(path, filter,
             std::make_shared<RestRequestRouter>(cb, notFoundHandler, description, true, argHelp),
             extractObject);
}

void
RestRequestRouter::
addHelpRoute(PathSpec path, RequestFilter filter)
{
    OnProcessRequest helpRoute
        = [=] (RestConnection & connection,
               const RestRequest & request,
               const RestRequestParsingContext & context)
        {
            Json::Value help;
            if (request.params.hasValue("autodoc")) {
                getAutodocHelp(help, "", set<string>());
            } else {
                getHelp(help, "", set<string>());
            }
            connection.sendResponse(200, help);
            return MR_YES;
        };

    addRoute(path, filter, "Get help on the available API commands",
             helpRoute, Json::Value());
}


void
RestRequestRouter::
addAutodocRoute(PathSpec autodocPath, PathSpec helpPath,
                const string & autodocFilesPath)
{
    Utf8String autodocPathStr = autodocPath.getPathDesc();
    OnProcessRequest rootRoute
        = [=] (RestConnection & connection,
               const RestRequest & request,
               const RestRequestParsingContext & context) {
        connection.sendRedirect(302, (autodocPathStr + "/index.html").rawString());
        return RestRequestRouter::MR_YES;
    };

    addRoute(autodocPathStr, "GET", "Main autodoc page",
             rootRoute, Json::Value());
    addRoute(autodocPathStr + "/", "GET", "Main autodoc page",
             rootRoute, Json::Value());

    OnProcessRequest autodocRoute
        = [=] (RestConnection & connection,
               const RestRequest & request,
               const RestRequestParsingContext & context) {

        Utf8String path = context.resources.back();

        if (path.find("..") != path.end()) {
            throw HttpReturnException(400, "not dealing with path with .. in it");
        }

        if (!path.removePrefix(autodocPathStr)) {
            throw HttpReturnException(400, "not serving file not under " + autodocPathStr);
        }

        Utf8String filename = path;
        // Remove any leading / characters
        while (filename.removePrefix("/"));

        if (filename == "autodoc") {
            connection.sendRedirect(302, (helpPath.getPathDesc() + "?autodoc").rawString());
            return RestRequestRouter::MR_YES;
        }

        Utf8String filenameToLoad = autodocFilesPath + "/" + filename;
        filenameToLoad.removePrefix("file://");

        ML::File_Read_Buffer buf(filenameToLoad.rawString());

        string mimeType = "text/plain";
        if (filename.endsWith(".html")) {
            mimeType = "text/html";
        }
        else if (filename.endsWith(".js")) {
            mimeType = "application/javascript";
        }
        else if (filename.endsWith(".css")) {
            mimeType = "text/css";
        }

        string result(buf.start(), buf.end());
        connection.sendResponse(200, result,  mimeType);
        return RestRequestRouter::MR_YES;
    };

    addRoute(Rx(autodocPathStr + "/.*", "<resource>"), "GET",
             "Static content", autodocRoute, Json::Value());
}

void
RestRequestRouter::
getHelp(Json::Value & result, const Utf8String & currentPath,
        const std::set<std::string> & verbs) const
{
    Json::Value & v = result[(currentPath.empty() ? "" : currentPath + " ")
                             + getVerbsStr(verbs)];

    v["description"] = description;
    if (!argHelp.isNull())
        v["arguments"] = argHelp;
    
    for (unsigned i = 0;  i < subRoutes.size();  ++i) {
        Utf8String path = currentPath + subRoutes[i].path.getPathDesc();
        Json::Value & sri = result[(path.empty() ? "" : path + " ")
                                   + getVerbsStr(subRoutes[i].filter.verbs)];
        subRoutes[i].path.getHelp(sri);
        subRoutes[i].filter.getHelp(sri);
        subRoutes[i].router->getHelp(result, path, subRoutes[i].filter.verbs);
    }
}

void
RestRequestRouter::
updateFromValueDescription(Json::Value & v, const ValueDescription * vd) const {
    const ValueKind kind = vd->kind;
    if (kind == ValueKind::INTEGER) {
        v["type"] = "integer";
    }
    else if (kind == ValueKind::BOOLEAN) {
        v["type"] = "boolean";
    }
    else if (kind == ValueKind::STRING) {
        v["type"] = "string";
    }
    else if (kind == ValueKind::ENUM) {
        v["description"].asString() + " (cppType: " + vd->typeName + ")";
        v["type"] = "string";
        vector<string> keys = vd->getEnumKeys();
        stringstream pattern;
        bool first = true;
        for (const string & k: keys) {
            if (!first) {
                pattern << "|";
            }
            pattern << k;
        };
        v["pattern"] = pattern.str();
    }
    else if (kind == ValueKind::LINK) {
        cerr << "Got link field as final value: " << vd->typeName << endl;
        v["description"].asString() + " (cppType: " + vd->typeName + ")";
        const ValueDescription * subVdPtr = &(vd->contained());
        cerr << subVdPtr->typeName << endl;
        v["type"] = "string";
    }
    else if (kind == ValueKind::FLOAT) {
        v["type"] = "float";
    }
    else if (kind == ValueKind::ARRAY) {
        v["type"] = "array";
        const ValueDescription * subVdPtr = &(vd->contained());
        updateFromValueDescription(v["items"], subVdPtr);
    }
    else if (kind == ValueKind::STRUCTURE) {
        v["description"].asString() + " (cppType: " + vd->typeName + ")";
        v["type"] = "object";
    }
    else if (kind == ValueKind::ATOM) {
        v["description"] =
            v["description"].asString() + " (cppType: " + vd->typeName + ")";
        if (vd->typeName == "MLDB::TimePeriod") {
            v["type"] = "string";
            v["pattern"] = "^[\\d]+(s|m|h|d)$";
        }
        else if (vd->typeName == "MLDB::Any") {
            v["type"] = "object";
        }
        else {
            v["type"] = "string";
        }
    }
    else if (kind == ValueKind::ANY) {
        //cppType == Json::Value
        v["type"] = "object";
    }
    else {
        cerr << "uncovered conversion case for kind: " << kind
             << " typeName: " << vd->typeName << endl;
        v["type"] = "object (cppType: " + vd->typeName + ")";
    }
}


void
RestRequestRouter::
addValueDescriptionToProperties(const ValueDescription * vd,
                                Json::Value & properties, int recur) const
{
    using namespace Json;
    if (recur > 2) {
        //Too many recursions
        return;
    }

    auto onField = [&] (const ValueDescription::FieldDescription & fd) {
        Value tmpObj;
        tmpObj["description"] = fd.comment;
        const ValueDescription * curr = fd.description.get();
        if (curr->kind == ValueKind::LINK) {
            curr = &(curr->contained());
            if (curr->kind == ValueKind::LINK) {
                cerr << "link of link not supported" << endl;
            }
        }
        updateFromValueDescription(tmpObj, curr);
        if (curr->kind == ValueKind::ARRAY) {
            const ValueDescription * subVdPtr = &(curr->contained());
            if (subVdPtr->kind == ValueKind::STRUCTURE) {
                if (vd == subVdPtr) {
                    tmpObj["items"]["type"] =
                        "object (recursive, cppType: " + curr->typeName + ")";
                    tmpObj["items"]["properties"] = objectValue;
                }
                else {
                    Value itemProperties;
                    addValueDescriptionToProperties(subVdPtr, itemProperties,
                                                    recur + 1);
                    tmpObj["items"]["items"]["properties"] = itemProperties;
                }
            }
            else {
                if (subVdPtr->kind == ValueKind::ARRAY) {
                    // unsupported "pair" type
                    tmpObj["items"]["type"] =
                        "object (cppType: " + curr->typeName + ")";
                }
                else {
                    updateFromValueDescription(tmpObj["items"], subVdPtr);
                }
            }
        }
        else if (curr->kind == ValueKind::STRUCTURE) {
            Value itemProperties;
            addValueDescriptionToProperties(curr,
                                            itemProperties, recur + 1);
            tmpObj["items"]["properties"] = itemProperties;
        }
        properties[fd.fieldName] = tmpObj;
    };
    vd->forEachField(nullptr, onField);
}


void
RestRequestRouter::
addJsonParamsToProperties(const Json::Value & params,
                          Json::Value & properties) const
{
    using namespace Json;
    for (Value param: params) {
        string cppType = param["cppType"].asString();
        const ValueDescription * vd = ValueDescription::get(cppType).get();
        if (vd->kind == ValueKind::STRUCTURE) {
            addValueDescriptionToProperties(vd, properties);
        }
        else {
            Value tmpObj;
            updateFromValueDescription(tmpObj, vd);
            tmpObj["description"] = param["description"].asString();
            properties[param["name"].asString()] = tmpObj;
        }
    }
}

void
RestRequestRouter::
getAutodocHelp(Json::Value & result, const Utf8String & currentPath,
               const std::set<std::string> & verbs) const
{
    using namespace Json;
    Value tmpResult;
    getHelp(tmpResult, "", set<string>());
    result["routes"]   = arrayValue;
    result["literate"] = arrayValue;
    result["config"]   = objectValue;
    for (ValueIterator it = tmpResult.begin() ; it != tmpResult.end() ; it++) {
        string key = it.key().asString();
        vector<string> parts = ML::split(it.key().asString());
        int size = parts.size();
        if (size == 0) {
            // the empty key contains the description
            continue;
        }
        if (size == 1) {
            // useless route
            continue;
        }
        ExcAssert(size == 2);
        if (parts[1] != "GET" && parts[1] != "POST" && parts[1] != "PUT"
                && parts[1] != "DELETE") {
            //unsupported verb + param
            continue;
        }

        Value curr = arrayValue;
        curr.append(parts[1] + " " + parts[0]);
        Value subObj;
        subObj["out"] = objectValue;
        subObj["out"]["required"] = arrayValue;
        subObj["out"]["type"] = "object";
        subObj["out"]["properties"] = objectValue;
        subObj["required_role"] = nullValue;
        subObj["docstring"] = (*it)["description"].asString();
        subObj["in"] = nullValue;
        subObj["in"]["required"] = arrayValue;
        subObj["in"]["type"] = "object";
        subObj["in"]["properties"] = objectValue;
        if ((*it).isMember("arguments") && (*it)["arguments"].isMember("jsonParams")) {
            addJsonParamsToProperties((*it)["arguments"]["jsonParams"],
                                      subObj["in"]["properties"]);
        }
        curr.append(subObj);
        result["routes"].append(curr);
    }
}

RestRequestRouter &
RestRequestRouter::
addSubRouter(PathSpec path,
             const Utf8String & description,
             ExtractObject extractObject,
             std::shared_ptr<RestRequestRouter> subRouter)
{
    // TODO: check it doesn't exist
    Route route;
    route.path = path;
    if (subRouter)
        route.router = subRouter;
    else route.router.reset(new RestRequestRouter());

    route.router->description = description;
    route.router->notFoundHandler = notFoundHandler;
    route.extractObject = extractObject;

    subRoutes.push_back(route);
    return *route.router;
}

void
RestRequestRouter::
defaultNotFoundHandler(RestConnection & connection,
                       const RestRequest & request) {
    connection.sendErrorResponse(404, "unknown resource " + request.verb + " " +  request.resource);
};

RestRequestMatchResult
sendExceptionResponse(RestConnection & connection,
                      const std::exception & exc)
{
    int defaultCode = 400;
    Json::Value val = extractException(exc, defaultCode);

    int code = defaultCode;
    if (val.isMember("httpCode")) {
        code = val["httpCode"].asInt();
    }

    connection.sendResponse(code, val);
    return RestRequestRouter::MR_ERROR;
}

Json::Value extractException(const std::exception & exc, int defaultCode)
{
    const HttpReturnException * http
        = dynamic_cast<const HttpReturnException *>(&exc);
    const std::bad_alloc * balloc
        = dynamic_cast<const std::bad_alloc *>(&exc);

    Json::Value val;
    val["error"] = exc.what();

    if (http) {
        val["httpCode"] = http->httpCode;
        if (!http->details.empty())
            val["details"] = jsonEncode(http->details);
    } else if (balloc) {
        val["error"] = "Out of memory.  A memory allocation failed when performing "
            "the operation.  Consider retrying with a smaller amount of data "
            "or running on a machine with more memory.  "
            "(std::bad_alloc)";
    }
    else {
        val["httpCode"] = defaultCode;
    }
    
    return val;
}



} // namespace MLDB
