/** rest_request_binding.h                                         -*- C++ -*-
    Jeremy Barnes, 15 November 2012
    Copyright (c) 2012 mldb.ai inc.  All rights reserved.
    This file is part of MLDB. Copyright 2015 mldb.ai inc. All rights reserved.

    Functionality to bind arbitrary functions into REST requests in a
    declarative manner.

    Warning: full of funky variadic template voodoo.
*/

#pragma once

#include "mldb/rest/rest_request_router.h"
#include "mldb/arch/demangle.h"
#include <boost/lexical_cast.hpp>
#include "json_codec.h"
#include "mldb/types/value_description.h"
#include "mldb/types/json_printing.h"
#include "mldb/rest/rest_request_params.h"
#include "mldb/rest/rest_request_params_types.h"
#include "mldb/jml/utils/positioned_types.h"

namespace MLDB {


/*****************************************************************************/
/* PARTIAL BIND                                                              */
/*****************************************************************************/

/** These functions bind the first 0 or 1 arguments of a callable and leave
    the rest unbound.

    Useful for when you don't know the number of arguments, so you can't
    write std::placeholder::_1, etc.
*/

template<typename T>
T partial_bind(T fn)
{
    return fn;
}

template<typename R, typename... Args, typename Obj, typename Ptr>
std::function<R (Args...)>
partial_bind(R (Obj::* pmf) (Args...) const,
             Ptr ptr)
{
    return [=] (Args&&... args) -> R
        {
            const Obj & obj = *ptr;
            return ((obj).*(pmf))(std::forward<Args>(args)...);
        };
}

template<typename R, typename... Args, typename Obj, typename Ptr>
std::function<R (Args...)>
partial_bind(R (Obj::* pmf) (Args...),
             Ptr ptr)
{
    return [=] (Args&&... args) -> R
        {
            Obj & obj = *ptr;
            return ((obj).*(pmf))(std::forward<Args>(args)...);
        };
}

template<typename R, typename Arg1, typename... Args, typename Bind1>
std::function<R (Args...)>
partial_bind(const std::function<R (Arg1, Args...)> & fn, Bind1 && bind1)
{
    return [=] (Args&&... args) -> R
        {
            return fn(bind1, std::forward<Args>(args)...);
        };
}


/*****************************************************************************/
/* PARTIAL BIND CONTEXT                                                      */
/*****************************************************************************/

/** Same as partial bind, but these return a function that also takes a
    RestRequestParsingContext, from which the Ptr can be extracted.
*/

template<typename Obj, typename Obj2>
Obj & derefPtrContext(Obj2 * ptr,
                      const RestRequestParsingContext & context)
{
    return static_cast<Obj &>(*ptr);
}

template<typename Obj, typename Obj2>
Obj & derefPtrContext(const std::shared_ptr<Obj2> & ptr,
                      const RestRequestParsingContext & context)
{
    return static_cast<Obj &>(*ptr);
}

template<typename Obj, typename Obj2>
Obj & derefPtrContext(const std::function<Obj2 * (const RestRequestParsingContext &)> & fn,
                      const RestRequestParsingContext & context)
{
    return static_cast<Obj &>(*fn(context));
}

template<typename Obj, typename Value, int Index>
Obj & derefPtrContext(const ObjectExtractor<Value, Index> & ptr,
                      const RestRequestParsingContext & context)
{
    return static_cast<Obj &>(context.getObjectAs<Obj>(Index));
}

template<typename R, typename... Args, typename Obj, typename Ptr>
std::function<R (const RestRequestParsingContext &, Args...)>
partial_bind_context(R (Obj::* pmf) (Args...),
                     Ptr && ptr)
{
    return [=] (const RestRequestParsingContext & cxt, Args&&... args) -> R
        {
            Obj & obj = derefPtrContext<Obj>(ptr, cxt);
            return ((obj).*(pmf))(std::forward<Args>(args)...);
        };
}

template<typename R, typename... Args, typename Obj, typename Ptr>
std::function<R (const RestRequestParsingContext &, Args...)>
partial_bind_context(R (Obj::* pmf) (Args...) const,
                     Ptr && ptr)
{
    return [=] (const RestRequestParsingContext & cxt, Args&&... args) -> R
        {
            const Obj & obj = derefPtrContext<const Obj>(ptr, cxt);
            return ((obj).*(pmf))(std::forward<Args>(args)...);
        };
}

#if 0
template<typename R, typename... Args, typename Obj, typename Obj2>
std::function<R (const RestRequestParsingContext &, Args...)>
partial_bind_context(R (Obj::* pmf) (Args...) const,
                     Obj2 * ptr)
{
    return [=] (const RestRequestParsingContext & cxt, Args&&... args) -> R
        {
            const Obj & obj = derefPtrContext<const Obj>(ptr, cxt);
            return ((obj).*(pmf))(std::forward<Args>(args)...);
        };
}

template<typename R, typename... Args, typename Obj, typename Obj2>
std::function<R (const RestRequestParsingContext &, Args...)>
partial_bind_context(R (Obj::* pmf) (Args...),
                     Obj2 * ptr)
{
    return [=] (const RestRequestParsingContext & cxt, Args&&... args) -> R
        {
            Obj & obj = derefPtrContext<Obj>(ptr, cxt);
            return ((obj).*(pmf))(std::forward<Args>(args)...);
        };
}

template<typename R, typename... Args, typename Obj, typename Obj2>
std::function<R (const RestRequestParsingContext & cxt, Args...)>
partial_bind_context(R (Obj::* pmf) (Args...) const,
                     const std::shared_ptr<Obj2> & ptr)
{
    return [=] (const RestRequestParsingContext & cxt, Args&&... args) -> R
        {
            const Obj & obj = derefPtrContext<const Obj>(ptr, cxt);
            return ((obj).*(pmf))(std::forward<Args>(args)...);
        };
}

template<typename R, typename... Args, typename Obj, typename Obj2>
std::function<R (const RestRequestParsingContext &, Args...)>
partial_bind_context(R (Obj::* pmf) (Args...),
                     const std::shared_ptr<Obj2> & ptr)
{
    return [=] (const RestRequestParsingContext & cxt, Args&&... args) -> R
        {
            Obj & obj = derefPtrContext<Obj>(ptr, cxt);
            return ((obj).*(pmf))(std::forward<Args>(args)...);
        };
}

template<typename R, typename... Args, typename Obj, typename Obj2>
std::function<R (const RestRequestParsingContext &, Args...)>
partial_bind_context(R (Obj::* pmf) (Args...),
                     const std::function<Obj2 * (const RestRequestParsingContext & context)> & fn)
{
    return [=] (const RestRequestParsingContext & cxt, Args&&... args) -> R
        {
            Obj & obj = derefPtrContext<Obj>(fn, cxt);
            return ((obj).*(pmf))(std::forward<Args>(args)...);
        };
}

template<typename R, typename... Args, typename Obj, typename Obj2>
std::function<R (const RestRequestParsingContext &, Args...)>
partial_bind_context(R (Obj::* pmf) (Args...) const,
                     const std::function<const Obj2 * (const RestRequestParsingContext & context)> & fn)
{
    return [=] (const RestRequestParsingContext & cxt, Args&&... args) -> R
        {
            const Obj & obj = derefPtrContext<const Obj>(fn, cxt);
            return ((obj).*(pmf))(std::forward<Args>(args)...);
        };
}

template<typename R, typename Arg1, typename... Args, typename Bind1>
std::function<R (const RestRequestParsingContext &, Args...)>
partial_bind_context(const std::function<R (Arg1, Args...)> & fn, Bind1 && bind1)
{
    return [=] (const RestRequestParsingContext &, Args&&... args) -> R
        {
            return fn(bind1, std::forward<Args>(args)...);
        };
}

template<typename R, typename... Args, typename Obj, typename Value, int Index>
std::function<R (const RestRequestParsingContext &, Args...)>
partial_bind_context(R (Obj::* pmf) (Args...) const,
                     const ObjectExtractor<Value, Index> & ptr)
{
    return [=] (const RestRequestParsingContext & context, Args&&... args) -> R
        {
            const Obj & obj = context.getObjectAs<Obj>(index);
            return ((obj).*(pmf))(std::forward<Args>(args)...);
        };
}

template<typename R, typename... Args, typename Obj, typename Value, int Index>
std::function<R (const RestRequestParsingContext &, Args...)>
partial_bind_context(R (Obj::* pmf) (Args...),
                     const ObjectExtractor<Value, Index> & ptr)
{
    return [=] (const RestRequestParsingContext & context, Args&&... args) -> R
        {
            Obj & obj = context.getObjectAs<Obj>(index);
            return ((obj).*(pmf))(std::forward<Args>(args)...);
        };
}
#endif


/*****************************************************************************/
/* CREATE PARAMETER EXTRACTOR                                                */
/*****************************************************************************/

/** These functions turn an argument to the request binding into a function
    that can generate the value required by the handler function.

*/

#if 0
/** By default, to create a parameter extractor we simply take a copy of the
    argument and return that.

    This is used to pass through direct parameter values.
*/
template<typename T>
std::function<T (RestConnection & connection,
                 const RestRequest & request,
                 const RestRequestParsingContext & context)>
createParameterExtractor(Json::Value & argHelp, const T & p, void * = 0)
{
    return [=] (RestConnection & connection,
                const RestRequest & request,
                const RestRequestParsingContext & context)
        {
            return p;
        };
}
#endif

std::function<std::string
              (RestConnection & connection,
               const RestRequest & request,
               const RestRequestParsingContext & context)>
createParameterExtractor(Json::Value & argHelp,
                         const StringPayload & p, void * = 0);

/** Pass the connection on */
std::function<RestConnection &
              (RestConnection & connection,
               const RestRequest & request,
               const RestRequestParsingContext & context)>
createParameterExtractor(Json::Value & argHelp,
                         const PassConnectionId &, void * = 0);

/** Pass the connection on */
std::function<const RestRequestParsingContext &
              (RestConnection & connection,
               const RestRequest & request,
               const RestRequestParsingContext & context)>
createParameterExtractor(Json::Value & argHelp,
                         const PassParsingContext &, void * = 0);

/** Pass the connection on */
std::function<const RestRequest &
              (RestConnection & connection,
               const RestRequest & request,
               const RestRequestParsingContext & context)>
createParameterExtractor(Json::Value & argHelp,
                         const PassRequest &, void * = 0);

/** Free function to be called in order to generate a parameter extractor
    for the given parameter.  See the CreateRestParameterGenerator class for more
    details.
*/
template<typename T, typename Codec>
static std::function<decltype(((Codec *)0)->decode(std::declval<std::string>()))
                     (RestConnection & connection,
                      const RestRequest & request,
                      const RestRequestParsingContext & context)>
createParameterExtractor(Json::Value & argHelp,
                         const RestParam<T, Codec> & p, void * = 0)
{
    ExcAssertNotEqual(p.name, "");

    Json::Value & v = argHelp["requestParams"];
    Json::Value & v2 = v[v.size()];
    if (!p.name.empty())
        v2["name"] = p.name;
    v2["description"] = p.description;
    v2["cppType"] = MLDB::type_name<T>();
    v2["encoding"] = "URI encoded";
    v2["location"] = "query string";

    return [=] (RestConnection & connection,
                const RestRequest & request,
                const RestRequestParsingContext & context)
        {
            //std::cerr << "getting value of " << p.name << std::endl;
            Utf8String paramValue = request.params.getValue(p.name);
            return p.codec.decode(paramValue);
        };
}

template<typename T, typename Codec>
static std::function<T (RestConnection & connection,
                        const RestRequest & request,
                        const RestRequestParsingContext & context)>
createParameterExtractor(Json::Value & argHelp,
                         const RestParamDefault<T, Codec> & p, void * = 0)
{
    ExcAssertNotEqual(p.name, "");

    Json::Value & v = argHelp["requestParams"];
    Json::Value & v2 = v[v.size()];
    if (!p.name.empty())
        v2["name"] = p.name;
    v2["description"] = p.description;
    v2["cppType"] = MLDB::type_name<T>();
    v2["encoding"] = "URI encoded";
    v2["location"] = "query string";
    v2["defaultValue"] = p.defaultValueStr;

    return [=] (RestConnection & connection,
                const RestRequest & request,
                const RestRequestParsingContext & context)
        {
            //std::cerr << "getting value of " << p.name << std::endl;
            std::string paramValue;
            T result;
            if (request.params.hasValue(p.name)) 
                result = p.codec.decode(request.params.getValue(p.name));
            else result = p.defaultValue;
            return result;
        };
}

/** Free function to be called in order to generate a parameter extractor
    for the given parameter.  See the CreateRestParameterGenerator class for more
    details.
*/
template<typename T>
static std::function<decltype(JsonCodec<T>::decode(std::declval<Json::Value>()))
                     (RestConnection & connection,
                      const RestRequest & request,
                      const RestRequestParsingContext & context)>
createParameterExtractor(Json::Value & argHelp,
                         const JsonParam<T> & p, void * = 0)
{
    Json::Value & v = argHelp["jsonParams"];
    Json::Value & v2 = v[v.size()];
    if (!p.name.empty())
        v2["name"] = p.name;
    v2["description"] = p.description;
    v2["cppType"] = MLDB::type_name<T>();
    v2["encoding"] = "JSON";
    v2["location"] = "Request Body";

    return [=] (RestConnection & connection,
                const RestRequest & request,
                const RestRequestParsingContext & context)
        {
            Json::Value parsed = Json::parse(request.payload);
            return JsonCodec<T>::decode(p.name.empty() ? parsed : parsed[p.name]);
        };
}

template<typename T, typename Codec>
static std::function<T (RestConnection & connection,
                      const RestRequest & request,
                      const RestRequestParsingContext & context)>
createParameterExtractor(Json::Value & argHelp,
                         const JsonParamDefault<T, Codec> & p, void * = 0)
{
    Json::Value & v = argHelp["jsonParams"];
    Json::Value & v2 = v[v.size()];
    if (!p.name.empty()) {
        v2["name"] = p.name;
    }
    v2["description"] = p.description;
    v2["cppType"] = MLDB::type_name<T>();
    v2["encoding"] = "JSON";
    v2["location"] = "Request Body";

    return [=] (RestConnection & connection,
                const RestRequest & request,
                const RestRequestParsingContext & context)
        {
            if (request.payload == "") {
                return p.defaultValue;
            }

            Json::Value parsed = Json::parse(request.payload);
            if (p.name.empty()) {
                return p.codec.decode(request.payload);
            }
            if (parsed.isMember(p.name)) {
                return p.codec.decode(parsed[p.name].toStyledString());
            }
            return p.defaultValue;
        };
}

template<typename T, typename Codec>
static std::function<decltype(JsonCodec<T>::decode(std::declval<Json::Value>()))
                     (RestConnection & connection,
                      const RestRequest & request,
                      const RestRequestParsingContext & context)>
createParameterExtractor(Json::Value & argHelp,
                         const HybridParamJsonDefault<T, Codec> & p, void * = 0)
{
    Json::Value desc;
    desc["name"] = p.name;
    desc["description"] = p.description;
    desc["cppType"] = MLDB::type_name<T>();
    desc["encoding"] = "URI encoded or JSON";
    desc["location"] = "query string or Request Body";

    for (const auto key: {"requestParams", "jsonParams"}) {
        Json::Value & v = argHelp[key];
        v[v.size()] = desc;
    }

    return [=] (RestConnection & connection,
                const RestRequest & request,
                const RestRequestParsingContext & context)
        {
            Json::Value parsed = request.payload.empty() ?
                Json::nullValue : Json::parse(request.payload);
            if (!request.params.empty() && !parsed.empty()) {
                throw HttpReturnException(
                    400, "You cannot mix query string and body parameters");
            }
            if (request.params.hasValue(p.name)) {
                return p.codec.decode(request.params.getValue(p.name));
            }
            if (parsed.isMember(p.name)) {
                return p.codec.decode(parsed[p.name].toStyledString());
            }
            return p.defaultValue;
        };
}

template<typename T, typename Codec>
static std::function<decltype(JsonCodec<T>::decode(std::declval<Json::Value>()))
                     (RestConnection & connection,
                      const RestRequest & request,
                      const RestRequestParsingContext & context)>
createParameterExtractor(Json::Value & argHelp,
                         const HybridParamDefault<T, Codec> & p, void * = 0)
{
    Json::Value desc;
    desc["name"] = p.name;
    desc["description"] = p.description;
    desc["cppType"] = MLDB::type_name<T>();
    desc["encoding"] = "URI encoded or JSON";
    desc["location"] = "query string or Request Body";

    for (const auto key: {"requestParams", "jsonParams"}) {
        Json::Value & v = argHelp[key];
        v[v.size()] = desc;
    }

    return [=] (RestConnection & connection,
                const RestRequest & request,
                const RestRequestParsingContext & context)
        {
            Json::Value parsed = request.payload.empty() ?
                Json::nullValue : Json::parse(request.payload);
            if (!request.params.empty() && !parsed.empty()) {
                throw HttpReturnException(
                    400, "You cannot mix query string and body parameters");
            }
            if (parsed.isMember(p.name)) {
                return p.jsonCodec.decode(parsed[p.name].toStyledString());
            }
            if (request.params.hasValue(p.name)) {
                return p.restCodec.decode(request.params.getValue(p.name));
            }
            return p.defaultValue;
        };
}

/** Free function to be called in order to generate a parameter extractor
    for the given parameter.  See the CreateRestParameterGenerator class for more
    details.
*/
template<typename T, typename Codec>
static std::function<decltype(((Codec *)0)->decode(std::declval<std::string>()))
                     (RestConnection & connection,
                      const RestRequest & request,
                      const RestRequestParsingContext & context)>
createParameterExtractor(Json::Value & argHelp,
                         const RequestParam<T, Codec> & p, void * = 0)
{
    Json::Value & v = argHelp["resourceParams"];
    Json::Value & v2 = v[v.size()];
    if (!p.name.empty())
        v2["name"] = p.name;
    v2["description"] = p.description;
    v2["cppType"] = MLDB::type_name<T>();
    v2["encoding"] = "URI encoded";
    v2["location"] = "URI";

    return [=] (RestConnection & connection,
                const RestRequest & request,
                const RestRequestParsingContext & context)
        {
            int index = p.index;
            //using namespace std;
            //cerr << "index " << index << " with "
            //     << context.resources.size() << " resources" << endl;
            if (index < 0)
                index = context.resources.size() + index;
            if (index >= context.resources.size()) {
                Utf8String knownResources;
                for (auto & r: context.resources)
                    knownResources
                        += (knownResources.empty() ? "":",")
                        +  r;
                throw MLDB::Exception("attempt to access missing resource %d of "
                                    "%zd (known is %s)",
                                    index, context.resources.size(),
                                    knownResources.rawData());
            }

            Utf8String paramValue = context.resources.at(index);
            return Codec::decode(paramValue);
        };
}

/** Parameter extractor to generate something of the type

    std::function<void (X)>

    from

    std::function<void (X, ConnectionId, RestRequest) >

    by binding in the last two parameters.

    This is used to create a callback to an asynchronous function that will
    finish off a request by sending back it's results.
*/
template<typename Fn, typename Return, typename... Args>
std::function<std::function<Return (Args...)>
              (RestConnection & connection,
               const RestRequest & request,
               const RestRequestParsingContext & context)>
createParameterExtractor(Json::Value argHelp,
                         const Fn & fn, std::function<Return (Args...)> * = 0)
{
    return [=] (RestConnection & connection,
                const RestRequest & request,
                const RestRequestParsingContext & context)
        {
            // TODO: deal with more/less than one parameter...
            return std::bind(fn, std::placeholders::_1, connection, request);
        };
}

template<typename Object, int Index>
inline static std::function<Object &
                            (RestConnection & connection,
                             const RestRequest & request,
                             const RestRequestParsingContext & context)>
createParameterExtractor(Json::Value & argHelp,
                         const ObjectExtractor<Object, Index> & obj,
                         void *)
{
    return [] (RestConnection & connection,
               const RestRequest & request,
               const RestRequestParsingContext & context)
        -> Object &
        {
            
            return context.getObjectAs<Object>(Index);
        };
}

/** Any std::function that takes a RestRequestParsingContext can be a
    parameter extractor.
*/
template<typename T>
inline static std::function<T
                            (RestConnection & connection,
                             const RestRequest & request,
                             const RestRequestParsingContext & context)>
createParameterExtractor(Json::Value & argHelp,
                         const std::function<T (const RestRequestParsingContext & context)> & fn,
                         void * = 0)
{
    return [=] (RestConnection & connection,
               const RestRequest & request,
               const RestRequestParsingContext & context)
        -> T
        {
            return fn(context);
        };
}

/// Type of the function to handle an exception in the REST binding
typedef std::function<RestRequestMatchResult
                      (const std::string & excStr,
                       std::exception_ptr exc,
                       RestConnection & connection,
                       const RestRequest & request,
                       const RestRequestParsingContext & context)> ExcFn;

/*************************************************************************/
/* CREATE GENERATOR                                                      */
/*************************************************************************/

/** When we bind a callback to a request, we need to generate all of the
    parameters for the callback.  These parameters are generated from the
    request contents.

    For example,

    if we bind cb : (x, y, z) -> result

    when we get a request r, we need to call

    cb (genx(r), geny(r), genz(r) )

    to create the parameters for the callback.

    This happens in two steps:

    1.  We create a list of generators (genx, geny, genz).  This is done
    at registration time.
    2.  When we get a request (and the parameter r), we then call each
    of the generators in turn with this parameter to generate the
    argument list to be passed to the callback.

    This class deals with creating and calling the generator for a given
    position it will be called for each of (genx, geny, genz).  The RestRequestBinder
    class is used to actually group them and apply them together to
    generate the callback arguments.
*/
        
template<typename X, typename... Params>
struct CreateRestParameterGenerator {
};

template<int Index, typename Arg, typename Param, typename... Params>
struct CreateRestParameterGenerator<ML::PositionedDualType<Index, Arg, Param>, Params...> {

    typedef decltype(createParameterExtractor(*(Json::Value *)0, std::declval<typename ML::ExtractArgAtPosition<0, Index, Params...>::type>(), (typename std::decay<Arg>::type *)0)) Generator;

    //typedef std::decay<Arg> Result;
    typedef decltype(std::declval<Generator>()
                     (*(RestConnection *)0,
                      std::declval<RestRequest>(),
                      std::declval<RestRequestParsingContext>())) Result;

    /** Create the generator */
    static Generator create(Json::Value & argHelp, Params&&... params)
    {
        auto param = ML::ExtractArgAtPosition<0, Index, Params...>
            ::extract(std::forward<Params>(params)...);
        return createParameterExtractor(argHelp,
                                        param,
                                        (typename std::decay<Arg>::type *)0);
    }

    /** Apply our generator (which is at index Index within gens,
        a std::tuple<...> of parameter generators) to the given
        rest request in order to generate a parameter for a
        callback.
    */
    template<typename Generators>
    static Result apply(const Generators & gens,
                        RestConnection & connection,
                        const RestRequest & request,
                        const RestRequestParsingContext & context)
    {
        return std::get<Index>(gens)(connection, request, context);
    }
};

template<typename T>
struct RestRequestBinder {
};

#define REST_BINDING_CATCH_ERRORS \
    catch (const std::exception & exc) { \
        return sendExceptionResponse(connection, exc); \
    } catch (...) { \
        connection.sendErrorResponse(400, "unknown exception"); \
        return RestRequestRouter::MR_ERROR; \
    }


/** Create a function that validates that the request doesn't have any unknown
    parameters, based upon the JSON help structure.

    It will return true for a valid request; an invalid request will be sent
    back.
*/
std::function<bool
              (RestConnection & connection,
               const RestRequest & request,
               const RestRequestParsingContext & context)>
createRequestValidater(const Json::Value & argHelp, std::set<Utf8String> ignore);

/** Return a set of ignored query parameters for the given request filter. */
std::set<Utf8String> getIgnoredArgs(const RequestFilter & filter);

template<typename... PositionedDualTypes>
struct RestRequestBinder<ML::TypeList<PositionedDualTypes...> > {

    /** Create a request handler that will call the given member
        function with parameters extracted from the request.
    */
    template<typename Return, typename Obj, typename... Args, typename Ptr,
             typename... Params>
    static
    std::pair<RestRequestRouter::OnProcessRequest, Json::Value>
    bindSync(std::set<Utf8String> ignoreArgs,
             Return (Obj::* pmf) (Args...),
             Ptr ptr,
             Params&&... params)
    {
        Json::Value argHelp;

        // Create a tuple of function objects that we can call with
        auto gens = std::make_tuple(CreateRestParameterGenerator<PositionedDualTypes, Params...>
                                    ::create(argHelp, std::forward<Params>(params)...)...);
        // Necessary to deal with a compiler bug
        auto sharedGens = std::make_shared<decltype(gens)>(std::move(gens));

        auto validater = createRequestValidater(argHelp, ignoreArgs);

        RestRequestRouter::OnProcessRequest result
            = [=] (RestConnection & connection,
                   const RestRequest & request,
                   const RestRequestParsingContext & context)
            {
                if (!validater(connection, request, context))
                    return RestRequestRouter::MR_YES;

                auto gens = *sharedGens;
                try {
                    Obj & obj = derefPtrContext<Obj>(ptr, context);
                    ((obj).*(pmf))(CreateRestParameterGenerator<PositionedDualTypes, Params...>
                                   ::apply(gens, connection, request, context)...
                                   );
                        
                    connection.sendResponse(200);
                } REST_BINDING_CATCH_ERRORS;

                return RestRequestRouter::MR_YES;
            };
            
        return make_pair(result, argHelp);
    }


    template<typename Return, typename Obj, typename... Args, typename Ptr,
             typename... Params>
    static
    std::pair<RestRequestRouter::OnProcessRequest, Json::Value>
    bindSync(std::set<Utf8String> ignoreArgs,
             Return (Obj::* pmf) (Args...) const,
             Ptr ptr,
             Params&&... params)
    {
        Json::Value argHelp;

        // Create a tuple of function objects that we can call with
        auto gens = std::make_tuple(CreateRestParameterGenerator<PositionedDualTypes, Params...>
                                    ::create(argHelp, std::forward<Params>(params)...)...);
        // Necessary to deal with a compiler bug
        auto sharedGens = std::make_shared<decltype(gens)>(std::move(gens));

        auto validater = createRequestValidater(argHelp, ignoreArgs);

        RestRequestRouter::OnProcessRequest result
            = [=] (RestConnection & connection,
                   const RestRequest & request,
                   const RestRequestParsingContext & context)
            {
                if (!validater(connection, request, context))
                    return RestRequestRouter::MR_YES;

                auto gens = *sharedGens;
                try {
                    const Obj & obj = derefPtrContext<const Obj>(ptr, context);
                    ((obj).*(pmf))(CreateRestParameterGenerator<PositionedDualTypes, Params...>
                                   ::apply(gens, connection, request, context)...
                                   );
                        
                    connection.sendResponse(200);
                } REST_BINDING_CATCH_ERRORS;

                return RestRequestRouter::MR_YES;
            };
            
        return make_pair(result, argHelp);
    }

    /** Create a request handler that will call the given member
        function with parameters extracted from the request.
    */
    template<class TransformResultFn,
             typename Return, typename Obj, typename... Args, typename Ptr,
             typename... Params>
    static
    std::pair<RestRequestRouter::OnProcessRequest, Json::Value>
    bindSyncReturn(std::set<Utf8String> ignoreArgs,
                   const TransformResultFn & fn,
                   Return (Obj::* pmf) (Args...),
                   Ptr ptr,
                   Params&&... params)
    {
        Json::Value argHelp;

        // Create a tuple of function objects that we can call with
        auto gens = std::make_tuple(CreateRestParameterGenerator<PositionedDualTypes, Params...>
                                    ::create(argHelp, std::forward<Params>(params)...)...);
        // Necessary to deal with a compiler bug
        auto sharedGens = std::make_shared<decltype(gens)>(std::move(gens));

        auto validater = createRequestValidater(argHelp, ignoreArgs);

        RestRequestRouter::OnProcessRequest result
            = [=] (RestConnection & connection,
                   const RestRequest & request,
                   const RestRequestParsingContext & context)
            {
                if (!validater(connection, request, context))
                    return RestRequestRouter::MR_YES;

                auto gens = *sharedGens;
                try {
                    Obj & obj = derefPtrContext<Obj>(ptr, context);
                    auto res = ((obj).*(pmf))(CreateRestParameterGenerator<PositionedDualTypes, Params...>
                                              ::apply(gens, connection, request, context)...
                                              );
                    connection.sendResponse(200, fn(res));
                } REST_BINDING_CATCH_ERRORS;

                return RestRequestRouter::MR_YES;
            };
            
        return make_pair(result, argHelp);
    }


    template<class TransformResultFn,
             typename Return, typename Obj, typename... Args, typename Ptr,
             typename... Params>
    static
    std::pair<RestRequestRouter::OnProcessRequest, Json::Value>
    bindSyncReturn(std::set<Utf8String> ignoreArgs,
                   const TransformResultFn & fn,
                   Return (Obj::* pmf) (Args...) const,
                   Ptr ptr,
                   Params&&... params)
    {
        Json::Value argHelp;

        // Create a tuple of function objects that we can call with
        auto gens = std::make_tuple(CreateRestParameterGenerator<PositionedDualTypes, Params...>
                                    ::create(argHelp, std::forward<Params>(params)...)...);
        // Necessary to deal with a compiler bug
        auto sharedGens = std::make_shared<decltype(gens)>(std::move(gens));

        auto validater = createRequestValidater(argHelp, ignoreArgs);

        RestRequestRouter::OnProcessRequest result
            = [=] (RestConnection & connection,
                   const RestRequest & request,
                   const RestRequestParsingContext & context)
            {
                if (!validater(connection, request, context))
                    return RestRequestRouter::MR_YES;

                auto gens = *sharedGens;
                try {
                    const Obj & obj = derefPtrContext<const Obj>(ptr, context);
                    auto res = ((obj).*(pmf))(CreateRestParameterGenerator<PositionedDualTypes, Params...>
                                              ::apply(gens, connection, request, context)...
                                              );
                    
                    connection.sendResponse(200, fn(res));
                } REST_BINDING_CATCH_ERRORS;

                return RestRequestRouter::MR_YES;
            };
            
        return make_pair(result, argHelp);
    }


    /** Create a request handler that will call the given member
        function with parameters extracted from the request.
    */
    template<typename Return, typename Obj, typename... Args, typename Ptr,
             typename... Params>
    static
    std::pair<RestRequestRouter::OnProcessRequest, Json::Value>
    bindSyncReturnStatus(std::set<Utf8String> ignoreArgs,
                         std::pair<int, Return> (Obj::* pmf) (Args...),
                         Ptr ptr,
                         Params&&... params)
    {
        Json::Value argHelp;

        // Create a tuple of function objects that we can call with
        auto gens = std::make_tuple(CreateRestParameterGenerator<PositionedDualTypes, Params...>
                                    ::create(argHelp, std::forward<Params>(params)...)...);
        // Necessary to deal with a compiler bug
        auto sharedGens = std::make_shared<decltype(gens)>(std::move(gens));

        auto validater = createRequestValidater(argHelp, ignoreArgs);

        RestRequestRouter::OnProcessRequest result
            = [=] (RestConnection & connection,
                   const RestRequest & request,
                   const RestRequestParsingContext & context)
            {
                if (!validater(connection, request, context))
                    return RestRequestRouter::MR_YES;

                auto gens = *sharedGens;
                try {
                    Obj & obj = derefPtrContext<Obj>(ptr, context);
                    auto res = ((obj).*(pmf))(CreateRestParameterGenerator<PositionedDualTypes, Params...>
                                              ::apply(gens, connection, request, context)...
                                              );
                    connection.sendResponse(res.first, res.second);
                } REST_BINDING_CATCH_ERRORS;

                return RestRequestRouter::MR_YES;
            };
            
        return make_pair(result, argHelp);
    }


    template<typename Return, typename Obj, typename... Args, typename Ptr,
             typename... Params>
    static
    std::pair<RestRequestRouter::OnProcessRequest, Json::Value>
    bindSyncReturnStatus(std::set<Utf8String> ignoreArgs,
                         Return (Obj::* pmf) (Args...) const,
                         Ptr ptr,
                         Params&&... params)
    {
        Json::Value argHelp;

        // Create a tuple of function objects that we can call with
        auto gens = std::make_tuple(CreateRestParameterGenerator<PositionedDualTypes, Params...>
                                    ::create(argHelp, std::forward<Params>(params)...)...);
        // Necessary to deal with a compiler bug
        auto sharedGens = std::make_shared<decltype(gens)>(std::move(gens));

        auto validater = createRequestValidater(argHelp, ignoreArgs);

        RestRequestRouter::OnProcessRequest result
            = [=] (RestConnection & connection,
                   const RestRequest & request,
                   const RestRequestParsingContext & context)
            {
                if (!validater(connection, request, context))
                    return RestRequestRouter::MR_YES;

                auto gens = *sharedGens;
                try {
                    const Obj & obj = derefPtrContext<const Obj>(ptr, context);
                    auto res = ((obj).*(pmf))(CreateRestParameterGenerator<PositionedDualTypes, Params...>
                                              ::apply(gens, connection, request, context)...
                                              );
                    
                    connection.sendResponse(res.first, res.second);
                } REST_BINDING_CATCH_ERRORS;

                return RestRequestRouter::MR_YES;
            };
            
        return make_pair(result, argHelp);
    }


    /** Create a request handler that will call the given member
        function with parameters extracted from the request.
    */
    template<typename Return, typename Obj, typename... Args, typename Ptr,
             typename... Params>
    static
    std::pair<RestRequestRouter::OnProcessRequest, Json::Value>
    bindAsync(std::set<Utf8String> ignoreArgs,
              Return (Obj::* pmf) (Args...),
              Ptr ptr,
              Params&&... params)
    {
        Json::Value argHelp;

        // Create a tuple of function objects that we can call with
        auto gens = std::make_tuple(CreateRestParameterGenerator<PositionedDualTypes, Params...>
                                    ::create(argHelp, std::forward<Params>(params)...)...);
        // Necessary to deal with a compiler bug
        auto sharedGens = std::make_shared<decltype(gens)>(std::move(gens));

        auto validater = createRequestValidater(argHelp, ignoreArgs);

        RestRequestRouter::OnProcessRequest result
            = [=] (RestConnection & connection,
                   const RestRequest & request,
                   const RestRequestParsingContext & context)
            {
                if (!validater(connection, request, context))
                    return RestRequestRouter::MR_YES;

                auto gens = *sharedGens;
                try {
                    Obj & obj = derefPtrContext<Obj>(ptr, context);
                    ((obj).*(pmf))(CreateRestParameterGenerator<PositionedDualTypes, Params...>
                                   ::apply(gens, connection, request, context)...
                                   );
                } REST_BINDING_CATCH_ERRORS;

                return RestRequestRouter::MR_YES;
            };
            
        return make_pair(result, argHelp);
    }

    /** Create a request handler that will call the given member
        function with parameters extracted from the request.
    */
    template<typename Return, typename Obj, typename... Args, typename Ptr,
             typename... Params>
    static
    std::pair<RestRequestRouter::OnProcessRequest, Json::Value>
    bindAsync(std::set<Utf8String> ignoreArgs,
              Return (Obj::* pmf) (Args...) const,
              Ptr ptr,
              Params&&... params)
    {
        Json::Value argHelp;

        // Create a tuple of function objects that we can call with
        auto gens = std::make_tuple(CreateRestParameterGenerator<PositionedDualTypes, Params...>
                                    ::create(argHelp, std::forward<Params>(params)...)...);
        // Necessary to deal with a compiler bug
        auto sharedGens = std::make_shared<decltype(gens)>(std::move(gens));

        auto validater = createRequestValidater(argHelp, ignoreArgs);

        RestRequestRouter::OnProcessRequest result
            = [=] (RestConnection & connection,
                   const RestRequest & request,
                   const RestRequestParsingContext & context)
            {
                if (!validater(connection, request, context))
                    return RestRequestRouter::MR_YES;

                auto gens = *sharedGens;
                try {
                    const Obj & obj = derefPtrContext<const Obj>(ptr, context);
                    ((obj).*(pmf))(CreateRestParameterGenerator<PositionedDualTypes, Params...>
                                   ::apply(gens, connection, request, context)...
                                   );
                } REST_BINDING_CATCH_ERRORS;

                return RestRequestRouter::MR_YES;
            };
            
        return make_pair(result, argHelp);
    }

    /** Create a request handler that will call the given member
        function with parameters extracted from the request.
    */
    template<typename Return, typename... Args,
             typename... Params, typename ThenFn>
    static
    std::pair<RestRequestRouter::OnProcessRequest, Json::Value>
    bindAsyncThen(std::set<Utf8String> ignoreArgs,
                  const ThenFn & then,
                  const ExcFn & excFn,
                  const std::function<Return (Args...)> & fn,
                  Params&&... params)
    {
        Json::Value argHelp;

        // Create a tuple of function objects that we can call with
        auto gens = std::make_tuple(CreateRestParameterGenerator<PositionedDualTypes, Params...>
                                    ::create(argHelp, std::forward<Params>(params)...)...);
        // Necessary to deal with a compiler bug
        auto sharedGens = std::make_shared<decltype(gens)>(std::move(gens));

        auto validater = createRequestValidater(argHelp, ignoreArgs);

        RestRequestRouter::OnProcessRequest result
            = [=] (RestConnection & connection,
                   const RestRequest & request,
                   const RestRequestParsingContext & context) -> RestRequestMatchResult
            {
                if (!validater(connection, request, context))
                    return RestRequestRouter::MR_YES;

                auto gens = *sharedGens;
                try {
                    return then(fn(CreateRestParameterGenerator<PositionedDualTypes, Params...>
                                   ::apply(gens, connection, request, context)...
                                   ),
                                connection, request, context);
                } catch (const std::exception & exc) {
                    if (excFn)
                        return excFn(exc.what(), std::current_exception(), 
                                     connection, request, context);
                    return sendExceptionResponse(connection, exc);
                } catch (...) {
                    if (excFn)
                        return excFn("unknown exception", std::current_exception(),
                                     connection, request, context);
                    connection.sendErrorResponse(400, "unknown exception");
                    return RestRequestRouter::MR_ERROR;
                }
            };
            
        return make_pair(result, argHelp);
    }

    /** Create a request handler that will call the given member
        function with parameters extracted from the request.

        The function also gets an implicit first parameter which is
        the RestRequestParsingContext.
    */
    template<typename Return, typename... Args,
             typename... Params, typename ThenFn>
    static
    std::pair<RestRequestRouter::OnProcessRequest, Json::Value>
    bindAsyncThenContext(std::set<Utf8String> ignoreArgs,
                         const ThenFn & then,
                         const ExcFn & excFn,
                         const std::function<Return (const RestRequestParsingContext &, Args...)> & fn,
                         Params&&... params)
    {
        Json::Value argHelp;

        // Create a tuple of function objects that we can call with
        auto gens = std::make_tuple(CreateRestParameterGenerator<PositionedDualTypes, Params...>
                                    ::create(argHelp, std::forward<Params>(params)...)...);
        // Necessary to deal with a compiler bug
        auto sharedGens = std::make_shared<decltype(gens)>(std::move(gens));

        auto validater = createRequestValidater(argHelp, ignoreArgs);

        RestRequestRouter::OnProcessRequest result
            = [=] (RestConnection & connection,
                   const RestRequest & request,
                   const RestRequestParsingContext & context) -> RestRequestMatchResult
            {
                if (!validater(connection, request, context))
                    return RestRequestRouter::MR_YES;

                auto gens = *sharedGens;
                try {
                    return then(fn(context, CreateRestParameterGenerator<PositionedDualTypes, Params...>
                                   ::apply(gens, connection, request, context)...
                                   ),
                                connection, request, context);
                } catch (const std::exception & exc) {
                    if (excFn)
                        return excFn(exc.what(), std::current_exception(), 
                                     connection, request, context);
                    return sendExceptionResponse(connection, exc);
                } catch (...) {
                    if (excFn)
                        return excFn("unknown exception", std::current_exception(),
                                     connection, request, context);
                    connection.sendErrorResponse(400, "unknown exception");
                    return RestRequestRouter::MR_ERROR;
                }
            };
            
        return make_pair(result, argHelp);
    }

};

template<typename Return, typename Obj, typename... Args, typename Ptr,
         typename TransformResult,
         typename... Params>
void
addRouteSyncReturn(RestRequestRouter & router,
                   PathSpec path, RequestFilter filter,
                   const std::string & description,
                   const std::string & resultDescription,
                   const TransformResult & transformResult,
                   Return (Obj::* pmf) (Args...),
                   Ptr ptr,
                   Params&&... params)
{
    static_assert(sizeof...(Args) == sizeof...(Params),
                  "member function and parameter arity must match");

    typedef ML::TypeList<Args...> ArgsList;
    typedef ML::TypeList<Params...> ParamsList;
    typedef ML::PositionedDualTypeList<0, ArgsList, ParamsList> PositionedTypes;

    auto res = RestRequestBinder<typename PositionedTypes::List>
        ::bindSyncReturn(getIgnoredArgs(filter), transformResult,
                         pmf, ptr, std::forward<Params>(params)...);
    auto & cb = res.first;
    auto & help = res.second;
    help["result"] = resultDescription;

    router.addRoute(path, filter, description, cb, help);
}

template<typename Return, typename Obj, typename... Args, typename Ptr,
         typename TransformResult,
         typename... Params>
void
addRouteSyncReturn(RestRequestRouter & router,
                   PathSpec path, RequestFilter filter,
                   const std::string & description,
                   const std::string & resultDescription,
                   const TransformResult & transformResult,
                   Return (Obj::* pmf) (Args...) const,
                   Ptr ptr,
                   Params&&... params)
{
    static_assert(sizeof...(Args) == sizeof...(Params),
                  "member function and parameter arity must match");

    typedef ML::TypeList<Args...> ArgsList;
    typedef ML::TypeList<Params...> ParamsList;
    typedef ML::PositionedDualTypeList<0, ArgsList, ParamsList> PositionedTypes;

    auto res = RestRequestBinder<typename PositionedTypes::List>
        ::bindSyncReturn(transformResult,
                         pmf, ptr, std::forward<Params>(params)...);
    auto & cb = res.first;
    auto & help = res.second;
    help["result"] = resultDescription;

    router.addRoute(path, filter, description, cb, help);
}

template<typename Return, typename Obj, typename... Args, typename Ptr,
         typename... Params>
void
addRouteReturnStatus(RestRequestRouter & router,
                     PathSpec path, RequestFilter filter,
                     const std::string & description,
                     const std::string & resultDescription,
                     std::pair<int, Return> (Obj::* pmf) (Args...),
                     Ptr ptr,
                     Params&&... params)
{
    static_assert(sizeof...(Args) == sizeof...(Params),
                  "member function and parameter arity must match");

    typedef ML::TypeList<Args...> ArgsList;
    typedef ML::TypeList<Params...> ParamsList;
    typedef ML::PositionedDualTypeList<0, ArgsList, ParamsList> PositionedTypes;

    auto res = RestRequestBinder<typename PositionedTypes::List>
        ::bindSyncReturnStatus(getIgnoredArgs(filter), pmf, ptr, std::forward<Params>(params)...);
    auto & cb = res.first;
    auto & help = res.second;
    help["result"] = resultDescription;

    router.addRoute(path, filter, description, cb, help);
}

template<typename Return, typename Obj, typename... Args, typename Ptr,
         typename... Params>
void
addRouteReturnStatus(RestRequestRouter & router,
                         PathSpec path, RequestFilter filter,
                         const std::string & description,
                         const std::string & resultDescription,
                         std::pair<int, Return> (Obj::* pmf) (Args...) const,
                         Ptr ptr,
                         Params&&... params)
{
    static_assert(sizeof...(Args) == sizeof...(Params),
                  "member function and parameter arity must match");

    typedef ML::TypeList<Args...> ArgsList;
    typedef ML::TypeList<Params...> ParamsList;
    typedef ML::PositionedDualTypeList<0, ArgsList, ParamsList> PositionedTypes;

    auto res = RestRequestBinder<typename PositionedTypes::List>
        ::bindSyncReturnStatus(getIgnoredArgs(filter), pmf, ptr, std::forward<Params>(params)...);
    auto & cb = res.first;
    auto & help = res.second;
    help["result"] = resultDescription;

    router.addRoute(path, filter, description, cb, help);
}

// Void return types don't need to convert their result
template<typename Return, typename Obj, typename... Args, typename Ptr,
         typename... Params>
void
addRouteSync(RestRequestRouter & router,
             PathSpec path, RequestFilter filter,
             const std::string & description,
             Return (Obj::* pmf) (Args...),
             Ptr ptr,
             Params&&... params)
{
    static_assert(sizeof...(Args) == sizeof...(Params),
                  "member function and parameter arity must match");

    typedef ML::TypeList<Args...> ArgsList;
    typedef ML::TypeList<Params...> ParamsList;
    typedef ML::PositionedDualTypeList<0, ArgsList, ParamsList> PositionedTypes;

    auto res = RestRequestBinder<typename PositionedTypes::List>
        ::bindSync(getIgnoredArgs(filter), pmf, ptr, std::forward<Params>(params)...);
    auto & cb = res.first;
    auto & help = res.second;
    
    router.addRoute(path, filter, description, cb, help);
}

// Void return types don't need to convert their result
template<typename Return, typename Obj, typename... Args, typename Ptr,
         typename... Params>
void
addRouteSync(RestRequestRouter & router,
             PathSpec path, RequestFilter filter,
             const std::string & description,
             Return (Obj::* pmf) (Args...) const,
             Ptr ptr,
             Params&&... params)
{
    static_assert(sizeof...(Args) == sizeof...(Params),
                  "member function and parameter arity must match");

    typedef ML::TypeList<Args...> ArgsList;
    typedef ML::TypeList<Params...> ParamsList;
    typedef ML::PositionedDualTypeList<0, ArgsList, ParamsList> PositionedTypes;

    auto res = RestRequestBinder<typename PositionedTypes::List>
        ::bindSync(getIgnoredArgs(filter), pmf, ptr, std::forward<Params>(params)...);
    auto & cb = res.first;
    auto & help = res.second;
        
    router.addRoute(path, filter, description, cb, help);
}

template<typename Return, typename Obj, typename... Args, typename Ptr,
         typename... Params>
void
addRouteAsync(RestRequestRouter & router,
              PathSpec path, RequestFilter filter,
              const std::string & description,
              Return (Obj::* pmf) (Args...),
              Ptr ptr,
              Params&&... params)
{
    static_assert(sizeof...(Args) == sizeof...(Params),
                  "member function and parameter arity must match");

    typedef ML::TypeList<Args...> ArgsList;
    typedef ML::TypeList<Params...> ParamsList;
    typedef ML::PositionedDualTypeList<0, ArgsList, ParamsList> PositionedTypes;

    auto res = RestRequestBinder<typename PositionedTypes::List>
        ::bindAsync(getIgnoredArgs(filter), pmf, ptr, std::forward<Params>(params)...);
    auto & cb = res.first;
    auto & help = res.second;

    router.addRoute(path, filter, description, cb, help);
}

template<typename Return, typename Obj, typename... Args, typename Ptr,
         typename... Params>
void
addRouteAsync(RestRequestRouter & router,
              PathSpec path, RequestFilter filter,
              const std::string & description,
              Return (Obj::* pmf) (Args...) const,
              Ptr ptr,
              Params&&... params)
{
    static_assert(sizeof...(Args) == sizeof...(Params),
                  "member function and parameter arity must match");

    typedef ML::TypeList<Args...> ArgsList;
    typedef ML::TypeList<Params...> ParamsList;
    typedef ML::PositionedDualTypeList<0, ArgsList, ParamsList> PositionedTypes;

    auto res = RestRequestBinder<typename PositionedTypes::List>
        ::bindAsync(getIgnoredArgs(filter), pmf, ptr, std::forward<Params>(params)...);
    auto & cb = res.first;
    auto & help = res.second;
        
    router.addRoute(path, filter, description, cb, help);
}

template<typename Return, typename... Args,
         typename... Params, typename ThenFn>
void
addRouteAsyncThen(RestRequestRouter & router,
                  PathSpec path, RequestFilter filter,
                  const std::string & description,
                  const std::string & resultDescription,
                  const ThenFn & then,
                  const ExcFn & exc,
                  const std::function<Return (Args...)> & fn,
                  Params&&... params)
{
    static_assert(sizeof...(Args) == sizeof...(Params),
                  "member function and parameter arity must match");

    typedef ML::TypeList<Args...> ArgsList;
    typedef ML::TypeList<Params...> ParamsList;
    typedef ML::PositionedDualTypeList<0, ArgsList, ParamsList> PositionedTypes;

    auto res = RestRequestBinder<typename PositionedTypes::List>
        ::bindAsyncThen(getIgnoredArgs(filter), then, exc, fn, std::forward<Params>(params)...);
    auto & cb = res.first;
    auto & help = res.second;
    help["result"] = resultDescription;

    router.addRoute(path, filter, description, cb, help);
}

template<typename Return, typename... Args, typename... Params>
void
addRouteSyncJsonReturn(RestRequestRouter & router,
                       PathSpec path, RequestFilter filter,
                       const std::string & description,
                       const std::string & resultDescription,
                       const std::function<Return (Args...)> & fn,
                       Params&&... params)
{
    auto then = [] (const Return & ret,
                    RestConnection & connection,
                    const RestRequest &,
                    const RestRequestParsingContext &)
        {
            static std::shared_ptr<ValueDescription> desc(getDefaultDescription((Return *)0));
            std::ostringstream out;
            StreamJsonPrintingContext context(out);
            desc->printJson(&ret, context);
            out << std::endl;
            connection.sendResponse(200, out.str(), "application/json");
            return RestRequestRouter::MR_YES;
        };

    addRouteAsyncThen(router, path, filter, description, resultDescription,
                      then, nullptr /* Exception handler */, fn,
                      std::forward<Params>(params)...);
}

template<typename Return, typename... Args,
         typename... Params, typename ThenFn>
void
addRouteAsyncThenContext(RestRequestRouter & router,
                         PathSpec path, RequestFilter filter,
                         const std::string & description,
                         const std::string & resultDescription,
                         const ThenFn & then,
                         const ExcFn & exc,
                         const std::function<Return (const RestRequestParsingContext & context, Args...)> & fn,
                         Params&&... params)
{
    static_assert(sizeof...(Args) == sizeof...(Params),
                  "member function and parameter arity must match");

    typedef ML::TypeList<Args...> ArgsList;
    typedef ML::TypeList<Params...> ParamsList;
    typedef ML::PositionedDualTypeList<0, ArgsList, ParamsList> PositionedTypes;

    auto res = RestRequestBinder<typename PositionedTypes::List>
        ::bindAsyncThenContext(getIgnoredArgs(filter), then, exc, fn, std::forward<Params>(params)...);
    auto & cb = res.first;
    auto & help = res.second;
    help["result"] = resultDescription;

    router.addRoute(path, filter, description, cb, help);
}

template<typename Return, typename... Args, typename... Params>
void
addRouteSyncJsonReturnContext(RestRequestRouter & router,
                              PathSpec path, RequestFilter filter,
                              const std::string & description,
                              const std::string & resultDescription,
                              const std::function<Return (const RestRequestParsingContext & context, Args...)> & fn,
                              Params&&... params)
{
    auto then = [] (const Return & ret,
                    RestConnection & connection,
                    const RestRequest &,
                    const RestRequestParsingContext &)
        {
            static std::shared_ptr<ValueDescription> desc(getDefaultDescription((Return *)0));
            std::ostringstream out;
            StreamJsonPrintingContext context(out);
            desc->printJson(&ret, context);
            out << std::endl;
            connection.sendResponse(200, out.str(), "application/json");
            return RestRequestRouter::MR_YES;
        };

    addRouteAsyncThenContext(router, path, filter, description, resultDescription,
                             then, nullptr /* Exception handler */, fn,
                             std::forward<Params>(params)...);
}

template<typename Return, typename Obj, typename... Args, typename Ptr,
         typename... Params>
void
addRouteSyncJsonReturn(RestRequestRouter & router,
                       PathSpec path, RequestFilter filter,
                       const std::string & description,
                       const std::string & resultDescription,
                       Return (Obj::* pmf) (Args...),
                       Ptr ptr,
                       Params&&... params)
{
    return addRouteSyncJsonReturnContext(router, path, filter, description,
                                         resultDescription,
                                         partial_bind_context(pmf, ptr),
                                         std::forward<Params>(params)...);
}

template<typename Return, typename Obj, typename... Args, typename Ptr,
         typename... Params>
void
addRouteSyncJsonReturnContext(RestRequestRouter & router,
                              PathSpec path, RequestFilter filter,
                              const std::string & description,
                              const std::string & resultDescription,
                              Return (Obj::* pmf) (Args...),
                              Ptr ptr,
                              Params&&... params)
{
    return addRouteSyncJsonReturnContext(router, path, filter, description,
                                         resultDescription,
                                         partial_bind_context(pmf, ptr),
                                         std::forward<Params>(params)...);
}

template<typename Return, typename Obj, typename... Args, typename Ptr,
         typename... Params>
void
addRouteSyncJsonReturn(RestRequestRouter & router,
                       PathSpec path, RequestFilter filter,
                       const std::string & description,
                       const std::string & resultDescription,
                       Return (Obj::* pmf) (Args...) const,
                       Ptr ptr,
                       Params&&... params)
{
    return addRouteSyncJsonReturnContext(router, path, filter, description,
                                         resultDescription,
                                         partial_bind_context(pmf, ptr),
                                         std::forward<Params>(params)...);
}



} // namespace MLDB
