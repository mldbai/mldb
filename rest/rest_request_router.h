/** rest_request_router.h                                          -*- C++ -*-
    Jeremy Barnes, 13 November 2012
    Copyright (c) 2012 mldb.ai inc.  All rights reserved.

    This file is part of MLDB. Copyright 2015 mldb.ai inc. All rights reserved.
*/

#pragma once

#include <set>

#include "mldb/rest/rest_request_fwd.h"
#include "mldb/rest/rest_request.h"
#include "mldb/rest/rest_connection.h"
#include "mldb/ext/jsoncpp/json.h"
#include "mldb/base/exc_assert.h"
#include "mldb/arch/demangle.h"
#include "mldb/arch/exception.h"
#include "mldb/types/value_description_fwd.h"
#include "mldb/types/regex.h"
#include "mldb/http/http_exception.h"
#include <set>


namespace MLDB {


/*****************************************************************************/
/* PATH SPEC                                                                 */
/*****************************************************************************/

/** This matches part of a path for a REST URI. */

struct PathSpec {
    enum Type {
        NONE,
        STRING,
        REGEX
    } type;

    /// Construct a PathSpec that matches notning
    PathSpec();
        
    /// Construct a PathSpec that matches a string, eg "/methods"
    PathSpec(const std::string & fullPath);

    /// Construct a PathSpec that matches a string, eg "/methods"
    PathSpec(const Utf8String & fullPath);

    /// Construct a PathSpec that matches a string, eg "/methods"
    PathSpec(const char * fullPath);

    /// Construct a PathSpec that mathces a regex, eg "/items/([0-9a-z_]+)"
    PathSpec(Regex regex);

    /// Fill out the given JSON object with help about how this path is matched
    void getHelp(Json::Value & result) const;
    
    /// Get the description string
    Utf8String getPathDesc() const;

    Utf8String path;   ///< Path or regex unparsed string
    Regex rex;         ///< Parsed regex, if type == REGEX
    Utf8String desc;   ///< Description for help

    /// Return the number of captured elements for this specification.  This is the
    /// number of strings that will be appended to the resources field of the context
    /// object.
    ///
    /// A straight path match will always add one.
    ///
    /// A regular expression will always add one, plus one for each capture in the
    /// regular expression (element enclosed in parantheses).

    int numCapturedElements() const;

    bool operator == (const PathSpec & other) const;

    bool operator != (const PathSpec & other) const;

    bool operator < (const PathSpec & other) const;
};

/// A shortcut way to construct a PathSpec that's a regular expression
PathSpec Rx(const Utf8String & regexString, const Utf8String & desc);

std::ostream & operator << (std::ostream & stream, const PathSpec & path);


/*****************************************************************************/
/* REQUEST PARAM FILTER                                                      */
/*****************************************************************************/

/** Filter that allows a route to match only if a particular parameter
    matches a given value.

    This allows for there to be multiple routes that are selected based upon
    a request parameter.
*/
struct RequestParamFilter {
    enum Location {
        QUERY,  ///< Parameter is in the query string
        HEADER  ///< Parameter is in a header
    };

    RequestParamFilter(Location location,
                       const std::string & param = "",
                       const std::string & value = "")
        : location(location), param(param), value(value)
    {
    }

    Location location;
    std::string param;
    std::string value;

    bool operator < (const RequestParamFilter & other) const;
};

std::ostream & operator << (std::ostream & stream, const RequestParamFilter & filter);

/*****************************************************************************/
/* REQUEST FILTER                                                            */
/*****************************************************************************/

/** Filter for a REST request route match by verb and filters. */

struct RequestFilter {

    /// Construct a request filter that matches nothing
    RequestFilter();

    /// Construct a request filter that matches a single verb, eg GET
    RequestFilter(const std::string & verb);

    /// Construct a request filter that matches a single verb, eg GET
    RequestFilter(const char * verb);

    /// Construct a request filter that matches any of a set of verbs,
    /// eg { GET, HEAD }
    RequestFilter(std::set<std::string> verbs);

    /// Construct a request filter that matches any of a set of verbs,
    /// eg { GET, HEAD }
    RequestFilter(const std::initializer_list<std::string> & verbs);

    /// Fill out the JSON object with help about the filters
    void getHelp(Json::Value & result) const;

    /// Return a list of query parameter that should be ignored
    std::set<Utf8String> getIgnoredQueryParameters() const;

private:
    /// Internal method to separate the list of verbs into either verbs or
    /// filters
    void parseVerbs();
    std::set<std::string> verbs;
    std::vector<RequestParamFilter> filters;
  
    friend class RestRequestRouter;
    friend std::ostream & operator << (std::ostream & stream, const RequestFilter & filter);

};





/*****************************************************************************/
/* REST REQUEST PARSING CONTEXT                                              */
/*****************************************************************************/

/** Parsing context for a REST request.  Tracks of how the request path
    is processed so that the entity names can be extracted later.
*/

struct RestRequestParsingContext {
    RestRequestParsingContext(const RestRequest & request)
        : remaining(request.resource)
    {
    }

    /** Add the given object. */
    template<typename T>
    void addObject(T * obj,
                   std::function<void (void *)> deleter = nullptr)
    {
        objects.emplace_back(obj, &typeid(T), std::move(deleter));
    }

    /** Add a shared pointer to the given object, incrementing the
        count so that it cannot be freed until this parsing context
        releases it.

        This is useful when an object may be deleted during request
        parsing, to make sure it stays alive until the request has
        completed.
    */
    template<typename T>
    void addSharedPtr(std::shared_ptr<T> ptr)
    {
        addObject(new std::shared_ptr<T>(std::move(ptr)),
                  [] (void * ptr) { delete (std::shared_ptr<T> *)ptr; });
    }

    /** Get the object at the given index on the context (defaults to the
        last), and return it and its type.

        Indexes below zero are interpreted as offsets from the end of the
        array.
    */
    std::pair<void *, const std::type_info *> getObject(int index = -1) const
    {
        if (index == -1)
            index = objects.size() + index;
        if (index < 0 || index >= objects.size())
            throw MLDB::Exception("Attempt to extract invalid object number");

        auto & res = objects[index];
        if (!res.obj || !res.type)
            throw MLDB::Exception("invalid object");

        return std::make_pair(res.obj, res.type);
    }

    /** Get the object at the given index on the context (defaults to the
        last), and convert it safely to the given type.

        Indexes below zero are interpreted as offsets from the end of the
        array.
    */
    template<typename As>
    As & getObjectAs(int index = -1) const
    {
        auto obj = getObject(index);

        const std::type_info * tp = &typeid(As);
        if (tp == obj.second)
            return *reinterpret_cast<As *>(obj.first);

        if (&typeid(std::shared_ptr<As>) == obj.second)
            return *reinterpret_cast<std::shared_ptr<As> *>(obj.first)->get();

        void * converted = nullptr; //ML::is_convertible(*obj.second,
                                    //          *tp,
                                    //          obj.first);
        if (!converted)
            throw MLDB::Exception("wanted to get object of type "
                                + MLDB::type_name<As>()
                                + " from incompatible object of type "
                                + demangle(obj.second->name()));

        return *reinterpret_cast<As *>(converted);
    }

    template<typename As>
    std::shared_ptr<As> getSharedPtrAs(int index = -1) const
    {
        return getObjectAs<std::shared_ptr<As> >(index);
    }

    /// List of resources (url components) in the path
    std::vector<Utf8String> resources;

    /// Objects
    struct ObjectEntry {
        ObjectEntry(void * obj = nullptr,
                    const std::type_info * type = nullptr,
                    std::function<void (void *) noexcept> deleter = nullptr) noexcept
            : obj(obj), type(type), deleter(std::move(deleter))
        {
        }

        ~ObjectEntry() noexcept
        {
            if (deleter)
                deleter(obj);
        }

        void * obj;
        const std::type_info * type;
        std::function<void (void *) noexcept> deleter;

        ObjectEntry(const ObjectEntry &) = delete;
        void operator = (const ObjectEntry &) = delete;

        ObjectEntry(ObjectEntry && other) noexcept
            : obj(other.obj), type(other.type), deleter(std::move(other.deleter))
        {
            other.obj = nullptr;
            other.deleter = nullptr;
        }

        ObjectEntry & operator = (ObjectEntry && other) noexcept
        {
            ObjectEntry newMe(std::move(other));
            swap(newMe);
            return *this;
        }

        void swap(ObjectEntry & other) noexcept
        {
            std::swap(obj, other.obj);
            std::swap(type, other.type);
            std::swap(deleter, other.deleter);
        }

#if 0
        // Needed for gcc 4.6
        ObjectEntry(const ObjectEntry & other)
            : obj(other.obj), type(other.type)
        {
            ObjectEntry & otherNonConst = (ObjectEntry &)other;
            deleter = std::move(otherNonConst.deleter);
            otherNonConst.obj = nullptr;
            otherNonConst.type = nullptr;
        }

        void operator = (const ObjectEntry & other)
        {
            ObjectEntry newMe(other);
            *this = std::move(newMe);
        }
#endif
    };

    /// List of extracted objects to which path components refer.  Both the
    /// object and its type are stored as well as a destructor function.
    /// They are shared pointers as the contexts are copied.
    std::vector<ObjectEntry> objects;

    /// Part of the resource that has not yet been consumed
    Utf8String remaining;

    /// Used to save the state so that whatever was pushed after can be
    /// removed and the object can get back to its old state (without making
    /// a copy).
    struct State {
        Utf8String remaining;
        int resourcesLength;
        int objectsLength;
    };

    /// Save the current state, to be restored in restoreState
    State saveState() const
    {
        State result;
        result.remaining = remaining;
        result.resourcesLength = resources.size();
        result.objectsLength = objects.size();
        return result;
    }

    /// Restore the current state
    void restoreState(State && state)
    {
        remaining = std::move(state.remaining);
        ExcAssertGreaterEqual(resources.size(), state.resourcesLength);
        resources.resize(state.resourcesLength);
        ExcAssertGreaterEqual(objects.size(), state.objectsLength);
        while (objects.size() > state.objectsLength)
            objects.pop_back();
    }

    /// Guard object to save the state and restore it on scope exit
    struct StateGuard {
        State state;
        RestRequestParsingContext * obj;

        StateGuard(RestRequestParsingContext * obj)
            : state(obj->saveState()),
              obj(obj)
        {
        }

        ~StateGuard()
        {
            obj->restoreState(std::move(state));
        }
    };

    RestRequestParsingContext(const RestRequestParsingContext &) = delete;
    void operator = (const RestRequestParsingContext &) = delete;
};

std::ostream & operator << (std::ostream & stream,
                            const RestRequestParsingContext & context);


/*****************************************************************************/
/* REST REQUEST ROUTER                                                       */
/*****************************************************************************/

struct RestRequestRouter {

    typedef RestConnection ConnectionId;
    typedef RestRequestMatchResult MatchResult;

    static constexpr RestRequestMatchResult MR_NO = MLDB::MR_NO;
    static constexpr RestRequestMatchResult MR_YES = MLDB::MR_YES;
    static constexpr RestRequestMatchResult MR_ERROR = MLDB::MR_ERROR;
    static constexpr RestRequestMatchResult MR_ASYNC = MLDB::MR_ASYNC;

    typedef std::function<RestRequestMatchResult (RestConnection & connection,
                                       const RestRequest & request,
                                       RestRequestParsingContext & context)>
                OnProcessRequest;

    typedef std::function<void (RestConnection & connection,
                                const RestRequest & request)>
                OnNotFoundRequest;

    RestRequestRouter();

    RestRequestRouter(const OnProcessRequest & processRequest,
                      const OnNotFoundRequest & notFoundHandler,
                      const Utf8String & description,
                      bool terminal,
                      const Json::Value & argHelp = Json::Value());

    virtual ~RestRequestRouter();
    
    /** Return a requestHandler that can be assigned to the
        RestServiceEndpoint.
    */
    typedef std::function<void (RestConnection & connection,
                                const RestRequest & request)> OnHandleRequest;

    OnHandleRequest requestHandler() const;

    virtual void handleRequest(RestConnection & connection,
                               const RestRequest & request) const;

    virtual RestRequestMatchResult
    processRequest(RestConnection & connection,
                   const RestRequest & request,
                   RestRequestParsingContext & context) const;

    virtual void options(std::set<std::string> & verbsAccepted,
                         Json::Value & help,
                         const RestRequest & request,
                         RestRequestParsingContext & context) const;

    /** Type of a function that is called by the route after matching to extract any
        objects referred to so that they can be added to the context and made
        available to futher event handlers.
        
        Sample usage:
        
        // Verify that the given subject is indeed present in the behaviour domain
        auto verifySubject = [=] (RestConnection & connection,
                                  const RestRequest & request,
                                  RestRequestParsingContext & context)
        {
            // Grab the dataset, which was matched previously with an addObject
            Dataset & dataset = context.getObjectAs<Dataset>();

            // Find the subject we just parsed from the path
            string subject = context.resources.back();

            SubjectId sid = SubjectId::fromString(subject);

            bool exists = dataset.knownSubject(sid);

            if (!exists && request.verb != "PUT") {
                Json::Value error;
                error["error"] = "subject '" + subject + "' doesn't exist in dataset";
                connection.sendResponse(404, error);
            }
        };

        auto & subject
            = subjects.addSubRouter(Rx("/([^/]*)", "/<subject>"),
                                    "operations on an individual subject",
                                    verifySubject);
    */
    
    typedef std::function<void(RestConnection & connection,
                               const RestRequest & request,
                               RestRequestParsingContext & context)> ExtractObject;

    template<typename T>
    static ExtractObject addObject(T * obj)
    {
        return [=] (RestConnection & connection,
                    const RestRequest & request,
                    RestRequestParsingContext & context)
            {
                context.addObject(obj);
            };
    }

    struct Route {
        PathSpec path;
        RequestFilter filter;
        std::shared_ptr<RestRequestRouter> router;
        ExtractObject extractObject;

        bool matchPath(RestRequestParsingContext & context) const;

        RestRequestMatchResult process(const RestRequest & request,
                            RestRequestParsingContext & context,
                            RestConnection & connection) const;

        void
        options(std::set<std::string> & verbsAccepted,
                Json::Value & help,
                const RestRequest & request,
                RestRequestParsingContext & context) const;
    };

    /** Route's filters are evaluated in the order routes have
        been registered.  As a consequence, routes with looser filter
        (e..g. {"PUT"}) MUST be added after identical route with tighter
        filter (e.g. {"PUT", "header:async=true"}).  An exception is
        thrown when a route is hidden by a looser one. 
    */
    /** Add a route that will match the given path and filter and will
        delegate to the given sub-route.
    */
    void addRoute(PathSpec path, RequestFilter filter,
                  const std::shared_ptr<RestRequestRouter> & handler,
                  ExtractObject extractObject = nullptr);

    /** Add a terminal route with the given path and filter that will call
        the given callback.
    */
    void addRoute(PathSpec path, RequestFilter filter,
                  const Utf8String & description,
                  const OnProcessRequest & cb,
                  const Json::Value & argHelp,
                  ExtractObject extractObject = nullptr);

    void addHelpRoute(PathSpec path, RequestFilter filter);
    void addAutodocRoute(PathSpec autodocPath, PathSpec helpPath,
                         const std::string & autodocFilesPath);

    virtual void getHelp(Json::Value & result,
                         const Utf8String & currentPath,
                         const std::set<std::string> & verbs) const;
    virtual void updateFromValueDescription(Json::Value & v,
                                            const ValueDescription * vd) const;
    virtual void addValueDescriptionToProperties(
        const ValueDescription * vd,
        Json::Value & properties, int recur = 0) const;
    virtual void addJsonParamsToProperties(const Json::Value & params,
                                           Json::Value & properties) const;
    virtual void getAutodocHelp(Json::Value & result,
                                const Utf8String & currentPath,
                                const std::set<std::string> & verbs) const;

    /** Create a generic sub router. */
    RestRequestRouter &
    addSubRouter(PathSpec path, const Utf8String & description,
                 ExtractObject extractObject = nullptr,
                 std::shared_ptr<RestRequestRouter> subRouter = nullptr);

    /** In the normal case, we don't create an ExtractObject function. */
    static ExtractObject getExtractObject(const void *)
    {
        return nullptr;
    }

    /** Where the class has a getObject() function that takes a RestRequestParsingContext,
        we do create an ExtractObject function.
    */
    template<typename T>
    static ExtractObject getExtractObject(T * val,
                                          decltype(std::declval<T *>()->getObject(std::declval<RestRequestParsingContext>())) * = 0)
    {
        return [=] (const ConnectionId & connection,
                    const RestRequest & request,
                    RestRequestParsingContext & context) -> int
            {
                return val->getObject(context);
            };
    }

    /** Create a sub router of a specific type. */
    template<typename T, typename... Args>
    T &
    addSubRouter(PathSpec path, const Utf8String & description, Args &&... args)
    {
        // TODO: check it doesn't exist
        Route route;
        route.path = path;
        auto res = std::make_shared<T>(std::forward<Args>(args)...);
        route.router = res;
        route.router->description = description;
        route.extractObject = getExtractObject(res.get());
        subRoutes.push_back(route);
        return *res;
    }

    static void defaultNotFoundHandler (RestConnection & connection,
                                        const RestRequest & request);
    
    OnProcessRequest rootHandler;
    OnNotFoundRequest notFoundHandler;
    std::vector<Route> subRoutes;
    Utf8String description;
    bool terminal;
    Json::Value argHelp;
};

/** Send an HTTP response in response to an exception. */
RestRequestMatchResult
sendExceptionResponse(RestConnection & connection,
                      const std::exception & exc);

/** Turn an exception into a structure containing the information contained
    within it.
*/
 Json::Value extractException(const std::exception & exc, int defaultCode);


} // namespace MLDB
