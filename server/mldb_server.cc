/** mldb_server.cc
    Jeremy Barnes, 12 December 2014
    Copyright (c) 2014 mldb.ai inc.  All rights reserved.

    This file is part of MLDB. Copyright 2015 mldb.ai inc. All rights reserved.

    Server for MLDB.
*/

#include "mldb/arch/arch.h"
#include "mldb/server/mldb_server.h"
#include "mldb/rest/etcd_peer_discovery.h"
#include "mldb/rest/asio_peer_server.h"
#include "mldb/rest/standalone_peer_server.h"
#include "mldb/rest/collection_config_store.h"
#include "mldb/rest/http_rest_endpoint.h"
#include "mldb/rest/rest_request_binding.h"
#include "mldb/rest/in_process_rest_connection.h"
#include "mldb/vfs/fs_utils.h"
#include "mldb/server/static_content_handler.h"
#include "mldb/server/plugin_manifest.h"
#include "mldb/server/plugin_resource.h"
#include "mldb/sql/sql_expression.h"
#include <signal.h>

#include "mldb/server/dataset_collection.h"
#include "mldb/server/plugin_collection.h"
#include "mldb/server/procedure_collection.h"
#include "mldb/server/function_collection.h"
#include "mldb/server/sensor_collection.h"
#include "mldb/server/credential_collection.h"
#include "mldb/server/dataset_context.h"
#include "mldb/vfs/fs_utils.h"
#include "mldb/vfs/filter_streams.h"
#include "mldb/server/analytics.h"
#include "mldb/types/meta_value_description.h"
#include "mldb/arch/simd.h"
#include "mldb/utils/log.h"


using namespace std;


namespace MLDB {

namespace {
bool supportsSystemRequirements() {
#if MLDB_INTEL_ISA
    return has_sse42();
#else
    return true;
#endif
}
} // file scope


// Creation functions exposed elsewhere
std::shared_ptr<PluginCollection>
createPluginCollection(MldbServer * server, RestRouteManager & routeManager);

std::shared_ptr<DatasetCollection>
createDatasetCollection(MldbServer * server, RestRouteManager & routeManager);

std::shared_ptr<ProcedureCollection>
createProcedureCollection(MldbServer * server, RestRouteManager & routeManager);

std::shared_ptr<FunctionCollection>
createFunctionCollection(MldbServer * server, RestRouteManager & routeManager);

std::shared_ptr<SensorCollection>
createSensorCollection(MldbServer * server, RestRouteManager & routeManager);

std::shared_ptr<CredentialRuleCollection>
createCredentialCollection(MldbServer * server, RestRouteManager & routeManager,
                      std::shared_ptr<CollectionConfigStore> configStore);

std::shared_ptr<TypeClassCollection>
createTypeClassCollection(MldbServer * server, RestRouteManager & routeManager);


/*****************************************************************************/
/* MLDB SERVER                                                               */
/*****************************************************************************/

MldbServer::
MldbServer(const std::string & serviceName,
           const std::string & etcdUri,
           const std::string & etcdPath,
           bool enableAccessLog,
           const std::string & httpBaseUrl)
    : ServicePeer(serviceName, "MLDB", "global", enableAccessLog),
      EventRecorder(serviceName, std::make_shared<NullEventService>()),
      httpBaseUrl(httpBaseUrl), versionNode(nullptr),
      logger(getMldbLog<MldbServer>())
{
    // Don't allow URIs without a scheme
    setGlobalAcceptUrisWithoutScheme(false);

    addRoutes();

    if (etcdUri != "")
        initDiscovery(std::make_shared<EtcdPeerDiscovery>(this, etcdUri, etcdPath));
    else
        initDiscovery(std::make_shared<SinglePeerDiscovery>(this));
}

MldbServer::
~MldbServer()
{
    shutdown();
}

bool
MldbServer::
init(std::string credentialsPath,
     std::string staticFilesPath,
     std::string staticDocPath,
     bool hideInternalEntities)
{
    auto server = std::make_shared<StandalonePeerServer>();

    preInit();
    initServer(server);
    if (initRoutes()) { // if initRoutes fails no need to add collections to routes
        initCollections(credentialsPath, staticFilesPath, staticDocPath, hideInternalEntities);
        return true;
    }
    return false;
}

void
MldbServer::
preInit()
{
    //Because of a multithread issue in boost, we need to call this to force boost::date_time to initialize in single thread
    //better do it as early as possible
    Date::now().weekday();
}

bool
MldbServer::
initRoutes()
{
    router.description = "Machine Learning Database REST API";

    RestRequestRouter::OnProcessRequest serviceInfoRoute
        = [=] (RestConnection & connection,
               const RestRequest & request,
               const RestRequestParsingContext & context) {
        Json::Value result;
        result["apiVersions"]["v1"] = "1.0.0";
        connection.sendResponse(200, result);
        return RestRequestRouter::MR_YES;
    };

    router.addHelpRoute("/v1/help", "GET");

    router.addRoute("/info", "GET", "Return service information (version, etc)",
                    serviceInfoRoute,
                    Json::Value());

    // Push our this pointer in to make sure that it's available to sub
    // routes
    auto addObject = [=] (RestConnection & connection,
                          const RestRequest & request,
                          RestRequestParsingContext & context)
        {
            context.addObject(this);
        };

    auto & versionNode = router.addSubRouter("/v1", "version 1 of API",
                                             addObject);

    RestRequestRouter::OnProcessRequest handleShutdown
        = [=] (RestConnection & connection,
               const RestRequest & request,
               const RestRequestParsingContext & context) {

        kill(getpid(), SIGUSR2);

        Json::Value result;
        result["shutdown"] = true;
        connection.sendResponse(200, result);
        return RestRequestRouter::MR_YES;
    };

    addRouteSyncJsonReturn(versionNode, "/typeInfo", {"GET"},
                           "Get type dictionary for a type",
                           "JSON description of type structure",
                           &MldbServer::getTypeInfo,
                           this,
                           RestParam<std::string>("type", "The type to look up"));

    versionNode.addRoute("/shutdown", "POST", "Shutdown the service",
                         handleShutdown,
                         Json::Value());


   // MLDB-1380 - make sure that the CPU support the minimal instruction sets
    if (supportsSystemRequirements()) {
        const auto queryStringDef = "The string representing the SQL query. "
                                    "Must be defined either as a query string "
                                    "parameter or the JSON body.";
        addRouteAsync(
            versionNode, "/query", { "GET" }, "Select from dataset",
            &MldbServer::runHttpQuery, this,
            HybridParamDefault<Utf8String>("q", queryStringDef, ""),
            PassConnectionId(),
            HybridParamDefault<std::string>("format",
                                            "Format of output",
                                            "full"),
            HybridParamDefault<bool>("headers",
                                     "Do we include headers on table format",
                                      true),
            HybridParamDefault<bool>("rowNames",
                                     "Do we include row names in output",
                                     true),
            HybridParamDefault<bool>("rowHashes",
                                     "Do we include row hashes in output",
                                     false),
            HybridParamDefault<bool>("sortColumns",
                                     "Do we sort the column names",
                                     false));

        addRouteAsync(
            versionNode, "/redirect/get", {"POST"}, "Redirect POST as GET with body. "
            "Use this route only with systems that do not support sending a GET with a body.",
            &MldbServer::handleRedirectToGet, this,
            PassConnectionId(),
            PassRequest(),
            JsonParam<std::string>("target", "the URI to redirect to"),
            JsonParam<Json::Value>("body", "The body to pass to the redirect"));

        this->versionNode = &versionNode;
        return true;
    } else {
        static constexpr auto errorMessage =
            "*** ERROR ***\n"
            "* MLDB requires a cpu with minimally SSE 4.2 instruction set. *\n"
            "* This system does not support SSE 4.2, therefore most of the *\n"
            "* functionality in MLDB has been disabled.                    *\n"
            "* Please try MLDB on a system with a more recent cpu.         *\n"
            "*** ERROR ***";

        versionNode.notFoundHandler = [&] (RestConnection & connection,
                                           const RestRequest & request) {
            connection.sendErrorResponse(500, errorMessage);
        };

        router.notFoundHandler = versionNode.notFoundHandler;
        logger->error() << errorMessage;
        this->versionNode = &versionNode;
        return false;
    }
}

void
MldbServer::
runHttpQuery(const Utf8String& query,
             RestConnection & connection,
             const std::string & format,
             bool createHeaders,
             bool rowNames,
             bool rowHashes,
             bool sortColumns) const
{
    auto stm = SelectStatement::parse(query.rawString());
    SqlExpressionMldbScope mldbContext(this);

    auto runQuery = [&] ()
        {
            return queryFromStatement(stm, mldbContext, nullptr /*onProgress*/);
        };

    MLDB::runHttpQuery(runQuery,
                       connection, format, createHeaders,
                       rowNames, rowHashes, sortColumns);
}

void
MldbServer::
handleRedirectToGet(RestConnection & connection,
                    const RestRequest & request,
                    const string & uri,
                    const Json::Value & body) const
{
    InProcessRestConnection redirectConnection;
    HttpHeader redirectHeader;
    redirectHeader.verb = "GET";
    redirectHeader.resource = uri;
    redirectHeader.headers = request.header.headers;

    RestRequest redirectRequest(redirectHeader, jsonEncodeStr(body));
    handleRequest(redirectConnection, redirectRequest);
                             
    Json::Value redirectResponse;
    Json::Reader reader;
    if (!reader.parse(redirectConnection.response, redirectResponse, false))
        throw HttpReturnException(500, "failed to parse the redirect call");
  
    if (200 > redirectConnection.responseCode || redirectConnection.responseCode >= 300)
        throw HttpReturnException(redirectConnection.responseCode, "failed to redirect call");
    
    connection.sendResponse(redirectConnection.responseCode, jsonEncodeStr(redirectResponse),
                            "application/json");
}

std::vector<MatrixNamedRow>
MldbServer::
query(const Utf8String& query) const
{
    auto stm = SelectStatement::parse(query.rawString());
    SqlExpressionMldbScope mldbContext(this);

    return queryFromStatement(stm, mldbContext, nullptr /*onProgress*/);
}

Json::Value
MldbServer::
getTypeInfo(const std::string & typeName)
{
    Json::Value result;
    auto vd = ValueDescription::get(typeName);
    if (!vd)
        return result;

    static std::shared_ptr<ValueDescriptionT<std::shared_ptr<const ValueDescription> > >
        desc = getValueDescriptionDescription(true /* detailed */);
    StructuredJsonPrintingContext context(result);
    desc->printJsonTyped(&vd, context);
    return result;
}

void
MldbServer::
initCollections(std::string credentialsPath,
                std::string staticFilesPath,
                std::string staticDocPath,
                bool hideInternalEntities)
{
    // MLDB-696 - ensure paths passed on the command line
    // are interpreted as file by default
    if (!credentialsPath.empty()
        && credentialsPath.find("://") == string::npos)
        credentialsPath = "file://" + credentialsPath;
    if (!staticFilesPath.empty()
        && staticFilesPath.find("://") == string::npos)
        staticFilesPath = "file://" + staticFilesPath;
    if (!staticDocPath.empty()
        && staticDocPath.find("://") == string::npos)
        staticDocPath = "file://" + staticDocPath;

    auto makeCredentialStore = [&credentialsPath] ()
        -> std::shared_ptr<CollectionConfigStore>
        {
            if (credentialsPath.empty())
                return nullptr;
            return std::make_shared<S3CollectionConfigStore>
            (credentialsPath);
        };

    ExcAssert(versionNode);
    routeManager.reset(new RestRouteManager(*versionNode, 1 /* elements in path: [ "/v1" ] */));

    plugins = createPluginCollection(this, *routeManager);
    datasets = createDatasetCollection(this, *routeManager);
    procedures = createProcedureCollection(this, *routeManager);
    functions = createFunctionCollection(this, *routeManager);
    sensors = createSensorCollection(this, *routeManager);
    credentials = createCredentialCollection(this, *routeManager, makeCredentialStore());
    types = createTypeClassCollection(this, *routeManager);

    plugins->loadConfig();
    datasets->loadConfig();
    procedures->loadConfig();
    functions->loadConfig();
    sensors->loadConfig();

    if (false) {
        logRequest = [&] (const HttpRestConnection & conn, const RestRequest & req)
            {
                this->recordHit("rest.request.count");
                this->recordHit("rest.request.verbs.%s", req.verb.c_str());
            };

        logResponse = [&] (const HttpRestConnection & conn,
                           int code,
                           const std::string & resp,
                           const std::string & contentType)
            {
                double processingTimeMs
                = Date::now().secondsSince(conn.startDate) * 1000.0;
                this->recordOutcome(processingTimeMs,
                                    "rest.response.processingTimeMs");
                this->recordHit("rest.response.codes.%d", code);
            };
    }

    // Serve up static documentation for the plugins
    serveDocumentationDirectory(router, "/doc",
                                staticDocPath, this, hideInternalEntities);

    serveDocumentationDirectory(router, "/resources",
                                staticFilesPath, this, hideInternalEntities);
}

void
MldbServer::
start()
{
    ServicePeer::start();
    // Graphite logging: just log a message bracketing service startup
    recordHit("serviceStarted");
}

void
MldbServer::
shutdown()
{
    httpEndpoint->closePeer();

    ServicePeer::shutdown();

    datasets.reset();
    procedures.reset();
    functions.reset();
    credentials.reset();
    sensors.reset();

    // Shutdown plugins last, since they may be needed to shut down the other
    // entities.
    plugins.reset();

    types.reset();

    // Graphite logging: just log a message bracketing service shutdown
    recordHit("serviceStopped");
}

static bool endsWith(const std::string & str,
                     const std::string & what)
{
    return str.rfind(what) == str.length() - what.length();
}

void
MldbServer::
scanPlugins(const std::string & dir_)
{
    DEBUG_MSG(logger) << "scanning plugins in directory " << dir_;

    std::string dir = dir_;

    auto foundPlugin = [&] (const std::string & dir,
                            std::istream & stream)
        {
            try {
                auto manifest = jsonDecodeStream<PluginManifest>(stream);
                if (manifest.config.type == "sharedLibrary") {
                    auto shlibConfig = manifest.config.params.convert<SharedLibraryConfig>();
                    // strip off the file:// prefix
                    shlibConfig.address = string(dir, 7);
                    shlibConfig.allowInsecureLoading = true;

                    manifest.config.params = shlibConfig;

                    auto plugin = plugins->obtainEntitySync(
                        manifest.config, nullptr /* on progress */);
                }
                else if (manifest.config.type == "python" ||
                         manifest.config.type == "javascript") {
                    auto config = manifest.config.params.convert<PluginResource>();
                    config.address = dir;
                    manifest.config.params = config;
                    auto plugin = plugins->obtainEntitySync(
                        manifest.config, nullptr /* on progress */);
                }
                else {
                    throw HttpReturnException(
                        500, "unknown plugin type to autoload at " + dir);
                }
            } catch (const HttpReturnException & exc) {
                logger->error() << "loading plugin " << dir << ": " << exc.what();
                logger->error() << "details:";
                logger->error() << jsonEncode(exc.details);
                logger->error() << "plugin will be ignored";
                return;
            } catch (const std::exception & exc) {
                logger->error() << "loading plugin " << dir << ": " << exc.what();
                logger->error() << "plugin will be ignored";
                return;
            }
        };

    auto info = tryGetUriObjectInfo(dir + "mldb_plugin.json");
    if (info) {
        filter_istream stream(dir + "mldb_plugin.json");
        foundPlugin(dir, stream);
    }
    else {
        auto onSubdir = [&] (const std::string & dirName,
                             int depth)
            {
                return true;
            };

        auto onFile = [&] (const std::string & uri,
                           const FsObjectInfo & info,
                           const OpenUriObject & open,
                           int depth)
            {
                if (endsWith(uri, "/mldb_plugin.json")) {
                    //filter_istream stream(open({}),
                    //                          uri, {});
                    filter_istream stream(uri);
                    foundPlugin(string(uri, 0, uri.length() - 16), stream);
                    return true;
                }
                return true;
            };

        try {
            forEachUriObject(dir, onFile, onSubdir);
        } catch (const HttpReturnException & exc) {
            logger->error() << "error scanning plugin directory "
                            << dir << ": " << exc.what();
            logger->error() << "details:";
            logger->error() << jsonEncode(exc.details);
            logger->error() << "plugins will be ignored";
            return;
        } catch (const std::exception & exc) {
            logger->error() << "error scanning plugin directory  "
                            << dir << ": " << exc.what();
            logger->error() << "plugins will be ignored";
            return;
        }
    }
}

Utf8String
MldbServer::
getPackageDocumentationPath(const Package & package) const
{
    // TODO: a plugin should tell MLDB what packages it provides.
    // Here we make an assumption that the package "pro" will
    // always be provided by the plugin "pro", but this is not
    // by any means guaranteed.

    if (package.packageName() == "builtin") {
        return "/doc/builtin/";
    }
    return "/v1/plugins/" + package.packageName() + "/doc/";
}

void
MldbServer::
setCacheDirectory(const std::string & dir)
{
    cacheDirectory_ = dir;
}

std::string
MldbServer::
getCacheDirectory() const
{
    return cacheDirectory_;
}

Utf8String
MldbServer::
prefixUrl(Utf8String url) const
{
    if (url.startsWith("/")) {
        return httpBaseUrl + url;
    }
    return url;
}

string
MldbServer::
prefixUrl(string url) const
{
    Utf8String str(url);
    return prefixUrl(str).rawString();
}

string
MldbServer::
prefixUrl(const char* url) const
{
    Utf8String str(url);
    return prefixUrl(str).rawString();
}

InProcessRestConnection
MldbServer::
restPerform(const std::string & verb,
            const Utf8String & resource,
            const RestParams & params,
            Json::Value payload,
            const RestParams & headers) const
{
    if (payload.isString()) {
        payload = Json::parse(payload.asString());
    }

    HttpHeader header;
    header.verb = verb;
    header.resource = resource.rawString();
    header.queryParams = params;
    for (auto & h: headers) {
        header.headers.insert({h.first.toLower().rawString(),
                               h.second.rawString()});
    }

    RestRequest request(header,
                        payload.isNull() ? "" : payload.toStringNoNewLine());

    InProcessRestConnection connection;

    handleRequest(connection, request);

    return connection;
}

InProcessRestConnection
MldbServer::
restGet(const Utf8String & resource, const RestParams & params) const {
    return restPerform("GET", resource, params);
}

InProcessRestConnection
MldbServer::
restDelete(const Utf8String & resource, const RestParams & params) const {
    return restPerform("DELETE", resource, params);
}

InProcessRestConnection
MldbServer::
restPut(const Utf8String & resource, const RestParams & params,
        const Json::Value payload) const {
    return restPerform("PUT", resource, params, std::move(payload));
}

InProcessRestConnection
MldbServer::
restPost(const Utf8String & resource, const RestParams & params,
         const Json::Value payload) const {
    return restPerform("POST", resource, params, std::move(payload));
}



namespace {
struct OnInit {
    OnInit()
    {
        setUrlDocumentationUri("/doc/builtin/Url.md");
    }
} onInit;
}  // file scope


/*****************************************************************************/
/* UTILITY FUNCTIONS                                                         */
/*****************************************************************************/

/** Create a request handler that redirects to the given place for internal
    documentation.
*/
TypeCustomRouteHandler
makeInternalDocRedirect(const Package & package, const Utf8String & relativePath)
{
    return [=] (RestDirectory * server,
                RestConnection & connection,
                const RestRequest & req,
                const RestRequestParsingContext & cxt)
        {
            Utf8String basePath = static_cast<MldbServer *>(server)
                ->getPackageDocumentationPath(package);
            connection.sendRedirect(301, (basePath + relativePath).rawString());
            return RestRequestRouter::MR_YES;
        };
}


const Package & builtinPackage()
{
    static const Package result("builtin");
    return result;
}

} // namespace MLDB

