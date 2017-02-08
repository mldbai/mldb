/** type_collection.cc
    Jeremy Barnes, 2 March 2015
    Copyright (c) 2015 mldb.ai inc.  All rights reserved.

    This file is part of MLDB. Copyright 2015 mldb.ai inc. All rights reserved.
    
    Implementation of our collection of types.
*/

#include "type_collection.h"
#include "mldb/server/mldb_server.h"
#include "mldb/server/function_collection.h"
#include "mldb/server/plugin_collection.h"
#include "mldb/server/procedure_collection.h"
#include "mldb/server/dataset_collection.h"
#include "mldb/rest/rest_collection_impl.h"
#include "mldb/server/external_plugin.h"
#include "mldb/rest/rest_request_binding.h"


using namespace std;



namespace MLDB {


std::shared_ptr<TypeClassCollection>
createTypeClassCollection(MldbServer * server, RestRouteManager & routeManager)
{
    auto result = std::make_shared<TypeClassCollection>(server);

    auto getCollection = [=] (const RestRequestParsingContext & context)
        {
            return result.get();
        };

    auto collectionRouteManager
        = std::make_shared<typename TypeClassCollection::RouteManager>
        (routeManager, *routeManager.parentNode,
         routeManager.resourceElementsMatched + 1,
         getCollection,
         "type", "types");
    TypeClassCollection::initRoutes(*collectionRouteManager);
    
    // Save our child route
    routeManager.childRoutes["types"] = collectionRouteManager;

    server->addEntity("types", *result);

    return result;
}


/*****************************************************************************/
/* TYPE COLLECTION                                                           */
/*****************************************************************************/

struct TypeEntry {
};

template<typename Base>
struct TypeCollection: public RestCollection<Utf8String, TypeEntry> {
    TypeCollection(const Utf8String & nounSingular,
                   const Utf8String & nounPlural,
                   MldbServer * server, RestEntity * parent)
        : RestCollection<Utf8String, TypeEntry>(nounSingular, nounPlural, parent),
          parent(parent), name(MLDB::type_name<Base>()),
          server(server)
    {
        entryWatch = PolyCollection<Base>::watchTypes(true /* catchup */);
        entryWatch.bind(std::bind(&TypeCollection::onNewEntry, this,
                                  std::placeholders::_1));
    }

    static void initRoutes(RouteManager & result)
    {
        // Get a list of types with details.  This needs to happen before
        // the default routes are initialized.

        RestCollection<Utf8String, TypeEntry>::initNodes(result);

        addRouteSyncJsonReturn(*result.collectionNode,
                               "", { "GET", "details=true"},
                               "Get the list of types, with details",
                               "Array with list of details",
                               &TypeCollection::getListWithDetails,
                               result.getCollection);

        RestCollection<Utf8String, TypeEntry>::initRoutes(result);

        // Handle documentation
        auto getDocRoute = [=] (RestConnection & connection,
                                const RestRequest & req,
                                const RestRequestParsingContext & cxt)
            {
                auto & server = cxt.getObjectAs<MldbServer>(0);
                auto type = result.getKey(cxt);
                
                return PolyCollection<Base>
                    ::handleDocRequest(&server, type, connection, req, cxt);
            };

        Json::Value help;
        help["result"] = "Documentation resource requested";

        result.valueNode->addRoute("/doc", { "GET" },
                                   "Access plugin type documentation",
                                   getDocRoute, help);

        // Handle configuration type
        auto getInfoRoute = [=] (RestConnection & connection,
                                const RestRequest & req,
                                const RestRequestParsingContext & cxt)
            {
                auto & server = cxt.getObjectAs<MldbServer>(0);
                auto type = result.getKey(cxt);
                
                return PolyCollection<Base>
                ::handleInfoRequest(&server, result.nounPlural,
                                    type, connection, req, cxt);
            };
        
        help["result"] = "Information on given plugin type";
        
        result.valueNode->addRoute("/info", { "GET" },
                                   "Access plugin type information",
                                   getInfoRoute, help);
        
        // Handle a custom route
        auto getCustomRoute = [=] (RestConnection & connection,
                                   const RestRequest & req,
                                   const RestRequestParsingContext & cxt)
            {
                auto & server = cxt.getObjectAs<MldbServer>(0);
                auto type = result.getKey(cxt);
                
                return PolyCollection<Base>
                    ::handleCustomRequest(&server, type, connection, req, cxt);
            };

        help["result"] = "Output of requested custom route";

        result.valueNode->addRoute(Rx("/routes/(.*)", "<route>"), { "GET", "POST", "PUT", "DELETE", "HEAD" },
                                      "Access plugin type custom routes",
                                      getCustomRoute, help);
    }

    bool isCollection() const { return true; }
    
    Utf8String getDescription() const { return "Collection of classes of type " + MLDB::type_name<Base>(); }

    Utf8String getName() const { return name; }

    RestEntity * getParent() const { return parent; }

    std::vector<std::pair<Utf8String, Json::Value> >
    getListWithDetails() const
    {
        std::vector<std::pair<Utf8String, Json::Value> > result;
        
        auto onEntry = [&] (const Utf8String & typeName,
                            const TypeEntry & entry)
            {
                result.emplace_back
                (typeName, PolyCollection<Base>::getTypeInfo(nounPlural, typeName));
                return true;
            };

        forEachEntry(onEntry);

        return result;
    }

    RestEntity * parent;
    Utf8String name;
    WatchT<Utf8String> entryWatch;
    MldbServer * server;

    /** This is called by the watch when a new instance of the given type
        appears.
    */
    void onNewEntry(const Utf8String & entry)
    {
        this->addEntry(entry, std::make_shared<TypeEntry>(), true /* mustAdd */);
    }
};

/*****************************************************************************/
/* TYPE CLASS COLLECTION                                                     */
/*****************************************************************************/

TypeClassCollection::
TypeClassCollection(MldbServer * server)
    : RestDirectory(server, "Operations on classes of types registered in MLDB"),
      plugins(new TypeCollection<Plugin>("plugin", "plugins", server, this)),
      datasets(new TypeCollection<Dataset>("dataset", "datasets", server, this)),
      procedures(new TypeCollection<Procedure>("procedure", "procedures", server, this)),
      functions(new TypeCollection<Function>("function", "functions", server, this)),
      pluginSetup(new TypeCollection<ExternalPluginSetup>("plugin.setup", "plugin.setups", server, this)),
      pluginStartup(new TypeCollection<ExternalPluginStartup>("plugin.startup", "plugin.startups", server, this))
{
    addEntity("plugins", plugins);
    addEntity("functions", functions);
    addEntity("procedures", procedures);
    addEntity("datasets", datasets);
    addEntity("plugin.setups", pluginSetup);
    addEntity("plugin.startups", pluginStartup);
}

template<typename Type>
void initRoutesOfType(std::shared_ptr<TypeCollection<Type> > TypeClassCollection::* var,
                      TypeClassCollection::RouteManager & manager,
                      const Utf8String & nounSingular,
                      const Utf8String & nounPlural)
{
    // To get the collection (type list) from the context, we need to go through
    // this convoluted mechanism
    auto getCollection = [=] (const RestRequestParsingContext & context)
        {
            return (dynamic_cast<TypeClassCollection &>(context.getObjectAs<RestDirectory>(1)).*var).get();
        };
    
    auto typeManager = std::make_shared<typename TypeCollection<Type>::RouteManager>
        (manager, *manager.collectionNode, manager.resourceElementsMatched + 2,
         getCollection, nounSingular, nounPlural);
    manager.childRoutes[nounSingular] = typeManager;
    TypeCollection<Type>::initRoutes(*typeManager);
}

void
TypeClassCollection::
initRoutes(RouteManager & manager)
{
    RestDirectory::initRoutes(manager);

    ExcAssert(manager.collectionNode);

    initRoutesOfType(&TypeClassCollection::plugins, manager, "plugin", "plugins");
    initRoutesOfType(&TypeClassCollection::functions, manager, "function", "functions");
    initRoutesOfType(&TypeClassCollection::procedures, manager, "procedure", "procedures");
    initRoutesOfType(&TypeClassCollection::datasets, manager, "dataset", "datasets");
    initRoutesOfType(&TypeClassCollection::pluginSetup, manager, "plugin.setup", "plugin.setups");
    initRoutesOfType(&TypeClassCollection::pluginSetup, manager, "plugin.startup", "plugin.startups");
}

} // namespace MLDB


