// This file is part of MLDB. Copyright 2015 mldb.ai inc. All rights reserved.

/** type_collection.h                                              -*- C++ -*-
    Jeremy Barnes, 2 March 2015
    Copyright (c) 2015 mldb.ai inc.  All rights reserved.

*/

#pragma once

#include "mldb/rest/rest_entity.h"


namespace MLDB {

struct MldbServer;
struct Plugin;
struct Algorithm;
struct Procedure;
struct Function;
struct Dataset;
struct ExternalPluginSetup;
struct ExternalPluginStartup;

template<typename Base> struct TypeCollection;


/*****************************************************************************/
/* TYPE CLASS COLLECTION                                                     */
/*****************************************************************************/

/** Our collection of types, categorized by parent type. */

struct TypeClassCollection: public RestDirectory {
    TypeClassCollection(MldbServer * server);

    static void initRoutes(RouteManager & manager);

    std::shared_ptr<TypeCollection<Plugin> > plugins;
    std::shared_ptr<TypeCollection<Dataset> > datasets;
    std::shared_ptr<TypeCollection<Algorithm> > algorithms;
    std::shared_ptr<TypeCollection<Procedure> > procedures;
    std::shared_ptr<TypeCollection<Function> > functions;
    std::shared_ptr<TypeCollection<ExternalPluginSetup> > pluginSetup;
    std::shared_ptr<TypeCollection<ExternalPluginStartup> > pluginStartup;
};


} // namespace MLDB

