/** mldb_entity.h                                                  -*- C++ -*-
    Jeremy Barnes, 2 March 2015
    Copyright (c) 2015 mldb.ai inc.  All rights reserved.

    This file is part of MLDB. Copyright 2015 mldb.ai inc. All rights reserved.

    Base class for MLDB entities (plugin, function, etc).
*/

#pragma once

#include "mldb/rest/poly_entity.h"
#include "mldb/utils/log_fwd.h"



struct RestDirectory;
struct ServicePeer;

namespace MLDB {

struct MldbServer;


/** This is the builtin package, which comes linked in to MLDB.  All
    entity types that are built in need to pass this as their
    package parameter.

    Defined in mldb_server.cc.
*/

const Package & builtinPackage();


/*****************************************************************************/
/* MLDB ENTITY                                                               */
/*****************************************************************************/

/** This is the base class of all entities within MLDB, containing the
    base functionality that they all support.
*/

struct MldbEntity: public PolyEntity {

    virtual Any getStatus() const = 0;

    virtual std::string getKind() const = 0;

    static MldbServer * getOwner(RestDirectory * peer)
    {
        // TODO: go through proper upcast that knows both types in cc file
        return (MldbServer *)peer;
    }

    static MldbServer * getOwner(ServicePeer * peer)
    {
        // TODO: go through proper upcast that knows both types in cc file
        return (MldbServer *)peer;
    }

    static RestDirectory * getPeer(MldbServer * server)
    {
        // TODO: go through proper downcast
        return (RestDirectory *)server;
    }

    static constexpr const char * INTERNAL_ENTITY  = "INTERNAL_ENTITY";
    std::shared_ptr<spdlog::logger> logger;
};


/*****************************************************************************/
/* UTILITY FUNCTIONS                                                         */
/*****************************************************************************/

/** Create a request handler that redirects to the given place for internal
    documentation.

    Defined in mldb_server.cc.
*/
TypeCustomRouteHandler 
makeInternalDocRedirect(const Package & package,
                        const Utf8String & relativePath);



} // namespace MLDB

