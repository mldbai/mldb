// This file is part of MLDB. Copyright 2015 mldb.ai inc. All rights reserved.

/** procedure_run_collection.h                                 -*- C++ -*-
    Jeremy Barnes, 4 December 2014
    Copyright (c) 2014 mldb.ai inc.  All rights reserved.

    Procedure training infrastructure.
*/

#pragma once

#include "mldb/core/procedure.h"
#include "mldb/rest/rest_collection.h"
#include "mldb/rest/service_peer.h"


namespace MLDB {

/*****************************************************************************/
/* PROCEDURE TRAINING COLLECTION                                             */
/*****************************************************************************/

struct ProcedureRunCollection
    : public RestConfigurableCollection<Utf8String,
                                        ProcedureRun,
                                        ProcedureRunConfig,
                                        ProcedureRunStatus> {

    ProcedureRunCollection(ServicePeer * server, Procedure * owner);
    
    ServicePeer * server;
    Procedure * procedure;
        
    static void initRoutes(RouteManager & manager);

    void init(std::shared_ptr<CollectionConfigStore> config);

    virtual Utf8String getKey(ProcedureRunConfig & config);

    virtual void setKey(ProcedureRunConfig & config, Utf8String key);

    virtual ProcedureRunStatus
    getStatusLoading(Utf8String key, const BackgroundTask & task) const;

    virtual std::shared_ptr<ProcedureRunConfig>
    getConfig(Utf8String key, const ProcedureRun & value) const;

    virtual ProcedureRunStatus
    getStatusFinished(Utf8String key, const ProcedureRun & value) const;

    virtual std::shared_ptr<ProcedureRun>
    construct(ProcedureRunConfig config, const OnProgress & onProgress) const;
};

//extern template class PolyCollection<MLDB::ProcedureRun>;
DECLARE_REST_COLLECTION_INSTANTIATIONS(Utf8String,
                                       ProcedureRun,
                                       ProcedureRunConfig,
                                       ProcedureRunStatus);

} // namespace MLDB






