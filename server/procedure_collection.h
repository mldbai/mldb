// This file is part of MLDB. Copyright 2015 mldb.ai inc. All rights reserved.

/** procedure_collection.h                                           -*- C++ -*-
    Jeremy Barnes, 4 December 2014
    Copyright (c) 2014 mldb.ai inc.  All rights reserved.

    Interface for procedures into MLDB.
*/

#pragma once

#include "mldb/core/procedure.h"
#include "mldb/rest/poly_collection.h"


namespace MLDB {

/*****************************************************************************/
/* PROCEDURE COLLECTION                                                      */
/*****************************************************************************/

struct ProcedureCollection: public PolyCollection<Procedure> {
    ProcedureCollection(MldbServer * server);

    static void initRoutes(RouteManager & manager);

    virtual Any getEntityStatus(const Procedure & procedure) const;

    /* 
     * [MLDB-951] Overrride these two functions to allow different execution of the 
     * request if the parameter runOnCreation is present in the config.
     */
    virtual PolyStatus handlePutSync(Utf8String key, PolyConfig config, bool mustBeNew /* = false */);
    virtual PolyStatus handlePut(Utf8String key, PolyConfig config, bool mustBeNew /* = false */);

private:
    virtual PolyStatus handlePutWithFirstRun(Utf8String key, PolyConfig config, bool mustBeNew, bool async);
    MldbServer * mldb;
};

extern template class PolyCollection<Procedure>;

} // namespace MLDB



