// This file is part of MLDB. Copyright 2015 Datacratic. All rights reserved.

/** function_collection.h                                           -*- C++ -*-
    Jeremy Barnes, 4 December 2014
    Copyright (c) 2014 Datacratic Inc.  All rights reserved.

    Interface for functions into MLDB.
*/

#pragma once

#include "mldb/core/function.h"
#include "mldb/rest/poly_collection.h"

namespace Datacratic {

struct RestConnection;

namespace MLDB {


/*****************************************************************************/
/* FUNCTION COLLECTION                                                      */
/*****************************************************************************/

struct FunctionCollection: public PolyCollection<Function> {
    FunctionCollection(MldbServer * server);

    static void initRoutes(RouteManager & manager);

    virtual Any getEntityStatus(const Function & function) const;

    void applyFunction(const Function * function,
                    const std::map<Utf8String, ExpressionValue> & input,
                    const std::vector<Utf8String> & keepPins,
                    RestConnection & connection) const;

    FunctionInfo getFunctionInfo(const Function * function) const;
};

} // namespace MLDB

extern template class PolyCollection<MLDB::Function>;

} // namespace Datacratic
