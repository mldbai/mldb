/** function_collection.h                                           -*- C++ -*-
    Jeremy Barnes, 4 December 2014
    Copyright (c) 2014 mldb.ai inc.  All rights reserved.
    This file is part of MLDB. Copyright 2015 mldb.ai inc. All rights reserved.

    Interface for functions into MLDB.
*/

#pragma once

#include "mldb/core/function.h"
#include "mldb/rest/poly_collection.h"

struct RestConnection;

namespace MLDB {


/*****************************************************************************/
/* FUNCTION COLLECTION                                                      */
/*****************************************************************************/

struct FunctionCollection: public PolyCollection<Function> {
    FunctionCollection(MldbServer * server);

    static void initRoutes(RouteManager & manager);

    virtual Any getEntityStatus(const Function & function) const;

    virtual std::shared_ptr<PolyEntity>
    construct(PolyConfig config, const OnProgress & onProgress) const;

    void applyFunction(const Function * function,
                       const std::map<Utf8String, ExpressionValue> & qsInput,
                       const std::vector<Utf8String> & keepValues,
                       const std::string & outputFormat,
                       RestConnection & connection) const;
    
    void applyBatch(const Function * function,
                    const Json::Value & inputs,
                    const std::string & inputFormat,
                    const std::string & outputFormat,
                    RestConnection & connection) const;
    
    static ExpressionValue call(MldbServer * server,
                               const Function * function,
                               const std::map<Utf8String, ExpressionValue> & input,
                               const std::vector<Utf8String> & keepPins);
    
    FunctionInfo getFunctionInfo(const Function * function) const;
};

extern template class PolyCollection<Function>;

} // namespace MLDB




