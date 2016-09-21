/* sensor.cc
   Jeremy Barnes, 21 January 2014
   Copyright (c) 2014 Datacratic Inc.  All rights reserved.

   This file is part of MLDB. Copyright 2015 Datacratic. All rights reserved.

   Sensor support.
*/

#include "mldb/core/sensor.h"
#include "mldb/core/mldb_engine.h"
#include "mldb/engine/sensor_collection.h"
#include "mldb/types/basic_value_descriptions.h"
#include "mldb/types/any_impl.h"
#include "mldb/sql/expression_value.h"
#include "mldb/sql/builtin_functions.h"
#include "mldb/rest/rest_request_router.h"

namespace MLDB {


std::shared_ptr<Sensor>
obtainSensor(MldbEngine * engine,
             const PolyConfig & config,
             const MldbEngine::OnProgress & onProgress)
{
    return engine->obtainSensorSync(config, onProgress);
}

std::shared_ptr<Sensor>
createSensor(MldbEngine * engine,
             const PolyConfig & config,
             const std::function<bool (const Json::Value & progress)> & onProgress)
{
    return engine->createSensorSync(config, onProgress);
}


/*****************************************************************************/
/* SENSOR                                                                    */
/*****************************************************************************/

Sensor::
Sensor(MldbEngine * engine)
    : engine(static_cast<MldbEngine *>(engine))
{
}

Sensor::
~Sensor()
{
}

Any
Sensor::
getStatus() const
{
    return Any();
}
    
Any
Sensor::
getVersion() const
{
    return Any();
}

RestRequestMatchResult
Sensor::
handleRequest(RestConnection & connection,
              const RestRequest & request,
              RestRequestParsingContext & context) const
{
    Json::Value error;
    error["error"] = "Sensor of type '" + type_name(*this)
        + "' does not respond to custom route '" + context.remaining + "'";
    error["details"]["verb"] = request.verb;
    error["details"]["resource"] = request.resource;
    connection.sendErrorResponse(400, error);
    return MR_ERROR;
}

RestRequestMatchResult
Sensor::
handleDocumentationRoute(RestConnection & connection,
                         const RestRequest & request,
                         RestRequestParsingContext & context) const
{
    return MR_NO;
}

RestRequestMatchResult
Sensor::
handleStaticRoute(RestConnection & connection,
                  const RestRequest & request,
                  RestRequestParsingContext & context) const
{
    return MR_NO;
}

BoundFunction readSensorFunction(const Utf8String & fnName,
                                 const std::vector<BoundSqlExpression> & args,
                                 SqlBindingScope & context)
{
    checkArgsSize(args.size(), 1, "read_sensor");

    Utf8String sensorId = args[0].constantValue().toUtf8String();
    
    PolyConfig config;
    config.id = sensorId;

    std::shared_ptr<Sensor> sensor
        = obtainSensor(context.getMldbEngine(),
                       config);
                       
    auto exec = [=] (const std::vector<ExpressionValue> & input,
                     const SqlRowScope & context)
        {
            return sensor->latest();
        };
    
    return BoundFunction(exec, sensor->resultInfo());
}

static RegisterFunction registerReadSensor("read_sensor", readSensorFunction);


} // namespace MLDB
