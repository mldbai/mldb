/** sensor_collection.h                                            -*- C++ -*-
    Jeremy Barnes, 4 December 2014
    Copyright (c) 2014 Datacratic Inc.  All rights reserved.
    This file is part of MLDB. Copyright 2015 Datacratic. All rights reserved.

    Interface for sensors into MLDB.
*/

#pragma once

#include "mldb/core/sensor.h"
#include "mldb/rest/poly_collection.h"

namespace Datacratic {
namespace MLDB {

/*****************************************************************************/
/* SENSOR COLLECTION                                                         */
/*****************************************************************************/

struct SensorCollection: public PolyCollection<Sensor> {
    SensorCollection(MldbServer * server);

    static void initRoutes(RouteManager & manager);

    virtual Any getEntityStatus(const Sensor & sensor) const;
};

} // namespace MLDB

extern template class PolyCollection<MLDB::Sensor>;

} // namespace Datacratic
