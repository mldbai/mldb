/** sensor_collection.h                                            -*- C++ -*-
    Jeremy Barnes, 4 December 2014
    Copyright (c) 2014 Datacratic Inc.  All rights reserved.
    This file is part of MLDB. Copyright 2015 Datacratic. All rights reserved.

    Interface for sensors into MLDB.
*/

#pragma once

#include "mldb/core/sensor.h"
#include "mldb/rest/poly_collection.h"

namespace MLDB {

/*****************************************************************************/
/* SENSOR COLLECTION                                                         */
/*****************************************************************************/

struct SensorCollection: public PolyCollection<Sensor> {
    SensorCollection(MldbEngine * engine);

    static void initRoutes(RouteManager & manager);

    virtual Any getEntityStatus(const Sensor & sensor) const;
};

extern template struct PolyCollection<MLDB::Sensor>;

} // namespace MLDB

