// This file is part of MLDB. Copyright 2015 mldb.ai inc. All rights reserved.

/** plugin.h                                                       -*- C++ -*-
    Jeremy Barnes, 4 December 2014
    Copyright (c) 2014 mldb.ai inc.  All rights reserved.

    Interface for plugins into MLDB.
*/

#pragma once

#include "mldb/core/plugin.h"
#include "mldb/rest/poly_collection.h"


namespace MLDB {

/*****************************************************************************/
/* PLUGIN COLLECTION                                                         */
/*****************************************************************************/

struct PluginCollection: public PolyCollection<Plugin> {
    PluginCollection(MldbServer * server);

    static void initRoutes(RouteManager & manager);

    virtual Any getEntityStatus(const Plugin & plugin) const;
};

extern template class PolyCollection<Plugin>;

} // namespace MLDB



