// This file is part of MLDB. Copyright 2015 Datacratic. All rights reserved.

/** algorithm.h                                                       -*- C++ -*-
    Jeremy Barnes, 4 December 2014
    Copyright (c) 2014 Datacratic Inc.  All rights reserved.

    Interface for algorithms into MLDB.
*/

#include "mldb/types/value_description.h"
#include "mldb_entity.h"

#pragma once

namespace Datacratic {
namespace MLDB {

/*****************************************************************************/
/* ALGORITHM                                                                 */
/*****************************************************************************/

struct Algorithm: public MldbEntity {
    Algorithm(MldbServer * server);

    virtual ~Algorithm();

    MldbServer * server;
    
    virtual Any getStatus() const = 0;

    virtual std::string getKind() const
    {
        return "algorithm";
    }
};

} // namespace MLDB
} // namespace Datacratic
