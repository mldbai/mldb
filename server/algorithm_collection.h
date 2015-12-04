// This file is part of MLDB. Copyright 2015 Datacratic. All rights reserved.

/** algorithm_collection.h                                           -*- C++ -*-
    Jeremy Barnes, 4 December 2014
    Copyright (c) 2014 Datacratic Inc.  All rights reserved.

    Interface for algorithms into MLDB.
*/

#pragma once

#include "mldb/server/algorithm.h"
#include "mldb/rest/poly_collection.h"

namespace Datacratic {
namespace MLDB {

/*****************************************************************************/
/* ALGORITHM COLLECTION                                                      */
/*****************************************************************************/

struct AlgorithmCollection: public PolyCollection<Algorithm> {
    AlgorithmCollection(MldbServer * server);

    virtual Any getEntityStatus(const Algorithm & algorithm) const;
};

} // namespace MLDB

extern template class PolyCollection<MLDB::Algorithm>;

} // namespace Datacratic
