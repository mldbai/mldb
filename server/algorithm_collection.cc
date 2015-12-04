// This file is part of MLDB. Copyright 2015 Datacratic. All rights reserved.

/** algorithm_collection.cc
    Jeremy Barnes, 24 November 2014
    Copyright (c) 2014 Datacratic Inc.  All rights reserved.

    Collection of algorithms.
*/

#include "mldb/server/algorithm_collection.h"
#include "mldb/rest/poly_collection_impl.h"
#include "mldb/server/mldb_server.h"


namespace Datacratic {
namespace MLDB {

std::shared_ptr<AlgorithmCollection>
createAlgorithmCollection(MldbServer * server, RestRouteManager & routeManager,
                          std::shared_ptr<CollectionConfigStore> configStore)
{
    return createCollection<AlgorithmCollection>(2, "algorithm", "algorithms",
                                                 server, routeManager,
                                                 configStore);
}

/*****************************************************************************/
/* ALGORITHM COLLECTION                                                      */
/*****************************************************************************/

AlgorithmCollection::
AlgorithmCollection(MldbServer * server)
    : PolyCollection<Algorithm>("algorithm", "algorithms", server)
{
}

Any
AlgorithmCollection::
getEntityStatus(const Algorithm & algorithm) const
{
    return algorithm.getStatus();
}

} // namespace MLDB

template class PolyCollection<MLDB::Algorithm>;

} // namespace Datacratic
