// This file is part of MLDB. Copyright 2015 Datacratic. All rights reserved.

/* algorithm.cc
   Jeremy Barnes, 21 January 2014
   Copyright (c) 2014 Datacratic Inc.  All rights reserved.

   Algorithm support.
*/

#include "mldb/server/algorithm.h"
#include "mldb/types/basic_value_descriptions.h"

namespace Datacratic {
namespace MLDB {

/*****************************************************************************/
/* ALGORITHM                                                                 */
/*****************************************************************************/

Algorithm::
Algorithm(MldbServer * server)
    : server(server)
{
}

Algorithm::
~Algorithm()
{
}

Any
Algorithm::
getStatus() const
{
    return Any();
}

} // namespace MLDB
} // namespace Datacratic

