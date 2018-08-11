// This file is part of MLDB. Copyright 2015 mldb.ai inc. All rights reserved.

/* distribution.cc
   Jeremy Barnes, 15 March 2005
   Copyright (c) 2005 Jeremy Barnes.  All rights reserved.
   


   ---

   Implementation of static distribution functions.
*/

#include "mldb/utils/distribution.h"
#include "mldb/arch/exception.h"

namespace MLDB {
namespace Stats {

#if 0
void wrong_sizes_exception()
{
    throw Exception("distribution: operation between different sizes");
}
#endif

} // namespace Stats
} // namespace MLDB
