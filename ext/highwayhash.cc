/** highwayhash.cc
    Jeremy Barnes, 12 July 2016
    Copyright (c) 2016 mldb.ai inc.  All rights reserved.

*/

#include "highwayhash.h"
#include "mldb/compiler/compiler.h"
#include <iostream>

using namespace std;

extern "C" {
#include "mldb/ext/highwayhash/highwayhash/c_bindings.h"
} // extern "C"


namespace MLDB {

uint64_t sipHash(const uint64_t* key, const char* bytes, const uint64_t size)
{
    return SipHash13C((const uint64_t *)key, bytes, size);
}

uint64_t highwayHash(const uint64_t* key, const char* bytes, const uint64_t size)
{
    return HighwayHash64(key, bytes, size);
}

} // namespace MLDB
