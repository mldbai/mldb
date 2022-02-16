/** highwayhash.cc
    Jeremy Barnes, 12 July 2016
    Copyright (c) 2016 mldb.ai inc.  All rights reserved.

*/

#include <cstdint>
#include <array>
#include "mldb/compiler/compiler.h"

namespace MLDB {

/// SIP hash of the value
uint64_t sipHash(const uint64_t* key, const char* bytes, const uint64_t size);

/// Highway tree hash of the value, dispatching to SSE4.1/4.2 as possible
uint64_t highwayHash(const uint64_t* key, const char* bytes,
                     const uint64_t size);

std::array<uint64_t, 2>
highwayHash128(const uint64_t* key, const char* bytes,
               const uint64_t size);

std::array<uint64_t, 4>
highwayHash256(const uint64_t* key, const char* bytes,
               const uint64_t size);

} // namespace MLDB
