/** highwayhash.cc
    Jeremy Barnes, 12 July 2016
    Copyright (c) 2016 mldb.ai inc.  All rights reserved.

*/

#include "highwayhash.h"
#include "mldb/compiler/compiler.h"
#include "mldb/arch/arch.h"
#include <iostream>

using namespace std;

// Ugly; these are needed before the c files are included because they
// put them in the wrong namespace
// cstdint's uint64_t is unsigned long on Linux; we need 'unsigned long long'
// for interoperability with other software.
typedef unsigned long long uint64;  // NOLINT

typedef unsigned int uint32;

extern "C" {
#include "mldb/ext/highwayhash/highwayhash/c_bindings.h"
} // extern "C"


#include "mldb/arch/simd.h"

namespace MLDB {

namespace {

typedef uint64 (*Hasher)
(const uint64* key, const char* bytes, const uint64 size);

Hasher highwayHashImpl = &ScalarHighwayTreeHashC;



struct AtInit {
    AtInit()
    {
        if (false)
            ;
#if MLDB_INTEL_ISA
        else if (has_avx2()) {
            highwayHashImpl = &HighwayTreeHashC;
        }
        else if (has_sse41()) {
            highwayHashImpl = &SSE41HighwayTreeHashC;
        }
#endif
    }
} atInit;

} // file scope



uint64_t sipHash(const uint64_t* key, const char* bytes, const uint64_t size)
{
    return SipHashC((const uint64 *)key, bytes, size);
}

uint64_t highwayHash(const uint64_t* key, const char* bytes, const uint64_t size)
{
    return highwayHashImpl((const uint64 *)key, bytes, size);
}

} // namespace MLDB
