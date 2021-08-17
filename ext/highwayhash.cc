/** highwayhash.cc
    Jeremy Barnes, 12 July 2016
    Copyright (c) 2016 mldb.ai inc.  All rights reserved.

*/

#include "highwayhash.h"
#include "mldb/compiler/compiler.h"
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

#if MLDB_INTEL_ISA

struct Regs {
    uint32_t eax, ebx, ecx, edx;
};

MLDB_ALWAYS_INLINE Regs cpuid(uint32_t request, uint32_t ecx = 0)
{
    Regs result = {0, 0, 0, 0};
    asm volatile
        (
#if defined(__i686__)         
         "sub  $0x40,  %%esp\n\t"
         "push %%ebx\n\t"
         "push %%edx\n\t"
#else
         "sub  $0x40,  %%rsp\n\t"
         "push %%rbx\n\t"
         "push %%rdx\n\t"
#endif
         "cpuid\n\t"

         "mov  %%eax,  0(%[addr])\n\t"
         "mov  %%ebx,  4(%[addr])\n\t"
         "mov  %%ecx,  8(%[addr])\n\t"
         "mov  %%edx, 12(%[addr])\n\t"
#if defined(__i686__)         
         "pop  %%edx\n\t"
         "pop  %%ebx\n\t"
         "add  $0x40,   %%esp\n\t"
#else
         "pop  %%rdx\n\t"
         "pop  %%rbx\n\t"
         "add  $0x40,   %%rsp\n\t"
#endif
         : "+a" (request), "+c" (ecx)
         : [addr] "S" (&result)
         : "cc", "memory"
         );
    return result;
}

MLDB_ALWAYS_INLINE bool has_sse41()
{
     return cpuid(1, 0).ecx & (1 << 19);
}

MLDB_ALWAYS_INLINE bool has_avx2()
{
    return cpuid(7, 0).ebx & (1 << 5);
}

#endif

typedef uint64 (*Hasher)
(const uint64* key, const char* bytes, const uint64 size);

Hasher highwayHashImpl = &HighwayHash64;

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
