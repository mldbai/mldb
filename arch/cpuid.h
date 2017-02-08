/* cpuid.h                                                         -*- C++ -*-
   Jeremy Barnes, 21 February 2007
   Copyright (c) 2007 Jeremy Barnes.  All rights reserved.
   This file is part of MLDB. Copyright 2015 mldb.ai inc. All rights reserved.

   Implementation of the CPUID-related functionality.
*/

#pragma once

#include <stdint.h>
#include <string>
#include "mldb/compiler/compiler.h"

namespace MLDB {

#if defined __i686__ || defined __amd64__ || defined __i586__ || defined __i386__


struct CPU_Info {

    CPU_Info();

    std::string vendor;
    std::string model;

    int cpuid_level;
    int cpuid_extlevel;

    union {
        struct {
            // 32 bits of standard flags
            uint32_t fpu:1;
            uint32_t vme:1;
            uint32_t de:1;
            uint32_t pse:1;
            uint32_t tsc:1;
            uint32_t msr:1;
            uint32_t pae:1;
            uint32_t mce:1;
            uint32_t cx8:1;
            uint32_t apic:1;
            uint32_t res1:1;
            uint32_t sep:1;
            uint32_t mtrr:1;
            uint32_t pge:1;
            uint32_t mca:1;
            uint32_t cmov:1;
            uint32_t pat:1;
            uint32_t pse36:1;
            uint32_t psn:1;
            uint32_t clflush:1;
            uint32_t res2:1;
            uint32_t dts:1;
            uint32_t acpi:1;
            uint32_t mmx:1;
            uint32_t fxsr:1;
            uint32_t sse:1;
            uint32_t sse2:1;
            uint32_t ss:1;
            uint32_t ht:1;
            uint32_t tm:1;
            uint32_t ia64:1;
            uint32_t pbe:1;
        };
        uint32_t standard1;
    };
    
    union {
        struct {
            // Extended flags for intel
            uint32_t sse3:1;      // 0
            uint32_t pclmulqdq:1; // 1
            uint32_t dtes64:1;    // 2
            uint32_t monitor:1;   // 3
            uint32_t ds_cpl:1;    // 4
            uint32_t vmx:1;       // 5
            uint32_t smx:1;       // 6
            uint32_t est:1;       // 7
            uint32_t tm2:1;       // 8
            uint32_t pni:1;       // 9
            uint32_t cid:1;       // 10
            uint32_t sdbg:1;      // 11
            uint32_t fma:1;       // 12
            uint32_t cx16:1;      // 13
            uint32_t xtpr:1;      // 14
            uint32_t pdcm:1;      // 15
            uint32_t res6:1;      // 16
            uint32_t pcid:1;      // 17
            uint32_t dca:1;       // 18
            uint32_t sse41:1;     // 19
            uint32_t sse42:1;     // 20
            uint32_t x2apic:1;    // 21
            uint32_t movbe:1;     // 22
            uint32_t popcnt:1;    // 23
            uint32_t tsc_deadline: 1;  // 24
            uint32_t aes:1;       // 25
            uint32_t xsave:1;     // 26
            uint32_t osxsave:1;   // 27
            uint32_t avx:1;       // 28
            uint32_t f16c:1;      // 29
            uint32_t rdrnd:1;     // 30
            uint32_t hypervisor:1;// 31
        };
        uint32_t standard2;
    };

    // Entended1 flags for AMD
    union {
        struct {
            uint32_t fpu_amd:1;  // 0
            uint32_t vme_amd:1;
            uint32_t de_amd:1;
            uint32_t pse_amd:1;
            uint32_t tsc_amd:1;  // 4
            uint32_t msr_amd:1;
            uint32_t pae_amd:1;
            uint32_t mce_amd:1;
            uint32_t cx8_amd:1;  // 8
            uint32_t apic_amd:1;
            uint32_t res1_amd:1;
            uint32_t syscall:1;
            uint32_t mtrr_amd:1; // 12
            uint32_t pge_amd:1;
            uint32_t mca_amd:1;
            uint32_t cmov_amd:1;
            uint32_t pat_amd:1;  // 16
            uint32_t pse36_amd:1;
            uint32_t res2_amd:2;
            uint32_t nx:1;       // 20
            uint32_t res3_amd:1; 
            uint32_t mmxext:1;
            uint32_t mmx_amd:1;
            uint32_t fxsr_amd:1; // 24
            uint32_t ffxsr:1;
            uint32_t res4_amd:1;
            uint32_t rdtscp:1;   
            uint32_t res5_amd:1; // 28
            uint32_t lm:1;
            uint32_t threednowext:1;
            uint32_t threednow:1;
        };
        uint32_t extended;
    };
    
    // Extended 2 flags for AMD
    union {
        struct {
            uint32_t lahfsahf:1;
            uint32_t res1_amd2:1;
            uint32_t svm:1;
            uint32_t cmplegacy:1;
            uint32_t res2_amd2:1;
            uint32_t altmovcr8:1;
            uint32_t res3_amd2:26;
        };
        uint32_t amd;
    };

    std::string print_flags();
};

void get_cpu_info(CPU_Info & info);

extern CPU_Info * static_cpu_info;

MLDB_ALWAYS_INLINE const CPU_Info & cpu_info()
{
    if (MLDB_UNLIKELY(!static_cpu_info))
        static_cpu_info = new CPU_Info;
    return *static_cpu_info;
}

uint32_t cpuid_flags();
std::string vendor_id();
std::string model_id();

struct Regs {
    uint32_t eax, ebx, ecx, edx;
};

Regs cpuid(uint32_t request, uint32_t ecx = 0);

#endif // __i686__

} // namespace MLDB
