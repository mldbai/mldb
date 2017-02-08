/** tf_load_kernels.cc
    Jeremy Barnes, 13 September 2016
    Copyright (c) 2016 mldb.ai inc.  All rights reserved.

    Detects what machine architecture Tensorflow is running on, and
    loads the appropriate kernels library for the architecture.
*/

#include "mldb/arch/arch.h"
#include <dlfcn.h>
#include <mutex>
#include <iostream>
#if MLDB_INTEL_ISA
#  include "mldb/arch/simd.h"
#endif
#include <cassert>

using namespace std;

namespace MLDB {

// This is defined in the MLDB server
static std::mutex dlopenMutex;

namespace {
struct AtInit {
    AtInit()
        : handle(nullptr)
    {
        // Also exclude anyone else from using dlopen, since it's not thread
        // safe.
        std::unique_lock<std::mutex> guard2(dlopenMutex);

        const char * error = nullptr;

#if MLDB_INTEL_ISA
        // Try avx2 first
        if (!handle && has_avx2()) {
            error = load("avx2");
        }
        // Next is avx1
        if (!handle && has_avx()) {
            error = load("avx");
        }
        // Next SSE 4.2
        if (!handle && has_sse42()) {
            error = load("sse42");
        }
#else
        error = load("generic");
#endif

        if (!handle) {
            cerr << "Couldn't load any TensorFlow kernels library: "
                 << error << endl;
            cerr << "Tensorflow operations will abort on kernel launch" << endl;
        }
    }

    ~AtInit()
    {
        if (handle)
            dlclose(handle);
    }

    void * handle;  ///< Shared library handle

    const char * load(const std::string & architecture)
    {
        string path = "libtensorflow-kernels-" + architecture + ".so";

        dlerror();  // clear existing error

        handle = dlopen(path.c_str(), RTLD_NOW | RTLD_LOCAL);
        if (!handle) {
            char * error = dlerror();
            assert(error);
            return error;
        }

        return nullptr;
    }
} atInit;

} // file scope
} // namespace MLDB

