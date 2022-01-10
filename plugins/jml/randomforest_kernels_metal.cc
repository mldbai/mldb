/** randomforest_kernels_metal.cc                              -*- C++ -*-
    Jeremy Barnes, 8 September 2021
    Copyright (c) 2021 Jeremy Barnes.  All rights reserved.
    This file is part of MLDB. Copyright 2016 mldb.ai inc. All rights reserved.

    Kernels for random forest algorithm.
*/

#include "mldb/builtin/metal/compute_kernel_metal.h"


using namespace std;


namespace MLDB {
namespace RF {

namespace {

static struct RegisterMetalKernels {

    RegisterMetalKernels()
    {
        auto compileLibrary = [] (MetalComputeContext & context) -> std::shared_ptr<MetalComputeFunctionLibrary>
        {
            if (false) {
                std::string fileName = "mldb/plugins/jml/randomforest_kernels.metal";
                return MetalComputeFunctionLibrary::compileFromSourceFile(context, fileName);
            }
            else {
                std::string fileName = "build/arm64/lib/randomforest_metal.metallib";
                return MetalComputeFunctionLibrary::loadMtllib(context, fileName);
            }
        };

        registerMetalLibrary("randomforest_kernels", compileLibrary);
    }

} registerMetalKernels;

} // file scope

} // namespace RF
} // namespace MLDB
