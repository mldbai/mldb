/** randomforest_kernels_opencl.cc                              -*- C++ -*-
    Jeremy Barnes, 8 September 2021
    Copyright (c) 2021 Jeremy Barnes.  All rights reserved.
    This file is part of MLDB. Copyright 2016 mldb.ai inc. All rights reserved.

    Kernels for random forest algorithm.
*/

#include "randomforest_kernels_opencl.h"
#include "randomforest_kernels.h"
#include "mldb/utils/environment.h"
#include "mldb/vfs/filter_streams.h"
#include "mldb/builtin/opencl/compute_kernel_opencl.h"
#include <future>
#include <array>


using namespace std;


namespace MLDB {
namespace RF {

namespace {

static struct RegisterOpenCLKernels {

    RegisterOpenCLKernels()
    {
        auto compileLibrary = [] (OpenCLComputeContext & context) -> std::shared_ptr<OpenCLComputeFunctionLibrary>
        {
            std::string fileName = "mldb/plugins/jml/randomforest_kernels.cl";
            return OpenCLComputeFunctionLibrary::compileFromSourceFile(context, fileName);
        };

        registerOpenCLLibrary("randomforest_kernels", compileLibrary);
    }

} registerOpenCLKernels;
} // file scope

} // namespace RF
} // namespace MLDB
