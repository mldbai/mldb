/** tf_cuda_handlers.cc
    Jeremy Barnes, 17 January 2017
    Copyright (c) 2017 mldb.ai inc.  All rights reserved.

*/

#include <cuda_runtime.h>
#include <string>
#include "tf_cuda_handlers.h"

// When Eigen launches a CUDA kernel, it will call this function
// to check if the kernel succeeded or not.  We override the
// default behaviour of crashing the entire program here, as there
// are transient events that cause failures temporarily (eg too
// many jobs submitted in parallel) that can be retried; crashing
// leads to a very poor user experience.
void mldbCheckForCudaSuccess(cudaError_t status)
{
    if (status == cudaSuccess)
        return;

    throw CudaLaunchException("CUDA kernel launch error: "
                              + std::string(cudaGetErrorName(status))
                              + std::string(cudaGetErrorString(status)));
}
