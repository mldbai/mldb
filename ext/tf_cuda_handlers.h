/* tf_cuda_handlers.h
 * Jean Raby, January 2017
 * Copyright (c) 2017 mldb.ai Inc. All rights reserved.
 */

#include <stdexcept>

struct CudaLaunchException: std::runtime_error {
    CudaLaunchException(std::string err): std::runtime_error(err){}
};
