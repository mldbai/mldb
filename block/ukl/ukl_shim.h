/** ukl_shim.h                                     -*- C++ -*-
    Jeremy Barnes, 31 January 2022
    Copyright (c) Jeremy Barnes.  All rights reserved.
    This file is part of MLDB.

    Shims for the ugly kernel language.
*/

#define UKL_STRINGIFY(x) x
#define UKL_CONCAT_3(w, x, y) w ## x ## y
#define UKL_CONCAT_STRINGS_3(w, x, y) UKL_CONCAT_3(w, x, y)
#define UKL_CONCAT_4(w, x, y, z) w ## x ## y ## z
#define UKL_CONCAT_STRINGS_4(w, x, y, z) UKL_CONCAT_4(w, x, y, z)

#define UKL_TOKEN(x) x
#define UKL_PASTE_6(w,x,y,z,a,b) UKL_TOKEN(w)UKL_TOKEN(x)UKL_TOKEN(y)UKL_TOKEN(z)UKL_TOKEN(a)UKL_TOKEN(b)

#if defined(UKL_IMPL)
#elif defined(__METAL_VERSION__)
#  define UKL_METAL 1
#  define UKL_IMPL metal
#  define UKL_PLATFORM_INCLUDE(x) UKL_PASTE_6(<,ukl_metal_,x,.,h,>)
#elif defined(__OPENCL_VERSION__)
#  define UKL_OPENCL 1
#  define UKL_IMPL opencl
#  define UKL_PLATFORM_INCLUDE(x) UKL_PASTE_6(<,ukl_opencl_,x,.,h,>)
#elif defined(__CUDA_ARCH__)
#  define UKL_CUDA 1
#  define UKL_IMPL cuda
#  define UKL_PLATFORM_INCLUDE(x) UKL_PASTE_6(<,ukl_cuda_,x,.,h,>)
#else
#  if ! defined(UKL_CPU_BACKEND)
#    warning No UKL backend defined; assuming UKL_CPU_BACKEND
#  endif
#  define UKL_CPU 1
#  define UKL_IMPL cpu
#  define UKL_PLATFORM_INCLUDE(x) UKL_PASTE_6(<,ukl_cpu_,x,.,h,>)
#endif /* detect ukl kernel type */

#define UKL_GET_INCLUDE(x) UKL_CONCAT_STRINGS_3(x, _, UKL_IMPL)

#define UKL_ADD_H(x) #x

#include UKL_PLATFORM_INCLUDE(shim)

//#include UKL_ADD_H(UKL_GET_INCLUDE(ukl_shim))
