// This file is part of MLDB. Copyright 2015 Datacratic. All rights reserved.

/* compiler.h                                                      -*- C++ -*-
   Jeremy Barnes, 1 February 2005
   Copyright (c) 2005 Jeremy Barnes.  All rights reserved.


   Compiler detection, etc.
*/

#pragma once

#ifdef __CUDACC__
# define JML_COMPILER_NVCC 1
#endif

#define JML_ALWAYS_INLINE __attribute__((__always_inline__)) inline 
#define JML_NORETURN __attribute__((__noreturn__))
#define JML_UNUSED  __attribute__((__unused__))
#define JML_PACKED  __attribute__((__packed__))
#define JML_PURE_FN __attribute__((__pure__))
#define JML_CONST_FN __attribute__((__const__))
#define JML_WEAK_FN __attribute__((__weak__))
#define JML_LIKELY(x) __builtin_expect((x), true)
#define JML_UNLIKELY(x) __builtin_expect((x), false)
#define JML_DEPRECATED __attribute__((__deprecated__))
#define JML_ALIGNED(x) __attribute__((__aligned__(x)))
#define JML_FORMAT_STRING(arg1, arg2) __attribute__((__format__ (printf, arg1, arg2)))

// Macro to catch all exceptions apart from stack unwinding exceptions...
// it's against the standard to do catch(...) without rethrowing.

#define JML_CATCH_ALL \
    catch (__cxxabiv1::__forced_unwind& ) { \
        throw;                       \
    } catch (...)

#if __GNUC__ == 4
#  if __CNUC_MINOR__ < 8
#     define JML_OVERRIDE 
#     define JML_NO_MOVE_IF_NOEXCEPT
#  else
#     define JML_OVERRIDE override
#     undef  JML_NO_MOVE_IF_NOECEPT
#  endif
#elif __GNUC__ > 4
#  define JML_OVERRIDE override
#endif

#ifdef __GXX_EXPERIMENTAL_CXX0X__
#  define jml_typeof(x) decltype(x)
#  define JML_HAS_RVALUE_REFERENCES 1
#else
#  define jml_typeof(x) typeof(x)
#endif

#ifdef JML_COMPILER_NVCC
# define JML_COMPUTE_METHOD __device__ __host__
# undef  JML_LIKELY
# define JML_LIKELY(x) x
# undef  JML_UNLIKELY
# define JML_UNLIKELY(x) x

#if 0 // CUDA 2.1 only
// Required so that nvcc works with optimization on under CUDA 2.1, GCC 4.3.3
static __typeof__(int (pthread_t)) __gthrw_pthread_cancel __attribute__((__alias__("pthread_cancel"))); 
#endif

#else
# define JML_COMPUTE_METHOD 
#endif

#ifdef __CUDACC__
# define JML_COMPILER_NVCC 1
#endif
