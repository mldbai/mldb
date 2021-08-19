/* compiler.h                                                      -*- C++ -*-
   Jeremy Barnes, 1 February 2005
   Copyright (c) 2005 Jeremy Barnes.  All rights reserved.
   This file is part of MLDB. Copyright 2015 mldb.ai inc. All rights reserved.

   Compiler detection, etc.
*/

#pragma once

#include "stdlib.h"

#ifdef __CUDACC__
# define MLDB_COMPILER_NVCC 1
#endif

#define MLDB_ALWAYS_INLINE __attribute__((__always_inline__)) inline 
#define MLDB_NORETURN __attribute__((__noreturn__))
#define MLDB_UNUSED  __attribute__((__unused__))
#define MLDB_PACKED  __attribute__((__packed__))
#define MLDB_PURE_FN __attribute__((__pure__))
#define MLDB_CONST_FN __attribute__((__const__))
#define MLDB_WEAK_FN __attribute__((__weak__))
#define MLDB_LIKELY(x) __builtin_expect(bool(x), true)
#define MLDB_UNLIKELY(x) __builtin_expect((x), false)
#define MLDB_DEPRECATED __attribute__((__deprecated__))
#define MLDB_ALIGNED(x) __attribute__((__aligned__(x)))
#define MLDB_FORMAT_STRING(arg1, arg2) __attribute__((__format__ (printf, arg1, arg2)))
#define MLDB_WARN_UNUSED_RESULT __attribute__((__warn_unused_result__))
#ifdef __clang__
#  define MLDB_UNUSED_PRIVATE_FIELD  __attribute__((__unused__))
#else
#  define MLDB_UNUSED_PRIVATE_FIELD
#endif

// Macro to catch all exceptions apart from stack unwinding exceptions...
// it's against the standard to do catch(...) without rethrowing.

#if MLDB_STDLIB_LLVM
#define MLDB_CATCH_ALL \
    catch (...)
#else
#define MLDB_CATCH_ALL \
    catch (__cxxabiv1::__forced_unwind& ) { \
        throw;                       \
    } catch (...)
#endif

#if __GNUC__ == 4
#  if __CNUC_MINOR__ < 8
#     define MLDB_NO_MOVE_IF_NOEXCEPT
#  else
#     undef  MLDB_NO_MOVE_IF_NOECEPT
#  endif
#endif

#if __GNUC__ == 5
#  if __CNUC_MINOR__ < 5
#     define MLDB_FILESYSTEM_REMOVE_ALL_BUG 1
#  else
#     undef  MLDB_FILESYSTEM_REMOVE_ALL_BUG
#  endif
#endif

#ifdef __GXX_EXPERIMENTAL_CXX0X__
#  define jml_typeof(x) decltype(x)
#  define MLDB_HAS_RVALUE_REFERENCES 1
#else
#  define jml_typeof(x) typeof(x)
#endif

#ifdef MLDB_COMPILER_NVCC
# define MLDB_COMPUTE_METHOD __device__ __host__
# undef  MLDB_LIKELY
# define MLDB_LIKELY(x) x
# undef  MLDB_UNLIKELY
# define MLDB_UNLIKELY(x) x

#if 0 // CUDA 2.1 only
// Required so that nvcc works with optimization on under CUDA 2.1, GCC 4.3.3
static __typeof__(int (pthread_t)) __gthrw_pthread_cancel __attribute__((__alias__("pthread_cancel"))); 
#endif

#else
# define MLDB_COMPUTE_METHOD 
#endif

#ifdef __CUDACC__
# define MLDB_COMPILER_NVCC 1
#endif

#if defined(__has_feature)
#  if __has_feature(thread_sanitizer)
#    define ATTRIBUTE_NO_SANITIZE_THREAD __attribute__((no_sanitize("thread")))
#    define SANITIZE_THREAD 1
#  else
#    define ATTRIBUTE_NO_SANITIZE_THREAD
#    define SANITIZE_THREAD 0
#  endif
#else
#    define ATTRIBUTE_NO_SANITIZE_THREAD
#    define SANITIZE_THREAD 1
#endif

#if defined(__has_feature)
#  if __has_feature(undefined_behavior_sanitizer)
#    define ATTRIBUTE_NO_SANITIZE_UNDEFINED __attribute__((no_sanitize("undefined")))
#    define SANITIZE_UNDEFINED 1
#  else
#    define ATTRIBUTE_NO_SANITIZE_UNDEFINED
#    define SANITIZE_UNDEFINED 0
#  endif
#else
#    define ATTRIBUTE_NO_SANITIZE_UNDEFINED
#    define SANITIZE_UNDEFINED 0
#endif

#if defined(__has_feature)
#  if __has_feature(address_sanitizer)
#    define ATTRIBUTE_NO_SANITIZE_ADDRESS __attribute__((no_sanitize("address")))
#    define SANITIZE_ADDRESS 1
#  else
#    define ATTRIBUTE_NO_SANITIZE_ADDRESS
#    define SANITIZE_ADDRESS 0
#  endif
#else
#    define ATTRIBUTE_NO_SANITIZE_ADDRESS
#    define SANITIZE_ADDRESSS 0
#endif

#if defined(__has_feature)
#  if __has_feature(memory_sanitizer)
#    define ATTRIBUTE_NO_SANITIZE_MEMORY __attribute__((no_sanitize("memory")))
#    define SANITIZE_MEMORY 1
#  else
#    define ATTRIBUTE_NO_SANITIZE_MEMORY
#    define SANITIZE_MEMORY 0
#  endif
#else
#    define ATTRIBUTE_NO_SANITIZE_MEMORY
#    define SANITIZE_MEMORY 0
#endif
