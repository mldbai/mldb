/* stdlib.h
   Jeremy Barnes, 18 October 2010
   Copyright (c) 2010 Jeremy Barnes.  All rights reserved.
   Copyright (c) 2010 mldb.ai inc.  All rights reserved.

   This file is part of MLDB. Copyright 2015 mldb.ai inc. All rights reserved.

   Sets macros based upon which standard library we're using
*/

#pragma once

#if __has_include(<version>)
#  include <version>
#else
#  include <ciso646>
#endif
#ifdef _LIBCPP_VERSION
#  define MLDB_STDLIB_LLVM 1
#  define MLDB_STDLIB_CLANG 1
#elif __GLIBCXX__ // Note: only version 6.1 or newer define this in ciso646
#  define MLDB_STDLIB_GCC 1
#elif _CPPLIB_VER // Note: used by Visual Studio
#  define MLDB_STDLIB_VS
#else
#  error Using an unknown C++ standard library
#endif
