/** filesystem_fwd.h                                                   -*- C++ -*-
    Jeremy Barnes, 22 April 2018
    Copyright (c) 2018 Element AI Inc.  All rights reserved.
    This file is part of MLDB. Copyright 2018 mldb.ai inc. All rights reserved.

    Simple include to find the compiler's filesystem implementation
    and pull it into namespace std.
*/

#pragma once

#include "mldb/compiler/stdlib.h"

#ifdef MLDB_STDLIB_CLANG
_LIBCPP_BEGIN_NAMESPACE_FILESYSTEM

struct path;

_LIBCPP_END_NAMESPACE_FILESYSTEM

#elif MLDB_STDLIB_GCC
#include <bits/fs_fwd.h>

#if 0
namespace std {
namespace filesystem {
class path;
} // namespace filesystem
} // namespace std
#endif

#else
#error "Unknown standard library"
#endif

namespace MLDB {
using std_filesystem_path = std::filesystem::path;
} // namespace MLDB
