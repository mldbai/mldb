/** filesystem_fwd.h                                                   -*- C++ -*-
    Jeremy Barnes, 22 April 2018
    Copyright (c) 2018 Element AI Inc.  All rights reserved.
    This file is part of MLDB. Copyright 2018 mldb.ai inc. All rights reserved.

    Simple include to find the compiler's filesystem implementation
    and pull it into namespace std.
*/

#pragma once

_LIBCPP_BEGIN_NAMESPACE_FILESYSTEM

struct path;

_LIBCPP_END_NAMESPACE_FILESYSTEM

namespace MLDB {
using std_filesystem_path = std::filesystem::path;
} //
