/** tmpdir.h                                                       -*- C++ -*-
    Jeremy Barnes, 22 April 2018
    Copyright (c) 2018 Element AI Inc.  All rights reserved.
    This file is part of MLDB. Copyright 2018 mldb.ai inc. All rights reserved.

    Function to securely create a unique directory.
*/

#pragma once

#include "mldb/compiler/filesystem.h"

namespace MLDB {

/** Creates a unique subdirectory under the given path, which is
    guaranteed to be pre-created and hard to guess.

    This is a wrapper around the mkstemp function.
*/
std::filesystem::path
make_unique_directory(const std::filesystem::path & current);

    
} // namespace MLDB

