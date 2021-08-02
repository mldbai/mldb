/** tmpdir.cc                                                      -*- C++ -*-
    Jeremy Barnes, 22 April 2018
    Copyright (c) 2018 Element AI Inc.  All rights reserved.
    This file is part of MLDB. Copyright 2018 mldb.ai inc. All rights reserved.

    Function to securely create a unique directory.
*/

#include "tmpdir.h"
#include <stdlib.h>
#include <unistd.h>
#include "mldb/arch/exception.h"
#include "mldb/base/exc_assert.h"

namespace MLDB {

/** Creates a unique subdirectory under the given path, which is
    guaranteed to be pre-created and hard to guess.

    This is a wrapper around the mkstemp function.
*/
std::filesystem::path
make_unique_directory(const std::filesystem::path & current)
{
    std::string path = current;
    ExcAssert(path.size() > 0 && path.back() == '/');
    char filename[path.length() + 8];
    std::copy(path.begin(), path.end(), filename);
    filename[path.length()] = '/';
    std::fill(filename + path.length() + 0,
              filename + path.length() + 6,
              'X');
    filename[path.length() + 6] = 0;
    
    char * res = ::mkdtemp(filename);
    if (!res) {
        throw Exception(errno, "make_unique_directory mkdtemp");
    }
    return std::string(res);
}        

    
} // namespace MLDB

