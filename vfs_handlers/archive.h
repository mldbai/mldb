// This file is part of MLDB. Copyright 2015 mldb.ai inc. All rights reserved.

/** archive.h                                                      -*- C++ -*-
    Jeremy Barnes, 16 September 2015
    Copyright (c) 2015 mldb.ai inc.  All rights reserved.

*/

#pragma once

#include "mldb/vfs/fs_utils.h"

namespace MLDB {

/** Iterate through the given archive (represented by a streambuf),
    calling the given callback for each object found.
*/
bool iterateArchive(std::streambuf * archive,
                    const OnUriObject & onObject);

} // namespace MLDB

