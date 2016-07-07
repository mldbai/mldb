// This file is part of MLDB. Copyright 2015 Datacratic. All rights reserved.

/** archive.h                                                      -*- C++ -*-
    Jeremy Barnes, 16 September 2015
    Copyright (c) 2015 Datacratic Inc.  All rights reserved.

*/

#pragma once

#include "mldb/vfs/fs_utils.h"

namespace Datacratic {

/** Iterate through the given archive (represented by a streambuf),
    calling the given callback for each object found.
*/
bool iterateArchive(std::streambuf * archive,
                    const OnUriObject & onObject);

} // namespace Datacratic

