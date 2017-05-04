// This file is part of MLDB. Copyright 2015 mldb.ai inc. All rights reserved.

/* mapped_value.cc
   Jeremy Barnes, 28 March 2012
   Copyright (c) 2012 mldb.ai inc.  All rights reserved.

*/

#include "mapped_value.h"


namespace MLDB {

uint64_t readTrailingOffset(const ML::File_Read_Buffer & file)
{
    if (file.end() - file.start() < 8)
        throw MLDB::Exception("can't read trailing offset that is too short");
    uint64_t ofs = *(uint64_t *)(file.end() - 8);
    return ofs;
}

} // namespace MLDB
