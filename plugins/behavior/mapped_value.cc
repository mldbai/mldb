/* mapped_value.cc
   Jeremy Barnes, 28 March 2012
   Copyright (c) 2012 mldb.ai inc.  All rights reserved.
   This file is part of MLDB. Copyright 2015 mldb.ai inc. All rights reserved.

*/

#include "mapped_value.h"


namespace MLDB {

uint64_t readTrailingOffset(const MLDB::File_Read_Buffer & file)
{
    if (file.end() - file.start() < 8)
        throw MLDB::Exception("can't read trailing offset that is too short");
    uint64_t ofs;
    std::memcpy(&ofs, file.end() - sizeof(ofs), sizeof(ofs));
    return ofs;
}

} // namespace MLDB
