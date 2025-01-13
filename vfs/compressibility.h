/* compressibility.h                                              -*- C++ -*-
   Jeremy Barnes, 6 November 2015
   Copyright (c) 2015 mldb.ai inc.  All rights reserved.

   This file is part of MLDB. Copyright 2015 mldb.ai inc. All rights reserved.

   Compressibility function.
*/
#pragma once

#include <iostream>

namespace MLDB {

struct CompressibilityStats {
    size_t bytesUncompressed;
    size_t bytesCompressed;
    double compressionRatio;

    CompressibilityStats & operator += (const CompressibilityStats & other);
};

CompressibilityStats calc_compressibility(const void * data, size_t numBytes);

std::ostream & operator << (std::ostream & stream, const CompressibilityStats & stats);

} // namespace MLDB
