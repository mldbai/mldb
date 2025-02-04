/* compressibility.cc                                           -*- C++ -*-
   Jeremy Barnes, 6 November 2015
   Copyright (c) 2015 mldb.ai inc.  All rights reserved.

   This file is part of MLDB. Copyright 2015 mldb.ai inc. All rights reserved.

   Enables MIN_LIMIT and MAX_LIMIT which automatically take on the type of their context.
*/

#include "compressibility.h"
#include "compressor.h"

namespace MLDB {

namespace {

static constexpr const char * COMPRESSIBILITY_COMPRESSOR = "zstd";
static constexpr int COMPRESSIBILITY_LEVEL = 9;

std::unique_ptr<Compressor> createCompressor()
{
    return std::unique_ptr<Compressor>
        (Compressor::create(COMPRESSIBILITY_COMPRESSOR, COMPRESSIBILITY_LEVEL));
}

size_t calcZstdHeaderLength()
{
    auto compressor = createCompressor();

    size_t compressedBytes = 0;
    auto onData = [&] (const char *, size_t len)
    {
        compressedBytes += len;
        return len;
    };
    compressor->finish(onData);
    return compressedBytes;
}

size_t zstdHeaderLength()
{
    static const size_t result = calcZstdHeaderLength();
    return result;
}

} // fiel scope

CompressibilityStats calc_compressibility(const void * data, size_t numBytes)
{
    auto compressor = createCompressor();

    size_t compressedBytes = 0;
    auto onData = [&] (const char *, size_t len)
    {
        compressedBytes += len;
        return len;
    };

    using namespace std;
    if (numBytes > 0)
        compressor->compress((const char *)data, numBytes, onData);
    compressor->finish(onData);

    CompressibilityStats result{ numBytes, compressedBytes - zstdHeaderLength(), 1.0 * (compressedBytes - zstdHeaderLength()) / numBytes };

    return result;
}

CompressibilityStats & CompressibilityStats::operator += (const CompressibilityStats & other)
{
    bytesUncompressed += other.bytesUncompressed;
    bytesCompressed += other.bytesCompressed;
    compressionRatio = 1.0 * bytesCompressed / bytesUncompressed;
    return *this;
}

} // namespace MLDB
