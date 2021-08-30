/* snappy.cc
   Jeremy Barnes, 27 November 2016
   Copyright (c) 2016 Mldb.ai Inc.  All rights reserved.
   This file is part of MLDB. Copyright 2016 mldb.ai Inc. All rights reserved.

   Snappy compressor and decompressors.
*/

#include "compressor.h"
#include "mldb/base/exc_assert.h"
#include "snappy.h"
#include <iostream>
#include "mldb/utils/possibly_dynamic_buffer.h"

using namespace std;

namespace MLDB {


/*****************************************************************************/
/* SNAPPY COMPRESSOR                                                         */
/*****************************************************************************/

struct SnappyCompressor: public Compressor {

    SnappyCompressor(int level)
    {
    }

    virtual void compress(const char * data, size_t len, const OnData & onData) override
    {
        PossiblyDynamicBuffer<char, 131072> buf(snappy::MaxCompressedLength(len));
        size_t bytesDone = 0;
        snappy::RawCompress(data, len, buf.data(), &bytesDone);

        size_t written = 0;
        while (written < bytesDone) {
            written += onData(buf.data() + written,
                              buf.size() - written);
        }
    }
    
    virtual void flush(FlushLevel flushLevel, const OnData & onData) override
    {
        // flush is a no-op since no buffering is done
    }

    virtual void finish(const OnData & onData) override
    {
        // finish is a no-op since no buffering is done
    }
};

static Compressor::Register<SnappyCompressor>
registerSnappyCompressor("snappy", {"snappy"});


/*****************************************************************************/
/* SNAPPY DECOMPRESSOR                                                       */
/*****************************************************************************/

struct SnappyDecompressor: public Decompressor {

    using Decompressor::decompress;
    using Decompressor::finish;
    
    virtual int64_t decompressedSize(const char * block, size_t blockLen,
                                     int64_t totalLen) const override
    {
        size_t decompressedSize;

        if (!snappy::GetUncompressedLength(block, blockLen, &decompressedSize)) {
            return LENGTH_INSUFFICIENT_DATA;
        }
        
        return decompressedSize;
    }
    
    virtual void
    decompress(const char * data, size_t len, const OnData & onData) override
    {
        size_t decompressedSize;
        if (!snappy::GetUncompressedLength(data, len, &decompressedSize))
            throw Exception("Invalid Snappy compressed data reading length");

        PossiblyDynamicBuffer<char, 131072> decompressed(decompressedSize);

        if (!snappy::RawUncompress(data, len, decompressed.data()))
            throw Exception("Invalid Snappy compressed data");
        
        size_t written = 0;
        while (written < decompressedSize) {
            written += onData(decompressed.data() + written,
                              decompressedSize - written);
        }
    }
    
    virtual void finish(const OnData & onData) override
    {
    }
};

static Decompressor::Register<SnappyDecompressor>
registerSnappyDecompressor("snappy", {"snappy"});

} // namespace MLDB

