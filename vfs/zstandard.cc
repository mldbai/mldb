/* zstandard.cc
   Jeremy Barnes, 27 November 2016
   Copyright (c) 2016 Mldb.ai Inc.  All rights reserved.
   This file is part of MLDB. Copyright 2016 mldb.ai Inc. All rights reserved.

   Zstandard compressor and decompressors.
*/

#include "compressor.h"
#include "mldb/base/exc_assert.h"
#include "mldb/ext/zstd/lib/zstd.h"
#include <zlib.h>
#include <iostream>

using namespace std;

namespace MLDB {


/*****************************************************************************/
/* ZSTANDARD COMPRESSOR                                                      */
/*****************************************************************************/

struct ZStandardCompressor: public Compressor {

    ZStandardCompressor()
        : stream(ZSTD_createCStream()),
          outDataSize(ZSTD_CStreamOutSize()),
          outData(new char[outDataSize]),
          outBuf{outData.get(),outDataSize,0}
    {
    }

    ZStandardCompressor(int level)
        : ZStandardCompressor()
    {
        open(level);
    }
    
    ~ZStandardCompressor()
    {
        if (stream)
            ZSTD_freeCStream(stream);
    }

    void open(int compressionLevel)
    {
        ZSTD_initCStream(stream, compressionLevel);
    }

    virtual size_t compress(const char * data, size_t len, const OnData & onData) override
    {
        ZSTD_inBuffer inBuf{data, len, 0};

        while (inBuf.pos < inBuf.size) {
            outBuf.pos = 0;
            size_t res = ZSTD_compressStream(stream, &outBuf, &inBuf);
            if (ZSTD_isError(res)) {
                throw Exception("Error compression zstandard stream: %s",
                                ZSTD_getErrorName(res));
            }
            writeAll(onData);
        }

        return len;
    }
    
    virtual size_t flush(FlushLevel flushLevel, const OnData & onData) override
    {
        size_t result = 0;
        size_t bytesLeft = -1;
        while (bytesLeft != 0) {
            outBuf.pos = 0;
            bytesLeft = ZSTD_flushStream(stream, &outBuf);
            result += writeAll(onData);
        }
        return result;
    }

    virtual size_t finish(const OnData & onData) override
    {
        size_t result = 0;
        size_t bytesLeft = -1;
        while (bytesLeft != 0) {
            outBuf.pos = 0;
            bytesLeft = ZSTD_endStream(stream, &outBuf);
            result += writeAll(onData);
        }
        return result;
    }

    size_t writeAll(const OnData & onData)
    {
        size_t written = 0;
        while (written < outBuf.pos) {
            written += onData(outData.get() + written,
                              outBuf.pos - written);
        }
        return written;
    }
    
    ZSTD_CStream * stream = nullptr;
    size_t outDataSize = 0;
    std::unique_ptr<char[]> outData;
    ZSTD_outBuffer outBuf;
};

static Compressor::Register<ZStandardCompressor>
registerZStandardCompressor("zstd", {"zst", "zstd"});


/*****************************************************************************/
/* ZSTANDARD DECOMPRESSOR                                                    */
/*****************************************************************************/

struct ZStandardDecompressor: public Decompressor {

    ZStandardDecompressor()
        : stream(ZSTD_createDStream()),
          outDataSize(ZSTD_DStreamOutSize()),
          outData(new char[outDataSize]),
          outBuf{outData.get(),outDataSize,0}
    {
        ZSTD_initDStream(stream);
    }

    ~ZStandardDecompressor()
    {
        if (stream)
            ZSTD_freeDStream(stream);
    }

    void open()
    {
    }

    virtual size_t decompress(const char * data, size_t len, const OnData & onData) override
    {
        ZSTD_inBuffer inBuf{data, len, 0};

        while (inBuf.pos < inBuf.size) {
            outBuf.pos = 0;
            size_t res = ZSTD_decompressStream(stream, &outBuf, &inBuf);
            if (ZSTD_isError(res)) {
                throw Exception("Error compression zstandard stream: %s",
                                ZSTD_getErrorName(res));
            }
            writeAll(onData);
            if (res == 0 && inBuf.pos < inBuf.size) {
                throw Exception("Extra bytes at end of decompressed zstandard stream");
            }
        }
        
        return len;
    }
    
    virtual size_t finish(const OnData & onData) override
    {
        return 0;
    }

    size_t writeAll(const OnData & onData)
    {
        size_t written = 0;
        while (written < outBuf.pos) {
            written += onData(outData.get() + written,
                              outBuf.pos - written);
        }
        return written;
    }
    
    ZSTD_DStream * stream = nullptr;
    size_t outDataSize = 0;
    std::unique_ptr<char[]> outData;
    ZSTD_outBuffer outBuf;
};

static Decompressor::Register<ZStandardDecompressor>
registerZStandardDecompressor("zstd", {"zst", "zstd"});

} // namespace MLDB

