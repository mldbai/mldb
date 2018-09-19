/* zlib.cc
   Jeremy Barnes, 19 September 2012
   Copyright (c) 2016 Mldb.ai Inc.  All rights reserved.
   This file is part of MLDB. Copyright 2016 mldb.ai Inc. All rights reserved.

   Implementation of the zlib compression format.
*/

#include "compressor.h"
#include "mldb/arch/endian.h"
#include "mldb/base/exc_assert.h"
#include <iostream>
#include <lzma.h>


using namespace std;


namespace MLDB {

/*****************************************************************************/
/* LZMA STREAM COMMON                                                        */
/*****************************************************************************/

struct LzmaStreamCommon: public lzma_stream {

    typedef Compressor::OnData OnData;
    typedef Compressor::FlushLevel FlushLevel;

    std::string lzma_strerror(int code)
    {
        switch (code) {
        case LZMA_OK: return "Operation completed successfully";
        case LZMA_STREAM_END: return "End of stream was reached";
        case LZMA_NO_CHECK: return "Input stream has no integrity check";
        case LZMA_UNSUPPORTED_CHECK: return "Cannot calculate the integrity check";
        case LZMA_GET_CHECK: return "Integrity check type is now available";
        case LZMA_MEM_ERROR: return "Cannot allocate memory";
        case LZMA_MEMLIMIT_ERROR: return "Memory usage limit was reached";
        case LZMA_FORMAT_ERROR: return "File format not recognized";
        case LZMA_OPTIONS_ERROR: return "Invalid or unsupported options";
        case LZMA_DATA_ERROR: return "Data is corrupt";
        case LZMA_BUF_ERROR: return "No progress is possible";
        case LZMA_PROG_ERROR: return "Programming error";
        default: return "lzma_ret(" + std::to_string(code) + ")";
        }
    }
    
    LzmaStreamCommon()
        : lzma_stream LZMA_STREAM_INIT
    {
    }

    ~LzmaStreamCommon()
    {
        lzma_end(this);
    }
    
    size_t pump(const char * data, size_t len, const OnData & onData,
                lzma_action flushLevel)
    {
        //cerr << "pump " << len << " bytes with flush level "
        //     << flushLevel << endl;

        size_t bufSize = 131072;
        char output[bufSize];
        next_in = (uint8_t *)data;
        avail_in = len;
        size_t result = 0;

        do {
            next_out = (uint8_t *)output;
            avail_out = bufSize;

            lzma_ret res = lzma_code(this, flushLevel);

            //cerr << "pumping " << len << " bytes through with flushLevel "
            //     << flushLevel << " returned " << res << endl;

            size_t bytesWritten = (const char *)next_out - output;

            switch (res) {
            case LZMA_OK:
                if (bytesWritten)
                    onData(output, bytesWritten);
                result += bytesWritten;
                break;

            case LZMA_STREAM_END:
                if (bytesWritten)
                    onData(output, bytesWritten);
                result += bytesWritten;
                return result;

            default:
                throw Exception("lzma2 error: " + lzma_strerror(res));
            };
        } while (avail_in != 0);
        
        if (flushLevel == LZMA_FINISH)
            throw Exception("finished without getting to LZMA_STREAM_END");

        return result;
    }

    size_t flush(FlushLevel flushLevel, const OnData & onData)
    {
        lzma_action lzmaFlushLevel;
        switch (flushLevel) {
        case Compressor::FLUSH_NONE:       lzmaFlushLevel = LZMA_RUN;       break;
        case Compressor::FLUSH_AVAILABLE:  lzmaFlushLevel = LZMA_SYNC_FLUSH;break;
        case Compressor::FLUSH_SYNC:       lzmaFlushLevel = LZMA_FULL_FLUSH;break;
        case Compressor::FLUSH_RESTART:    lzmaFlushLevel = LZMA_FINISH;    break;
        default:
            throw Exception("bad flush level");
        }
        
        return pump(0, 0, onData, lzmaFlushLevel);
    }

    size_t finish(const OnData & onData)
    {
        return pump(0, 0, onData, LZMA_FINISH);
    }
};


/*****************************************************************************/
/* LZMA COMPRESSOR                                                           */
/*****************************************************************************/

struct LzmaCompressor : public Compressor, public LzmaStreamCommon {

    typedef Compressor::OnData OnData;
    typedef Compressor::FlushLevel FlushLevel;
    
    LzmaCompressor(int compressionLevel)
    {
        if (compressionLevel == -1)
            compressionLevel = 5;

        lzma_ret res = lzma_easy_encoder(this, compressionLevel,
                                    LZMA_CHECK_CRC64);
        
        if (res != LZMA_OK)
            throw Exception("LZMA_lzmaCompressInit failed: "
                            + lzma_strerror(res));
    }

    virtual size_t compress(const char * data, size_t len,
                            const OnData & onData)
    {
        return pump(data, len, onData, LZMA_RUN);
    }
    
    virtual size_t flush(FlushLevel flushLevel, const OnData & onData)
    {
        return LzmaStreamCommon::flush(flushLevel, onData);
    }

    virtual size_t finish(const OnData & onData)
    {
        return LzmaStreamCommon::finish(onData);
    }
};

static Compressor::Register<LzmaCompressor>
registerLzmaCompressor("lzma", {"xz"});


/*****************************************************************************/
/* LZMA DECOMPRESSOR                                                         */
/*****************************************************************************/

struct LzmaDecompressor: public Decompressor, public LzmaStreamCommon {

    typedef Decompressor::OnData OnData;

    LzmaDecompressor()
    {
        //cerr << "creating decompressor" << endl;

        lzma_ret res = lzma_auto_decoder(this, UINT64_MAX /* memlimit */,
                                         0 /* flags */);
        if (res != LZMA_OK)
            throw Exception("LZMA_decompressInit failed: " + lzma_strerror(res));
    }

    virtual int64_t decompressedSize(const char * block, size_t blockLen,
                                     int64_t totalLen) const override
    {
        // TODO: can probably do better than this...
        return LENGTH_UNKNOWN;
    }
    
    virtual size_t decompress(const char * data, size_t len,
                              const OnData & onData) override
    {
        return pump(data, len, onData, LZMA_RUN);
    }
    
    virtual size_t finish(const OnData & onData) override
    {
        return 0;
    }
};

static Decompressor::Register<LzmaDecompressor>
registerLzmaDecompressor("lzma", {"xz", "lzma"});

} // namespace MLDB
