/* zlib.cc
   Jeremy Barnes, 19 September 2012
   Copyright (c) 2016 Mldb.ai Inc.  All rights reserved.
   This file is part of MLDB. Copyright 2016 mldb.ai Inc. All rights reserved.

   Implementation of the zlib compression format.
*/

#include "compressor.h"
#include "mldb/arch/endian.h"
#include <bzlib.h>
#include "mldb/base/exc_assert.h"
#include <iostream>


using namespace std;


namespace MLDB {

/*****************************************************************************/
/* BZLIB STREAM COMMON                                                        */
/*****************************************************************************/

struct BzlibStreamCommon: public bz_stream {

    typedef Compressor::OnData OnData;
    typedef Compressor::FlushLevel FlushLevel;

    static std::string bzerror(int code)
    {
        
        switch (code) {
        case BZ_OK: return "OK";
        case BZ_RUN_OK: return "RUN_OK";
        case BZ_FLUSH_OK: return "FLUSH_OK";
        case BZ_FINISH_OK: return "FINISH_OK";
        case BZ_STREAM_END: return "STREAM_END";
        case BZ_SEQUENCE_ERROR: return "SEQUENCE_ERROR";
        case BZ_PARAM_ERROR: return "PARAM_ERROR";
        case BZ_MEM_ERROR: return "MEM_ERROR";
        case BZ_DATA_ERROR: return "DATA_ERROR";
        case BZ_DATA_ERROR_MAGIC: return "DATA_ERROR_MAGIC";
        case BZ_IO_ERROR: return "IO_ERROR";
        case BZ_UNEXPECTED_EOF: return "UNEXPECTED_EOF";
        case BZ_OUTBUFF_FULL: return "OUTBUFF_FULL";
        case BZ_CONFIG_ERROR: return "CONFIG_ERROR";
        default: return "UNKNOWN ERROR";
        }
    }
    
    BzlibStreamCommon()
    {
        bzalloc = nullptr;
        bzfree = nullptr;
        opaque = nullptr;
    }
    
    int (*process) (bz_stream * stream, int flush) = nullptr;
    
    size_t pump(const char * data, size_t len, const OnData & onData,
                int flushLevel)
    {
        //cerr << "pump " << len << " bytes with flush level "
        //     << flushLevel << endl;

        size_t bufSize = 131072;
        char output[bufSize];
        next_in = (char *)data;
        avail_in = len;
        size_t result = 0;

        do {
            next_out = (char *)output;
            avail_out = bufSize;

            int res = process(this, flushLevel);

            //cerr << "pumping " << len << " bytes through with flushLevel "
            //     << flushLevel << " returned " << res << endl;

            size_t bytesWritten = (const char *)next_out - output;

            switch (res) {
            case BZ_RUN_OK:
            case BZ_FLUSH_OK:
                if (bytesWritten)
                    onData(output, bytesWritten);
                result += bytesWritten;
                break;

            case BZ_FINISH_OK:
            case BZ_STREAM_END:
                if (bytesWritten)
                    onData(output, bytesWritten);
                result += bytesWritten;
                return result;

            default:
                throw Exception("bzip2 error: " + bzerror(res));
            };
        } while (avail_in != 0);
        
        if (flushLevel == BZ_FINISH)
            throw Exception("finished without getting to BZ_STREAM_END");

        return result;
    }

    size_t flush(FlushLevel flushLevel, const OnData & onData)
    {
        int bzlibFlushLevel;
        switch (flushLevel) {
        case Compressor::FLUSH_NONE:       bzlibFlushLevel = BZ_RUN;       break;
        case Compressor::FLUSH_AVAILABLE:  bzlibFlushLevel = BZ_RUN;       break;
        case Compressor::FLUSH_SYNC:       bzlibFlushLevel = BZ_FLUSH;     break;
        case Compressor::FLUSH_RESTART:    bzlibFlushLevel = BZ_FINISH;    break;
        default:
            throw Exception("bad flush level");
        }
        
        return pump(0, 0, onData, bzlibFlushLevel);
    }

    size_t finish(const OnData & onData)
    {
        return pump(0, 0, onData, BZ_FINISH);
    }
};


/*****************************************************************************/
/* BZIP COMPRESSOR                                                           */
/*****************************************************************************/

struct BzipCompressor : public Compressor, public BzlibStreamCommon {

    typedef Compressor::OnData OnData;
    typedef Compressor::FlushLevel FlushLevel;
    
    BzipCompressor(int compressionLevel)
    {
        if (compressionLevel == -1)
            compressionLevel = 9; // speed is unaffected by block size, from doc
        int res = BZ2_bzCompressInit(this, compressionLevel, 0 /* verbosity */,
                                     0 /* default work factor */);
        if (res != BZ_OK)
            throw Exception("BZ2_bzCompressInit failed: " + bzerror(res));

        this->process = &BZ2_bzCompress;
    }

    virtual ~BzipCompressor()
    {
        BZ2_bzCompressEnd(this);
    }

    virtual void compress(const char * data, size_t len,
                          const OnData & onData)
    {
        pump(data, len, onData, BZ_RUN);
    }
    
    virtual void flush(FlushLevel flushLevel, const OnData & onData) override
    {
        BzlibStreamCommon::flush(flushLevel, onData);
    }

    virtual void finish(const OnData & onData) override
    {
        BzlibStreamCommon::finish(onData);
    }
};

static Compressor::Register<BzipCompressor>
registerBzipCompressor("bzip2", {"bz2", "bzip2"});


/*****************************************************************************/
/* BZIP DECOMPRESSOR                                                         */
/*****************************************************************************/

struct BzipDecompressor: public Decompressor, public BzlibStreamCommon {

    typedef Decompressor::OnData OnData;

    static int bz_decompress(bz_stream * stream, int flush)
    {
        ExcAssertEqual(flush, BZ_RUN);
        int result = BZ2_bzDecompress(stream);
        //cerr << "bz2 decompress returned " << bzerror(result) << endl;
        return result;
    }
    
    BzipDecompressor()
    {
        //cerr << "creating decompressor" << endl;

        int res = BZ2_bzDecompressInit(this, 0 /* verbosity */, 0 /* small */);
        if (res != BZ_OK)
            throw Exception("BZ2_decompressInit failed: " + bzerror(res));
        this->process = &bz_decompress;
    }

    ~BzipDecompressor()
    {
        BZ2_bzDecompressEnd(this);
    }

    virtual int64_t decompressedSize(const char * block, size_t blockLen,
                                     int64_t totalLen) const override
    {
        // TODO: can probably do better than this...
        return LENGTH_UNKNOWN;
    }
    
    virtual void decompress(const char * data, size_t len,
                            const OnData & onData) override
    {
        pump(data, len, onData, BZ_RUN);
    }
    
    virtual void finish(const OnData & onData) override
    {
    }
};

static Decompressor::Register<BzipDecompressor>
registerBzipDecompressor("bzip2", {"bz2", "bzip2"});

} // namespace MLDB
