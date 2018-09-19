/* zlib.cc
   Jeremy Barnes, 19 September 2012
   Copyright (c) 2016 Mldb.ai Inc.  All rights reserved.
   This file is part of MLDB. Copyright 2016 mldb.ai Inc. All rights reserved.

   Implementation of the zlib compression format.
*/

#include "compressor.h"
#include "mldb/arch/endian.h"
#include <zlib.h>
#include "mldb/base/exc_assert.h"
#include <iostream>


using namespace std;


namespace MLDB {

/*****************************************************************************/
/* ZLIB STREAM COMMON                                                        */
/*****************************************************************************/

struct ZlibStreamCommon: public z_stream {

    typedef Compressor::OnData OnData;
    typedef Compressor::FlushLevel FlushLevel;
    
    ZlibStreamCommon()
    {
        zalloc = nullptr;
        zfree = nullptr;
        opaque = nullptr;
    }
    
    int (*process) (z_streamp stream, int flush) = nullptr;
    
    size_t pump(const char * data, size_t len, const OnData & onData,
                int flushLevel)
    {
        size_t bufSize = 131072;
        char output[bufSize];
        next_in = (Bytef *)data;
        avail_in = len;
        size_t result = 0;

        do {
            next_out = (Bytef *)output;
            avail_out = bufSize;

            int res = process(this, flushLevel);

            //cerr << "pumping " << len << " bytes through with flushLevel "
            //     << flushLevel << " returned " << res << endl;

            size_t bytesWritten = (const char *)next_out - output;

            switch (res) {
            case Z_OK:
                if (bytesWritten)
                    onData(output, bytesWritten);
                result += bytesWritten;
                break;

            case Z_STREAM_ERROR:
                throw Exception("Stream error on zlib");

            case Z_STREAM_END:
                if (bytesWritten)
                    onData(output, bytesWritten);
                result += bytesWritten;
                return result;

            default:
                throw Exception("unknown output from zlib: "
                                + string(zError(res)));
            };
        } while (avail_in != 0);

        if (flushLevel == Z_FINISH)
            throw Exception("finished without getting to Z_STREAM_END");

        return result;
    }

    void flush(FlushLevel flushLevel, const OnData & onData)
    {
        int zlibFlushLevel;
        switch (flushLevel) {
        case Compressor::FLUSH_NONE:       zlibFlushLevel = Z_NO_FLUSH;       break;
        case Compressor::FLUSH_AVAILABLE:  zlibFlushLevel = Z_PARTIAL_FLUSH;  break;
        case Compressor::FLUSH_SYNC:       zlibFlushLevel = Z_SYNC_FLUSH;     break;
        case Compressor::FLUSH_RESTART:    zlibFlushLevel = Z_FULL_FLUSH;     break;
        default:
            throw Exception("bad flush level");
        }

        pump(0, 0, onData, zlibFlushLevel);
    }

    void finish(const OnData & onData)
    {
        pump(0, 0, onData, Z_FINISH);
    }
};


/*****************************************************************************/
/* GZIP COMPRESSOR                                                           */
/*****************************************************************************/

struct GzipCompressor : public Compressor, public ZlibStreamCommon {

    typedef Compressor::OnData OnData;
    typedef Compressor::FlushLevel FlushLevel;
    
    GzipCompressor(int compressionLevel)
    {
        int res = deflateInit2(this, compressionLevel, Z_DEFLATED, 15 + 16, 9,
                               Z_DEFAULT_STRATEGY);
        if (res != Z_OK)
            throw Exception("deflateInit2 failed");

        this->process = &deflate;
    }

    virtual ~GzipCompressor()
    {
        deflateEnd(this);
    }

    virtual void compress(const char * data, size_t len,
                            const OnData & onData)
    {
        pump(data, len, onData, Z_NO_FLUSH);
    }
    
    virtual void flush(FlushLevel flushLevel, const OnData & onData) override
    {
        ZlibStreamCommon::flush(flushLevel, onData);
    }

    virtual void finish(const OnData & onData) override
    {
        ZlibStreamCommon::finish(onData);
    }
};

static Compressor::Register<GzipCompressor>
registerGzipCompressor("gzip", {"gz"});


/*****************************************************************************/
/* GZIP DECOMPRESSOR                                                         */
/*****************************************************************************/

struct GzipHeaderFields {
    uint16_le id = 0x1f8b;
    uint8_t compressionMethod = 0;
    uint8_t flags = 0;
    uint32_le timestamp = 0;
    uint8_t xflags = 0;
    uint8_t os = 0;
} MLDB_PACKED;

static_assert(sizeof(GzipHeaderFields) == 10, "Gzip header fields wrong size");

struct GzipHeaderReader {
    enum State {
        HEADER,
        EXTRA_LEN,
        EXTRA,
        FILENAME,
        COMMENT,
        CHECKSUM,
        FINISHED
    };

    enum {
        FTEXT = 0x01,
        FHCRC = 0x02,
        FEXTRA = 0x04,
        FNAME = 0x08,
        FCOMMENT = 0x10
    };
    
    GzipHeaderFields header;
    uint16_le extraLen = 0;
    std::string extra;
    std::string filename;
    std::string comment;
    uint16_le checksum;

    GzipHeaderReader()
        : state(HEADER),
          out((char *)&header),
          remaining(sizeof(header))
          
    {
    }
    
    bool process(char c)
    {
        if (out) {
            *out++ = c;
            --remaining;
            if (remaining)
                return false;
        }
        else if (buf) {
            if (c != 0) {
                *buf += c;
                return false;
            }
        }
        else {
            ExcAssert(false);
        }

        //cerr << "finished field " << state << endl;
        //cerr << "id " << header.id << " " << header.id / 256 << " " << header.id % 256 << endl;
        //cerr << "flags " << (int)header.flags << endl;
        
        // Finished current field
        switch (state) {
        case HEADER:
            // Finished header
            if (header.flags & FEXTRA) {
                state = EXTRA_LEN;
                out = (char *)(&extraLen);
                buf = nullptr;
                remaining = 2;
                break;
            }
            // fall through
        case EXTRA:
            if (header.flags & FNAME) {
                state = FILENAME;
                out = nullptr;
                buf = &filename;
                break;
            }
            // fall through
        case FILENAME:
            if (header.flags & FCOMMENT) {
                state = COMMENT;
                out = nullptr;
                buf = &comment;
                break;
            }
            // fall through
        case COMMENT:
            if (header.flags & FHCRC) {
                state = CHECKSUM;
                out = (char *)&checksum;
                buf = nullptr;
                break;
            }
            // fall through
        case CHECKSUM:
            state = FINISHED;
            break;
        case FINISHED:
            break;
        case EXTRA_LEN:
            extra.resize(extraLen);
            out = extra.data();
            remaining = extraLen;
            buf = nullptr;
            state = EXTRA;
            break;
        }

        return state == FINISHED;
    }

    size_t process(const char * p, size_t len)
    {
        size_t result = 0;
        while (len--) {
            ++result;
            if (process(*p++)) {
                //cerr << "header had " << result << " chars" << endl;
                return result;
            }
        }
        return result;
    }

    bool done() const
    {
        return state == FINISHED;
    }
    
    State state = HEADER;

    // Exactly one of out or buf is not-null.  If out is non-null, then
    // remaining characters will be written to out.  If buf is non-null,
    // then characters until a null will be appended to that string.
    char * out = nullptr;
    ssize_t remaining = 0;

    std::string * buf = nullptr;
};

struct GzipDecompressor: public Decompressor, public ZlibStreamCommon {

    GzipHeaderReader header;

    typedef Decompressor::OnData OnData;
    
    GzipDecompressor()
    {
        int res = inflateInit2(this, -15 /* 15 windowBits, no header */);
        if (res != Z_OK)
            throw Exception("deflateInit2 failed");
        this->process = &inflate;
    }

    ~GzipDecompressor()
    {
        inflateEnd(this);
    }

    virtual int64_t decompressedSize(const char * block, size_t blockLen,
                                     int64_t totalLen) const override
    {
        return LENGTH_UNKNOWN;
    }
    
    virtual void decompress(const char * data, size_t len,
                            const OnData & onData) override
    {
        size_t headerDone = 0;
        if (!header.done()) {
            headerDone = header.process(data, len);
            //cerr << "header used " << headerDone << " characters" << endl;
            if (headerDone == len)
                return;
            ExcAssert(header.done());
        }

        pump(data + headerDone,
             len - headerDone, onData,
             Z_NO_FLUSH);
    }
    
    virtual void finish(const OnData & onData) override
    {
        pump(0, 0, onData, Z_FINISH);
    }
};

static Decompressor::Register<GzipDecompressor>
registerGzipDecompressor("gzip", {"gz", "gzip"});

} // namespace MLDB
