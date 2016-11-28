/* compressor.cc
   Jeremy Barnes, 19 September 2012
   Copyright (c) 2016 Mldb.ai Inc.  All rights reserved.
   This file is part of MLDB. Copyright 2016 mldb.ai Inc. All rights reserved.

   Implementation of compressor abstraction.
*/

#include "compressor.h"
#include "mldb/base/exc_assert.h"
#include <zlib.h>
#include <iostream>
#include <mutex>
#include <map>

using namespace std;

namespace MLDB {


/*****************************************************************************/
/* COMPRESSOR                                                                */
/*****************************************************************************/

Compressor::
~Compressor()
{
}

namespace {

std::mutex mutex;
std::map<std::string, std::string> extensions;
std::map<std::string, Compressor::Info> compressors;
std::map<std::string, Decompressor::Info> decompressors;

} // file scope

std::string
Compressor::
filenameToCompression(const std::string & filename)
{
    std::unique_lock<std::mutex> guard(mutex);

    for (string::size_type pos = filename.rfind('.');
         pos != string::npos && pos > 0;
         pos = filename.rfind('.', pos - 1)) {
        
        std::string extension(filename, pos + 1);

        while (!extension.empty() && extension[extension.size() - 1] == '~') {
            extension = string(extension, 0, extension.size() - 1);
        }

        if (extension.empty())
            return std::string();

        auto it = extensions.find(extension);
        if (it != extensions.end()) {
            return it->second;
        }
    }

    return std::string();
}

Compressor *
Compressor::
create(const std::string & compression,
       int level)
{
    std::unique_lock<std::mutex> guard(mutex);
    auto it = compressors.find(compression);
    if (it == compressors.end()) {
        throw Exception("unknown compression %s:%d", compression.c_str(),
                        level);
    }
    return it->second.create(level);
}

std::shared_ptr<void>
Compressor::
registerCompressor(const std::string & name,
                   const std::vector<std::string> & compressorExtensions,
                   std::function<Compressor * (int level)> create)
{
    Info info{ name, compressorExtensions, std::move(create)};
    std::unique_lock<std::mutex> guard(mutex);
    if (!compressors.emplace(name, info).second) {
        throw Exception("Attempt to double register compressor " + name);
    }

    for (auto & ex: compressorExtensions) {
        extensions.emplace(ex, name);
    }
    
    return nullptr;
}

const Compressor::Info &
Compressor::
getCompressorInfo(const std::string & compressor)
{
    std::unique_lock<std::mutex> guard(mutex);
    auto it = compressors.find(compressor);
    if (it == compressors.end()) {
        throw Exception("Unknown compressor " + compressor);
    }
    return it->second;
}


/*****************************************************************************/
/* DECOMPRESSOR                                                              */
/*****************************************************************************/

Decompressor::
~Decompressor()
{
}

Decompressor *
Decompressor::
create(const std::string & decompression)
{
    std::unique_lock<std::mutex> guard(mutex);
    auto it = decompressors.find(decompression);
    if (it == decompressors.end()) {
        throw Exception("unknown decompression %s", decompression.c_str());
    }
    return it->second.create();
}

std::shared_ptr<void>
Decompressor::
registerDecompressor(const std::string & name,
                     const std::vector<std::string> & compressorExtensions,
                     std::function<Decompressor * ()> create)
{
    Info info{ name, compressorExtensions, std::move(create)};
    std::unique_lock<std::mutex> guard(mutex);
    if (!decompressors.emplace(name, info).second) {
        throw Exception("Attempt to double register compressor " + name);
    }

    for (auto & ex: compressorExtensions) {
        extensions.emplace(ex, name);
    }
    
    return nullptr;
}

const Decompressor::Info &
Decompressor::
getDecompressorInfo(const std::string & decompressor)
{
    std::unique_lock<std::mutex> guard(mutex);
    auto it = decompressors.find(decompressor);
    if (it == decompressors.end()) {
        throw Exception("Unknown decompressor " + decompressor);
    }
    return it->second;
}


/*****************************************************************************/
/* NULL COMPRESSOR                                                           */
/*****************************************************************************/

struct NullCompressor : public Compressor {

    NullCompressor(int level = 0);

    virtual ~NullCompressor();

    virtual size_t compress(const char * data, size_t len,
                            const OnData & onData);
    
    virtual size_t flush(FlushLevel flushLevel, const OnData & onData);

    virtual size_t finish(const OnData & onData);
};

NullCompressor::
NullCompressor(int level)
{
}

NullCompressor::
~NullCompressor()
{
}

size_t
NullCompressor::
compress(const char * data, size_t len, const OnData & onData)
{
    size_t done = 0;

    while (done < len)
        done += onData(data + done, len - done);
    
    ExcAssertEqual(done, len);

    return done;
}
    
size_t
NullCompressor::
flush(FlushLevel flushLevel, const OnData & onData)
{
    return 0;
}

size_t
NullCompressor::
finish(const OnData & onData)
{
    return 0;
}

static Compressor::Register<NullCompressor>
registerNullCompressor("null", {});


/*****************************************************************************/
/* GZIP COMPRESSOR                                                           */
/*****************************************************************************/

struct GzipCompressor : public Compressor {

    GzipCompressor(int level);

    virtual ~GzipCompressor();

    void open(int level);

    virtual size_t compress(const char * data, size_t len,
                            const OnData & onData);
    
    virtual size_t flush(FlushLevel flushLevel, const OnData & onData);

    virtual size_t finish(const OnData & onData);

private:
    struct Itl;
    std::unique_ptr<Itl> itl;
};

struct GzipCompressor::Itl : public z_stream {

    Itl(int compressionLevel)
    {
        zalloc = 0;
        zfree = 0;
        opaque = 0;
        int res = deflateInit2(this, compressionLevel, Z_DEFLATED, 15 + 16, 9,
                               Z_DEFAULT_STRATEGY);
        if (res != Z_OK)
            throw Exception("deflateInit2 failed");
    }

    ~Itl()
    {
        deflateEnd(this);
    }

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

            int res = deflate(this, flushLevel);

            
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
                throw Exception("unknown output from deflate");
            };
        } while (avail_in != 0);

        if (flushLevel == Z_FINISH)
            throw Exception("finished without getting to Z_STREAM_END");

        return result;
    }


    size_t compress(const char * data, size_t len, const OnData & onData)
    {
        return pump(data, len, onData, Z_NO_FLUSH);
    }

    size_t flush(FlushLevel flushLevel, const OnData & onData)
    {
        int zlibFlushLevel;
        switch (flushLevel) {
        case FLUSH_NONE:       zlibFlushLevel = Z_NO_FLUSH;       break;
        case FLUSH_AVAILABLE:  zlibFlushLevel = Z_PARTIAL_FLUSH;  break;
        case FLUSH_SYNC:       zlibFlushLevel = Z_SYNC_FLUSH;     break;
        case FLUSH_RESTART:    zlibFlushLevel = Z_FULL_FLUSH;     break;
        default:
            throw Exception("bad flush level");
        }

        return pump(0, 0, onData, zlibFlushLevel);
    }

    size_t finish(const OnData & onData)
    {
        return pump(0, 0, onData, Z_FINISH);
    }
};

GzipCompressor::
GzipCompressor(int compressionLevel)
{
    itl.reset(new Itl(compressionLevel));
}

GzipCompressor::
~GzipCompressor()
{
}

void
GzipCompressor::
open(int compressionLevel)
{
    itl.reset(new Itl(compressionLevel));
}

size_t
GzipCompressor::
compress(const char * data, size_t len, const OnData & onData)
{
    return itl->compress(data, len, onData);
}
    
size_t
GzipCompressor::
flush(FlushLevel flushLevel, const OnData & onData)
{
    return itl->flush(flushLevel, onData);
}

size_t
GzipCompressor::
finish(const OnData & onData)
{
    return itl->finish(onData);
}

static Compressor::Register<GzipCompressor>
registerGzipCompressor("gzip", {"gz"});

#if 0
/*****************************************************************************/
/* LZMA COMPRESSOR                                                           */
/*****************************************************************************/

struct LzmaCompressor : public Compressor {

    LzmaCompressor();

    LzmaCompressor(int level);

    ~LzmaCompressor();

    virtual size_t compress(const char * data, size_t len,
                            const OnData & onData);
    
    virtual size_t flush(FlushLevel flushLevel, const OnData & onData);

    virtual size_t finish(const OnData & onData);

private:
    struct Itl;
    std::unique_ptr<Itl> itl;
};
#endif

} // namespace Mldb
