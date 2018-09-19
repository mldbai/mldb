/* compressor.cc
   Jeremy Barnes, 19 September 2012
   Copyright (c) 2016 Mldb.ai Inc.  All rights reserved.
   This file is part of MLDB. Copyright 2016 mldb.ai Inc. All rights reserved.

   Implementation of compressor abstraction.
*/

#include "compressor.h"
#include "mldb/base/exc_assert.h"
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
static Compressor::Register<NullCompressor>
registerNoneCompressor("none", {});


/*****************************************************************************/
/* NULL DECOMPRESSOR                                                           */
/*****************************************************************************/

struct NullDecompressor : public Decompressor {

    virtual ~NullDecompressor();

    virtual int64_t decompressedSize(const char * block, size_t blockLen,
                                     int64_t totalLen) const override;

    virtual size_t decompress(const char * data, size_t len,
                              const OnData & onData) override;

    virtual size_t finish(const OnData & onData) override;
};

NullDecompressor::
~NullDecompressor()
{
}

int64_t
NullDecompressor::
decompressedSize(const char * block, size_t blockLen,
                 int64_t totalLen) const
{
    return totalLen;
}

size_t
NullDecompressor::
decompress(const char * data, size_t len, const OnData & onData)
{
    size_t done = 0;

    while (done < len)
        done += onData(data + done, len - done);
    
    ExcAssertEqual(done, len);

    return done;
}
    
size_t
NullDecompressor::
finish(const OnData & onData)
{
    return 0;
}

static Decompressor::Register<NullDecompressor>
registerNullDecompressor("null", {});
static Decompressor::Register<NullDecompressor>
registerNoneDecompressor("none", {});


} // namespace MLDB
