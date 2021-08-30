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
#include <cstring>
#include "mldb/base/thread_pool.h"


using namespace std;

namespace MLDB {


/*****************************************************************************/
/* COMPRESSOR                                                                */
/*****************************************************************************/

Compressor::
~Compressor()
{
}

void
Compressor::
notifyInputSize(uint64_t inputSize)
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

void
Decompressor::
decompress(std::shared_ptr<const char> data, size_t len,
           const OnSharedData & onSharedData,
           const Allocate & allocate)
{
    auto onData = [&] (const char * p, size_t len)
        {
            auto block = allocate(len);
            std::memcpy(block.get(), p, len);
            onSharedData(std::move(block), len);
            return len;
        };

    decompress(data.get(), len, onData);
}

void
Decompressor::
finish(const OnSharedData & onSharedData,
       const Allocate & allocate)
{
    auto onData = [&] (const char * p, size_t len)
        {
            auto block = allocate(len);
            std::memcpy(block.get(), p, len);
            onSharedData(std::move(block), len);
            return len;
        };

    finish(onData);
}

bool
Decompressor::
forEachBlockParallel(size_t requestedBlockSize,
                     const GetDataFunction & getData,
                     const ForEachBlockFunction & onBlock,
                     const Allocate & allocate,
                     int maxParallelism)
{
    bool finished = false;
    size_t blockNumber = 0;
    size_t currentOffset = 0;
    size_t numChars = 0;
    std::shared_ptr<const char> buf;

    ThreadWorkGroup tp(maxParallelism);
    
    while (std::get<0>((std::tie(buf, numChars) = getData(requestedBlockSize)))) {
        auto onData = [&] (std::shared_ptr<const char> data, size_t len) -> size_t
            {
                if (finished)
                    return len;

                size_t myBlockNumber = blockNumber++;
                size_t myOffset = currentOffset;

                auto doBlock = [myBlockNumber, myOffset, data = std::move(data),
                                len, &finished, &onBlock] ()
                {
                    if (finished)
                        return;
                    if (!onBlock(myBlockNumber, myOffset, std::move(data), len)) {
                        finished = true;
                    }
                };

                if (maxParallelism > 0) {
                    tp.add(std::move(doBlock));
                }
                else {
                    doBlock();
                }
                
                currentOffset += len;
                return len;
            };
        
        decompress(buf, numChars, onData, allocate);
    }

    tp.waitForAll();
    
    return !finished;
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

    virtual void compress(const char * data, size_t len,
                          const OnData & onData) override;
    
    virtual void flush(FlushLevel flushLevel, const OnData & onData) override;

    virtual void finish(const OnData & onData) override;
};

NullCompressor::
NullCompressor(int level)
{
}

NullCompressor::
~NullCompressor()
{
}

void
NullCompressor::
compress(const char * data, size_t len, const OnData & onData)
{
    size_t done = 0;

    while (done < len)
        done += onData(data + done, len - done);
    
    ExcAssertEqual(done, len);
}
    
void
NullCompressor::
flush(FlushLevel flushLevel, const OnData & onData)
{
}

void
NullCompressor::
finish(const OnData & onData)
{
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
    
    virtual void decompress(const char * data, size_t len,
                              const OnData & onData) override;

    
    virtual void finish(const OnData & onData) override;

    using Decompressor::decompress;
    using Decompressor::finish;
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

void
NullDecompressor::
decompress(const char * data, size_t len, const OnData & onData)
{
    size_t done = 0;

    while (done < len)
        done += onData(data + done, len - done);
    
    ExcAssertEqual(done, len);
}
    
void
NullDecompressor::
finish(const OnData & onData)
{
}

static Decompressor::Register<NullDecompressor>
registerNullDecompressor("null", {});
static Decompressor::Register<NullDecompressor>
registerNoneDecompressor("none", {});


} // namespace MLDB
