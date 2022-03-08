/* zstandard.cc
   Jeremy Barnes, 27 November 2016
   Copyright (c) 2016 Mldb.ai Inc.  All rights reserved.
   This file is part of MLDB. Copyright 2016 mldb.ai Inc. All rights reserved.

   Zstandard compressor and decompressors.
*/

#define ZSTD_STATIC_LINKING_ONLY 1

#include "compressor.h"
#include "mldb/base/exc_assert.h"
#include "mldb/ext/zstd/lib/zstd.h"
#include "mldb/base/thread_pool.h"
#include "mldb/arch/exception.h"
#include "mldb/arch/endian.h"
#include <zlib.h>
#include <iostream>
#include <span>

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

    virtual void compress(const char * data, size_t len, const OnData & onData) override
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
    }
    
    virtual void flush(FlushLevel flushLevel, const OnData & onData) override
    {
        size_t bytesLeft = -1;
        while (bytesLeft != 0) {
            outBuf.pos = 0;
            bytesLeft = ZSTD_flushStream(stream, &outBuf);
            writeAll(onData);
        }
    }

    virtual void finish(const OnData & onData) override
    {
        size_t bytesLeft = -1;
        while (bytesLeft != 0) {
            outBuf.pos = 0;
            bytesLeft = ZSTD_endStream(stream, &outBuf);
            writeAll(onData);
        }
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

    using Decompressor::decompress;
    using Decompressor::finish;

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

    virtual int64_t decompressedSize(const char * block, size_t blockLen,
                                     int64_t totalLen) const override
    {
        unsigned long long res = ZSTD_getFrameContentSize(block, blockLen);
        if (res == ZSTD_CONTENTSIZE_UNKNOWN) {
            return LENGTH_UNKNOWN;  // 0 means zero length OR unknown
        }
        else if (res == ZSTD_CONTENTSIZE_ERROR) {
            return LENGTH_INSUFFICIENT_DATA;
        }
        else return res;        
    }
    
    virtual void decompress(const char * data, size_t len,
                            const OnData & onData) override
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
    }

    virtual bool
    forEachBlockParallel(size_t requestedBlockSize,
                         const GetDataFunction & getData,
                         const ForEachBlockFunction & onBlock,
                         const Allocate & allocate,
                         int maxParallelism) override
    {
        return Decompressor::forEachBlockParallel(requestedBlockSize, getData, onBlock, allocate, maxParallelism);
        // To do this, we would need to:
        // - Get fixed sized chunks from the underlying stream
        // - Pass each to a stream
        // - For each one, scan for the magic characters to indicate the beginning
        // - Decompress the blocks (taking into account the possibility that there are
        //   spurious magic markers)
        // - Scan into the next one's blocks to finish off the last one
        // - Verify that each before and after block agrees on what they are scanning for
#if 0
        ThreadWorkGroup tp(maxParallelism);

        std::shared_ptr<const char> lastBlock;
        std::shared_ptr<const char> currentBlock;
        const char * currentData = nullptr;
        size_t currentBlockLeft = 0;

        auto makeAvail = [&] (size_t numBytes) -> std::span<const char>
        {
            if (numBytes > currentBlockLeft) {
                auto [newBlock, newBlockSize] = getData(numBytes);
                if (currentBlockLeft == 0) {
                    // First block or previous one completely used
                    cerr << "first block" << endl;
                    currentBlock = std::move(newBlock);
                    currentBlockLeft = newBlockSize;
                    currentData = currentBlock.get();
                }
                else {
                    if (newBlock.get() == currentData + currentBlockLeft) {
                        // Contiguous; must be mapped
                        cerr << "contiguous" << endl;
                        lastBlock = std::move(currentBlock);
                        currentBlock = std::move(newBlock);
                        currentBlockLeft += newBlockSize;
                    }
                    else {
                        // Combine them together (slow, should try to avoid making this happen)
                        cerr << "combined" << endl;
                        std::shared_ptr<char> combinedBlock
                            (new char[newBlockSize + currentBlockLeft],
                             [] (auto p) { delete[] p; });
                        memcpy(combinedBlock.get(), currentData, currentBlockLeft);
                        memcpy(combinedBlock.get() + currentBlockLeft, newBlock.get(), newBlockSize);
                        lastBlock = {};
                        currentBlock = std::move(combinedBlock);
                        currentData = combinedBlock.get();
                        currentBlockLeft += newBlockSize;
                    }
                }
            }

            ExcAssertLessEqual(numBytes, currentBlockLeft);

            return { currentData, numBytes };
        };

        auto consume = [&] (size_t numBytes)
        {
            ExcAssertGreaterEqual(currentBlockLeft, numBytes);
            currentData += numBytes;
            currentBlockLeft -= numBytes;
        };

        //auto magicBytes = makeAvail(4);
        //uint32_le magic;
        //std::memcpy(&magic, magicBytes.data(), 4);
        //if (magic != ZSTD_MAGICNUMBER)
        //    throw MLDB::Exception("Corrupted zstandard file: magic number is wrong");

        std::shared_ptr<ZSTD_DCtx> context(ZSTD_createDCtx(), ZSTD_freeDCtx);
        std::span<const char> header = makeAvail(ZSTD_FRAMEHEADERSIZE_MAX);
        ZSTD_frameHeader frameHeader;
        cerr << "Frame header: max size " << ZSTD_FRAMEHEADERSIZE_MAX << endl;
        hex_dump(header.data(), header.size());
        size_t headerResult = ZSTD_getFrameHeader(&frameHeader, header.data(), header.size());
        if (headerResult != 0)
            throw MLDB::Exception("Corrupted ZStandard file: frame header not available");
        //consume(frameHeader.headerSize);

        cerr << "dict id " << frameHeader.dictID << endl;
        cerr << "content size " << frameHeader.frameContentSize << endl;
        cerr << "window size " << frameHeader.windowSize << endl;
        cerr << "frame type " << frameHeader.frameType << endl;
        cerr << "header size " << frameHeader.headerSize << endl;
        cerr << "block size max " << frameHeader.blockSizeMax << endl;

        size_t outputBufSize = frameHeader.windowSize * 2;
        std::shared_ptr<char[]> outputBuf(new char[outputBufSize]);
        char * outputBufPtr = outputBuf.get();
        char * outputBufEnd = outputBufPtr + outputBufSize;

        size_t res = ZSTD_decompressBegin(context.get());
        //cerr << "res = " << res << endl;
        if (ZSTD_isError(res)) {
            throw Exception("Error beginning decompression: %s",
                            ZSTD_getErrorName(res));
        }
        for (size_t i = 0;  i < 5;  ++i) {
            size_t nextSize = ZSTD_nextSrcSizeToDecompress(context.get());
            cerr << "next size is " << nextSize << endl;
            if (ZSTD_isError(nextSize)) {
                throw Exception("Error getting size to decompress: %s",
                                ZSTD_getErrorName(nextSize));
            }
            auto data = makeAvail(nextSize);
            consume(nextSize);
            size_t written = ZSTD_decompressContinue(context.get(), outputBufPtr, outputBufEnd - outputBufPtr,
                                                     data.data(), data.size());
            cerr << "written " << written << " bytes" << endl;
            if (ZSTD_isError(written)) {
                throw Exception("Error continuing decompression: %s",
                                ZSTD_getErrorName(written));
            }
            outputBufPtr += written;
        }

        throw MLDB::Exception("not finished");
#endif
    }

    virtual void finish(const OnData & onData) override
    {
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

