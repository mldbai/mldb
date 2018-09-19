/* lz4.cc
   Jeremy Barnes, 19 September 2012
   Copyright (c) 2016 Mldb.ai Inc.  All rights reserved.
   This file is part of MLDB. Copyright 2016 mldb.ai Inc. All rights reserved.

   Implementation of the zlib compression format.
*/

#include "compressor.h"
#include "mldb/arch/endian.h"
#include "mldb/base/exc_assert.h"
#include <iostream>
#include "lz4_filter.h"
#include "mldb/base/exc_assert.h"

using namespace std;


namespace MLDB {


/*****************************************************************************/
/* LZ4 COMPRESSOR                                                            */
/*****************************************************************************/

struct Lz4Compressor : public Compressor {

    typedef Compressor::OnData OnData;
    typedef Compressor::FlushLevel FlushLevel;
    
    Lz4Compressor(int level, uint8_t blockSizeId = 7)
        : head(blockSizeId,
               true /* independent blocks */,
               false /* block checksum */,
               false /* stream checksum */),
          writeHeader(true),
          pos(0)
    {
        buffer.resize(head.blockSize());
        compressFn = level < 3 ? LZ4_compress : LZ4_compressHC;
        
        if (head.streamChecksum()) {
            streamChecksumState = XXH32_createState();
            if (XXH32_reset(streamChecksumState, lz4::ChecksumSeed) != XXH_OK) {
                throw Exception("Error with XXhash checksum initialization");
            }
        }
    }

    virtual ~Lz4Compressor()
    {
        if (streamChecksumState)
            XXH32_freeState(streamChecksumState);
    }

    virtual size_t compress(const char * s, size_t n,
                            const OnData & onData)
    {
        if (writeHeader) {
            head.write(onData);
            writeHeader = false;
        }

        size_t toWrite = n;
        while (toWrite > 0) {
            size_t toCopy = std::min<size_t>(toWrite, buffer.size() - pos);
            std::memcpy(buffer.data() + pos, s, toCopy);

            toWrite -= toCopy;
            pos += toCopy;
            s += toCopy;

            if (pos == buffer.size()) flush(FLUSH_SYNC, onData);
        }
        
        return n;
    }
    
    virtual size_t flush(FlushLevel flushLevel, const OnData & onData)
    {
        if (pos == 0)
            return 0;

        if (head.streamChecksum())
            XXH32_update(streamChecksumState, buffer.data(), pos);

        size_t bytesToAlloc = LZ4_compressBound(pos);
        ExcAssert(bytesToAlloc);
        char* compressed = new char[bytesToAlloc];
        Scope_Exit(delete[] compressed);
        
        auto compressedSize = compressFn(buffer.data(), compressed, pos);

        auto writeChecksum = [&](const char* data, size_t n) {
            if (!head.blockChecksum()) return;
            uint32_le checksum = XXH32(data, n, lz4::ChecksumSeed);
            compressedSize += write(onData, &checksum, sizeof(checksum));
        };

        if (compressedSize > 0) {
            uint32_le head = compressedSize;
            compressedSize += write(onData, &head, sizeof(head));
            compressedSize += write(onData, compressed, head);
            writeChecksum(compressed, head);
        }
        else {
            uint32_le head = pos | lz4::NotCompressedMask; // uncompressed flag.
            compressedSize += write(onData, &head, sizeof(uint32_t));
            compressedSize += write(onData, buffer.data(), pos);
            writeChecksum(buffer.data(), pos);
        }
        
        pos = 0;

        return compressedSize;
    }

    virtual size_t finish(const OnData & onData)
    {
        size_t result = 0;
        if (writeHeader) result += head.write(onData);
        if (pos) result += flush(FLUSH_RESTART, onData);

        const uint32_le eos = 0;
        result += write(onData, &eos, sizeof(eos));

        if (head.streamChecksum()) {
            uint32_le checksum = XXH32_digest(streamChecksumState);
            result += write(onData, &checksum, sizeof(checksum));
        }
        return result;
    }

    // write all data
    size_t write(const OnData & onData, const void * mem, size_t len)
    {
        size_t done = 0;
        while (done < len) {
            done += onData(((const char *)mem) + done,
                           len - done);
        }
        return done;
    }
    
    lz4::Header head;
    int (*compressFn)(const char*, char*, int);

    bool writeHeader;
    std::vector<char> buffer;
    size_t pos;
    XXH32_state_t* streamChecksumState = nullptr;
};

static Compressor::Register<Lz4Compressor>
registerLz4Compressor("lz4", {"lz4"});


/*****************************************************************************/
/* LZ4 DECOMPRESSOR                                                         */
/*****************************************************************************/


struct Lz4Decompressor: public Decompressor {

    typedef Decompressor::OnData OnData;
    
    Lz4Decompressor()
    {
        setCur(HEADER, header);
    }

    ~Lz4Decompressor()
    {
        if (streamChecksumState)
            XXH32_freeState(streamChecksumState);
    }
    
    virtual int64_t decompressedSize(const char * block, size_t blockLen,
                                     int64_t totalLen) const override
    {
        return LENGTH_UNKNOWN;
    }
    
    virtual size_t decompress(const char * data, size_t len,
                              const OnData & onData) override
    {
        if (!cur) {
            throw Exception("Extra junk at end of compressed lz4 data");
        }

        size_t done = 0;
        while (done < len) {
            size_t toRead = std::min<size_t>(limit - cur, len - done);
            std::memcpy(cur, data + done, toRead);
            done += toRead;
            cur += toRead;
            
            if (cur == limit) {
                // we've finished our field
                // Switch to the next state
                switch (state) {

                case HEADER:
                    header.validate();

                    // Finished our file header
                    setCur(BLOCK_HEADER, blockHeader);
                    if (header.streamChecksum()) {
                        streamChecksumState = XXH32_createState();
                        if (XXH32_reset(streamChecksumState, lz4::ChecksumSeed) != XXH_OK) {
                            throw Exception("Error with XXhash checksum initialization");
                        }
                    }
                    break;

                case BLOCK_HEADER:
                    // Finished our block header
                    if (blockHeader == 0) {
                        // end of stream
                        if (header.streamChecksum()) {
                            setCur(STREAM_CHECKSUM, streamChecksum);
                        }
                        else {
                            setCur(FINISHED, nullptr, 0);
                        }
                    }
                    else {
                        uint32_t blockSize = blockHeader;
                        blockSize &= ~lz4::NotCompressedMask;
                        blockData.resize(blockSize);

                        setCur(BLOCK_DATA, blockData);
                    }
                    break;

                case BLOCK_DATA:
                    // Finished our block data
                    if (header.blockChecksum()) {
                        setCur(BLOCK_CHECKSUM, blockChecksum);
                        break;
                    }
                    // fall through if there is no block checksum

                case BLOCK_CHECKSUM: {
                    // Finished our block, or block + checksum
                    if (header.blockChecksum()) {
                        uint32_t checksum = XXH32(blockData.data(),
                                                  blockData.size(),
                                                  lz4::ChecksumSeed);
                        if (checksum != blockChecksum)
                            throw lz4_error("invalid checksum");
                    }
                    
                    std::string output;
                    
                    if (blockHeader & lz4::NotCompressedMask) {
                        output = std::move(blockData);
                    }
                    else {
                        output.resize(header.blockSize());
                        
                        auto decompressed
                            = LZ4_decompress_safe
                                (blockData.data(), output.data(),
                                 blockData.size(), output.size());
                        
                        if (decompressed < 0)
                            throw lz4_error("malformed lz4 stream");

                        output.resize(decompressed);
                    }
                    write(onData, output.data(), output.length());
                    if (header.streamChecksum()) {
                        XXH32_update(streamChecksumState, output.data(), output.size());
                    }
                    setCur(BLOCK_HEADER, blockHeader);
                    break;
                }

                case STREAM_CHECKSUM: {
                    // Finished reading the stream checksum
                    uint32_t checksum = XXH32_digest(streamChecksumState);
                    if (checksum != streamChecksum)
                        throw lz4_error("invalid checksum");
                    setCur(FINISHED, nullptr, 0);
                    break;
                }

                case FINISHED:
                    throw Exception("Extra data after finished");
                }
            }
        }

        return done;
    }
    
    virtual size_t finish(const OnData & onData) override
    {
        if (state != FINISHED)
            throw Exception("lz4 stream is truncated");
        return 0;
    }

    // write all data
    size_t write(const OnData & onData, const void * mem, size_t len)
    {
        size_t done = 0;
        while (done < len) {
            done += onData(((const char *)mem) + done,
                           len - done);
        }
        return done;
    }

    enum State {
        HEADER,
        BLOCK_HEADER,
        BLOCK_DATA,
        BLOCK_CHECKSUM,
        STREAM_CHECKSUM,
        FINISHED
    } state = HEADER;

    char * cur = nullptr;
    char * limit = nullptr;

    void setCur(State state, const void * data, size_t len)
    {
        this->state = state;
        this->cur = (char *)data;
        this->limit = this->cur + len;
    }
    
    template<typename T>
    void setCur(State state, T & field)
    {
        setCur(state, &field, sizeof(field));
    }
    void setCur(State state, std::string & field)
    {
        setCur(state, field.data(), field.length());
    }
    
    lz4::Header header;
    uint32_le blockHeader = 0;
    std::string blockData;
    uint32_le blockChecksum = 0;
    uint32_le streamChecksum = 0;

    XXH32_state_t* streamChecksumState = nullptr;
};

static Decompressor::Register<Lz4Decompressor>
registerLz4Decompressor("lz4", {"lz4"});

} // namespace MLDB
