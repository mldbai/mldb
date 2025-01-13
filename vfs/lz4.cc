/* lz4.cc
   Jeremy Barnes, 19 September 2012
   Copyright (c) 2016 Mldb.ai Inc.  All rights reserved.
   This file is part of MLDB. Copyright 2016 mldb.ai Inc. All rights reserved.

   Implementation of the zlib compression format.
*/

#include "compressor.h"
#include "mldb/ext/xxhash/xxhash.h"
#include "mldb/ext/lz4/lz4.h"
#include "mldb/ext/lz4/lz4hc.h"
#include "mldb/ext/lz4/lz4frame.h"
#include "mldb/arch/endian.h"
#include "mldb/base/exc_assert.h"
#include "mldb/base/thread_pool.h"
#include <iostream>
#include "mldb/base/scope.h"
#include <cstring>
#include <atomic>


using namespace std;


namespace MLDB {

/******************************************************************************/
/* LZ4 ERROR                                                                  */
/******************************************************************************/

struct lz4_error : public std::ios_base::failure
{
    explicit lz4_error(const std::string& msg) : failure(msg) {}
};

namespace lz4 {


/******************************************************************************/
/* UTILS                                                                      */
/******************************************************************************/

static constexpr uint32_t ChecksumSeed = 0;
static constexpr uint32_t NotCompressedMask = 0x80000000;

inline void checkBlockId(int id)
{
    if (id >= 4 && id <= 7) return;
    throw lz4_error("invalid block size id: " + std::to_string(id));
}

template<typename Sink, typename T>
void write(Sink& sink, T* typedData, size_t size)
{
    char* data = (char*) typedData;

    while (size > 0) {
        size_t written = sink.write(sink, data, size);
        if (!written) throw lz4_error("unable to write bytes");

        data += written;
        size -= written;
    }
}

template<typename T>
void write(const std::function<size_t (const char *, size_t)> & onData,
           T* typedData, size_t size)
{
    size_t done = 0;
    while (done < size) {
        done += onData(((const char *)typedData) + done, size - done);
    }
}

template<typename Source, typename T>
void read(Source& src, T* typedData, size_t size)
{
    char* data = (char*) typedData;

    while (size > 0) {
        ssize_t read = src.read(src, data, size);
        if (read < 0) throw lz4_error("premature end of stream");

        data += read;
        size -= read;
    }
}

/******************************************************************************/
/* HEADER                                                                     */
/******************************************************************************/

struct MLDB_PACKED Header
{
    Header() : magic{0} {}
    Header( int blockId,
            bool blockIndependence,
            bool blockChecksum,
            bool streamChecksum,
            bool contentSize) :
        magic{MagicConst}, options{0, 0}
    {
        const uint8_t version = 1; // 2 bits

        checkBlockId(blockId);

        options[0] |= version << 6;
        options[0] |= blockIndependence << 5;
        options[0] |= blockChecksum << 4;
        options[0] |= contentSize << 3;
        options[0] |= streamChecksum << 2;
        options[1] |= blockId << 4;
    }

    explicit operator bool() { return magic; }

    void setContentSize(bool hasContentSize)
    {
        options[0] &= ~(1 << 3);
        options[0] |= (hasContentSize << 3);
    }
    
    int version() const            { return (options[0] >> 6) & 0x3; }
    bool blockIndependence() const { return (options[0] >> 5) & 1; }
    bool blockChecksum() const     { return (options[0] >> 4) & 1; }
    bool contentSize() const       { return (options[0] >> 3) & 1; }
    bool streamChecksum() const    { return (options[0] >> 2) & 1; }
    int blockId() const            { return (options[1] >> 4) & 0x7; }
    size_t blockSize() const       { return 1 << (8 + 2 * blockId()); }

    void validate(uint8_t checkBits, uint64_le knownContentSize)
    {
        if (magic != MagicConst)
            throw lz4_error("invalid magic number");

        if (version() != 1)
            throw lz4_error("unsupported lz4 version");

        if (!blockIndependence())
            throw lz4_error("unsupported option: block dependence");

        checkBlockId(blockId());

        if (checkBits != checksumOptions(knownContentSize))
            throw lz4_error("corrupted options");
    }
    
    template<typename Sink>
    size_t write(Sink& sink, const uint64_le & knownContentSize)
    {
        uint8_t checkBits = checksumOptions(knownContentSize);
        lz4::write(sink, this, sizeof(*this));
        if (contentSize()) {
            lz4::write(sink, &knownContentSize, sizeof(knownContentSize));
        }
        lz4::write(sink, &checkBits, 1);
        return sizeof(*this);
    }

    std::string asString(uint64_t lengthWritten)
    {
        std::string result;
        const std::function<size_t (const char *, size_t)> onData
            = [&] (const char * data, size_t len) -> size_t
        {
            result.append(data, len);
            return len;
        };
        write(onData, lengthWritten);
        return result;
    }

    uint8_t checksumOptions(const uint64_le & knownContentSize) const
    {
        if (contentSize()) {
            // Includes the hash of the content size
            uint8_t buf[10];
            buf[0] = options[0];
            buf[1] = options[1];
            memcpy(buf + 2, &knownContentSize, 8);
            return XXH32(buf, 10, ChecksumSeed) >> 8;
        }

        return XXH32(options, 2, ChecksumSeed) >> 8;
    }

    static constexpr uint32_t MagicConst = 0x184D2204;
    LittleEndianPod<uint32_t> magic;
    uint8_t options[2];
};

static_assert(sizeof(Header) == 6, "sizeof(lz4::Header) == 6");

// Simple wrapper around LZ4_compress_default that matches the signature of
// LZ4_compress_HC by ignoring the compression parameter
static int compress_default_compression(const char* src, char* dst, int srcSize,
                                        int dstCapacity, int compression /* unused */)
{
    return LZ4_compress_default(src, dst, srcSize, dstCapacity);
}

} // namespace lz4

/*****************************************************************************/
/* LZ4 COMPRESSOR                                                            */
/*****************************************************************************/

struct Lz4Compressor : public Compressor {

    typedef Compressor::OnData OnData;
    typedef Compressor::FlushLevel FlushLevel;
    
    Lz4Compressor(int level, uint8_t blockSizeId = 7,
                  uint64_t contentSize = -1)
        : head(blockSizeId,
               true /* independent blocks */,
               false /* block checksum */,
               false /* stream checksum */,
               contentSize != 0 /* write content size */),
          contentSize(contentSize),
          writeHeader(true),
          pos(0)
    {
        buffer.resize(head.blockSize());
        compressFn = level < 3 ? lz4::compress_default_compression : LZ4_compress_HC;
        compressionLevel = level;
        
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

    virtual void notifyInputSize(uint64_t inputSize) override
    {
        if (!writeHeader) {
            throw Exception("lz4 input size notified too late");
        }
        head.setContentSize(true);
        this->contentSize = inputSize;
    }

    virtual bool canFixupLength() const override
    {
        return true;
    }

    virtual std::string newHeaderForLength(uint64_t lengthWritten) const override
    {
        lz4::Header fixedHeader = this->head;
        fixedHeader.setContentSize(lengthWritten);
        std::string result = fixedHeader.asString(lengthWritten);
        return result;
    }

    virtual void compress(const char * s, size_t n,
                          const OnData & onData) override
    {
        if (writeHeader) {
            auto asStringDebug = head.asString(contentSize);
            head.write(onData, contentSize);
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
    }
    
    virtual void flush(FlushLevel flushLevel, const OnData & onData) override
    {
        if (pos == 0)
            return;

        if (head.streamChecksum())
            XXH32_update(streamChecksumState, buffer.data(), pos);

        size_t bytesToAlloc = LZ4_compressBound(pos);
        ExcAssert(bytesToAlloc);
        char* compressed = new char[bytesToAlloc];
        Scope_Exit(delete[] compressed);
        
        auto compressedSize = compressFn(buffer.data(), compressed, pos, bytesToAlloc, compressionLevel);

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
    }

    virtual void finish(const OnData & onData) override
    {
        if (writeHeader) head.write(onData, contentSize);
        if (pos) flush(FLUSH_RESTART, onData);

        const uint32_le eos = 0;
        write(onData, &eos, sizeof(eos));
        
        if (head.streamChecksum()) {
            uint32_le checksum = XXH32_digest(streamChecksumState);
            write(onData, &checksum, sizeof(checksum));
        }
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
    uint64_le contentSize;
    int (*compressFn)(const char*, char*, int, int, int);
    int compressionLevel = -10000;

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
        if (blockLen < sizeof(lz4::Header) + sizeof(uint64_t) + 1) {
            return LENGTH_INSUFFICIENT_DATA;
        }
        lz4::Header head;
        memcpy(&head, block, sizeof(head));

        if (!head.contentSize()) {
            return LENGTH_UNKNOWN;
        }
        uint64_le contentSize;
        memcpy(&contentSize, block + sizeof(head), sizeof(contentSize));

        uint8_t checksum = block[sizeof(head) + sizeof(contentSize)];
        if (head.checksumOptions(contentSize) != checksum) {
            throw Exception("lz4 header checksum mismatch");
        }
        
        if (contentSize == 0)
            return LENGTH_UNKNOWN;
        return contentSize;
    }

    std::pair<std::shared_ptr<const char>, size_t>
    decompressBlock(std::shared_ptr<const char> blockData,
                    uint32_t blockHeader,
                    uint32_t blockChecksum,
                    const Allocate & allocate) const
    {
        size_t blockLength = blockHeader & ~lz4::NotCompressedMask;
        bool uncompressed = blockHeader & lz4::NotCompressedMask;
        
        //cerr << "decompressing " << blockLength << " bytes" << endl;
        
        if (header.blockChecksum()) {
            uint32_t checksum = XXH32(blockData.get(),
                                      blockLength,
                                      lz4::ChecksumSeed);
            if (checksum != blockChecksum)
                throw lz4_error("invalid checksum");
        }
        
        if (uncompressed) {
            return { std::move(blockData), blockLength };
        }
        else {
            auto outputData = allocate(header.blockSize());
            auto decompressed
                = LZ4_decompress_safe
                (blockData.get(), outputData.get(),
                 blockLength, header.blockSize());

            //cerr << "decompressed " << decompressed << " of maximum "
            //     << output.size()
                //     << " with checksum "
                //     << XXH32(blockData.get(), blockLength, lz4::ChecksumSeed)
            //     << endl;
            
            if (decompressed < 0)
                throw lz4_error(string("malformed lz4 stream: ") + LZ4F_getErrorName(decompressed));

            return { std::move(outputData), decompressed };
        }
    }

    static void memDeallocate(char * c)
    {
        delete[] c;
    }
    
    static std::shared_ptr<char> memAllocate(size_t n)
    {
        return std::shared_ptr<char>(new char[n], memDeallocate);
    }
    
    virtual void decompress(const char * data, size_t len,
                            const OnData & onData) override
    {
        std::shared_ptr<const char> sharedData(data, [] (const char *) {});
        auto onSharedData = [&] (std::shared_ptr<const char> data,
                                 size_t len)
            {
                size_t done = 0;
                while (done < len) {
                    done += onData(data.get() + done, len - done);
                }
            };

        decompress(std::move(sharedData), len, onSharedData, memAllocate);
    }

    virtual void decompress(std::shared_ptr<const char> data__, size_t len,
                            const OnSharedData & onData,
                            const Allocate & allocate) override
    {
        if (!cur) {
            throw Exception("Extra junk at end of compressed lz4 data");
        }

        const char * data = data__.get();
        
        size_t done = 0;
        while (done < len) {
            //cerr << "state " << state << endl;
            //cerr << "header.blockSize() = " << header.blockSize() << endl;
            //cerr << "done = " << done << " len = " << len << endl;
            
            size_t toRead = std::min<size_t>(limit - cur, len - done);
            //cerr << "reading " << toRead << " of " << (limit - cur) << endl;
            if (data != cur)
                std::memcpy(cur, data + done, toRead);
            done += toRead;
            cur += toRead;

            
            if (cur == limit) {
                //cerr << "finished state " << state << endl;
                // we've finished our field
                // Switch to the next state
                switch (state) {

                case HEADER:

                    if (header.contentSize()) {
                        setCur(CONTENT_SIZE, knownContentSize);
                    }
                    else {
                        // Finished our file header
                        setCur(HEADER_CHECKSUM, checkBits);
                    }
                    break;

                case CONTENT_SIZE:
                    setCur(HEADER_CHECKSUM, checkBits);
                    break;
                    
                case HEADER_CHECKSUM:
                    header.validate(checkBits, knownContentSize);
                    
                    if (header.streamChecksum()) {
                        streamChecksumState = XXH32_createState();
                        if (XXH32_reset(streamChecksumState, lz4::ChecksumSeed) != XXH_OK) {
                            throw Exception("Error with XXhash checksum initialization");
                        }
                    }
                    
                    setCur(BLOCK_HEADER, blockHeader);

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
                        blockData = allocate(blockLength());

                        setCur(BLOCK_DATA, blockData.get(), blockLength());
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

                    std::shared_ptr<const char> outputData;
                    size_t outputLength;

                    std::tie(outputData, outputLength)
                        = decompressBlock(std::move(blockData),
                                          blockHeader,
                                          blockChecksum,
                                          allocate);

                    onData(outputData, outputLength);

                    if (header.streamChecksum()) {
                        XXH32_update(streamChecksumState, outputData.get(), outputLength);
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
    }
    
    virtual void finish(const OnData & onData) override
    {
        if (state != FINISHED)
            throw Exception("lz4 stream is truncated");
    }

    virtual void finish(const OnSharedData & onData, const Allocate & allocate) override
    {
        if (state != FINISHED)
            throw Exception("lz4 stream is truncated");
    }

    virtual bool
    forEachBlockParallel(size_t requestedBlockSize,
                         const GetDataFunction & getData,
                         const ForEachBlockFunction & onBlock,
                         const Allocate & allocate,
                         int maxParallelism) override
    {
        //return Decompressor::forEachBlockParallel(requestedBlockSize, getData, onBlock);
        ThreadWorkGroup tp(maxParallelism);

        std::shared_ptr<const char> buf;
        size_t bufLen = 0;
        size_t bufOffset = 0;
        
        // Entirely fill the given buffer, throw if we can't
        auto getAll = [&] (char * out, size_t len)
            {
                //cerr << endl << endl << "getAll for " << len << endl;
                size_t done = 0;
                while (done < len) {
                    //cerr << "  ===== done = " << done << " len = " << len << endl;
                    //cerr << "bufOffset = " << bufOffset << " bufLen = " << bufLen << endl;
                    
                    // Try to get some more data
                    if (bufOffset == bufLen) {
                        std::tie(buf, bufLen) = getData(requestedBlockSize);
                        bufOffset = 0;

                        //cerr << "*** Getting new block" << endl;
                        //cerr << "bufLen = " << bufLen << endl;
                        
                        if (!buf) {
                            throw Exception("Early EOF for lz4 stream");
                        }
                    }

                    size_t todo = std::min(len - done, bufLen - bufOffset);
                    //cerr << " -=-=-=-=- bufOffset = " << bufOffset << " bufLen = " << bufLen
                    //     << " todo = " << todo << endl ;
                    memcpy(out + done, buf.get() + bufOffset, todo);

                    done += todo;
                    bufOffset += todo;
                }

                ExcAssertEqual(len, done);

                //cerr << "--------------- finished getAll" << endl;
            };
        
        // Give it everything to move to the next state
        auto pumpState = [&] ()
            {
                getAll(cur, limit - cur);
                decompress(cur, limit - cur, nullptr /* onData */);
            };

        while (state < BLOCK_HEADER)
            pumpState();

        std::atomic<bool> aborted(false);

        size_t blockNumber = 0;
        size_t blockOffset = 0;
        size_t blockSize = header.blockSize();

        
        while (state != FINISHED && state != STREAM_CHECKSUM && !aborted) {
            while (state != FINISHED && state != STREAM_CHECKSUM && state != BLOCK_DATA)
                pumpState();

            if (state == FINISHED || state == STREAM_CHECKSUM || aborted)
                break;

            std::shared_ptr<const char> ourBlockData;
            
            // Next state is to read the block.
            if (bufLen - bufOffset >= blockLength()) {
                //If we have enough input data, we don't
                // need to copy; we can simply use it straight from there
                ourBlockData = std::shared_ptr<const char>(buf, buf.get() + bufOffset);
                bufOffset += blockLength();
            }
            else {
                // Read the block data, copying in to the buffer we recently created
                getAll(cur, limit - cur);
                ourBlockData = blockData;
            }
            
            if (header.blockChecksum()) {
                setCur(BLOCK_CHECKSUM, blockChecksum);
                getAll(cur, limit - cur);
            }
            
            auto processBlock = [ourBlockData = std::move(ourBlockData),
                                 blockHeader = this->blockHeader,
                                 blockChecksum = this->blockChecksum,
                                 blockNumber,
                                 blockOffset,
                                 this,
                                 &aborted,
                                 &onBlock,
                                 &allocate] ()
                {

                    std::shared_ptr<const char> outputData;
                    size_t outputLength;

                    std::tie(outputData, outputLength)
                        = decompressBlock(std::move(ourBlockData),
                                          blockHeader, blockChecksum, allocate);
                    if (!onBlock(blockNumber, blockOffset,
                                 outputData, outputLength))
                        aborted = true;

                    //if (header.streamChecksum()) {
                    //    XXH32_update(streamChecksumState, output.data(), output.size());
                    //}
                };

            ++blockNumber;
            blockOffset += blockSize;
            
            // Finish processing in a new thread
            if (maxParallelism > 1)
                tp.add(std::move(processBlock));
            else processBlock();
            
            // Start of a new block again
            setCur(BLOCK_HEADER, blockHeader);
        }

        tp.waitForAll();
        
        return !aborted;
    }
    
    // write all data
    void write(const OnData & onData, const void * mem, size_t len)
    {
        size_t done = 0;
        while (done < len) {
            done += onData(((const char *)mem) + done,
                           len - done);
        }
    }

    enum State {
        HEADER,
        CONTENT_SIZE,
        HEADER_CHECKSUM,
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
    uint64_le knownContentSize = 0;
    uint8_t checkBits = 0;
    uint32_le blockHeader = 0;
    uint32_t blockLength() const { return blockHeader & ~lz4::NotCompressedMask; };
    std::shared_ptr<char> blockData;
    uint32_le blockChecksum = 0;
    uint32_le streamChecksum = 0;
    
    XXH32_state_t* streamChecksumState = nullptr;
};

static Decompressor::Register<Lz4Decompressor>
registerLz4Decompressor("lz4", {"lz4"});

} // namespace MLDB
