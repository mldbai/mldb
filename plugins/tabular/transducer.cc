/** transducer.h                                                   -*- C++ -*-
    Jeremy Barnes, 16 April 2018
    Copyright (c) 2018 Element AI Inc.  All rights reserved.
    This file is part of MLDB. Copyright 2018 mldb.ai inc. All rights reserved.

    Definitions for transducer classes, used to predictively produce
    dataset elements.
*/

#include "transducer.h"
#include "mldb/ext/zstd/lib/dictBuilder/zdict.h"
#include "mldb/ext/zstd/lib/zstd.h"
#include "mldb/types/annotated_exception.h"

#include "mldb/block/memory_region.h"
#include "mldb/base/scope.h"
#include "mldb/types/basic_value_descriptions.h"

#include "frozen_tables.h"

#include <atomic>
#include <mutex>
#include <bitset>


using namespace std;


namespace MLDB {


/*****************************************************************************/
/* STRING TRANSDUCER                                                         */
/*****************************************************************************/

namespace {

std::mutex typeRegistryMutex;

std::map<std::string, std::function<std::shared_ptr<StringTransducer> (StructuredReconstituter &)> > & getTypeRegistry()
{
    static std::map<std::string, std::function<std::shared_ptr<StringTransducer> (StructuredReconstituter &)> > result;
    return result;
}


} // file scope

std::shared_ptr<const void>
StringTransducer::
registerType(const std::string & type,
             std::function<std::shared_ptr<StringTransducer>
                 (StructuredReconstituter &)> create)
{
    static auto & registry = getTypeRegistry();
    
    std::unique_lock<std::mutex> guard(typeRegistryMutex);
    if (!registry.emplace(type, std::move(create)).second) {
        throw Exception("Attempt to double register StringTransducer type "
                        + type);
    }

    // Auto-deregister when we lose access to the handle, to enable shared
    // library unloading etc.
    auto del = [type] (const void *)
        {
            std::unique_lock<std::mutex> guard(typeRegistryMutex);
            auto & registry = getTypeRegistry();
            registry.erase(type);
        };

    return std::shared_ptr<const void>(nullptr, del);
}

std::shared_ptr<StringTransducer>
StringTransducer::
thaw(StructuredReconstituter & reconst)
{
    cerr << "reconst is " << reconst.getContext() << endl;

    for (auto & d: reconst.getDirectory()) {
        cerr << "contains " << d.name << endl;
    }
    
    static const auto & registry = getTypeRegistry();

    // First, get the type
    string type = reconst.getObject<std::string>("t");
    
    std::unique_lock<std::mutex> guard(typeRegistryMutex);
    auto it = registry.find(type);

    if (it == registry.end()) {
        throw Exception("Unknown StringTransducer type " + type);
    }

    // TODO: two-phase that releases lock once first construction is done,
    // or takes a reference to a shared ptr
    return it->second(*reconst.getStructure("d"));
}

void
StringTransducer::
serialize(StructuredSerializer & serializer) const
{
    serializer.newObject("t", type());
    serializeParameters(*serializer.newStructure("d"));
}


/*****************************************************************************/
/* IDENTITY STRING TRANSDUCER                                                */
/*****************************************************************************/

IdentityStringTransducer::
IdentityStringTransducer()
{
}
    
IdentityStringTransducer::
IdentityStringTransducer(StructuredSerializer & serializer)
{
}

std::string_view
IdentityStringTransducer::
generateAll(std::string_view input,
            char * outputBuffer,
            size_t outputLength) const
{
    return input;
}

size_t
IdentityStringTransducer::
getOutputLength(std::string_view input) const
{
    return input.length();
}

size_t
IdentityStringTransducer::
getTemporaryBufferSize(std::string_view input,
                       ssize_t outputLength) const
{
    return 0;
}

bool
IdentityStringTransducer::
needsTemporaryBuffer() const
{
    return false;
}

bool
IdentityStringTransducer::
canGetOutputLength() const
{
    return true;  // can be provided
}

std::string
IdentityStringTransducer::
type() const
{
    return "id";
}

void
IdentityStringTransducer::
serializeParameters(StructuredSerializer & serializer) const
{
    // Nothing to freeze here
}

size_t
IdentityStringTransducer::
memusage() const
{
    return sizeof(*this);
}


/*****************************************************************************/
/* ZSTD STRING TRANSDUCER                                                    */
/*****************************************************************************/

struct ZstdStringTransducerCommon {
    ZstdStringTransducerCommon()
        : dict(nullptr), cdict(nullptr)
    {
    }

    ~ZstdStringTransducerCommon()
    {
        ZSTD_DDict * prev = dict.exchange(nullptr);
        if (prev) {
            ZSTD_freeDDict(prev);
        }

        ZSTD_CDict * cprev = cdict.exchange(nullptr);
        if (cprev) {
            ZSTD_freeCDict(cprev);
        }
    }

    FrozenMemoryRegion formatData;
    
    mutable std::atomic<ZSTD_DDict *> dict;
    mutable std::atomic<ZSTD_CDict *> cdict;

    mutable std::mutex contextPoolLock;
    mutable std::vector<std::shared_ptr<ZSTD_DCtx> > contextPool;
    mutable std::vector<std::shared_ptr<ZSTD_CCtx> > compressContextPool;

    void serializeParameters(StructuredSerializer & serializer) const
    {
        serializer.newEntry("dict")->copy(formatData);
    }

    size_t memusage() const
    {
        return formatData.memusage();
        // the other things are caches, so don't count towards permanent
        // memory usage
    }
};

struct ZstdCompressor: public StringTransducer {
    
    ZstdCompressor(std::shared_ptr<ZstdStringTransducerCommon> itl)
        : itl(itl)
    {
    }
    
    virtual std::string_view
    generateAll(std::string_view input,
                char * outputBuffer,
                size_t outputLength) const override
    {
        std::shared_ptr<ZSTD_CCtx> context;
            
        {
            std::unique_lock<std::mutex> guard(itl->contextPoolLock);
            if (!itl->compressContextPool.empty()) {
                context = itl->compressContextPool.back();
                itl->compressContextPool.pop_back();
            }
        }
    
        auto freeContext = [&] ()
            {
                if (!context)
                    return;
                std::unique_lock<std::mutex> guard(itl->contextPoolLock);
                itl->compressContextPool.emplace_back(std::move(context));
            };
        
        Scope_Exit(freeContext());

        if (!context) {
            context.reset(ZSTD_createCCtx(), ZSTD_freeCCtx);
        }

        if (!itl->cdict.load()) {
            ZSTD_CDict * dict = ZSTD_createCDict(itl->formatData.data(),
                                                 itl->formatData.length(),
                                                 10 /* compression level */);
            ZSTD_CDict * previous = nullptr;
            if (!itl->cdict.compare_exchange_strong(previous, dict)) {
                ZSTD_freeCDict(dict);
            }
        }
        
        size_t res
            = ZSTD_compress_usingCDict(context.get(),
                                       outputBuffer, outputLength,
                                       input.data(), input.size(),
                                       itl->cdict.load());

        if (ZSTD_isError(res)) {
            throw AnnotatedException(500, "Error with compressing: "
                                     + string(ZSTD_getErrorName(res)));
        }
        
        return std::string_view(outputBuffer, res);
    }
    
    virtual size_t getOutputLength(std::string_view input) const override
    {
        throw AnnotatedException(500, "Can't get output length for zstd compressor");
    }
    
    virtual size_t getTemporaryBufferSize(std::string_view input,
                                          ssize_t outputLength) const override
    {
        return ZSTD_compressBound(input.size());
    }
    
    virtual bool needsTemporaryBuffer() const override
    {
        return true;
    }
    
    virtual bool canGetOutputLength() const override
    {
        return false;
    }
    
    virtual std::string type() const override
    {
        return "zsc";
    }

    virtual void serializeParameters(StructuredSerializer & serializer)
        const override
    {
        return itl->serializeParameters(serializer);
    }

    virtual size_t memusage() const override
    {
        return sizeof(*this) + itl->memusage();
    }

    std::shared_ptr<ZstdStringTransducerCommon> itl;
};

struct ZstdDecompressor: public StringTransducer {
    
    ZstdDecompressor(std::shared_ptr<ZstdStringTransducerCommon> itl)
        : itl(itl)
    {
    }

    virtual std::string_view
    generateAll(std::string_view input,
                char * outputBuffer,
                size_t outputLength) const override
    {
        size_t storageSize = input.length();
        const char * data = input.data();
    
        std::shared_ptr<ZSTD_DCtx> context;
            
        {
            std::unique_lock<std::mutex> guard(itl->contextPoolLock);
            if (!itl->contextPool.empty()) {
                context = itl->contextPool.back();
                itl->contextPool.pop_back();
            }
            guard.unlock();

            if (!context)
                context.reset(ZSTD_createDCtx(), ZSTD_freeDCtx);
        }
    
        auto freeContext = [&] ()
            {
                if (!context)
                    return;
                std::unique_lock<std::mutex> guard(itl->contextPoolLock);
                itl->contextPool.emplace_back(std::move(context));
            };

        Scope_Exit(freeContext());
                    
        if (!context) {
            context.reset(ZSTD_createDCtx(), ZSTD_freeDCtx);
        }
    
        if (!itl->dict.load()) {
            ZSTD_DDict * dict = ZSTD_createDDict(itl->formatData.data(),
                                                 itl->formatData.length());
            ZSTD_DDict * previous = nullptr;
            if (!itl->dict.compare_exchange_strong(previous, dict)) {
                ZSTD_freeDDict(dict);
            }
        }
    
        auto res = ZSTD_decompress_usingDDict(context.get(),
                                              outputBuffer,
                                              outputLength,
                                              data, storageSize,
                                              itl->dict.load());
    
        if (ZSTD_isError(res)) {
            throw AnnotatedException(500, "Error with decompressing: "
                                     + string(ZSTD_getErrorName(res)));
        }

        return std::string_view(outputBuffer, res);
    }
    
    virtual size_t getOutputLength(std::string_view input) const override
    {
        const char * data = input.data();
        auto res = ZSTD_getDecompressedSize(data, input.length());
        if (ZSTD_isError(res)) {
            throw AnnotatedException(500, "Error with decompressing: "
                                     + string(ZSTD_getErrorName(res)));
        }
        return res;
    }
    
    virtual size_t getTemporaryBufferSize(std::string_view input,
                                          ssize_t outputLength) const override
    {
        auto res = ZSTD_getDecompressedSize(input.data(), input.length());
        if (ZSTD_isError(res)) {
            throw AnnotatedException(500, "Error with decompressing: "
                                     + string(ZSTD_getErrorName(res)));
        }

        return res;
    }

    virtual bool needsTemporaryBuffer() const override
    {
        return true;
    }

    virtual bool canGetOutputLength() const override
    {
        return true;  // can be provided
    }

    virtual std::string type() const override
    {
        return "zs";
    }

    virtual void serializeParameters(StructuredSerializer & serializer)
        const override
    {
        itl->serializeParameters(serializer);
    }

    virtual size_t memusage() const override
    {
        return sizeof(*this)
            + itl->memusage();
    }

    std::shared_ptr<ZstdStringTransducerCommon> itl;
};

std::pair<std::shared_ptr<StringTransducer>,
          std::shared_ptr<StringTransducer> >
trainZstdTransducer(const std::vector<std::string> & blobs,
                    MappedSerializer & serializer)
{
    // Let the dictionary size be 5% of the total
    size_t dictionarySize = 131072;//32768; //info->totalBytes / 20;

    std::string dictionary(dictionarySize, '\0');

    static constexpr size_t TOTAL_SAMPLE_SIZE = 1024 * 1024;

    // Give it the first 1MB to train on
    std::string sampleBuffer;
    sampleBuffer.reserve(2 * TOTAL_SAMPLE_SIZE);

    std::vector<size_t> sampleSizes;
    size_t currentOffset = 0;
    size_t valNumber = 0;

    // Accumulate the first 1MB of strings in a contiguous buffer

    for (auto & v: blobs) {
        if (currentOffset > TOTAL_SAMPLE_SIZE)
            break;
        size_t sampleSize = v.length();
        sampleBuffer.append(v);
        sampleSizes.push_back(sampleSize);
        currentOffset += sampleSize;
        valNumber += 1;
    }

    Date before = Date::now();

    // Perform the dictionary training
    size_t res = ZDICT_trainFromBuffer(&dictionary[0],
                                       dictionary.size(),
                                       sampleBuffer.data(),
                                       sampleSizes.data(),
                                       sampleSizes.size());
    if (ZDICT_isError(res)) {
        throw AnnotatedException(500, "Error with dictionary: "
                                 + string(ZDICT_getErrorName(res)));
    }
        
    Date after = Date::now();
    double elapsed = after.secondsSince(before);
    before = after;

    dictionary.resize(res);

    cerr << "created dictionary of " << res << " bytes from "
         << currentOffset << " bytes of samples in "
         << elapsed << " seconds" << endl;

    MutableMemoryRegion dictRegion
        = serializer.allocateWritable(dictionary.length(), 1);
    std::memcpy(dictRegion.data(), dictionary.data(), dictionary.length());
    auto formatData = dictRegion.freeze();

    auto data = std::make_shared<ZstdStringTransducerCommon>();
    data->formatData = std::move(formatData);

    return { std::make_shared<ZstdCompressor>(data),
             std::make_shared<ZstdDecompressor>(data) };
}


/*****************************************************************************/
/* ID TRANSDUCER                                                             */
/*****************************************************************************/

struct TableCharacterTransducer: public CharacterTransducer {
    TableCharacterTransducer(const std::bitset<256> & table)
    {
        // If we have all set, then there is no advantage
        ExcAssertLess(table.count(), 255);

        for (size_t i = 0;  i < table.size();  ++i) {
            if (table.test(i)) {
                this->table.emplace_back(i);
                this->index[i] = this->table.size();
            }
        }
    }

    virtual ~TableCharacterTransducer()
    {
    }

    virtual char decode(uint32_t input) const
    {
        return table.at(input);
    }

    virtual uint32_t encode(unsigned char input) const
    {
        if (input[index] == 0)
            throw Exception(500, "Logic error in char transducer encode");
        return index[input] - 1;
    }

    virtual size_t memusage() const
    {
        return sizeof(this)
            + table.capacity();
    }
    
    std::vector<unsigned char> table;
    unsigned char index[256] = {0};
};

namespace {

struct NullStringTransducer: public StringTransducer {
    virtual ~NullStringTransducer()
    {
    }

    virtual std::string_view
    generateAll(std::string_view input,
                char * outputBuffer,
                size_t outputLength) const override
    {
        return std::string_view();
    }
    
    virtual size_t getOutputLength(std::string_view input) const override
    {
        return 0;
    }
    
    virtual size_t getTemporaryBufferSize(std::string_view input,
                                          ssize_t outputLength) const override
    {
        return 0;
    }

    virtual bool needsTemporaryBuffer() const override
    {
        return false;
    }

    virtual bool canGetOutputLength() const override
    {
        return true;
    }
    
    virtual std::string type() const override
    {
        return "null";
    }

    virtual void serializeParameters(StructuredSerializer & serializer)
        const override
    {
    }

    size_t memusage() const override
    {
        return sizeof(*this);
    }
};

struct PositionInfo {
    uint32_t counts[256] = {0};
    uint32_t uniqueCounts = 0;
    std::bitset<256> bits;

    int intNum = 0;  ///< Which of the integers we encode this in?
    uint64_t baseMultiplier = 0;  ///< Base multiplier of this bit
    uint16_t posMultiplier = 0;
    uint64_t bitWidth = 0;
    
    void update(unsigned char c)
    {
        uniqueCounts += (counts[c]++ == 0);
        bits.set(c);
    }

    std::unique_ptr<CharacterTransducer> train() const
    {
        return std::unique_ptr<CharacterTransducer>
            (new TableCharacterTransducer(bits));
    }
};

struct IntInfo {
    uint32_t bitWidth = 0;   ///< How many bits in this integer
};

struct IdTransducerInfo {
    std::vector<PositionInfo> positions;
    std::vector<std::unique_ptr<CharacterTransducer> > transducers;
    std::vector<IntInfo> ints;
    size_t totalOutputBytes = 0;

    void freeze(StructuredSerializer & serializer) const
    {
    }

    size_t memusage() const
    {
        size_t result
            = sizeof(*this)
            + positions.capacity() * sizeof(positions[0])
            + transducers.capacity() * sizeof(transducers[0])
            + ints.capacity() * sizeof(ints[0]);
        for (auto & t: transducers) {
            result += t->memusage();
        }
        return result;
    }

    static IdTransducerInfo *
    thaw(StructuredReconstituter & reconstituter)
    {
        throw Exception("thaw");
    }
};

struct ForwardIdTransducer: public StringTransducer {

    ForwardIdTransducer(std::shared_ptr<IdTransducerInfo> info)
        : info(std::move(info))
    {
    }
    
    std::string_view generateAll(std::string_view input,
                                 char * outputBuffer,
                                 size_t outputLength) const
    {
        //cerr << "generateAll for " << input << endl;
        //cerr << "outputLength = " << outputLength << endl;
        ExcAssertEqual(outputLength, info->totalOutputBytes);

        int currentInt = 0;
        uint64_t current = 0;
        size_t outputPos = 0;
        
        auto doneCurrent = [&] ()
            {
                //cerr << "doneCurrent " << current << " in "
                //<< info->ints[currentInt].bitWidth << " bits of "
                //<< log2(current) << endl;

                // If there is no entropy at all in the input, then we don't
                // even write one byte
                if (outputLength == 0)
                    return;

                size_t width = info->ints[currentInt].bitWidth;
                for (size_t i = 0;  i < width;  i += 8) {
                    //cerr << "writing byte " << (current % 256) << endl;
                    ExcAssertLess(outputPos, outputLength);
                    outputBuffer[outputPos++] = current % 256;
                    current = current >> 8;
                }

                ExcAssertEqual(current, 0);
                
                current = 0;
                currentInt += 1;
            };

        for (size_t i = 0;  i < input.length();  ++i) {
            uint64_t contrib = info->transducers[i]->encode(input[i]);
            ExcAssertLess(contrib, info->positions[i].posMultiplier);
            
            //cerr << "position " << i << " char " << input[i] << " contrib "
            //     << contrib << " current " << current << " mult "
            //     << info->positions[i].posMultiplier << " base "
            //     << info->positions[i].baseMultiplier
            //     << endl;
            
            current += contrib * info->positions[i].baseMultiplier;
            if (i == input.length() - 1
                || info->positions[i + 1].intNum != currentInt) {
                doneCurrent();
            }
        }

        return std::string_view(outputBuffer, outputLength);
    }
    
    size_t getOutputLength(std::string_view input) const
    {
        return info->totalOutputBytes;
    }

    size_t getTemporaryBufferSize(std::string_view input,
                                  ssize_t outputLength) const
    {
        return info->totalOutputBytes;
    }

    bool needsTemporaryBuffer() const
    {
        return true;
    }

    bool canGetOutputLength() const
    {
        return false;
    }

    std::string type() const
    {
        return "fid";
    }

    void serializeParameters(StructuredSerializer & serializer) const
    {
        info->freeze(serializer);
    }

    size_t memusage() const
    {
        return sizeof(*this) + info->memusage();
    }

    std::shared_ptr<IdTransducerInfo> info;
};

struct BackwardIdTransducer: public StringTransducer {

    BackwardIdTransducer(StructuredReconstituter & reconstituter)
    {
    }

    BackwardIdTransducer(std::shared_ptr<IdTransducerInfo> info)
        : info(std::move(info))
    {
    }

    std::string_view
    generateAll(std::string_view input,
                char * outputBuffer,
                size_t outputLength) const
    {
        const auto & positions = info->positions;
        const auto & transducers = info->transducers;
        
        if (!info->ints.empty()) {
            ExcAssertEqual(input.length(),
                           (info->ints.size() - 1) * 8
                           + (info->ints.back().bitWidth + 7) / 8);
        }

        ExcAssertEqual(outputLength, positions.size());

        int intNumber = -1;
        int offset = 0;
        auto getNewInt = [&] () -> uint64_t
            {
                ++intNumber;
                if (intNumber == 0 && info->ints.empty()) {
                    return 0;
                }
                ExcAssertLess(intNumber, info->ints.size());
                int numBytes = (info->ints[intNumber].bitWidth + 7) / 8;

                //cerr << "getNewInt with " << numBytes << " bytes"
                //     << endl;

                uint64_t result = 0;
                for (size_t i = 0;  i < numBytes;  ++i) {
                    //cerr << "reading byte " << (unsigned)(unsigned char)input[offset + i] << endl;
                    result = result | ((uint64_t)(unsigned char)input[offset + i] << (i*8));
                }
                offset += numBytes;

                //cerr << "got new int " << result << endl;
                
                return result;
            };
        
        uint64_t current = 0;

        for (size_t i = 0;  i < info->positions.size();  ++i) {
            if (positions[i].intNum != intNumber) {
                ExcAssertEqual(current, 0);
                current = getNewInt();
            }

            //cerr << "position " << i << " current = " << current
            //     << " int " << positions[i].intNum << " mult "
            //     << positions[i].posMultiplier << endl;
            
            uint64_t nextMultiplier = positions[i].posMultiplier;
#if 0
            if (i == positions.size() - 1
                || positions[i + 1].intNum != positions[i].intNum) {
                nextMultiplier = -1;
            }
            else nextMultiplier = positions[i + 1].posMultiplier;
#endif
            
            //cerr << "i = " << i << " current = " << current
            //     << " intNum = " << positions[i].intNum << " nextMultiplier = "
            //     << nextMultiplier << " thisPos = "
            //     << current % nextMultiplier << endl;
            
            uint64_t thisPos = current % nextMultiplier;
            current = current / nextMultiplier;

            char c = transducers[i]->decode(thisPos);

            //cerr << "   contrib = " << thisPos << " c = " << c << endl;

            outputBuffer[i] = c;
        }

        return std::string_view(outputBuffer, outputLength);
    }
    
    size_t getOutputLength(std::string_view input) const
    {
        throw AnnotatedException
            (400, "Transducer does not implement getSize()");
    }

    size_t getTemporaryBufferSize(std::string_view input,
                                  ssize_t outputSize) const
    {
        return outputSize;
    }

    bool needsTemporaryBuffer() const
    {
        return true;
    }
    
    bool canGetOutputLength() const
    {
        return false;
    }

    std::string type() const
    {
        return "bid";
    }

    void serializeParameters(StructuredSerializer & serializer) const
    {
        info->freeze(serializer);
    }
    
    size_t memusage() const
    {
        return sizeof(*this) + info->memusage();
    }

    std::shared_ptr<IdTransducerInfo> info;
};

static StringTransducer::Register<BackwardIdTransducer> registerId("bid");


} // file scope

std::pair<std::shared_ptr<StringTransducer>,
          std::shared_ptr<StringTransducer> >
trainIdTransducer(const std::vector<std::string> & blobs,
                  const StringStats & stats,
                  MappedSerializer & serializer)
{
    std::vector<uint32_t> lengthsFound;
    // Which is our short length?

    for (int i = 0;  i < 256;  ++i) {
        if (stats.shortLengthDistribution[i] > 0) {
            lengthsFound.push_back(i);
        }
    }

    if (lengthsFound.empty()) {
        return { std::make_shared<NullStringTransducer>(),
                 std::make_shared<NullStringTransducer>() };
    }
    
    size_t maxLength = lengthsFound.back();
    
    // Look for a restricted subset of characters per position
    std::vector<PositionInfo> charsPerPosition(maxLength);
    std::vector<IntInfo> ints;
    
    for (const std::string & b: blobs) {
        for (size_t i = 0;  i < b.size();  ++i) {
            charsPerPosition[i].update(b[i]);
        }
    }

    double bits = 0;
    uint64_t total = 1;
    int intNum = 0;
    std::vector<std::unique_ptr<CharacterTransducer> > transducers;
    
    for (size_t p = 0;  p < maxLength;  ++p) {
        //cerr << p << "=" << charsPerPosition[p].uniqueCounts << " ";
        bits += log2(charsPerPosition[p].uniqueCounts);

        if (bits > 64 && charsPerPosition[p].uniqueCounts > 1) {
            ints.push_back({64});
            total = 1;
            ++intNum;
            bits = log2(charsPerPosition[p].uniqueCounts);
        }

        charsPerPosition[p].posMultiplier = charsPerPosition[p].uniqueCounts;
        charsPerPosition[p].baseMultiplier = total;
        charsPerPosition[p].intNum = intNum;

        total *= charsPerPosition[p].uniqueCounts;

        transducers.emplace_back(charsPerPosition[p].train());
    }

    // There is one extra integer to hold the leftover bits
    if (bits > 0) {
        //cerr << "leftover " << bits << " bits" << endl;
        ints.push_back({uint32_t(std::ceil(bits))});
    }
    
    size_t totalOutputBytes = 8 * intNum + (std::ceil(bits) + 7) / 8;

    //cerr << " bits = " << bits << " totalOutputBytes = " << totalOutputBytes
    //     << " total = " << total << endl;
    
    auto info = std::make_shared<IdTransducerInfo>();
    info->positions = std::move(charsPerPosition);
    info->totalOutputBytes = totalOutputBytes;
    info->transducers = std::move(transducers);
    info->ints = std::move(ints);
    
    return { std::make_shared<ForwardIdTransducer>(info),
             std::make_shared<BackwardIdTransducer>(info) };
}


} // namespace MLDB
