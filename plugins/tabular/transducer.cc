/** transducer.h                                                   -*- C++ -*-
    Jeremy Barnes, 16 April 2018
    Copyright (c) 2018 Element AI Inc.  All rights reserved.
    This file is part of MLDB. Copyright 2018 mldb.ai inc. All rights reserved.

    Definitions for transducer classes, used to predictively produce
    dataset elements.
*/

#include "transducer.h"
#include "mldb/ext/zstd/lib/zdict.h"
#include "mldb/ext/zstd/lib/zstd.h"
#include "mldb/types/annotated_exception.h"

#include "mldb/block/memory_region.h"
#include "mldb/base/scope.h"
#include "mldb/types/basic_value_descriptions.h"
#include "mldb/utils/possibly_dynamic_buffer.h"

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

struct ZstdStringTransducer::Itl {
    ~Itl()
    {
        {
            ZSTD_DDict * prev = ddict.exchange(nullptr);
            if (prev) {
                ZSTD_freeDDict(prev);
            }
        }

        {
            ZSTD_CDict * prev = cdict.exchange(nullptr);
            if (prev) {
                ZSTD_freeCDict(prev);
            }
        }
    }

    FrozenMemoryRegion formatData;
    
    // For decompression
    mutable std::atomic<ZSTD_DDict *> ddict = nullptr;
    mutable std::mutex dContextPoolLock;
    mutable std::vector<std::shared_ptr<ZSTD_DCtx> > dContextPool;

    // For compression
    mutable std::atomic<ZSTD_CDict *> cdict = nullptr;
    mutable std::mutex cContextPoolLock;
    mutable std::vector<std::shared_ptr<ZSTD_CCtx> > cContextPool;

    size_t memusage() const
    {
        return formatData.memusage();
        // the other things are caches, so don't count towards permanent
        // memory usage
    }

    std::pair<std::shared_ptr<ZSTD_DCtx>, const ZSTD_DDict *>
    getDecompressionContext()
    {
        std::shared_ptr<ZSTD_DCtx> context;
        {
            std::unique_lock<std::mutex> guard(dContextPoolLock);
            if (!dContextPool.empty()) {
                context = dContextPool.back();
                dContextPool.pop_back();
            }
        }
        
        if (!context) {
            context.reset(ZSTD_createDCtx(),
                        [] (ZSTD_DCtx * context) { ZSTD_freeDCtx(context); });
        }

        // Ownership of context is held in this lambda
        auto freeContext = [this, context] (void *)
            {
                if (!context)
                    return;
                std::unique_lock<std::mutex> guard(dContextPoolLock);
                dContextPool.emplace_back(std::move(context));
            };

        context = std::shared_ptr<ZSTD_DCtx>(context.get(), freeContext);

        if (!ddict.load()) {
            ZSTD_DDict * dict = ZSTD_createDDict(formatData.data(),
                                                 formatData.length());
            ZSTD_DDict * previous = nullptr;
            if (!this->ddict.compare_exchange_strong(previous, dict)) {
                ZSTD_freeDDict(dict);
            }
        }

        return { std::move(context), ddict.load() };
    }    

    std::pair<std::shared_ptr<ZSTD_CCtx>, const ZSTD_CDict *>
    getCompressionContext()
    {
        std::shared_ptr<ZSTD_CCtx> context;
        {
            std::unique_lock<std::mutex> guard(cContextPoolLock);
            if (!cContextPool.empty()) {
                context = cContextPool.back();
                cContextPool.pop_back();
            }
        }
        
        if (!context) {
            context.reset(ZSTD_createCCtx(),
                        [] (ZSTD_CCtx * context) { ZSTD_freeCCtx(context); });
        }

        // Ownership of context is held in this lambda
        auto freeContext = [this, context] (void *)
            {
                std::unique_lock<std::mutex> guard(cContextPoolLock);
                this->cContextPool.emplace_back(std::move(context));
            };

        context = std::shared_ptr<ZSTD_CCtx>(context.get(), freeContext);

        if (!cdict.load()) {
            ZSTD_CDict * dict = ZSTD_createCDict(formatData.data(),
                                                 formatData.length(), 5  /* compression level */);
            ZSTD_CDict * previous = nullptr;
            if (!this->cdict.compare_exchange_strong(previous, dict)) {
                ZSTD_freeCDict(dict);
            }
        }

        return { std::move(context), cdict.load() };
    }    

};

ZstdStringTransducer::
ZstdStringTransducer(StructuredSerializer & serializer)
{

}

ZstdStringTransducer::
ZstdStringTransducer(std::shared_ptr<Itl> itl)
    : itl(std::move(itl))
{
}

ZstdStringTransducer::
~ZstdStringTransducer()
{
}

std::pair<std::shared_ptr<ZstdStringCompressor>,
          std::shared_ptr<ZstdStringDecompressor> >
ZstdStringTransducer::
train(const std::vector<std::string> & blobs,
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
        
    dictionary.resize(res);

    Date after = Date::now();
    double elapsed = after.secondsSince(before);

    cerr << "created dictionary of " << res << " bytes from "
         << currentOffset << " bytes of samples in "
         << elapsed << " seconds" << endl;

    auto itl = std::make_shared<Itl>();
    auto dictRegion
        = serializer.allocateWritable(dictionary.length(), 1);
    std::memcpy(dictRegion.data(), dictionary.data(), dictionary.length());
    itl->formatData = dictRegion.freeze();

    return { std::make_shared<ZstdStringCompressor>(itl), std::make_shared<ZstdStringDecompressor>(itl) };
}

void
ZstdStringTransducer::
serializeParameters(StructuredSerializer & serializer) const
{
    serializer.newEntry("dict")->copy(itl->formatData);
}

size_t
ZstdStringTransducer::
memusage() const
{
    return sizeof(*this)
        + itl->memusage();
}


/*****************************************************************************/
/* ZSTD STRING COMPRESSOR                                                    */
/*****************************************************************************/

std::string_view
ZstdStringCompressor::
generateAll(std::string_view input,
            char * outputBuffer,
            size_t outputLength) const
{
    std::shared_ptr<ZSTD_CCtx> context;
    const ZSTD_CDict * dict;
    std::tie(context, dict) = itl->getCompressionContext();

    size_t res
        = ZSTD_compress_usingCDict(context.get(),
                                   outputBuffer, outputLength,
                                   input.data(), input.length(),
                                   dict);

    if (ZSTD_isError(res)) {
        throw AnnotatedException(500, "Error with compressing: "
                                    + string(ZSTD_getErrorName(res)));
    }

    return { outputBuffer, res };
}

size_t
ZstdStringCompressor::
getTemporaryBufferSize(std::string_view input,
                       ssize_t outputLength) const
{
    return ZSTD_compressBound(input.length());
}

bool
ZstdStringCompressor::
needsTemporaryBuffer() const
{
    return true;
}

bool
ZstdStringCompressor::
canGetOutputLength() const
{
    return false;
}

size_t
ZstdStringCompressor::
getOutputLength(std::string_view input) const
{
    throw MLDB::Exception("canGetOutputLength is false");
}

std::string
ZstdStringCompressor::
type() const
{
    return "zsc";
}


/*****************************************************************************/
/* ZSTD STRING DECOMPRESSOR                                                  */
/*****************************************************************************/

std::string_view
ZstdStringDecompressor::
generateAll(std::string_view input,
            char * outputBuffer,
            size_t outputLength) const
{
    std::shared_ptr<ZSTD_DCtx> context;
    const ZSTD_DDict * dict;
    std::tie(context, dict) = itl->getDecompressionContext();

    size_t storageSize = input.length();
    const char * data = input.data();
    
    auto res = ZSTD_decompress_usingDDict(context.get(),
                                          outputBuffer,
                                          outputLength,
                                          data, storageSize,
                                          dict);
    
    if (ZSTD_isError(res)) {
        throw AnnotatedException(500, "Error with decompressing: "
                                  + string(ZSTD_getErrorName(res)));
    }

    return std::string_view(outputBuffer, res);
}

size_t
ZstdStringDecompressor::
getOutputLength(std::string_view input) const
{
    const char * data = input.data();
    auto res = ZSTD_getDecompressedSize(data, input.length());
    if (ZSTD_isError(res)) {
        throw AnnotatedException(500, "Error with decompressing: "
                                  + string(ZSTD_getErrorName(res)));
    }
    return res;
}

size_t
ZstdStringDecompressor::
getTemporaryBufferSize(std::string_view input,
                       ssize_t outputLength) const
{
    auto res = ZSTD_getDecompressedSize(input.data(), input.length());
    if (ZSTD_isError(res)) {
        throw AnnotatedException(500, "Error with decompressing: "
                                  + string(ZSTD_getErrorName(res)));
    }

    return res;
}

bool
ZstdStringDecompressor::
needsTemporaryBuffer() const
{
    return true;
}

bool
ZstdStringDecompressor::
canGetOutputLength() const
{
    return true;  // can be provided
}

std::string
ZstdStringDecompressor::
type() const
{
    return "zsd";
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
        ExcAssertEqual(outputLength, info->totalOutputBytes);

        int currentInt = 0;
        uint64_t current = 0;
        size_t outputPos = 0;
        
        auto doneCurrent = [&] ()
            {
                ExcAssertLess(currentInt, info->ints.size());
                size_t width = info->ints[currentInt].bitWidth;
                for (size_t i = 0;  i < width;  i += 8) {
                    //cerr << "writing byte " << (current % 256) << endl;
                    outputBuffer[outputPos++] = current % 256;
                    current = current >> 8;
                }

                ExcAssertEqual(current, 0);
                
                current = 0;
                currentInt += 1;
            };

        for (size_t i = 0;  i < input.length();  ++i) {
            uint64_t contrib = info->transducers[i]->encode(input[i]);
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
        auto getNewInt = [&] () -> uint64_t
            {
                ++intNumber;
                ExcAssertLess(intNumber, info->ints.size());
                int numBytes = (info->ints[intNumber].bitWidth + 7) / 8;

                //cerr << "getNewInt with " << numBytes << " bytes"
                //     << endl;

                uint64_t result = 0;
                for (size_t i = 0;  i < numBytes;  ++i) {
                    //cerr << "reading byte " << (unsigned)(unsigned char)input[i] << endl;
                    result = result | ((unsigned char)input[i] << (i*8));
                }


                
                return result;
            };
        
        uint64_t current = 0;

        for (size_t i = 0;  i < info->positions.size();  ++i) {
            if (positions[i].intNum != intNumber) {
                ExcAssertEqual(current, 0);
                current = getNewInt();
            }

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

    ExcAssert(!lengthsFound.empty());
    
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
        cerr << p << "=" << charsPerPosition[p].uniqueCounts << " ";
        bits += log2(charsPerPosition[p].uniqueCounts);

        if (bits > 64 && charsPerPosition[p].uniqueCounts > 1) {
            ints.push_back({64});
            total = 1;
            ++intNum;
            bits = 0;
        }

        charsPerPosition[p].posMultiplier = charsPerPosition[p].uniqueCounts;
        charsPerPosition[p].baseMultiplier = total;
        charsPerPosition[p].intNum = intNum;

        total *= charsPerPosition[p].uniqueCounts;

        transducers.emplace_back(charsPerPosition[p].train());
    }

    // There is one extra integer to hold the leftover bits
    if (bits > 0) {
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
