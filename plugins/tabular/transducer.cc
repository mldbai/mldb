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
#include "mldb/utils/vector_utils.h"
#include "mldb/types/vector_description.h"

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
IdentityStringTransducer(StructuredReconstituter & reconstituter)
{
    // No parameters to reconstitute
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
    serializer.newObject("dummy", string());
    // Nothing to freeze here
}

size_t
IdentityStringTransducer::
memusage() const
{
    return sizeof(*this);
}

static StringTransducer::Register<IdentityStringTransducer> regId("id");


/*****************************************************************************/
/* ZSTD STRING TRANSDUCER                                                    */
/*****************************************************************************/

struct ZstdStringTransducer::Itl {
    Itl() = default;

    Itl(StructuredReconstituter & reconstituter)
    {
        formatData = reconstituter.getRegion("dict");
    }

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
ZstdStringTransducer(StructuredReconstituter & reconstituter)
    : itl(std::make_shared<Itl>(reconstituter))
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
    size_t res = ZDICT_trainFromBuffer(dictionary.data(),
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

static StringTransducer::Register<ZstdStringCompressor> regZsc("zsc");


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

static StringTransducer::Register<ZstdStringDecompressor> regZsd("zsd");


/*****************************************************************************/
/* ID TRANSDUCER                                                             */
/*****************************************************************************/

struct TableCharacterTransducer: public CharacterTransducer {
    TableCharacterTransducer()
    {
        // For reconstitution only
    }
    
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

    TableCharacterTransducer(StructuredReconstituter & reconstituter)
    {
        reconstituter.getObject("t", table);
        initIndex();
    }

    void initIndex()
    {
        for (size_t i = 0;  i < table.size();  ++i) {
            index[table[i]] = i + 1;
        }
    }

    void freeze(StructuredSerializer & serializer)
    {
        serializer.newObject("t", table);
    }

    virtual ~TableCharacterTransducer()
    {
    }

    virtual unsigned char decode(uint32_t input) const
    {
        return table.at(input);
    }

    virtual uint32_t encode(unsigned char input) const
    {
        if (input[index] == 0)
            throw Exception(500, "Logic error in char transducer encode");
        uint8_t result = index[input] - 1;
        ExcAssertEqual((int)decode(result), (int)input);
        return result;
    }

    virtual size_t memusage() const
    {
        return sizeof(this)
            + table.capacity();
    }
    
    std::vector<unsigned char> table;
    uint8_t index[256] = {0};
};

DECLARE_STRUCTURE_DESCRIPTION(TableCharacterTransducer);
DEFINE_STRUCTURE_DESCRIPTION_INLINE(TableCharacterTransducer)
{
    setVersion(1);
    addField("table", &TableCharacterTransducer::table, "");

    // Once we're done parsing, initialize the index
    onPostValidate = [] (TableCharacterTransducer * transducer, JsonParsingContext &)
    {
        transducer->initIndex();
    };
}

struct IdTransducerPositionInfo {
    uint32_t counts[256] = {0};
    uint32_t uniqueCounts = 0;
    std::bitset<256> bits;

    int intNum = 0;  ///< Which of the integers we encode this in?
    uint64_t baseMultiplier = 0;  ///< Base multiplier of this bit
    uint64_t modulus = 0;         ///< BaseMultiplier of next bit
    uint64_t posMultiplier = 0;  // Only really need one byte
    
    // Encode a character to its portion in the 64 bit modulo encoding
    uint64_t encode(unsigned char contrib) const
    {
        ExcAssertLess(contrib, posMultiplier);
        uint64_t result = contrib * baseMultiplier;
        ExcAssertEqual(decode(result), contrib);
        return result;
    }

    // Extract this position's value from the 64 bit multiple encoding
    unsigned char decode(uint64_t encoded) const
    {
        if (modulus != 0)
            encoded %= modulus;
        uint64_t contrib = encoded / baseMultiplier;
        ExcAssertLess(contrib, posMultiplier);
        return contrib;
    }

    void updateStats(unsigned char c)
    {
        uniqueCounts += (counts[c]++ == 0);
        bits.set(c);
    }

    TableCharacterTransducer train() const
    {
        return { bits };
    }
};

DECLARE_STRUCTURE_DESCRIPTION(IdTransducerPositionInfo);
DEFINE_STRUCTURE_DESCRIPTION(IdTransducerPositionInfo);

IdTransducerPositionInfoDescription::IdTransducerPositionInfoDescription()
{
    setVersion(1);
    addField("intNum", &IdTransducerPositionInfo::intNum, "");
    addField("baseMultiplier", &IdTransducerPositionInfo::baseMultiplier, "");
    addField("modulus", &IdTransducerPositionInfo::modulus, "");
    addField("posMultiplier", &IdTransducerPositionInfo::posMultiplier, "");
}

namespace {
    using PositionInfo = IdTransducerPositionInfo;
} // file scope

struct IdTransducerInfo {
    std::vector<PositionInfo> positions;
    std::vector<TableCharacterTransducer> transducers;
    size_t totalBits = 0;         // Total number of bits to write
    size_t numInts() const { return (totalBits + 63) / 64; } // Total number of (64 bit) integers making up this ID
    size_t lastIntWidth() const { return totalBits % 64; }   // Width of the last of these
    size_t totalOutputBytes() const { return (totalBits + 7) / 8; }  // How many bytes each one produces
    size_t widthOfInt(size_t intNumber) { return intNumber == numInts() - 1 ? lastIntWidth() : 64; }

    IdTransducerInfo() = default;

    IdTransducerInfo(StructuredReconstituter & reconstituter)
    {
        reconstituter.getObject("p", positions);
        reconstituter.getObject("t", transducers);
        reconstituter.getObject("b", totalBits);
    }

    void freeze(StructuredSerializer & serializer) const
    {
        serializer.newObject("p", positions);
        serializer.newObject("t", transducers);
        serializer.newObject("b", totalBits);
    }

    size_t memusage() const
    {
        size_t result
            = sizeof(*this)
            + positions.capacity() * sizeof(positions[0])
            + transducers.capacity() * sizeof(transducers[0]);
        for (auto & t: transducers) {
            result += t.memusage();
        }
        return result;
    }
};

struct ForwardIdTransducer: public StringTransducer {

    ForwardIdTransducer(std::shared_ptr<IdTransducerInfo> info)
        : info(std::move(info))
    {
    }
    
    ForwardIdTransducer(StructuredReconstituter & reconstituter)
        : info(std::make_shared<IdTransducerInfo>(reconstituter))
    {
    }

    std::string_view generateAll(std::string_view input,
                                 char * outputBuffer,
                                 size_t outputLength) const
    {
        ExcAssertEqual(outputLength, info->totalOutputBytes());

        int currentInt = 0;
        uint64_t current = 0;
        size_t outputPos = 0;

        // debug
        size_t startPos = 0;
        std::vector<uint64_t> contribs;

        auto doneCurrent = [&] ()
            {
                // If our transducer requires no bits, there is nothing to write
                if (info->totalBits == 0) {
                    ExcAssert(current == 0);
                    return;
                }
                //cerr << "doneCurrent: currentInt = " << currentInt << endl;
                ExcAssertLess(currentInt, info->numInts());
                size_t width = info->widthOfInt(currentInt);
                //cerr << "writing int: current = " << current << endl;
                for (size_t i = 0;  i < width;  i += 8) {
                    //cerr << "  writing byte " << (current % 256) << endl;
                    outputBuffer[outputPos++] = current % 256;
                    current = current >> 8;
                }

                ExcAssertEqual(current, 0);
                
                current = 0;
                currentInt += 1;
            };

        for (size_t i = 0;  i < input.length();  ++i) {
            uint64_t contrib = info->transducers[i].encode(input[i]);
            contribs.push_back(contrib);
            uint64_t encoded = info->positions[i].encode(contrib);
            current += encoded;
            ExcAssertEqual(info->positions[i].decode(encoded), contrib);
            ExcAssertEqual(info->positions[i].decode(current), contrib);
            //cerr << "doing character " << i << " of " << input.length()
            //     << " intNum = " << info->positions[i].intNum
            //     << " baseMultiplier = " << info->positions[i].baseMultiplier
//                 << " encoder = " << MLDB::type_name(*info->transducers[i])
            //     << " encoded " << encoded
            //     << " current = " << current
            //     << " contrib = " << contrib
            //     << endl;


            for (size_t j = startPos;  j < i;  ++j) {
                //cerr << "  *** checking decode of previous position " << j << " = " << contribs[j] << endl;
                ExcAssertEqual((int)info->positions[j].decode(current), (int)contribs[j]);
            }
            //cerr << "  *** done checking decode" << endl;

            if (i == input.length() - 1
                || info->positions[i + 1].intNum != currentInt) {
                doneCurrent();
                startPos = i + 1;
            }
        }

        ExcAssertEqual(currentInt, info->numInts());

        return std::string_view(outputBuffer, outputLength);
    }
    
    size_t getOutputLength(std::string_view input) const
    {
        return info->totalOutputBytes();
    }

    size_t getTemporaryBufferSize(std::string_view input,
                                  ssize_t outputLength) const
    {
        return info->totalOutputBytes();
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

static StringTransducer::Register<ForwardIdTransducer> regIdF("fid");

struct BackwardIdTransducer: public StringTransducer {

    BackwardIdTransducer(std::shared_ptr<IdTransducerInfo> info)
        : info(std::move(info))
    {
    }

    BackwardIdTransducer(StructuredReconstituter & reconstituter)
        : info(std::make_shared<IdTransducerInfo>(reconstituter))
    {
    }

    std::string_view
    generateAll(std::string_view input,
                char * outputBuffer,
                size_t outputLength) const
    {
        const auto & positions = info->positions;
        const auto & transducers = info->transducers;
        
        ExcAssertEqual(outputLength, positions.size());

        int intNumber = -1;
        size_t inputPos = 0;
        auto getNewInt = [&] () -> uint64_t
            {
                // If we always produce the same string, there is no input information
                if (info->totalBits == 0)
                    return 0;
                ++intNumber;
                ExcAssertLess(intNumber, info->numInts());
                size_t numBytes = (info->widthOfInt(intNumber) + 7) / 8;

                //cerr << "getNewInt with " << numBytes << " bytes"
                //     << endl;

                ExcAssertLessEqual(inputPos + numBytes, input.length());

                uint64_t result = 0;
                for (size_t i = 0;  i < numBytes;  ++i) {
                    uint64_t thisByte = (unsigned char)input[i + inputPos];
                    //cerr << "reading byte " << thisByte << endl;
                    thisByte <<= (i*8);
                    result = result | thisByte;
                }

                //cerr << "read int " << result << endl;

                inputPos += numBytes;
                
                return result;
            };
        
        uint64_t current = 0;

        for (size_t i = 0;  i < info->positions.size();  ++i) {
            //cerr << "position " << i << " intNum " << positions[i].intNum << " intNumber " << intNumber << " current " << current << endl;
            if (positions[i].intNum != intNumber) {
                current = getNewInt();
            }

            uint64_t contrib = info->positions[i].decode(current);

            //cerr << "i = " << i << " current = " << current
            //     << " intNum = " << positions[i].intNum << " baseMultiplier = "
            //     << positions[i].baseMultiplier << " posMultiplier " << positions[i].posMultiplier
            //     << " contrib = " << contrib << endl;
            
            char c = transducers[i].decode(contrib);
            outputBuffer[i] = c;
        }

        ExcAssertEqual(inputPos, input.length());

        return std::string_view(outputBuffer, outputLength);
    }
    
    size_t getOutputLength(std::string_view input) const
    {
        return info->positions.size();
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

std::pair<std::shared_ptr<StringTransducer>,
          std::shared_ptr<StringTransducer> >
trainIdTransducer(const std::vector<std::string> & blobs,
                  const StringStats & stats,
                  MappedSerializer & serializer)
{
    //cerr << "training transducer: blobs " << blobs << endl;

    std::vector<uint32_t> lengthsFound;
    // Which is our short length?

    for (int i = 0;  i < 256;  ++i) {
        if (stats.shortLengthDistribution[i] > 0) {
            lengthsFound.push_back(i);
        }
    }

    for (auto & l: stats.longLengthDistribution) {
        lengthsFound.push_back(l.first);
    }

    size_t maxLength = lengthsFound.empty() ? 0 : lengthsFound.back();
    
    //cerr << "maxLength = " << maxLength << endl;

    // Look for a restricted subset of characters per position
    std::vector<PositionInfo> charsPerPosition(maxLength);
    
    for (const std::string & b: blobs) {
        for (size_t i = 0;  i < b.size();  ++i) {
            charsPerPosition[i].updateStats(b[i]);
        }
    }

    double bits = 0;
    double totalBits = 0;
    uint64_t writtenBits = 0;
    uint64_t total = 1;
    int intNum = 0;
    std::vector<TableCharacterTransducer> transducers;
    
    for (size_t p = 0;  p < maxLength;  ++p) {
        //cerr << p << "=" << charsPerPosition[p].uniqueCounts << " ";
        double positionBits = log2(charsPerPosition[p].uniqueCounts);
        bits += positionBits;
        totalBits += log2(charsPerPosition[p].uniqueCounts);

        if (bits > 64 && charsPerPosition[p].uniqueCounts > 1) {
            writtenBits += 64;
            total = 1;
            ++intNum;
            bits = positionBits;
        }

        //cerr << positionBits << " " << intNum << " " << total << " " << endl;

        charsPerPosition[p].posMultiplier = charsPerPosition[p].uniqueCounts;
        charsPerPosition[p].baseMultiplier = total;
        charsPerPosition[p].modulus = total * charsPerPosition[p].uniqueCounts;
        charsPerPosition[p].intNum = intNum;

        total *= charsPerPosition[p].uniqueCounts;

        transducers.emplace_back(charsPerPosition[p].train());
    }

    writtenBits += std::ceil(bits);

    cerr << endl << "totalBits = " << totalBits << " writtenBits = " << writtenBits << endl;
    ExcAssertGreaterEqual(writtenBits, totalBits);
              
    //cerr << " bits = " << bits << " totalOutputBytes = " << totalOutputBytes
    //     << " total = " << total << endl;
    
    auto info = std::make_shared<IdTransducerInfo>();
    info->positions = std::move(charsPerPosition);
    info->totalBits = writtenBits;
    info->transducers = std::move(transducers);
    
    auto forward = std::make_shared<ForwardIdTransducer>(info);
    auto backward = std::make_shared<BackwardIdTransducer>(info);

#if 0    
    for (auto & s: blobs) {
        size_t len = forward->getOutputLength(s);
        char buf[len];
        string_view enc = forward->generateAll(s, buf, len);
        size_t len2 = backward->getOutputLength(enc);
        ExcAssertEqual(len2, s.size());
        char outbuf[s.size()];
        string_view dec = backward->generateAll(enc, outbuf, len2);
        ExcAssertEqual(dec, s);
        cerr << "sucessfully decoded blob " << s << " to " << dec << endl;
    }
#endif

    return { std::move(forward), std::move(backward) };
}


} // namespace MLDB
