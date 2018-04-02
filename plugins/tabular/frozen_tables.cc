/** frozen_tables.cc
    Jeremy Barnes, 12 June 2017
    Copyright (c) 2017 Element AI Inc.  All rights reserved.
    This file is part of MLDB. Copyright 2017 mldb.ai inc. All rights reserved.
*/

#include "frozen_tables.h"
#include "mldb/http/http_exception.h"
#include "mldb/utils/possibly_dynamic_buffer.h"
#include "mldb/types/basic_value_descriptions.h"

#include "mldb/ext/zstd/lib/dictBuilder/zdict.h"
#include "mldb/ext/zstd/lib/zstd.h"
#include "mldb/base/scope.h"
#include <mutex>


using namespace std;


namespace MLDB {


/*****************************************************************************/
/* FROZEN INTEGER TABLE                                                      */
/*****************************************************************************/

IMPLEMENT_STRUCTURE_DESCRIPTION(FrozenIntegerTableMetadata)
{
    setVersion(1);
    addAuto("numEntries", &FrozenIntegerTableMetadata::numEntries, "");
    addAuto("entryBits", &FrozenIntegerTableMetadata::entryBits, "");
    addAuto("offset", &FrozenIntegerTableMetadata::offset, "");
    addAuto("slope", &FrozenIntegerTableMetadata::slope, "");
}

size_t
FrozenIntegerTable::
memusage() const
{
    return storage.memusage();
}

size_t
FrozenIntegerTable::
size() const
{
    return md.numEntries;
}

uint64_t
FrozenIntegerTable::
get(size_t i) const
{
    ExcAssertLess(i, md.numEntries);
    ML::Bit_Extractor<uint64_t> bits(storage.data());
    bits.advance(i * md.entryBits);
    int64_t val = bits.extract<uint64_t>(md.entryBits);
    //cerr << "getting element " << i << " gave val " << val
    //     << " yielding " << decode(i, val) << " with offset "
    //     << offset << " and slope " << slope << endl;
    return decode(i, val);
}

void
FrozenIntegerTable::
serialize(StructuredSerializer & serializer) const
{
    serializer.newObject("md.json", md);
    serializer.addRegion(storage, "ints");
}


/*****************************************************************************/
/* MUTABLE INTEGER TABLE                                                     */
/*****************************************************************************/

uint64_t
MutableIntegerTable::
add(uint64_t val)
{
    values.emplace_back(val);
    minValue = std::min(minValue, val);
    monotonic = monotonic && val >= maxValue;
    maxValue = std::max(maxValue, val);
    return values.size() - 1;
}

void
MutableIntegerTable::
reserve(size_t numValues)
{
    values.reserve(numValues);
}
    
size_t
MutableIntegerTable::
size() const
{
    return values.size();
}

size_t
MutableIntegerTable::
bytesRequired() const
{
    // TODO: calculate with slope
    uint64_t range = maxValue - minValue;
    uint8_t bits = bitsToHoldRange(range);
    size_t numWords = (bits * values.size() + 63) / 64;
#if 0
    cerr << "**** MIT bytes required" << endl;
    cerr << "range = " << range << " minValue = " << minValue
         << " maxValue = " << maxValue << " bits = " << bits
         << " numWords = " << numWords << " values.size() = "
         << values.size() << endl;
#endif
    return numWords * 8;
}

FrozenIntegerTable
MutableIntegerTable::
freeze(MappedSerializer & serializer)
{
    FrozenIntegerTable result;
    uint64_t range = maxValue - minValue;
    uint8_t bits = bitsToHoldRange(range);

#if 0
    cerr << "*** Freezing integer table" << endl;
    cerr << "minValue = " << minValue << " maxValue = "
         << maxValue << " range = " << range << endl;
    cerr << "bits = " << (int)bits << " size = " << values.size() << endl;
#endif
    result.md.offset = minValue;
    result.md.entryBits = bits;
    result.md.numEntries = values.size();
    result.md.slope = 0.0;

    if (values.size() > 1 && monotonic) {
        // TODO: what we are really trying to do here is find the
        // slope and intercept such that all values are above
        // the line, and the infinity norm is minimised.  We can
        // do that in a more principled way...
        double slope = (values.back() - values[0]) / (values.size() - 1.0);

        //static std::mutex mutex;
        //std::unique_lock<std::mutex> guard(mutex);
            
        //cerr << "monotonic " << values.size() << " from "
        //     << minValue << " to " << maxValue << " has slope "
        //     << slope << endl;

        uint64_t maxNegOffset = 0, maxPosOffset = 0;
        for (size_t i = 1;  i < values.size();  ++i) {
            uint64_t predicted = minValue + i * slope;
            uint64_t actual = values[i];

            //cerr << "i = " << i << " predicted " << predicted
            //     << " actual " << actual << endl;

            if (predicted < actual) {
                maxPosOffset = std::max(maxPosOffset, actual - predicted);
            }
            else {
                maxNegOffset = std::max(maxNegOffset, predicted - actual);
            }
        }

        uint8_t offsetBits = bitsToHoldCount(maxNegOffset + maxPosOffset + 2);
        if (offsetBits < bits) {
            result.md.offset = minValue - maxNegOffset;
            result.md.entryBits = offsetBits;
            result.md.slope = slope;

#if 0
            cerr << "integer range with slope " << slope
                 << " goes from " << (int)bits << " to "
                 << (int)offsetBits << " bits per entry" << endl;
            cerr << "maxNegOffset = " << maxNegOffset << endl;
            cerr << "maxPosOffset = " << maxPosOffset << endl;
            cerr << "minValue = " << minValue << endl;
            cerr << "offset = " << result.offset << endl;
            cerr << "slope = " << result.slope << endl;
#endif
        }
    }

    size_t numWords = (result.md.entryBits * values.size() + 63) / 64;
    auto mutableStorage = serializer.allocateWritableT<uint64_t>(numWords);
    uint64_t * data = mutableStorage.data();

    ML::Bit_Writer<uint64_t> writer(data);
    for (size_t i = 0;  i < values.size();  ++i) {
        uint64_t predicted = result.md.offset + uint64_t(i * result.md.slope);
        uint64_t residual = values[i] - predicted;
        //cerr << "value = " << values[i] << endl;
        //cerr << "predicted = " << predicted << endl;
        //cerr << "storing residual " << residual << " at " << i << endl;

        if (result.md.slope != 0.0) {
            //cerr << "predicted " << predicted << " val " << values[i]
            //     << endl;
            //cerr << "residual " << residual << " for entry " << i << endl;
        }
        writer.write(residual, result.md.entryBits);
    }

    values.clear();
    values.shrink_to_fit();

    result.storage = mutableStorage.freeze();

    return result;
}


/*****************************************************************************/
/* FROZEN BLOB TABLE                                                         */
/*****************************************************************************/

IMPLEMENT_STRUCTURE_DESCRIPTION(FrozenBlobTableMetadata)
{
    setVersion(1);
    addAuto("format", &FrozenBlobTableMetadata::format, "");
}

struct FrozenBlobTable::Itl {
    Itl()
        : dict(nullptr)
    {
    }

    ~Itl()
    {
        ZSTD_DDict * prev = dict.exchange(nullptr);
        if (prev) {
            ZSTD_freeDDict(prev);
        }
    }

    std::atomic<ZSTD_DDict *> dict;

    std::mutex contextPoolLock;
    std::vector<std::shared_ptr<ZSTD_DCtx> > contextPool;
};

FrozenBlobTable::
FrozenBlobTable()
    : itl(new Itl())
{
}

size_t
FrozenBlobTable::
getSize(uint32_t index) const
{
    size_t offset0 = (index == 0) ? 0 : offset.get(index - 1);
    size_t offset1 = offset.get(index);
    size_t storageSize = offset1 - offset0;
 
    switch (md.format) {
    case UNCOMPRESSED:
        return storageSize;
    case ZSTD: {
        const char * data = blobData.data() + offset0;
        auto res = ZSTD_getDecompressedSize(data, storageSize);
        if (ZSTD_isError(res)) {
            throw HttpReturnException(500, "Error with decompressing: "
                                      + string(ZSTD_getErrorName(res)));
        }
        return res;
    }
    }

    throw HttpReturnException(600, "Invalid format for frozen blob table");
}
    
size_t
FrozenBlobTable::
getBufferSize(uint32_t index) const
{
    switch (md.format) {
    case UNCOMPRESSED:
        return 0;
    case ZSTD: {
        size_t offset0 = (index == 0) ? 0 : offset.get(index - 1);
        size_t offset1 = offset.get(index);
        size_t storageSize = offset1 - offset0;
        const char * data = blobData.data() + offset0;
        auto res = ZSTD_getDecompressedSize(data, storageSize);
        if (ZSTD_isError(res)) {
            throw HttpReturnException(500, "Error with decompressing: "
                                      + string(ZSTD_getErrorName(res)));
        }
        return res;
    }
    }

    throw HttpReturnException(600, "Invalid format for frozen blob table");
}

bool
FrozenBlobTable::
needsBuffer(uint32_t index) const
{
    return md.format == ZSTD;
}

const char *
FrozenBlobTable::
getContents(uint32_t index,
            char * tempBuffer,
            size_t tempBufferSize) const
{
    size_t offset0 = (index == 0) ? 0 : offset.get(index - 1);

    switch (md.format) {
    case UNCOMPRESSED:
        return blobData.data() + offset0;
    case ZSTD: {
        size_t offset1 = offset.get(index);
        size_t storageSize = offset1 - offset0;
        const char * data = blobData.data() + offset0;

        std::shared_ptr<ZSTD_DCtx> context;

        {
            std::unique_lock<std::mutex> guard(itl->contextPoolLock);
            if (!itl->contextPool.empty()) {
                context = itl->contextPool.back();
                itl->contextPool.pop_back();
            }
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
            context.reset(ZSTD_createDCtx(),
                          [] (ZSTD_DCtx * context) { ZSTD_freeDCtx(context); });
        }

        if (!itl->dict.load()) {
            ZSTD_DDict * dict = ZSTD_createDDict(formatData.data(),
                                                 formatData.length());
            ZSTD_DDict * previous = nullptr;
            if (!itl->dict.compare_exchange_strong(previous, dict)) {
                ZSTD_freeDDict(dict);
            }
        }

        auto res = ZSTD_decompress_usingDDict(context.get(),
                                              tempBuffer,
                                              tempBufferSize,
                                              data, storageSize,
                                              itl->dict.load());
        
        if (ZSTD_isError(res)) {
            throw HttpReturnException(500, "Error with decompressing: "
                                      + string(ZSTD_getErrorName(res)));
        }
        return tempBuffer;
    }
    }

    throw HttpReturnException(600, "Invalid format for frozen blob table");
}

size_t
FrozenBlobTable::
memusage() const
{
    return formatData.memusage()
        + blobData.memusage()
        + offset.memusage();
}

size_t
FrozenBlobTable::
size() const
{
    return offset.size();
}

void
FrozenBlobTable::
serialize(StructuredSerializer & serializer) const
{
    serializer.newObject("md.json", md);
    serializer.addRegion(formatData, "fmt");
    serializer.addRegion(blobData, "blob");
    offset.serialize(*serializer.newStructure("offsets"));
}


/*****************************************************************************/
/* MUTABLE BLOB TABLE                                                        */
/*****************************************************************************/

size_t
MutableBlobTable::
add(std::string blob)
{
    size_t result = blobs.size();
    totalBytes += blob.size();
    offsets.add(totalBytes);
    blobs.emplace_back(std::move(blob));
    return result;
}

FrozenBlobTable
MutableBlobTable::
freezeCompressed(MappedSerializer & serializer)
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
        throw HttpReturnException(500, "Error with dictionary: "
                                  + string(ZDICT_getErrorName(res)));
    }
        
    Date after = Date::now();
    double elapsed = after.secondsSince(before);
    before = after;

    dictionary.resize(res);

    cerr << "created dictionary of " << res << " bytes from "
         << currentOffset << " bytes of samples in "
         << elapsed << " seconds" << endl;

    // Now compress another 1MB to see what the ratio is...
    std::shared_ptr<ZSTD_CDict> dict
        (ZSTD_createCDict(dictionary.data(), res, 1 /* compression level */),
         ZSTD_freeCDict);
        
    std::shared_ptr<ZSTD_CCtx> context
        (ZSTD_createCCtx(), ZSTD_freeCCtx);

    size_t compressedBytes = 0;
    size_t numSamples = 0;
    size_t uncompressedBytes = 0;

    MutableBlobTable compressedBlobs;

    for (size_t i = 0 /*valNumber*/;  i < blobs.size();  ++i) {
        const std::string & v = blobs[i];
        size_t len = v.length();
        PossiblyDynamicBuffer<char, 65536> buf(ZSTD_compressBound(len));

        size_t res
            = ZSTD_compress_usingCDict(context.get(),
                                       buf.data(), buf.size(),
                                       v.data(), len,
                                       dict.get());

        if (ZSTD_isError(res)) {
            throw HttpReturnException(500, "Error with compressing: "
                                      + string(ZSTD_getErrorName(res)));
        }

        compressedBlobs.add(std::string(buf.data(), res));

        uncompressedBytes += len;
        compressedBytes += res;
        numSamples += 1;
    }

    after = Date::now();
    elapsed = after.secondsSince(before);

    cerr << "compressed " << numSamples << " samples with "
         << uncompressedBytes << " bytes to " << compressedBytes
         << " bytes at " << 100.0 * compressedBytes / uncompressedBytes
         << "% compression at "
         << numSamples / elapsed << "/s" << endl;

    FrozenBlobTable frozenCompressedBlobs
        = compressedBlobs.freezeUncompressed(serializer);

    cerr << "frozenCompressedBlobs.memusage() = "
         << frozenCompressedBlobs.memusage() << endl;

    FrozenBlobTable result;
    MutableMemoryRegion dictRegion
        = serializer.allocateWritable(dictionary.length(), 1);
    std::memcpy(dictRegion.data(), dictionary.data(), dictionary.length());
    result.formatData = dictRegion.freeze();
    result.md.format = ZSTD;
    result.blobData = std::move(frozenCompressedBlobs.blobData);
    result.offset = std::move(frozenCompressedBlobs.offset);

    cerr << "result.memusage() = "
         << result.memusage() << endl;

    return result;
    
#if 0
    ssize_t totalBytesRequired = sizeof(CompressedStringFrozenColumn)
        + dictionarySize
        + compressedBytes
        + 4 * column.indexedVals.size();

    cerr << "result is " << totalBytesRequired << endl;

    return result;
#endif
}

FrozenBlobTable
MutableBlobTable::
freeze(MappedSerializer & serializer)
{
    double bytesPerEntry = 1.0 * totalBytes / blobs.size();
    
    if (totalBytes > 10000000 // 10MB
        && bytesPerEntry > 64) {
        return freezeCompressed(serializer);
    }

    return freezeUncompressed(serializer);
}

FrozenBlobTable
MutableBlobTable::
freezeUncompressed(MappedSerializer & serializer)
{
    FrozenIntegerTable frozenOffsets
        = offsets.freeze(serializer);
    MutableMemoryRegion region
        = serializer.allocateWritable(totalBytes, 1 /* alignment */);

    char * c = region.data();

    size_t currentOffset = 0;

    for (size_t i = 0;  i < blobs.size();  ++i) {
        size_t length = blobs[i].length();
        std::memcpy(c, blobs[i].data(), length);
        c += length;
        currentOffset += length;
    }

    ExcAssertEqual(c - region.data(), totalBytes);
    ExcAssertEqual(currentOffset, totalBytes);

    FrozenBlobTable result;
    result.blobData = region.freeze();
    result.offset = std::move(frozenOffsets);
    return result;
}


/*****************************************************************************/
/* FROZEN CELL VALUE TABLE                                                   */
/*****************************************************************************/

CellValue
FrozenCellValueTable::
operator [] (size_t index) const
{
    static uint8_t format
        = CellValue::serializationFormat(true /* known length */);
    
    if (blobs.needsBuffer(index)) {
        size_t bytesRequired = blobs.getBufferSize(index);

        // Nulls are serialized as zero byte blobs; all others take at least
        // one byte so there is no ambiguity
        if (bytesRequired == 0)
            return CellValue();

        // Create a buffer for the blob, and reconstitute it
        PossiblyDynamicBuffer<char, 4096> buf(bytesRequired);
        const char * contents = blobs.getContents(index, buf.data(), buf.size());
        // Decode the output
        return CellValue::reconstitute(contents, bytesRequired, format,
                                       true /* known length */).first;
    }
    else {
        const char * contents = blobs.getContents(index, nullptr, 0);
        size_t length = blobs.getSize(index);

        // Nulls are serialized as zero byte blobs; all others take at least
        // one byte so there is no ambiguity
        if (length == 0)
            return CellValue();

        // Decode the output directly in place
        return CellValue::reconstitute(contents, length, format,
                                       true /* known length */).first;
    }
}

uint64_t
FrozenCellValueTable::
memusage() const
{
    return blobs.memusage() + sizeof(*this);
}

size_t
FrozenCellValueTable::
size() const
{
    return blobs.size();
}

void
FrozenCellValueTable::
serialize(StructuredSerializer & serializer) const
{
    blobs.serialize(serializer);
}


/*****************************************************************************/
/* MUTABLE CELL VALUE TABLE                                                  */
/*****************************************************************************/

void
MutableCellValueTable::
reserve(size_t numValues)
{
}

size_t
MutableCellValueTable::
add(const CellValue & val)
{
    if (val.empty()) {
        return blobs.add(std::string());
    }
    size_t bytes = val.serializedBytes(true /* exact length */);
    std::string buf(bytes, 0);
    val.serialize(&buf[0], bytes, true /* exact length */);
    return blobs.add(std::move(buf));
}

void
MutableCellValueTable::
set(uint64_t index, const CellValue & val)
{
    ExcAssertGreaterEqual(index, blobs.size());
    while (index > blobs.size())
        blobs.add(std::string());  // implicit nulls are added
    add(val);
}

FrozenCellValueTable
MutableCellValueTable::
freeze(MappedSerializer & serializer)
{
    FrozenCellValueTable result;
    result.blobs = blobs.freeze(serializer);
    return result;
}


/*****************************************************************************/
/* MUTABLE CELL VALUE SET                                                    */
/*****************************************************************************/

std::pair<FrozenCellValueSet, std::vector<uint32_t> >
MutableCellValueSet::
freeze(MappedSerializer & serializer)
{
    // For now, we don't remap.  Later we will...
    std::vector<uint32_t> remapping;
    remapping.reserve(indexes.size());

    MutableIntegerTable offsets;
    size_t totalOffset = 0;
        
    for (size_t i = 0;  i < others.size();  ++i) {
        totalOffset += others[i].serializedBytes(true /* exact length */);
        offsets.add(totalOffset);
        remapping.push_back(i);
    }

    FrozenIntegerTable frozenOffsets
        = offsets.freeze(serializer);
    MutableMemoryRegion region
        = serializer.allocateWritable(totalOffset, 8);

    char * c = region.data();

    size_t currentOffset = 0;

    for (size_t i = 0;  i < others.size();  ++i) {
        size_t length = frozenOffsets.get(i) - currentOffset;
        c = others[i].serialize(c, length, true /* exact length */);
        currentOffset += length;
        ExcAssertEqual(c - region.data(), currentOffset);
    }

    ExcAssertEqual(c - region.data(), totalOffset);
    ExcAssertEqual(currentOffset, totalOffset);

    FrozenCellValueSet result;
    result.offsets = std::move(frozenOffsets);
    result.cells = region.freeze();
    return std::make_pair(std::move(result), std::move(remapping));
}

void
MutableCellValueSet::
add(CellValue val)
{
    indexes.emplace_back(OTHER, others.size());
    others.emplace_back(std::move(val));
    return;

    switch (val.cellType()) {
    case CellValue::EMPTY:
        throw HttpReturnException(500, "Can't add null value to CellValueSet");
    case CellValue::INTEGER:
        indexes.emplace_back(INT, intValues.add(val.toInt()));
        return;
    case CellValue::FLOAT:
        indexes.emplace_back(DOUBLE, doubleValues.add(val.toDouble()));
        doubleValues.add(val.toInt());
        return;
    case CellValue::ASCII_STRING:
    case CellValue::UTF8_STRING:
        indexes.emplace_back(STRING, stringValues.add(val.toUtf8String().stealRawString()));
        return;
    case CellValue::BLOB:
        indexes.emplace_back(BLOB,
                             blobValues.add(std::string((const char *)val.blobData(),
                                                        val.blobLength())));
        return;
    case CellValue::PATH:
        indexes.emplace_back(PATH, pathValues.add(val.coerceToPath()));
        return;
    case CellValue::TIMESTAMP:
    case CellValue::TIMEINTERVAL:
        indexes.emplace_back(OTHER, others.size());
        others.emplace_back(std::move(val));
        return;
    default:
        break;
    }

    throw HttpReturnException
        (500, "Couldn't add unknown cell to MutableCellValueSet");
}

} // namespace MLDB
