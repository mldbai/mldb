/** frozen_tables.cc
    Jeremy Barnes, 12 June 2017
    Copyright (c) 2017 Element AI Inc.  All rights reserved.
    This file is part of MLDB. Copyright 2017 mldb.ai inc. All rights reserved.
*/

#include "frozen_tables.h"
#include "mldb/types/annotated_exception.h"
#include "mldb/utils/possibly_dynamic_buffer.h"
#include "mldb/types/basic_value_descriptions.h"

#include "mldb/ext/zstd/lib/zdict.h"
#include "mldb/ext/zstd/lib/zstd.h"

#include "transducer.h"
#include "mldb/utils/possibly_dynamic_buffer.h"
#include "mldb/types/basic_value_descriptions.h"

#include "mldb/base/scope.h"
#include <mutex>
#include <bitset>


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
    MLDB::Bit_Extractor<uint64_t> bits(storage.data());
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
    serializer.newObject("md", md);
    serializer.addRegion(storage, "i");
}

void
FrozenIntegerTable::
reconstitute(StructuredReconstituter & reconstituter)
{
    reconstituter.getObject("md", md);
    storage = reconstituter.getRegion("i");
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

    MLDB::Bit_Writer<uint64_t> writer(data);
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

struct FrozenBlobTableMetadata {
};

IMPLEMENT_STRUCTURE_DESCRIPTION(FrozenBlobTableMetadata)
{
    setVersion(1);
}

struct FrozenBlobTable::Itl {
    Itl()
    {
    }

    ~Itl()
    {
    }

    FrozenBlobTableMetadata md;
    FrozenMemoryRegion blobData;
    FrozenIntegerTable offset;
    FrozenIntegerTable length;  // How much longer is uncompressed than cmprsd
    std::shared_ptr<StringTransducer> transducer;

    // We have six interfaces that we need to handle in a device independent
    // manner:
    // - getSize(index, offset)
    // - getContents(index)
    // - getBufferSize(index), which is trivial
    // - needsBuffer(index), which is trivial
    // - size, which is trivial

    size_t
    getSize(uint32_t index) const
    {
        size_t offset0 = (index == 0) ? 0 : offset.get(index - 1);
        size_t offset1 = offset.get(index);
        size_t compressedLength = offset1 - offset0;
        const char * data = blobData.data() + offset0;
        if (transducer->canGetOutputLength()) {
            return transducer
                ->getOutputLength(string_view(data, compressedLength));
        }
        else {
            size_t decompressedLength = compressedLength + length.get(index);
            return decompressedLength;
        }
    }
    
    size_t
    getBufferSize(uint32_t index) const
    {
        size_t offset0 = (index == 0) ? 0 : offset.get(index - 1);
        size_t offset1 = offset.get(index);
        size_t compressedLength = offset1 - offset0;
        const char * data = blobData.data() + offset0;
        size_t decompressedLength = compressedLength + length.get(index);
        return transducer
            ->getTemporaryBufferSize(string_view(data, compressedLength),
                                     decompressedLength);
    }

    bool
    needsBuffer(uint32_t index) const
    {
        return transducer->needsTemporaryBuffer();
    }

    string_view
    getContents(uint32_t index,
                char * tempBuffer,
                size_t tempBufferSize) const
    {
        size_t offset0 = (index == 0) ? 0 : offset.get(index - 1);
        size_t offset1 = offset.get(index);
        size_t compressedLength = offset1 - offset0;
        std::string_view input(blobData.data() + offset0, compressedLength);
        return transducer->generateAll(input, tempBuffer, tempBufferSize);
    }

    size_t
    memusage() const
    {
        size_t result
            = blobData.memusage()
            + offset.memusage()
            + length.memusage()
            + transducer->memusage();
        return result;
    }

    size_t
    size() const
    {
        return offset.size();
    }

    void
    serialize(StructuredSerializer & serializer) const
    {
        serializer.newObject("md", md);
        serializer.addRegion(blobData, "bl");
        offset.serialize(*serializer.newStructure("of"));
        length.serialize(*serializer.newStructure("l"));
        transducer->serialize(*serializer.newStructure("tr"));
    }

    void
    reconstitute(StructuredReconstituter & reconstituter)
    {
        reconstituter.getObject("md", md);
        blobData = reconstituter.getRegion("bl");
        offset.reconstitute(*reconstituter.getStructure("of"));
        length.reconstitute(*reconstituter.getStructure("l"));
        transducer = StringTransducer::thaw(*reconstituter.getStructure("tr"));
    }
};

FrozenBlobTable::
FrozenBlobTable()
    : itl(new Itl())
{
}

FrozenBlobTable::
~FrozenBlobTable()
{
}

size_t
FrozenBlobTable::
getSize(uint32_t index) const
{
    return itl->getSize(index);
}
    
size_t
FrozenBlobTable::
getBufferSize(uint32_t index) const
{
    return itl->getBufferSize(index);
}

bool
FrozenBlobTable::
needsBuffer(uint32_t index) const
{
    return itl->needsBuffer(index);
}

string_view
FrozenBlobTable::
getContents(uint32_t index,
            char * tempBuffer,
            size_t tempBufferSize) const
{
    return itl->getContents(index, tempBuffer, tempBufferSize);
}

size_t
FrozenBlobTable::
memusage() const
{
    return itl->memusage();
}

size_t
FrozenBlobTable::
size() const
{
    return itl->size();
}

void
FrozenBlobTable::
serialize(StructuredSerializer & serializer) const
{
    itl->serialize(serializer);
}

void
FrozenBlobTable::
reconstitute(StructuredReconstituter & reconstituter)
{
    itl->reconstitute(reconstituter);
}

void
StringStats::
add(std::string_view blob)
{
    totalBytes += blob.size();
    if (blob.size() <= 255) {
        uniqueShortLengths += (shortLengthDistribution[blob.size()]++ == 0);
    }
    else {
        uniqueLongLengths += (longLengthDistribution[blob.size()]++ == 0);
    }

    for (unsigned char c: blob) {
        uniqueBytes += (byteDistribution[c - 0U]++ == 0);
    }
}


/*****************************************************************************/
/* MUTABLE BLOB TABLE                                                        */
/*****************************************************************************/

MutableBlobTable::
MutableBlobTable()
{
}

size_t
MutableBlobTable::
add(std::string && blob)
{
    size_t result = blobs.size();
    stats.add(blob);
    blobs.emplace_back(std::move(blob));
    offsets.add(stats.totalBytes);
    return result;
}

size_t
MutableBlobTable::
add(std::string_view blob)
{
    return add(std::string(blob.cbegin(), blob.cend()));
}

FrozenBlobTable
MutableBlobTable::
freeze(MappedSerializer & serializer)
{
    //cerr << "freezing with " << uniqueShortLengths << " short lengths"
    //     << " and " << uniqueBytes << " unique bytes" << endl;

    double bytesPerEntry = 1.0 * stats.totalBytes / blobs.size();

    std::shared_ptr<StringTransducer> forward, reverse;
    
    if (stats.totalBytes > 10000000 // 10MB
        && bytesPerEntry > 64) {
        std::tie(forward, reverse)
            = ZstdStringTransducer::train(blobs, serializer);
    }
    if (!forward
        && stats.uniqueLongLengths == 0
        && stats.uniqueShortLengths == 1) {
        std::tie(forward, reverse)
            = trainIdTransducer(blobs, stats, serializer);
    }
    if (!forward) {
        return freezeUncompressed(serializer);
    }

    MutableBlobTable compressedBlobs;

#if 0    
    std::function<void ()> compress = compressor.getHost("generateAll");
    std::function<void ()> length = compressor.getHost("length");

    // The pipeline we create for this operation is as follows:
    // in order map over i = 0..blobs.size()
    //    extract offset[i] as end
    //    extract offset[i - 1] as start
    //    length = end - start
    //    add length to lengths
    //    data = compress from (offset[i] to offset[i - 1])
    //    add data to blobs
    //
    // (parallel map [i 0 blobs.size()]
    //  (let [end offset(i)
    //        start offset(- i 1)
    //        len (- end start)
    //        bl blob(data start end)
    //        data compress(bl)]
    //   (insert lengths i length)
    //   (insert blobs i blob)))
    //
    // FOREACH

    Parameter<uint32_t> i("i");
    Constant<uint32_t> one(1);
    Function<uint64_t (uint64_t)> getOffset = offsets.getFunction("get");
    Plus<uint64_t> iPlusOne(i, one);
    
    
    Function<void (size_t)> mapper("mapper", blob);

    Parameter<size_t> i("i");
    Function mapper("mapper", i);
    Variable start = mapper.local<uint64_t>("start");
    Variable end = mapper.local<uint64_t>("end");
    Variable length = mapper.local<uint64_t>("length");
    Assign a1(start, Call(getOffset, i - 1));
    Assign a2(end, Call(getOffset, i));
    Assign a3(length, end - start);
    
    Operation operation
        = parallelMapInOrderReduce(Range(0, i), mapper, reducer);

#endif

    size_t compressedBytes = 0;
    size_t numSamples = 0;
    size_t uncompressedBytes = 0;

    Date before = Date::now();

    // How much longer the uncompressed version is than the compressed
    MutableIntegerTable lengths;
    
    for (size_t i = 0 /*valNumber*/;  i < blobs.size();  ++i) {
        const std::string & v = blobs[i];
        size_t len = v.length();
        size_t bufferLen
            = forward->getTemporaryBufferSize(string_view(v.data(), len),
                                              len);
        
        PossiblyDynamicBuffer<char, 4096> buf(bufferLen);
        
        std::string_view res
            = forward->generateAll(string_view(v.data(), len),
                                   buf.data(), bufferLen);

        uncompressedBytes += len;
        compressedBytes += res.size();
        numSamples += 1;

        compressedBlobs.add(res);
        lengths.add(len - res.size());
        offsets.add(compressedBytes);
    }

    Date after = Date::now();
    double elapsed = after.secondsSince(before);

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

    result.itl->blobData = std::move(frozenCompressedBlobs.itl->blobData);
    result.itl->offset = std::move(frozenCompressedBlobs.itl->offset);
    result.itl->length = lengths.freeze(serializer);
    result.itl->transducer = std::move(reverse);
    

#if 0
    MutableBlobTable compressedBlobs;

    
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
    result.itl->blobData = region.freeze();
    result.itl->offset = std::move(frozenOffsets);
#endif

    return result;
}

FrozenBlobTable
MutableBlobTable::
freezeUncompressed(MappedSerializer & serializer)
{
    auto buf = serializer.allocateWritable(stats.totalBytes, 1 /* alignment */);

    char * data = buf.data();

    for (size_t i = 0 /*valNumber*/;  i < blobs.size();  ++i) {
        const std::string & v = blobs[i];
        size_t len = v.length();
        std::memcpy(data, v.data(), len);
        data += len;
    }

    FrozenBlobTable result;
    result.itl->blobData = buf.freeze();
    result.itl->offset = offsets.freeze(serializer);
    result.itl->transducer.reset(new IdentityStringTransducer());
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
        std::string_view contents
            = blobs.getContents(index, buf.data(), buf.size());
        // Decode the output
        return CellValue::reconstitute(contents.data(), contents.length(),
                                       format,
                                       true /* known length */).first;
    }
    else {
        std::string_view contents = blobs.getContents(index, nullptr, 0);
        size_t length = contents.length();

        // Nulls are serialized as zero byte blobs; all others take at least
        // one byte so there is no ambiguity
        if (length == 0)
            return CellValue();

        // Decode the output directly in place
        return CellValue::reconstitute(contents.data(), contents.length(),
                                       format,
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

void
FrozenCellValueTable::
reconstitute(StructuredReconstituter & reconstituter)
{
    blobs.reconstitute(reconstituter);
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
        throw AnnotatedException(500, "Can't add null value to CellValueSet");
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

    throw AnnotatedException
        (500, "Couldn't add unknown cell to MutableCellValueSet");
}


/*****************************************************************************/
/* FROZEN CELL VALUE SET                                                     */
/*****************************************************************************/

void
FrozenCellValueSet::
serialize(StructuredSerializer & serializer) const
{
    offsets.serialize(serializer);
    serializer.addRegion(cells, "c");
}

void
FrozenCellValueSet::
reconstitute(StructuredReconstituter & reconstituter)
{
    offsets.reconstitute(reconstituter);
    cells = reconstituter.getRegion("c");
}

} // namespace MLDB
