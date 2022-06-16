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
#include "string_table.h"
#include "mapped_string_table.h"


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

uint64_t
FrozenIntegerTable::
getDefault(size_t i, uint64_t def) const
{
    if (i >= md.numEntries)
        return def;
    return get(i);
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
/* FROZEN DOUBLE TABLE                                                       */
/*****************************************************************************/

IMPLEMENT_STRUCTURE_DESCRIPTION(FrozenDoubleTableMetadata)
{
    setVersion(1);
    addAuto("version", &FrozenDoubleTableMetadata::version, "");
}

void
FrozenDoubleTable::
serialize(StructuredSerializer & serializer) const
{
    serializer.newObject("md", md);
    underlying.serialize(*serializer.newStructure("dbl"));
}

void
FrozenDoubleTable::
reconstitute(StructuredReconstituter & reconstituter)
{
    reconstituter.getObject("md", md);
    ExcAssertEqual(md.version, 1);
    underlying.reconstitute(*reconstituter.getStructure("dbl"));
}


/*****************************************************************************/
/* MUTABLE DOUBLE TABLE                                                      */
/*****************************************************************************/

FrozenDoubleTable
MutableDoubleTable::
freeze(MappedSerializer & serializer)
{
    FrozenDoubleTable result;
    result.underlying = underlying.freeze(serializer);
    return result;
}

std::pair<FrozenDoubleTable, std::vector<uint32_t>>
MutableDoubleTable::
freezeRemapped(MappedSerializer & serializer)
{
    FrozenDoubleTable result;
    std::vector<uint32_t> remapping;
    std::tie(result.underlying, remapping) = underlying.freezeRemapped(serializer);
    return { std::move(result), std::move(remapping) };
}


/*****************************************************************************/
/* FROZEN BLOB TABLE                                                         */
/*****************************************************************************/

struct FrozenBlobTableMetadata {
    uint8_t version = 0;
    uint8_t format = 0;
};

IMPLEMENT_STRUCTURE_DESCRIPTION(FrozenBlobTableMetadata)
{
    setVersion(1);
    addAuto("version", &FrozenBlobTableMetadata::version, "");
    addAuto("format", &FrozenBlobTableMetadata::format, "");
}

#if 0
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
            + (transducer ? transducer->memusage() : 0);
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
        ExcAssertEqual(md.version, 1);
        blobData = reconstituter.getRegion("bl");
        offset.reconstitute(*reconstituter.getStructure("of"));
        length.reconstitute(*reconstituter.getStructure("l"));
        transducer = StringTransducer::thaw(*reconstituter.getStructure("tr"));
    }
};
#else
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

    const MappedOptimizedStringTable & blobs() const { return *reinterpret_cast<const MappedOptimizedStringTable *>(blobData.data()); }

    size_t
    getSize(uint32_t index) const
    {
        if (md.format == 0) {
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
        else {
            MLDB_THROW_UNIMPLEMENTED();
        }
    }
    
    size_t
    getBufferSize(uint32_t index) const
    {
        if (md.format == 0) {
            size_t offset0 = (index == 0) ? 0 : offset.get(index - 1);
            size_t offset1 = offset.get(index);
            size_t compressedLength = offset1 - offset0;
            const char * data = blobData.data() + offset0;
            size_t decompressedLength = compressedLength + length.get(index);
            return transducer
                ->getTemporaryBufferSize(string_view(data, compressedLength),
                                        decompressedLength);

        }
        else {
            MLDB_THROW_UNIMPLEMENTED();
        }
    }

    bool
    needsBuffer(uint32_t index) const
    {
        return true;
    }

    string_view
    getContents(uint32_t index,
                char * tempBuffer,
                size_t tempBufferSize) const
    {
        if (md.format == 0) {
            size_t offset0 = (index == 0) ? 0 : offset.get(index - 1);
            size_t offset1 = offset.get(index);
            size_t compressedLength = offset1 - offset0;
            std::string_view input(blobData.data() + offset0, compressedLength);
            return transducer->generateAll(input, tempBuffer, tempBufferSize);
        }
        else {
            MLDB_THROW_UNIMPLEMENTED();
        }
    }

    size_t
    memusage() const
    {
        size_t result
            = blobData.memusage()
            + sizeof(*this);

        if (md.format == 0) {
            result +=
                + offset.memusage()
                + length.memusage()
                + (transducer ? transducer->memusage() : 0);
        }
        return result;
    }

    size_t
    size() const
    {
        if (md.format == 0) {
            return offset.size();
        }
        else {
            return blobs().size();
        }
    }

    void
    serialize(StructuredSerializer & serializer) const
    {
        serializer.newObject("md", md);
        if (md.format == 0) {
            serializer.addRegion(blobData, "bl");
            offset.serialize(*serializer.newStructure("of"));
            length.serialize(*serializer.newStructure("l"));
            transducer->serialize(*serializer.newStructure("tr"));
        }
        else {
            serializer.addRegion(blobData, "st");
        }
    }

    void
    reconstitute(StructuredReconstituter & reconstituter)
    {
        reconstituter.getObject("md", md);
        ExcAssertEqual(md.version, 1);
        if (md.format == 0) {
            blobData = reconstituter.getRegion("bl");
            offset.reconstitute(*reconstituter.getStructure("of"));
            length.reconstitute(*reconstituter.getStructure("l"));
            transducer = StringTransducer::thaw(*reconstituter.getStructure("tr"));
        }
        else {
            blobData = reconstituter.getRegion("st");
        }
    }
};
#endif

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

CellValue
FrozenBlobTable::
operator [] (size_t index) const
{
    if (needsBuffer(index)) {
        size_t bytesRequired = getBufferSize(index);

        // Nulls are serialized as zero byte blobs; all others take at least
        // one byte so there is no ambiguity
        if (bytesRequired == 0)
            return CellValue();

        // Create a buffer for the blob, and reconstitute it
        PossiblyDynamicBuffer<char, 4096> buf(bytesRequired);
        std::string_view contents
            = getContents(index, buf.data(), buf.size());
        return CellValue::blob(contents.data(), contents.size());
    }
    else {
        std::string_view contents = getContents(index, nullptr, 0);
        size_t length = contents.length();

        // Nulls are serialized as zero byte blobs; all others take at least
        // one byte so there is no ambiguity
        if (length == 0)
            return CellValue::blob(nullptr, 0);

        // Decode the output directly in place
        return CellValue::blob(contents.data(), contents.length());
    }
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
    //cerr << "freezing with " << stats.uniqueShortLengths << " short lengths"
    //     << " and " << stats.uniqueBytes << " unique bytes" << endl;

    double bytesPerEntry = 1.0 * stats.totalBytes / blobs.size();

    std::shared_ptr<StringTransducer> forward, reverse;
    
    if (stats.totalBytes > 10000000 // 10MB
        && bytesPerEntry > 64) {
        //cerr << "zstd transducer" << endl;
        std::tie(forward, reverse)
            = ZstdStringTransducer::train(blobs, serializer);
    }
    if (!forward
        && stats.uniqueLongLengths == 0
        && stats.uniqueShortLengths == 1) {
        //cerr << "ID transducer" << endl;
        std::tie(forward, reverse)
            = trainIdTransducer(blobs, stats, serializer);
    }
    if (!forward && stats.totalBytes >= 4096) {
        //cerr << "String table; total bytes " << stats.totalBytes << endl;
        StringTable table;
        table.reserve(blobs.size(), stats.totalBytes);
        //cerr << "adding " << blobs.size() << " blobs" << endl;
        for (auto & bl: blobs) {
        //    if (table.size() < 100) cerr << "  " << bl << endl;
            table.add(bl);
        }
        
        //static std::mutex mutex;
        //std::unique_lock guard{mutex};
        OptimizedStringTable optimized = optimize_string_table(table);

        MappingContext context;
        MappedOptimizedStringTable & st = context.alloc<MappedOptimizedStringTable>();
        MLDB::freeze(context, st, optimized);

        FrozenBlobTable result;
        auto itl = result.itl;
        itl->md.version = 1;
        itl->md.format = 1;
        auto blobData = serializer.allocateWritable(context.getOffset(), alignof(MappedOptimizedStringTable));
        std::memcpy(blobData.data(), context.getMemory(), context.getOffset());
        itl->blobData = blobData.freeze();
        return result;

        //FrozenBlobTable result = freezeUncompressed(serializer);

        //cerr << "optimized " << stats.totalBytes << " bytes to " << optimized.memUsageIndirect({})
        //     << " (instead of " << result.memusage() << ") for "
        //     << blobs.size() << " blobs of length " << 1.0 * stats.totalBytes / blobs.size() << endl;


#if 0
        if (stats.totalBytes < optimized.memUsageIndirect({})) {
            cerr << "incompressible string table" << endl;
            for (size_t i = 0;  i < std::min<size_t>(table.size(), 100);  ++i) {
                cerr << i << "\t" << blobs[i] << endl;
            }
        }
#endif
        //cerr << "naive blob table length " << result.memusage() << endl;
        return result;
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

    if (numSamples > 1000 && false) {
        cerr << "compressed " << numSamples << " samples with "
            << uncompressedBytes << " bytes to " << compressedBytes
            << " bytes at " << 100.0 * compressedBytes / uncompressedBytes
            << "% compression at "
            << numSamples / elapsed << "/s" << endl;
    }

    FrozenBlobTable frozenCompressedBlobs
        = compressedBlobs.freezeUncompressed(serializer);
    
    //cerr << "frozenCompressedBlobs.memusage() = "
    //     << frozenCompressedBlobs.memusage() << endl;

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
/* FROZEN STRING TABLE                                                       */
/*****************************************************************************/


IMPLEMENT_STRUCTURE_DESCRIPTION(FrozenStringTableMetadata)
{
    setVersion(1);
    addAuto("version", &FrozenStringTableMetadata::version, "");
}

void
FrozenStringTable::
serialize(StructuredSerializer & serializer) const
{
    serializer.newObject("md", md);
    underlying.serialize(*serializer.newStructure("str"));
}

void
FrozenStringTable::
reconstitute(StructuredReconstituter & reconstituter)
{
    reconstituter.getObject("md", md);
    ExcAssertEqual(md.version, 1);
    underlying.reconstitute(*reconstituter.getStructure("str"));
}


/*****************************************************************************/
/* MUTABLE STRING TABLE                                                      */
/*****************************************************************************/

size_t
MutableStringTable::
add(const char * contents, size_t length)
{
    return underlying.add(string_view{contents, length});
}

size_t
MutableStringTable::
add(std::string && contents)
{
    return underlying.add(contents);
}

size_t
MutableStringTable::
add(const std::string & contents)
{
    return underlying.add(contents);
}

FrozenStringTable
MutableStringTable::
freeze(MappedSerializer & serializer)
{
    FrozenStringTable result;
    result.underlying = this->underlying.freeze(serializer);
    return result;    
}

std::pair<FrozenStringTable, std::vector<uint32_t>>
MutableStringTable::
freezeRemapped(MappedSerializer & serializer)
{
    FrozenStringTable result;
    std::vector<uint32_t> remapping;

    std::tie(result.underlying, remapping) = underlying.freezeRemapped(serializer);

    return { std::move(result), std::move(remapping) };
}


/*****************************************************************************/
/* FROZEN PATH TABLE                                                         */
/*****************************************************************************/

IMPLEMENT_STRUCTURE_DESCRIPTION(FrozenPathTableMetadata)
{
    setVersion(1);
    addAuto("version", &FrozenPathTableMetadata::version, "");
}

void
FrozenPathTable::
serialize(StructuredSerializer & serializer) const
{
    serializer.newObject("md", md);
    underlying.serialize(*serializer.newStructure("dbl"));
}

void
FrozenPathTable::
reconstitute(StructuredReconstituter & reconstituter)
{
    reconstituter.getObject("md", md);
    ExcAssertEqual(md.version, 1);
    underlying.reconstitute(*reconstituter.getStructure("dbl"));
}


/*****************************************************************************/
/* MUTABLE PATH TABLE                                                        */
/*****************************************************************************/

FrozenPathTable
MutablePathTable::
freeze(MappedSerializer & serializer)
{
    FrozenPathTable result;
    result.underlying = this->underlying.freeze(serializer);
    return result;
}

std::pair<FrozenPathTable, std::vector<uint32_t>>
MutablePathTable::
freezeRemapped(MappedSerializer & serializer)
{
    FrozenPathTable result;
    std::vector<uint32_t> remapping;

    std::tie(result.underlying, remapping) = underlying.freezeRemapped(serializer);

    return { std::move(result), std::move(remapping) };
}


/*****************************************************************************/
/* FROZEN TIMESTAMP TABLE                                                    */
/*****************************************************************************/

IMPLEMENT_STRUCTURE_DESCRIPTION(FrozenTimestampTableMetadata)
{
    setVersion(1);
    addAuto("version", &FrozenTimestampTableMetadata::version, "");
}


void
FrozenTimestampTable::
serialize(StructuredSerializer & serializer) const
{
    serializer.newObject("md", md);
    underlying.serialize(*serializer.newStructure("dbl"));
}

void
FrozenTimestampTable::
reconstitute(StructuredReconstituter & reconstituter)
{
    reconstituter.getObject("md", md);
    ExcAssertEqual(md.version, 1);
    underlying.reconstitute(*reconstituter.getStructure("dbl"));
}


/*****************************************************************************/
/* MUTABLE TIMESTAMP TABLE                                                   */
/*****************************************************************************/

FrozenTimestampTable
MutableTimestampTable::
freeze(MappedSerializer & serializer)
{
    FrozenTimestampTable result;
    result.underlying = this->underlying.freeze(serializer);
    return result;
}

std::pair<FrozenTimestampTable, std::vector<uint32_t>>
MutableTimestampTable::
freezeRemapped(MappedSerializer & serializer)
{
    FrozenTimestampTable result;
    std::vector<uint32_t> remapping;

    std::tie(result.underlying, remapping) = underlying.freezeRemapped(serializer);

    return { std::move(result), std::move(remapping) };
}


/*****************************************************************************/
/* FROZEN CELL VALUE TABLE                                                   */
/*****************************************************************************/

IMPLEMENT_STRUCTURE_DESCRIPTION(FrozenCellValueTableMetadata)
{
    setVersion(1);
    addAuto("version", &FrozenCellValueTableMetadata::version, "");
}

CellValue
FrozenCellValueTable::
operator [] (size_t index) const
{
    static uint8_t format
        = CellValue::serializationFormat(true /* known length */);
    
    if (underlying.needsBuffer(index)) {
        size_t bytesRequired = underlying.getBufferSize(index);

        // Nulls are serialized as zero byte blobs; all others take at least
        // one byte so there is no ambiguity
        if (bytesRequired == 0)
            return CellValue();

        // Create a buffer for the blob, and reconstitute it
        PossiblyDynamicBuffer<char, 4096> buf(bytesRequired);
        std::string_view contents
            = underlying.getContents(index, buf.data(), buf.size());
        // Decode the output
        return CellValue::reconstitute(contents.data(), contents.length(),
                                       format,
                                       true /* known length */).first;
    }
    else {
        std::string_view contents = underlying.getContents(index, nullptr, 0);
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
    return underlying.memusage() + sizeof(*this);
}

size_t
FrozenCellValueTable::
size() const
{
    return underlying.size();
}

void
FrozenCellValueTable::
serialize(StructuredSerializer & serializer) const
{
    underlying.serialize(serializer);
}

void
FrozenCellValueTable::
reconstitute(StructuredReconstituter & reconstituter)
{
    underlying.reconstitute(reconstituter);
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
        return underlying.add(std::string());
    }
    size_t bytes = val.serializedBytes(true /* exact length */);
    std::string buf(bytes, 0);
    val.serialize(&buf[0], bytes, true /* exact length */);
    return underlying.add(std::move(buf));
}

void
MutableCellValueTable::
set(uint64_t index, const CellValue & val)
{
    ExcAssertGreaterEqual(index, underlying.size());
    while (index > underlying.size())
        underlying.add(std::string());  // implicit nulls are added
    add(val);
}

FrozenCellValueTable
MutableCellValueTable::
freeze(MappedSerializer & serializer)
{
    FrozenCellValueTable result;
    result.underlying = underlying.freeze(serializer);
    return result;
}


/*****************************************************************************/
/* MUTABLE CELL VALUE SET                                                    */
/*****************************************************************************/

// Apply op(fieldName, enumValue) for each index kind in the mutable CellValue set
#define FOR_EACH_INDEX_KIND(op) \
    op(others, MutableCellValueSet::OTHER) \
    op(intValues, MutableCellValueSet::INT) \
    op(doubleValues, MutableCellValueSet::DOUBLE) \
    op(timestampValues, MutableCellValueSet::TIMESTAMP) \
    op(stringValues, MutableCellValueSet::STRING) \
    op(blobValues, MutableCellValueSet::BLOB) \
    op(pathValues, MutableCellValueSet::PATH) \

size_t
FrozenCellValueSet::
bytesForValues(std::span<const CellValue> vals)
{
    // TODO: we should be able to do better than this...
    size_t result = 0;
    for (const CellValue & v: vals) {
        result += v.serializedBytes(true);
    }
    return result;
}

std::pair<FrozenCellValueSet, std::vector<uint32_t> >
MutableCellValueSet::
freeze(MappedSerializer & serializer)
{
    std::vector<uint32_t> remapping;
    remapping.resize(indexes.size());

    std::array<uint32_t, NUM_TYPES + 1> startOffsets;
    size_t current = 0;
#define doStartOffset(field, index) \
    { startOffsets[index] = current;  current += field.size(); }
    FOR_EACH_INDEX_KIND(doStartOffset);
    startOffsets[NUM_TYPES] = current;

    //cerr << "total of " << current << " values in table" << endl;
    std::vector<std::vector<uint32_t>> remappings(NUM_TYPES);

    FrozenCellValueSet result;

    auto freezeTable = [&] (int n, auto & table)
    {
        //cerr << "freezing table " << n << " of type " << demangle(typeid(table)) << " with " << table.size() << " entries" << endl;
        auto [tableOut, remapping] = table.freezeRemapped(serializer);
        remappings[n] = std::move(remapping);
        return tableOut;
    };

#define DO_TABLE(field, num) { result.field = freezeTable(num, field); }
    FOR_EACH_INDEX_KIND(DO_TABLE)

    // For each value, we remap into the index in the new table
    for (size_t i = 0;  i < indexes.size();  ++i) {
        uint32_t tp = indexes[i].index;  // what type (changes which list its in)
        uint32_t startOffset = startOffsets[tp];
        const auto & rm = remappings.at(tp);
        if (rm.empty()) {
            remapping[i] = startOffset + indexes[i].entry;
        }
        else {
            remapping[i] = startOffset + rm.at(indexes[i].entry);
        }
    }

    //cerr << "remapping = " << remapping << endl;

    //if (stringValues.underlying.size() == 1) {
    //    cerr << "Unique string value: " << stringValues.underlying.blobs.at(0) << endl;
    //}

    MutableIntegerTable startOffsetSerialized;
    for (auto & o: startOffsets)
        startOffsetSerialized.add(o);

    result.startOffsets = startOffsetSerialized.freeze(serializer);

    //MutableIntegerTable offsets;
    //size_t totalOffset = 0;

    
#if 0
    for (size_t i = 0;  i < others.size();  ++i) {
        totalOffset += others[i].serializedBytes(true /* exact length */);
        offsets.add(totalOffset);
        remapping.push_back(i);
    }
#endif

#if 0
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

    result.offsets = std::move(frozenOffsets);
    result.cells = region.freeze();
#endif

    return std::make_pair(std::move(result), std::move(remapping));
}

void
MutableCellValueSet::
add(CellValue val)
{
    //indexes.emplace_back(OTHER, others.size());
    //others.emplace_back(std::move(val));
    //return;

    switch (val.cellType()) {
    case CellValue::EMPTY:
        throw AnnotatedException(500, "Can't add null value to CellValueSet");
    case CellValue::INTEGER:
        indexes.emplace_back(INT, intValues.add(val.toInt()));
        return;
    case CellValue::FLOAT:
        indexes.emplace_back(DOUBLE, doubleValues.add(val.toDouble()));
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
        indexes.emplace_back(OTHER, others.add(std::move(val)));
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

IMPLEMENT_STRUCTURE_DESCRIPTION(FrozenCellValueSetMetadata)
{
    setVersion(1);
    addAuto("version", &FrozenCellValueSetMetadata::version, "");
}

uint64_t
FrozenCellValueSet::
memusage() const
{
    uint64_t result = sizeof(*this) + startOffsets.memusage();
#define ADD_FIELD(field, n) { result += field.memusage(); }
    FOR_EACH_INDEX_KIND(ADD_FIELD);
    return result;
}

CellValue
FrozenCellValueSet::
operator [] (size_t index) const
{
    size_t op = 0;
#define GET_FIELD(field, n) { auto o = startOffsets[n + 1]; if (index < o) return field[index - op]; else op = o; }
    FOR_EACH_INDEX_KIND(GET_FIELD)
    MLDB_THROW_RANGE_ERROR("Index out of range: %lli >= %lli", index, size());
}

void
FrozenCellValueSet::
serialize(StructuredSerializer & serializer) const
{
    serializer.newObject("md", md);
    startOffsets.serialize(*serializer.newStructure("sofs"));
    
#define SERIALIZE_FIELD(field, n) { if (startOffsets[n] < startOffsets[n+1]) field.serialize(*serializer.newStructure(n)); }
    FOR_EACH_INDEX_KIND(SERIALIZE_FIELD)
}

void
FrozenCellValueSet::
reconstitute(StructuredReconstituter & reconstituter)
{
    reconstituter.getObject("md", md);
    ExcAssertEqual(md.version, 1);
    startOffsets.reconstitute(*reconstituter.getStructure("sofs"));

#define RECONSTITUTE_FIELD(field, n) { if (startOffsets[n] < startOffsets[n+1]) field.reconstitute(*reconstituter.getStructure(n)); }
    FOR_EACH_INDEX_KIND(RECONSTITUTE_FIELD)
}

bool
FrozenCellValueSet::
forEachDistinctValue(std::function<bool (CellValue &)> fn) const
{
    MLDB_THROW_UNIMPLEMENTED();

#if 0
    std::vector<CellValue> vals;
    vals.reserve(size());
    for (size_t i = 0;  i < size();  ++i) {
        vals.emplace_back(operator [] (i));
    }
    std::sort(vals.begin(), vals.end());
    for (size_t i = 0;  i < vals.size();  ++i) {
        if (i > 0 && vals[i] == vals[i - 1])
            continue;
        if (!fn(vals[i]))
            return false;
    }
    return true;
#endif
}

#if 0
template<typename Fn>
bool forEachDistinctValue(Fn && fn) const
{
    std::vector<CellValue> vals;
    vals.reserve(size());
    for (size_t i = 0;  i < size();  ++i) {
        vals.emplace_back(operator [] (i));
    }
    std::sort(vals.begin(), vals.end());
    for (size_t i = 0;  i < vals.size();  ++i) {
        if (i > 0 && vals[i] == vals[i - 1])
            continue;
        if (!fn(vals[i]))
            return false;
    }
    return true;
}
#endif

#if 0
template<typename Fn>
bool forEachDistinctValue(Fn && fn) const
{
    std::vector<CellValue> vals;
    vals.reserve(size());
    for (size_t i = 0;  i < size();  ++i) {
        vals.emplace_back(operator [] (i));
    }
    std::sort(vals.begin(), vals.end());
    for (size_t i = 0;  i < vals.size();  ++i) {
        if (i > 0 && vals[i] == vals[i - 1])
            continue;
        if (!fn(vals[i]))
            return false;
    }
    return true;
}
#endif

} // namespace MLDB
