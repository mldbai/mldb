/** frozen_column.cc                                               -*- C++ -*-
    Jeremy Barnes, 27 March 2016
    This file is part of MLDB. Copyright 2016 mldb.ai inc. All rights reserved.

    Implementation of code to freeze columns into a binary format.
*/

#include "frozen_tables.h"
#include "frozen_column.h"
#include "tabular_dataset_column.h"
#include "mldb/http/http_exception.h"
#include "mldb/types/basic_value_descriptions.h"

#include "mldb/ext/zstd/lib/dictBuilder/zdict.h"
#include "mldb/ext/zstd/lib/zstd.h"

#include "mldb/utils/possibly_dynamic_buffer.h"


using namespace std;


namespace MLDB {

/*****************************************************************************/
/* STRING FROZEN COLUMN                                                      */
/*****************************************************************************/

/// Frozen column that stores each value as a string
struct CompressedStringFrozenColumn: public FrozenColumn {

    // This stores the underlying doubles or CellValues 
    std::shared_ptr<const FrozenColumn> unwrapped;

    CompressedStringFrozenColumn(TabularDatasetColumn & column,
                                 MappedSerializer & serializer,
                                 const ColumnFreezeParameters & params)
        : columnTypes(column.columnTypes)
    {
        ExcAssert(!column.isFrozen);
        // Convert the values to unwrapped doubles
        column.valueIndex.clear();
        size_t numNulls = column.columnTypes.numNulls;
        column.columnTypes = ColumnTypes();
        for (auto & v: column.indexedVals) {
            v = v.coerceToNumber();
            column.columnTypes.update(v);
        }
        column.columnTypes.numNulls = numNulls;

        unwrapped = column.freeze(serializer, params);
    }

    // Wrap a double (or null) into a string (or null)
    static CellValue wrap(CellValue val)
    {
        if (val.empty())
            return val;
        return val.coerceToString();
    }


    virtual bool forEach(const ForEachRowFn & onRow) const
    {
        auto onRow2 = [&] (size_t rowNum, const CellValue & val)
            {
                return onRow(rowNum, wrap(val));
            };

        return unwrapped->forEach(onRow2);
    }

    virtual bool forEachDense(const ForEachRowFn & onRow) const
    {
        auto onRow2 = [&] (size_t rowNum, const CellValue & val)
            {
                return onRow(rowNum, wrap(val));
            };

        return unwrapped->forEachDense(onRow2);
    }

    virtual CellValue get(uint32_t rowIndex) const
    {
        return wrap(unwrapped->get(rowIndex));
    }

    virtual size_t size() const
    {
        return unwrapped->size();
    }

    virtual size_t memusage() const
    {
        return sizeof(*this)
            + unwrapped->memusage();
    }

    virtual bool
    forEachDistinctValue(std::function<bool (const CellValue &)> fn) const
    {
        auto fn2 = [&] (const CellValue & v)
            {
                return fn(wrap(v));
            };

        return unwrapped->forEachDistinctValue(fn2);
    }

    virtual size_t nonNullRowCount() const override
    {
        return unwrapped->nonNullRowCount();
    }

    ColumnTypes columnTypes;

    virtual ColumnTypes getColumnTypes() const
    {
        return columnTypes;
    }

    virtual std::string format() const
    {
        return "Sc";
    }

    virtual void serialize(StructuredSerializer & serializer) const
    {
        unwrapped->serialize(serializer);
    }
};

struct CompressedStringFrozenColumnFormat: public FrozenColumnFormat {
    
    virtual ~CompressedStringFrozenColumnFormat()
    {
    }

    virtual std::string format() const override
    {
        return "Sc";
    }

    struct CachedInfo {
        uint64_t totalStrings = 0;
        uint64_t totalBytes = 0;
        std::string dict;
    };

    virtual bool isFeasible(const TabularDatasetColumn & column,
                            const ColumnFreezeParameters & params,
                            std::shared_ptr<void> & cachedInfo) const override
    {
        auto info = std::make_shared<CachedInfo>();
        cachedInfo = info;

        for (auto & v: column.indexedVals) {
            if (v.isString()) {
                info->totalBytes += v.toStringLength();
                info->totalStrings += 1;
            }
        }

        return column.columnTypes.numStrings
            && column.columnTypes.onlyStringsAndNulls()
            && info->totalBytes > 50000000 // 50MB
            && info->totalBytes / info->totalStrings > 64;
    }

    virtual ssize_t columnSize(const TabularDatasetColumn & column,
                               const ColumnFreezeParameters & params,
                               ssize_t previousBest,
                               std::shared_ptr<void> & cachedInfo) const override
    {
        auto info = std::static_pointer_cast<CachedInfo>(cachedInfo);

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

        for (auto & v: column.indexedVals) {
            if (currentOffset > TOTAL_SAMPLE_SIZE)
                break;
            size_t sampleSize = v.toStringLength();
            sampleBuffer.append(v.toUtf8String().rawString());
            sampleSizes.push_back(sampleSize);
            currentOffset += sampleSize;
            valNumber += 1;
        }

        Date before = Date::now();

        // Perform the dictionary training
        size_t result = ZDICT_trainFromBuffer(&dictionary[0],
                                              dictionary.size(),
                                              sampleBuffer.data(),
                                              sampleSizes.data(),
                                              sampleSizes.size());
        if (ZDICT_isError(result)) {
            throw HttpReturnException(500, "Error with dictionary: "
                                      + string(ZDICT_getErrorName(result)));
        }
        
        Date after = Date::now();
        double elapsed = after.secondsSince(before);
        before = after;

        cerr << "created dictionary of " << result << " bytes from "
             << currentOffset << " bytes of samples in "
             << elapsed << " seconds" << endl;

        // Now compress another 1MB to see what the ratio is...
        std::shared_ptr<ZSTD_CDict> dict
            (ZSTD_createCDict(dictionary.data(), result, 1 /* compression level */),
             ZSTD_freeCDict);
        
        std::shared_ptr<ZSTD_CCtx> context
            (ZSTD_createCCtx(), ZSTD_freeCCtx);

        size_t compressedBytes = 0;
        size_t numSamples = 0;
        size_t uncompressedBytes = 0;
        for (size_t i = 0 /*valNumber*/;  i < column.indexedVals.size();  ++i) {
            const CellValue & v = column.indexedVals[i];
            size_t len = v.toStringLength();
            PossiblyDynamicBuffer<char, 65536> buf(ZSTD_compressBound(len));

            size_t result
                = ZSTD_compress_usingCDict(context.get(),
                                           buf.data(), buf.size(),
                                           v.stringChars(), len,
                                           dict.get());

            if (ZSTD_isError(result)) {
                throw HttpReturnException(500, "Error with compressing: "
                                          + string(ZSTD_getErrorName(result)));
            }

            uncompressedBytes += len;
            compressedBytes += result;
            numSamples += 1;

            //if (uncompressedBytes > 1024 * 1024)
            //    break;
        }

        after = Date::now();
        elapsed = after.secondsSince(before);

        cerr << "compressed " << numSamples << " samples with "
             << uncompressedBytes << " bytes to " << compressedBytes
             << " bytes at " << 100.0 * compressedBytes / uncompressedBytes
             << "% compression at "
             << numSamples / elapsed << "/s" << endl;

        ssize_t totalBytesRequired = sizeof(CompressedStringFrozenColumn)
            + dictionarySize
            + compressedBytes
            + 4 * column.indexedVals.size();

        cerr << "result is " << totalBytesRequired << endl;

        return result;
    }
    
    virtual FrozenColumn *
    freeze(TabularDatasetColumn & column,
           MappedSerializer & serializer,
           const ColumnFreezeParameters & params,
           std::shared_ptr<void> cachedInfo) const override
    {
        return new CompressedStringFrozenColumn(column, serializer, params);
    }

    virtual FrozenColumn *
    reconstitute(MappedReconstituter & reconstituter) const override
    {
        throw HttpReturnException(600, "Tabular reconstitution not finished");
    }
};

//static RegisterFrozenColumnFormatT<CompressedStringFrozenColumnFormat> regCompressedString;

} // namespace MLDB
