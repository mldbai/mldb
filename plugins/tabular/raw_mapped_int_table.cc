/* raw_mapped_int_table.cc                                              -*- C++ -*-
   Jeremy Barnes, 6 November 2015
   Copyright (c) 2015 mldb.ai inc.  All rights reserved.

   This file is part of MLDB. Copyright 2015 mldb.ai inc. All rights reserved.
*/

#include "raw_mapped_int_table.h"
#include "mldb/vfs/compressibility.h"
#include "mldb/utils/ostream_span.h"

using namespace std;

namespace MLDB {

uint32_t RawMappedIntTable::at(uint32_t pos) const
{
    switch (type_) {
        case BIT_COMPRESSED:
            return bitcmp_.at(pos);
        case RLE:
            return rle_.at(pos);
        case FACTORED:
            return factored_.at(pos);
        default:
            MLDB_THROW_LOGIC_ERROR("RawMappedIntTable::at()");
    }
}

uint32_t RawMappedIntTable::countValues(uint32_t startPos, uint32_t endPos, uint32_t value) const
{
    switch (type_) {
        case BIT_COMPRESSED:
            return bitcmp_.countValues(startPos, endPos, value);
        case RLE:
            return rle_.countValues(startPos, endPos, value);
        case FACTORED:
            return factored_.countValues(startPos, endPos, value);
        default:
            MLDB_THROW_LOGIC_ERROR("RawMappedIntTable::at()");
    }
}

uint32_t RawMappedIntTable::size() const
{
    switch (type_) {
        case BIT_COMPRESSED:
            return bitcmp_.size();
        case RLE:
            return rle_.size();
        case FACTORED:
            return factored_.size();
        default:
            using namespace std;
            cerr << "type = " << type_ << endl;
            cerr << "address = " << this << endl;
            MLDB_THROW_LOGIC_ERROR("RawMappedIntTable::size()");
    }
}

static_assert(sizeof(RawMappedIntTable) == 16);

size_t RawMappedIntTable::indirectBytesRequired(const IntTableStats<uint32_t> & stats, bool allow64Bits)
{
    return raw_mapped_indirect_bytes(stats, allow64Bits);
}

size_t raw_mapped_indirect_bytes(const IntTableStats<uint32_t> & stats, bool allow64Bits)
{
    return useTableOfType(stats, allow64Bits).second;
}

size_t raw_mapped_indirect_bytes(size_t size, uint32_t maxValue, uint32_t numRuns, bool allow64Bits)
{
    IntTableStats<uint32_t> stats = {size, 0, maxValue, numRuns, 0 /* maxRunLength */};
    return raw_mapped_indirect_bytes(stats, allow64Bits);
}

size_t raw_mapped_indirect_bytes(size_t size, uint32_t maxValue, bool allow64Bits)
{
    return raw_mapped_indirect_bytes(size, maxValue, size, allow64Bits);
}

std::pair<IntTableType, size_t> useTableOfType(const IntTableStats<uint32_t> & stats, bool allow64Bits)
{
    if (stats.size == 0)
        return { BIT_COMPRESSED, 0 };
    size_t bytes_bit_compressed = decltype(std::declval<RawMappedIntTable>().bitcmp_)::indirectBytesRequired(stats);
    size_t bytes_rle_encoded = decltype(std::declval<RawMappedIntTable>().rle_)::indirectBytesRequired(stats);
    size_t bytes_factored = decltype(std::declval<RawMappedIntTable>().factored_)::indirectBytesRequired(stats, allow64Bits);
    size_t minBytes = std::min(std::min(bytes_bit_compressed, bytes_rle_encoded), bytes_factored);

    if (bytes_bit_compressed == minBytes)
        return { BIT_COMPRESSED, bytes_bit_compressed };
    else if (bytes_factored == minBytes)
        return { FACTORED, bytes_factored };
    else return { RLE, bytes_rle_encoded };
}

void freeze(MappingContext & context, RawMappedIntTable & output, std::span<const uint32_t> input)
{
    auto start = context.getOffset();

    //cerr << "freezing " << input.size() << " values to " << &output << endl;
    output = RawMappedIntTable();
    ExcAssert(output.type_ == 0);

    bool allow64Bits = context.getOption(ALLOW_64_BIT_FACTORS);
    bool trace = context.getOption(TRACE);
    //trace = true;

    auto [type, size] = useTableOfType(input, allow64Bits);
    switch (type) {
        case BIT_COMPRESSED: {
            if (trace)
                cerr << "  *** choosing bit compressed" << endl;
            auto packedResiduals = bit_compressed_encode(input);
            freeze_field(context, "bitcmp", output.bitcmp_, packedResiduals);
            output.type_ = BIT_COMPRESSED;
            break;
        }
        case RLE: {
            if (trace)
                cerr << "  *** choosing RLE" << endl;
            auto rleResiduals = runLengthEncode(input);
            freeze_field(context, "rle", output.rle_, rleResiduals);
            output.type_ = RLE;
            break;
        }
        case FACTORED: {
            if (trace)
                cerr << "  *** choosing Factored" << endl;
            auto factoredResiduals = factored_encode(input);
            freeze_field(context, "factored", output.factored_, factoredResiduals);
            output.type_ = FACTORED;
            break;
        }
        default:
            MLDB_THROW_LOGIC_ERROR("unknown int table type");
    }

    bool validate = context.getOption(VALIDATE);
    ExcAssert(output.size() == input.size());
    for (size_t i = 0;  i < input.size() && validate;  ++i) {
        ExcAssert(output.at(i) == input[i]);
    }
    if (context.getOffset() - start != size) {
        cerr << "raw mapped bytes wrong: expected " << size << " vs actual " << context.getOffset() - start << endl;
        cerr << input << endl;
        cerr << "type = " << type << endl;
    }
    ExcAssert(context.getOffset() - start == size);

    if (trace) {
        auto end = context.getOffset();

        static size_t totalRawBytes = 0;
        static size_t numRawTables = 0;
        totalRawBytes += (end - start);

        auto [bytesUncompressed, bytesCompressed, compressionRatio]
            = calc_compressibility((const char *)context.getMemory() + start, end - start);

        cerr << "freeze Raw #" << ++numRawTables << " at " << hex << start << " to " << end << dec << " with "
            << output.size() << " values in " << (end - start)
            << " bytes type " << type << " at "
            << 8.0 * (end - start) / output.size() << " bits/entry; total " << totalRawBytes << endl;
        switch (type) {
            case BIT_COMPRESSED: {
                cerr << "    bit compressed with " << (int)output.bitcmp_.width() << " bit width" << endl;
                break;
            }
            case RLE: {
                cerr << "    rle with " << output.rle_.runs.size() << " runs and " << (int)output.rle_.values.width() << " bits/value " << endl;
                break;
            }
            case FACTORED: {
                cerr << "    factored with  " << output.factored_.factor() << " factor" << endl;
                break;
            }
            default:
                MLDB_THROW_LOGIC_ERROR("unknown int table type");
        }
    
        cerr << "compressed: " << bytesCompressed << " ratio " << compressionRatio << endl;
        if (compressionRatio < 0.8) {
            cerr << "   ";
            for (size_t i = 0;  i < 100 && i < output.size();  ++i) {
                cerr << " " << input[i];
            }
            if (output.size() < 100)
                cerr << "...";
            cerr << endl;
        }
    }
}

void freeze(MappingContext & context, RawMappedIntTable & output, std::span<const uint16_t> input)
{
    freeze(context, output, std::vector<uint32_t>(input.begin(), input.end()));
}

void freeze(MappingContext & context, RawMappedIntTable & output, std::span<const uint8_t> input)
{
    freeze(context, output, std::vector<uint32_t>(input.begin(), input.end()));
}

} // namespace MLDB
