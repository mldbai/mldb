/* run_length_int_table.h                                              -*- C++ -*-
   Jeremy Barnes, 6 November 2015
   Copyright (c) 2015 mldb.ai inc.  All rights reserved.

   This file is part of MLDB. Copyright 2015 mldb.ai inc. All rights reserved.
*/

#pragma once

#include "mmap.h"
#include "int_table.h"
#include "bit_compressed_int_table.h"
#include <vector>
#include <span>
#include <map>
#include "mldb/types/value_description_fwd.h"


namespace MLDB {

struct RunLengthIntTableImpl;
struct MappedRunLengthIntTableImpl;

struct RunLengthIntTable {
    std::vector<uint32_t> runs;  // Sorted position of beginning of run, with a zero at the start
    std::vector<uint32_t> values; // Value corresponding to each run

    using value_type = uint32_t;

    uint32_t size() const { return runs.back(); }
    bool empty() const { return size() == 0; }

    uint32_t at(uint32_t pos) const;
    uint32_t countValues(uint32_t startPos, uint32_t endPos, uint32_t value) const;

    using Iterator = IntTableIterator<const RunLengthIntTable>;

    Iterator begin() const { return { {0}, this}; }
    Iterator end() const { return { {size()}, this }; }

    const RunLengthIntTableImpl & impl() const { return reinterpret_cast<const RunLengthIntTableImpl &>(*this); }
};

struct MappedRunLengthIntTable {
    // The type bits are in runs, so we don't need an extra metadata word here
    MappedBitCompressedIntTable runs; // Sorted position of beginning of run, with a zero at the start
    MappedBitCompressedIntTable values; // Value corresponding to each run

    using value_type = uint32_t;

    uint32_t size() const { return runs.at(runs.size() - 1); }
    bool empty() const { return size() == 0; }

    uint32_t at(uint32_t pos) const;
    uint32_t countValues(uint32_t startPos, uint32_t endPos, uint32_t value) const;

    using Iterator = IntTableIterator<const MappedRunLengthIntTable>;

    Iterator begin() const { return { {0}, this}; }
    Iterator end() const { return { {size()}, this }; }

    const MappedRunLengthIntTableImpl & impl() const { return reinterpret_cast<const MappedRunLengthIntTableImpl &>(*this); }

    static size_t indirectBytesRequired(const IntTableStats<uint32_t> & stats);
    static constexpr bool indirectBytesRequiredIsExact = true;
};

void freeze(MappingContext & context, MappedRunLengthIntTable & output, const std::span<const uint32_t> & input);
void freeze(MappingContext & context, MappedRunLengthIntTable & output, const RunLengthIntTable & input);

size_t run_length_indirect_bytes(const IntTableStats<uint32_t> & stats);
RunLengthIntTable runLengthEncode(const std::span<const uint32_t> & values, bool validate = false);

DECLARE_STRUCTURE_DESCRIPTION(MappedRunLengthIntTable);

} // namespace MLDB
