/* mapped_selector_table.h                                              -*- C++ -*-
   Jeremy Barnes, 6 November 2015
   Copyright (c) 2015 mldb.ai inc.  All rights reserved.

   This file is part of MLDB. Copyright 2015 mldb.ai inc. All rights reserved.
*/

#pragma once
#include "raw_mapped_int_table.h"

namespace MLDB {

struct SelectorTableStats: public IntTableStats<uint8_t> {
    SelectorTableStats() = default;
    template<typename Container>
    SelectorTableStats(const Container & input, std::enable_if_t<std::is_convertible_v<Container, std::span<const uint8_t>>> * = nullptr)
    {
        init(input);
    }

    void init(std::span<const uint8_t> vals);

    uint32_t maxCount = 0;

    std::vector<uint32_t> counts;
    IntTableStats<uint32_t> countStats;
};

// This is an int table that holds a set of "selectors", one per data point.  It can
// return:
// 1.  The value of the selector that contains a given data point;
// 2.  The count of points for this selector, so that each one can be stored contiguously
//
// It allows mixture models (where there are mutiple intermingled series) to be stored
// efficiently.  It allows access to the integral of the probability distribution function
// up to a given point.
//
// Only 256 different entries can be selected between using this data structure.

struct MappedSelectorTable {
    uint32_t maxSelector_:8 = 0;       // What is the highest value of the selector?
    uint32_t skippedSelector_:8 = 0;   // Which selector is skipped?   Only meaningful if skipSelector_ is true
    uint32_t countEveryNBits_:5 = 0;   // We store a cumulative count every 2^n of selector totals
    uint32_t skipSelector_:1 = 0;      // Do we skip a selector?
    uint32_t unused_:10 = 0;

    RawMappedIntTable selector_;      // For each entry, which selector is chosen
    RawMappedIntTable countEntries_;  // Sparse periodic table of cumulative selector counts to limit countValues range

    // Look up the selector for the given index.  This returns both the selector and the
    // number of times that this selector has been chosen earlier.
    std::pair<uint8_t, uint32_t> at(uint32_t index) const;

    size_t size() const;

    static size_t indirectBytesRequired(const SelectorTableStats & stats);
    static constexpr bool indirectBytesRequiredIsExact = false;
};

size_t mapped_selector_table_bytes_required(const SelectorTableStats & stats);

void freeze(MappingContext & context, MappedSelectorTable & output, const std::span<const uint8_t> & inputs);

} // namespace MLDB
