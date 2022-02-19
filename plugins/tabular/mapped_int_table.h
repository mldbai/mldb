/* mapped_int_table.h                                              -*- C++ -*-
   Jeremy Barnes, 6 November 2015
   Copyright (c) 2015 mldb.ai inc.  All rights reserved.

   This file is part of MLDB. Copyright 2015 mldb.ai inc. All rights reserved.
*/

#pragma once

#include <iostream>
#include <cmath>
#include "mmap.h"
#include "raw_mapped_int_table.h"
#include "mapped_selector_table.h"
#include "predictor.h"
#include "mldb/utils/map_to.h"

namespace MLDB {

// Options to control tracing/debugging of the int table optimization
extern MappingOption<int> INT_TABLE_TRACE_LEVEL;  // 0 (default) = off, after that more and more info

// Our integer compression code needs to have a to_int function available to convert
// int-like things to real ints.  Here we implement a default version for all
// integral types.
template<typename T>
T to_int(T val, typename std::enable_if<std::is_integral_v<T>>::type * = 0)
{
    return val;
}

template<typename T>
T from_int(unsigned val, const T * = nullptr, typename std::enable_if<std::is_integral_v<T>>::type * = 0)
{
    return val;
}

struct MappedIntTableBase {
    MappedVector<Predictor> predictors_;  // the actual predictors

    struct Range {
        uint32_t isIndexed_:1;
        uint32_t unused:31;
        RawMappedIntTable activePredictors_;
        union {
            struct {
                MappedSelectorTable selectors_; // selects which of the predictors encodes it
                MappedVector<RawMappedIntTable> residuals_; // holds the residual after decoding, per selector
            } indexed;
            struct {
                RawMappedIntTable selectors_;
                RawMappedIntTable residuals_;
            } raw;
        };
        size_t numPoints() const { return isIndexed_ ? indexed.selectors_.size() : raw.selectors_.size(); }
    };

    RawMappedIntTable rangeStarts_;
    MappedVector<Range> ranges_;

    // Returns the raw uint32_t value at a given point, using several levels of indirection and a
    // prediction to reduce the storage required.  This function is logarithmic in the number of
    // ranges and outliers and possibly contiguous runs in the data, but should be effectively
    // linear (with a LARGE constant) in the number of values due to limitations on the previous
    // in the encoding process.
    uint32_t getRaw(uint32_t x) const;

    bool empty() const
    {
        return size() == 0;
    }

    uint32_t size() const
    {
        if (rangeStarts_.empty())
            return 0;
        return rangeStarts_.at(rangeStarts_.size() - 1);
    }

    static size_t indirectBytesRequired(const IntTableStats<uint32_t> & stats);
    static constexpr bool indirectBytesRequiredIsExact = false;
};

// Mapped version of a table of integers or integer-like objects.  The to_int and 
// from_int functions must be defined for this type for it to work.
template<typename T>
struct MappedIntTable: public MappedIntTableBase {

    using value_type = T;

    T at(uint32_t pos) const
    {
        return from_int(getRaw(pos), (T *)0);
    }

    using Iterator = IntTableIterator<const MappedIntTable>;

    Iterator begin() const { return { {0}, this }; }
    Iterator end() const { return { {size()}, this }; }
};

template<typename T>
std::ostream & operator << (std::ostream & stream, const MappedIntTable<T> & t)
{
    return stream << std::vector<T>(t.begin(), t.end());
}

void freeze(MappingContext & context, MappedIntTableBase & output, std::span<const uint32_t> input);

template <typename T0, typename Container>
void freeze(MappingContext & context, MappedIntTable<T0> & output, const Container & input)
{
    using T1 = std::remove_reference_t<decltype(*input.begin())>;

    if (input.empty()) {
        output = MappedIntTable<T0>{};
        return;
    }

    auto ints = mapTo<std::vector<uint32_t> >(input.begin(), input.end(), [] (T1 i) { return to_int(i); });

    freeze(context, (MappedIntTableBase &)output, ints);
}

} // namespace MLDB
