/* min_max.h                                              -*- C++ -*-
   Jeremy Barnes, 6 November 2015
   Copyright (c) 2015 mldb.ai inc.  All rights reserved.

   This file is part of MLDB. Copyright 2015 mldb.ai inc. All rights reserved.

   Enables MIN_LIMIT and MAX_LIMIT which automatically take on the type of their context.
*/

#pragma once

#include <limits>
#include <iostream>

namespace MLDB {

static constexpr struct MaxValue {
    template<typename T> constexpr operator T () const { return std::numeric_limits<T>::max(); }
    template<typename T> constexpr bool operator == (const T & other) const
    {
        return std::numeric_limits<T>::max() == other;
    }
} MAX_LIMIT;

static constexpr struct MinValue {
    template<typename T> constexpr operator T () const { return std::numeric_limits<T>::min(); }
    template<typename T> constexpr bool operator == (const T & other) const
    {
        return std::numeric_limits<T>::min() == other;
    }
} MIN_LIMIT;

// Accumulate basic bound information about a series (minimum and maximum values, at which
// indexes they occur, whether they are unique or there are multiple values of them).
template<typename Value, typename Index = size_t>
struct MinMaxIndex {
    size_t count = 0;
    static constexpr Index NO_INDEX = MAX_LIMIT;
    Index minIndex = NO_INDEX;
    Index maxIndex = NO_INDEX;
    Value minValue = MAX_LIMIT;
    Value maxValue = MIN_LIMIT;
    size_t numMinValues = 0;
    size_t numMaxValues = 0;

    void clear()
    {
        *this = MinMaxIndex();
    }

    Value range() const
    {
        if (count == 0) return 0;
        return maxValue - minValue;
    }

    void operator () (Value value)
    {
        return operator () (value, count);
    }

    void operator () (Value value, Index index)
    {
        if (value < minValue || count == 0) {
            minValue = value;
            minIndex = index;
            numMinValues = 1;
        }
        else if (value == minValue) {
            ++numMinValues;
        }

        if (value > maxValue || count == 0) {
            maxValue = value;
            maxIndex = index;
            numMaxValues = 1;
        }
        else if (value == maxValue) {
            ++numMaxValues;
        }
        ++count;
    }
};

template<typename Value, typename Size>
inline std::ostream & operator << (std::ostream & stream, const MinMaxIndex<Value, Size> & mmi)
{
    stream << "n=" << mmi.count << " ";
    if (mmi.count > 0) {
        stream << " " << mmi.minValue << "(@" << mmi.minIndex;
        if (mmi.numMinValues > 1) {
            stream << "*" << mmi.numMinValues;
        }
        stream << ")-" << mmi.maxValue << "(@" << mmi.maxIndex;
        if (mmi.numMaxValues > 1) {
            stream << "*" << mmi.numMaxValues;
        }
        stream << ")";
    }
    return stream;
}

} // namespace MLDB
