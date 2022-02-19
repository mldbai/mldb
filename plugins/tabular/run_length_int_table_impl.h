/* run_length_int_table_impl.h                                              -*- C++ -*-
   Jeremy Barnes, 6 November 2015
   Copyright (c) 2015 mldb.ai inc.  All rights reserved.

   This file is part of MLDB. Copyright 2015 mldb.ai inc. All rights reserved.
*/

#pragma once

#include "run_length_int_table.h"

namespace MLDB {

template<typename Base>
struct RunLengthIntTableImplT: public Base {
    using value_type = uint32_t;
    using Base::runs;
    using Base::values;

    uint32_t size() const
    {
        return runs.at(runs.size() - 1);
    }

    uint32_t getIndexForPosition(uint32_t pos, bool needValid) const
    {
        if (pos > size() - needValid)
            MLDB_THROW_RANGE_ERROR("RunLengthIntTable");
        uint32_t index = std::lower_bound(runs.begin(), runs.end(), pos) - runs.begin();
        return index - (pos < runs.at(index));
    }

    uint32_t at(uint32_t pos) const
    {
        return values.at(getIndexForPosition(pos, true));
    }

    uint32_t countValues(uint32_t pos, uint32_t endPos, uint32_t value) const
    {
        if (pos > endPos)
            MLDB_THROW_RANGE_ERROR("RunLengthIntTable");

        uint32_t length = endPos - pos;
        uint32_t index = getIndexForPosition(pos, false);

        pos -= runs.at(index);
        uint32_t result = 0;

        // For each run, see if its value matches and adjust, until we have no more
        // to count
        for (; length > 0; ++index) {
            auto runLength = std::min(length, runs.at(index + 1) - runs.at(index) - pos);
            result += values.at(index) == value ? runLength : 0;
            length -= runLength;
            pos = 0;
        }

        return result;
    }

    bool empty() const
    {
        return size() == 0;
    }

    using Iterator = IntTableIterator<const RunLengthIntTableImplT>;

    Iterator begin() const { return { {0}, this }; }
    Iterator end() const { return { {size()}, this }; }
};

struct RunLengthIntTableImpl: public RunLengthIntTableImplT<RunLengthIntTable> {
};

inline uint32_t RunLengthIntTable::at(uint32_t pos) const
{
    return impl().at(pos);
}

inline uint32_t RunLengthIntTable::countValues(uint32_t startPos, uint32_t endPos, uint32_t value) const
{
    return impl().countValues(startPos, endPos, value);
}

struct MappedRunLengthIntTableImpl: public RunLengthIntTableImplT<MappedRunLengthIntTable> {
};

inline uint32_t MappedRunLengthIntTable::at(uint32_t pos) const
{
    return impl().at(pos);
}

inline uint32_t MappedRunLengthIntTable::countValues(uint32_t startPos, uint32_t endPos, uint32_t value) const
{
    return impl().countValues(startPos, endPos, value);
}

inline size_t MappedRunLengthIntTable::indirectBytesRequired(const IntTableStats<uint32_t> & stats)
{
    return run_length_indirect_bytes(stats);
}

} // namespace MLDB

