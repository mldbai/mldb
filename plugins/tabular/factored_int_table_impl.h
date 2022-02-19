/* factored_int_table_impl.h                                              -*- C++ -*-
   Jeremy Barnes, 6 November 2015
   Copyright (c) 2015 mldb.ai inc.  All rights reserved.

   This file is part of MLDB. Copyright 2015 mldb.ai inc. All rights reserved.
*/

#pragma once
#include "factored_int_table.h"
#include "factor_packing.h"
#include "mldb/ext/fastmod.h"
#include "mldb/utils/min_max.h"
#include "mldb/base/exc_assert.h"

namespace MLDB {

template<typename Base>
struct FactoredIntTableImplT: public Base {
    using Base::size;
    using Base::factor;
    using Base::numFactors;
    using Base::divModNumFactors;
    using Base::divModFactor32;
    using Base::divModFactor64;
    using Base::extractFactor32;
    using Base::extractFactor64;
    using Base::is64Bits;
    using Base::getWord32;
    using Base::getWord64;
    using Base::data;
    using value_type = typename Base::value_type;

    uint32_t at(uint32_t pos) const
    {
        if (pos >= size()) {
            MLDB_THROW_RANGE_ERROR("FactoredIntTableImpl");
        }
        if (numFactors() == 0) {
            return 0;
        }

        auto [ofs, idx] = divModNumFactors(pos);

        if (is64Bits()) {
            return extractFactor64(idx, getWord64(ofs));
        }
        else {
            return extractFactor32(idx, getWord32(ofs));
        }
    }

    uint32_t countValues(uint32_t startPos, uint32_t endPos, uint32_t value) const
    {
        if (startPos > endPos || endPos > size()) {
            MLDB_THROW_RANGE_ERROR("FactoredIntTableImpl");
        }

        uint32_t len = endPos - startPos;

        // Skip to the right place
        auto [word, ofs] = divModNumFactors(startPos);

        uint32_t result = 0;

        while (len > 0) {
            if (is64Bits()) {
                uint64_t val = getWord64(word++);

                // skip any at the start
                if (ofs > 0) {
                    val /= pow64(factor(), ofs);
                }

                // extract and verify each factor
                for (uint32_t i = ofs;  i < numFactors() && len > 0;  ++i, --len) {
                    auto [newVal, mod] = divModFactor64(val);
                    result += (mod == value);
                    val = newVal;
                }
            }
            else {
                uint32_t val = getWord32(word++);

                // skip any at the start
                if (ofs > 0) {
                    val /= pow32(factor(), ofs);
                }

                // extract and verify each factor
                for (uint32_t i = ofs;  i < numFactors() && len > 0;  ++i, --len) {
                    auto [newVal, mod] = divModFactor32(val);
                    result += (mod == value);
                    val = newVal;
                }
            }
            ofs = 0;
        }

        return result;
    }

    bool empty() const
    {
        return size() == 0;
    }

    using Iterator = IntTableIterator<const FactoredIntTableImplT>;

    Iterator begin() const { return { {0}, this}; }
    Iterator end() const { return { {size()}, this }; }
};

template<typename Base>
struct WritableFactoredIntTableImplT: public FactoredIntTableImplT<Base> {
    using Base::size;
    using Base::factor;
    using Base::numFactors;
    using Base::divModNumFactors;
    using Base::is64Bits;
    using Base::setWord32;
    using Base::setWord64;
    using Base::getWord32;
    using Base::getWord64;
    using Base::data;
    using value_type = typename Base::value_type;
    
    void set(uint32_t pos, uint32_t value)
    {
        //using namespace std;
        //cerr << "setting element " << pos << " to value " << value << endl;
        if (pos >= size()) {
            MLDB_THROW_RANGE_ERROR("WritableFactoredIntTableImpl out of range");
        }
        if (numFactors() == 0) {
            // Nothing to store, so just check the value is zero...
            if (value != 0) {
                MLDB_THROW_LOGIC_ERROR("WritableFactoredIntTableImpl non-zero in zero factor");
            }
            return;
        }
        if (value >= factor() && factor() != 0) {
            using namespace std;
            cerr << "value = " << value << endl;
            cerr << "factor() = " << factor() << endl;
            cerr << "numFactors() = " << numFactors() << endl;
            MLDB_THROW_LOGIC_ERROR("WritableFactoredIntTableImpl out of factor range");
        }

        auto [ofs, idx] = divModNumFactors(pos);

        if (is64Bits()) {
            setWord64(ofs, insert_factor64(factor(), idx, getWord64(ofs), value));
        }
        else {
            setWord32(ofs, insert_factor32(factor(), idx, getWord32(ofs), value));
        }

        if (this->at(pos) != value) {
            using namespace std;
            cerr << "this->at(pos) = " << this->at(pos) << endl;
            cerr << "value = " << value << endl;
            cerr << "pos = " << pos << endl;
            cerr << "is64Bits() = " << is64Bits() << endl;
            cerr << "factor() = " << factor() << endl;
            cerr << "numFactors() = " << numFactors() << endl;
        }
        ExcAssert(this->at(pos) == value);
    }

    template<typename GetValueFn>
    void set_range(GetValueFn && getValue, uint32_t pos = 0)
    {
        // todo: optimize as we can just do this with a series of multiplications...
        if (pos >= size()) {
            MLDB_THROW_RANGE_ERROR("WritableFactoredIntTableImpl set_range");
        }

        uint32_t value;
        bool more;
        std::tie(value, more) = getValue();
        static_assert(!std::is_same_v<decltype(getValue().first), bool>, "Mixed up return of getValue; first should be uint32_t, second bool");

        for (uint32_t i = 0;  more;  ++i) {
            if (pos + i >= size()) {
                MLDB_THROW_RANGE_ERROR("WritableFactoredIntTableImpl off the end");
            }

            set(pos + i, value);

            std::tie(value, more) = getValue();
        }
    }

    template<typename InputIterator>
    void set_range(InputIterator first, InputIterator last, uint32_t pos = 0)
    {
        auto getValue = [&] () -> std::pair<uint32_t, bool>
        {
            if (first == last) {
                return { 0, false };
            }
            return std::make_pair(*first++, true);
        };

        set_range(getValue, pos);
    }
};

extern template struct WritableFactoredIntTableImplT<FactoredIntTable>;


struct FactoredIntTableImpl: public WritableFactoredIntTableImplT<FactoredIntTable> {
    FactoredIntTableImpl() = default;
    
    void initialize(uint32_t size, uint32_t maxValue, bool allow64Bits)
    {
        auto [is64, factor, numFactorsPerWord, numBytesToAlloc] = factored_int_table_solution(size, maxValue, allow64Bits);

        this->is64_ = is64;
        this->factor_ = factor;
        this->numFactors_ = numFactorsPerWord;
        this->size_ = size;
        data_.resize(numBytesToAlloc / 4);
    }

    FactoredIntTableImpl(uint32_t size, uint32_t maxValue, bool allow64Bits = allow64BitFactors)
    {
        initialize(size, maxValue, allow64Bits);
    }
};

FactoredIntTable::FactoredIntTable(uint32_t size, uint32_t maxValue, bool allow64Bits)
{
    impl().initialize(size, maxValue, allow64Bits);
}

inline void FactoredIntTable::set_range(std::span<const uint32_t> vals, uint32_t startPos)
{
    impl().set_range(vals.begin(), vals.end(), startPos);
}

inline uint32_t FactoredIntTable::at(uint32_t pos) const
{
    return impl().at(pos);
}

inline uint32_t FactoredIntTable::countValues(uint32_t startPos, uint32_t endPos, uint32_t value) const
{
    return impl().countValues(startPos, endPos, value);
}

void FactoredIntTable::set(uint32_t pos, uint32_t value)
{
    impl().set(pos, value);
}

inline std::pair<uint32_t, uint32_t> FactoredIntTable::divModNumFactors(uint32_t val) const { return { val / numFactors(), val % numFactors() }; }
inline std::pair<uint32_t, uint32_t> FactoredIntTable::divModFactor32(uint32_t val) const { return { val / factor(), val % factor() }; }
inline std::pair<uint64_t, uint32_t> FactoredIntTable::divModFactor64(uint64_t val) const { return { val / factor(), val % factor() }; }
inline uint32_t FactoredIntTable::extractFactor32(uint32_t idx, uint32_t val) const { return MLDB::extract_factor32(factor(), idx, val); }
inline uint32_t FactoredIntTable::extractFactor64(uint32_t idx, uint64_t val) const { return MLDB::extract_factor64(factor(), idx, val); }

inline uint32_t FactoredIntTable::getWord32(size_t idx) const { return data_[idx]; }
inline uint64_t FactoredIntTable::getWord64(size_t idx) const
{
    // should be able to optimize to a single 64 bit load on little endian
    return (uint64_t)data_[idx * 2 + 1] << 32 | data_[idx * 2];
}

inline void FactoredIntTable::setWord32(size_t idx, uint32_t val)
{
    data_[idx] = val;
}

inline void FactoredIntTable::setWord64(size_t idx, uint64_t val)
{
    data_[idx * 2 + 1] = val >> 32;
    data_[idx * 2] = val;
    ExcAssert(getWord64(idx) == val);
}

extern template struct FactoredIntTableImplT<MappedFactoredIntTable>;

struct MappedFactoredIntTableImpl: public FactoredIntTableImplT<MappedFactoredIntTable> {
};

inline uint32_t MappedFactoredIntTable::at(uint32_t pos) const
{
    return impl().at(pos);
}

inline uint32_t MappedFactoredIntTable::countValues(uint32_t startPos, uint32_t endPos, uint32_t value) const
{
    return impl().countValues(startPos, endPos, value);
}

inline std::pair<uint32_t, uint32_t> MappedFactoredIntTable::divModNumFactors(uint32_t val) const { return { val / numFactors(), val % numFactors() }; }
inline std::pair<uint32_t, uint32_t> MappedFactoredIntTable::divModFactor32(uint32_t val) const { return { val / factor(), val % factor() }; }
inline std::pair<uint64_t, uint32_t> MappedFactoredIntTable::divModFactor64(uint64_t val) const { return { val / factor(), val % factor() }; }
inline uint32_t MappedFactoredIntTable::extractFactor32(uint32_t idx, uint32_t val) const { return MLDB::extract_factor32(factor(), idx, val); }
inline uint32_t MappedFactoredIntTable::extractFactor64(uint32_t idx, uint64_t val) const { return MLDB::extract_factor64(factor(), idx, val); }

inline uint32_t MappedFactoredIntTable::getWord32(size_t idx) const
{
    if (size_ < MappedFactoredIntTable::INTERNAL_OFFSET)
        return internal_[idx];
    return data_.get()[idx];
}
inline uint64_t MappedFactoredIntTable::getWord64(size_t idx) const
{
    // should be able to optimize to a single 64 bit load on little endian
    return (uint64_t)getWord32(idx * 2 + 1) << 32 | getWord32(idx * 2);
}

inline size_t MappedFactoredIntTable::indirectBytesRequired(const IntTableStats<uint32_t> & stats, bool allow64Bits)
{
    size_t numWords = factored_int_table_solution(stats.size, stats.maxValue, allow64Bits).bytesToAllocate / sizeof(uint32_t);
    if (numWords <= 1)
        return 0;
    return numWords * sizeof(uint32_t);
}

template<size_t InternalWords>
size_t InternalMappedFactoredIntTable<InternalWords>::indirectBytesRequired(const IntTableStats<uint32_t> & stats, bool allow64Bits)
{
    size_t numWords = factored_int_table_solution(stats.size, stats.maxValue, allow64Bits).bytesToAllocate / sizeof(uint32_t);
    if (numWords <= InternalWords + 1)
        return 0;
    return numWords * sizeof(uint32_t);
}

} // namespace MLDB
