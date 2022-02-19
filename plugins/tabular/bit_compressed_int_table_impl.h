/* bit_compressed_int_table_impl.h                                              -*- C++ -*-
   Jeremy Barnes, 6 November 2015
   Copyright (c) 2015 mldb.ai inc.  All rights reserved.

   This file is part of MLDB. Copyright 2015 mldb.ai inc. All rights reserved.
*/


#pragma once

#include "mldb/arch/bit_range_ops.h"
#include "factor_packing.h" // bits
#include "mmap.h"
#include <bit>
#include <vector>

namespace MLDB {

template<typename WordT = uint32_t>
size_t constexpr bit_compressed_indirect_bytes(size_t totalLength, uint32_t maxValue)
{
    constexpr size_t WORD_BITS = sizeof(WordT) * 8;
    int bitsPerValue = bits(maxValue);
    uint64_t totalBits = (uint64_t)totalLength * bitsPerValue;
    size_t totalWords = (totalBits + WORD_BITS - 1) / WORD_BITS;
    return totalWords * sizeof(WordT);
}

inline uint32_t MappedBitCompressedIntTable::getWord(size_t n) const
{
    if (size_ < INTERNAL_OFFSET)
        return internal_[n];
    else return data_.get()[n];
}

template<typename Base>
struct BitCompressedIntTableImplT: public Base {
    using Base::size;
    using Base::width;
    using Base::getWord;
    using value_type = typename Base::value_type;

    BitCompressedIntTableImplT() = default;

    /// Extracts bits starting from the least-significant bits of the buffer.
    uint32_t extract(uint64_t offset /* number of BITS */, shift_t bits) const
    {
        size_t bit_ofs = offset % (8 * sizeof(uint32_t));
        size_t word = offset / (8 * sizeof(uint32_t));

        if (MLDB_UNLIKELY(bits <= 0)) return 0;

        uint32_t result;
        if (bit_ofs + bits <= (8 * sizeof(uint32_t)))
            result = extract_bit_range<uint32_t>(getWord(word), 0, bit_ofs, bits);
        else
            result
                = extract_bit_range<uint32_t>(getWord(word), getWord(word + 1), bit_ofs, bits);
        return result;
    }

#if 0
    /// Increase the cursor in the bit buffer. For performance reasons, no
    /// bound checking is performed and it is up to the caller to ensure that
    /// the sum of the current offset and the "bits" parameter is within
    /// [0, sizeof(buffer)[.
    void advance(ssize_t bits)
    {
        bit_ofs += bits;
        data += (bit_ofs / (sizeof(Data) * 8));
        bit_ofs %= sizeof(Data) * 8;
    }
#endif

    uint32_t at(uint32_t pos) const
    {
        if (pos >= size()) {
            using namespace std;
            cerr << "pos = " << pos << endl;
            cerr << "size() = " << size() << endl;
            MLDB_THROW_RANGE_ERROR("BitCompressedIntTableImpl::at()");
        }
        //using namespace std;
        //cerr << "extracting " << (int)width() << " bits at " << pos << endl;
        if (width() == 32)
            return getWord(pos);
        else if (width() == 0)
            return 0;

        return extract(width() * pos, width());
    }

    uint32_t countValues(uint32_t startPos, uint32_t endPos, uint32_t value) const
    {
        if (endPos < startPos || endPos > size()) {
            MLDB_THROW_RANGE_ERROR("BitCompressedIntTableImpl::countValues()");
        }
        if (width() != 32 && value > 1U << width()) {
            return 0;
        }

        uint32_t result = 0;
        switch (width()) {
            case 0:
                return endPos - startPos;
            //case 1:  // todo population count
            //case 8:  // todo 8 bit access
            //case 16: // todo 16 bit access
            case 32: {
                for (uint32_t pos = startPos;  pos < endPos;  ++pos) {
                    result += getWord(pos) == value;
                }
                break;
            }
            default: {
                for (uint32_t pos = startPos;  pos < endPos;  ++pos) {
                    result += extract(pos * width(), width()) == value;
                }
                break;
            }
        }
        return result;
    }

    bool empty() const
    {
        return size() == 0;
    }

    using Iterator = IntTableIterator<const BitCompressedIntTableImplT>;

    Iterator begin() const { return { {0}, this}; }
    Iterator end() const { return { {size()}, this }; }
};

template<typename Base>
struct WritableBitCompressedIntTableImplT: public BitCompressedIntTableImplT<Base> {
    using Base::size;
    using Base::width;
    using Base::data;
    using Base::setWord;
    using value_type = typename Base::value_type;
    
    void set(uint32_t pos, uint32_t value)
    {
        if (pos >= size()) {
            MLDB_THROW_RANGE_ERROR("WritableBitCompressedIntTableImpl::set()");
        }
        if (width() == 32) {
            setWord(pos, value);
            return;
        }

        // TODO: use setWord() instead
        Bit_Writer<uint32_t> writer(data());
        writer.skip(width() * pos);
        writer.write(value, width());
    }

    template<typename GetValueFn>
    void set_range(GetValueFn && getValue, uint32_t pos = 0)
    {
        //using namespace std;
        //cerr << "setting " << pos << " of " << size() << endl;
        if (pos >= size()) {
            MLDB_THROW_RANGE_ERROR("WritableBitCompressedIntTableImpl::set_range()");
        }
        Bit_Writer<uint32_t> writer(data());
        writer.skip(width() * pos);
        uint32_t value;
        bool more;
        std::tie(value, more) = getValue();
        static_assert(!std::is_same_v<decltype(getValue().first), bool>, "Mixed up return of getValue; first should be uint32_t, second bool");
        for (uint32_t i = 0;  more;  ++i) {
            //using namespace std;
            //cerr << "setting " << pos + i << " of " << size() << " with " << (int)width() << " bits to " << value << endl;
            if (pos >= size()) {
                MLDB_THROW_RANGE_ERROR("WritableBitCompressedIntTableImpl::set_range() too many elements");
            }
            if (width() == 32) {
                data()[pos + i] = value;
            }
            else {
                writer.write(value, width());
            }
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

extern template struct WritableBitCompressedIntTableImplT<BitCompressedIntTable>;

struct BitCompressedIntTableImpl: public WritableBitCompressedIntTableImplT<BitCompressedIntTable> {    
    
    void initialize(uint32_t size, int width)
    {
        ExcAssert(width >= 0 && width <= 32);
        this->size_ = size;
        this->width_ = width;
        this->data_.resize((size * width + 31) / 32);
    }
};

struct MappedBitCompressedIntTableImpl: public BitCompressedIntTableImplT<MappedBitCompressedIntTable> {
};

extern template struct BitCompressedIntTableImplT<MappedBitCompressedIntTable>;

} // namespace MLDB
