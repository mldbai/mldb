/* bit_stream_writer.h                                                     -*- C++ -*-
   Jeremy Barnes, 23 March 2009
   Copyright (c) 2009 Jeremy Barnes.  All rights reserved.

   This file is part of MLDB. Copyright 2015 mldb.ai inc. All rights reserved.

   Operations for operating over a range of bits.
*/

#pragma once

namespace MLDB {

template<typename Container, typename Size = size_t>
struct BitStreamWriterT {
    using int_type = typename Container::value_type;
    static_assert(std::is_integral_v<int_type>, "Underlying type must be integral");
    static_assert(std::is_unsigned_v<int_type>, "Underlying type must be unsigned");
    static constexpr Size WIDTH = 8 * sizeof(int_type);

    // Writes n zeros followed by a one
    void write_n(Size n)
    {
        auto b = leftover_bits();
        if (b < n) {
            // Need to alloc some extra space
            auto num_bits_to_alloc = n - b;
            auto num_words_to_alloc = (num_bits_to_alloc-1) / WIDTH + 1;
            bits.resize(bits.size() + num_words_to_alloc, 0);
        }

        // At this point we're guaranteed to have enough bits to store it
        auto bit = len 

        if (b > n) {
            // the bits all fit
            Size x = len & MASK;
        }
        size_t num_words = 
        if (n >= b) {
            write_bits(0, b);
            n -= b;
            while (n >= WIDTH) {
                write_aligned_bits(0);
                n -= 64;
            }
        }

        write_zeros(n);
        write_one();
    }

    // Writes the given number of zero bits
    void write_zeros(Size num)
    {

    }

    // Writes a single one bit
    void write_one()
    {
        write_bits(1, 1);
    }

    // Writes the lowest nbits bits in val
    void write_bits(int_type val, uint8_t nbits)
    {
        if (nbits == 0)
            return;
        auto b = leftover_bits();
        if (b == 0) {
            bits.emplace_back(0);
            b = WIDTH;
        }
        Size n = std::min<Size>(b, nbits);
        Size x = len % WIDTH;
        int_type mask = (1 << n) - 1;
        bits.back() |= (val & mask) << x;
        len += n;

        if (nbits > n) {
            write_bits(val >> n, nbits - n);
        }
    }

    void write_aligned_bits(int_type val)
    {
        ExcAssertEqual(leftover_bits(), 0);
        bits.push_back(val);
    }

    Size leftover_bits() const { Size b = len % WIDTH;  return b ? WIDTH - b : 0; }

    Container bits;
    Size len = 0;
};

} // namespace MLDB
