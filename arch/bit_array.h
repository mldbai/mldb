/* bit_range_ops.h                                                 -*- C++ -*-
   Jeremy Barnes, 23 March 2009
   Copyright (c) 2009 Jeremy Barnes.  All rights reserved.

   This file is part of MLDB. Copyright 2015 mldb.ai inc. All rights reserved.

   Operations for operating over a range of bits.
*/

#pragma once

#include <limits>
#include <vector>
#include <cstdint>
#include <functional>
#include "bit_range_ops.h"

#include <iostream>
using namespace std;

namespace MLDB {

template<typename T>
struct BitArray {

    typedef uint16_t shift_t;
    typedef std::size_t size_t;

    BitArray(shift_t numBits)
    {
        init(numBits);
    }
    
    static constexpr shift_t TBITS = sizeof(T) * 8;

    static constexpr T createMask(int bits)
    {
        return bits == TBITS ? (T)-1 : ((T)1 << bits) - 1;
    }

    T createMask() const
    {
        return createMask(numBits);
    }
    
    struct State {
        T mask0;
        T mask1;
        shift_t shift0;
        shift_t shift1;
        //shift_t shift2;
        shift_t inc;
        shift_t next;

        T readUnmasked(const T * data) const
        {
            return shrd(data[0], data[1], shift0);

            //return
            //    ((data[0] >> shift0) & mask0)
            //    | ((data[1] & mask1) << shift1);
        }
    };

    shift_t numBits;
    
    std::vector<State> states;

    T resultMask;
    
    struct Reader {
        Reader(const BitArray * owner,
               const T * data)
            : numBits(owner->numBits), data(data), owner(owner)
        {
        }

        int cur = 0;
        int numBits = 0;
        const T * data;
        const BitArray * owner;
        
        MLDB_ALWAYS_INLINE T readUnmasked()
        {
            T result = shrd(data[0], data[1], cur);
            cur += numBits;
            data += cur / TBITS;
            cur %= TBITS;
            return result;
        }

        MLDB_ALWAYS_INLINE T read()
        {
            return readUnmasked() & owner->resultMask;
        }
    };

    struct Writer {
        Writer(const BitArray * owner,
               T * data)
            : data(data), toWrite(0), owner(owner)
        {
        }
        
        int cur = 0;
        T * data;
        T toWrite;
        const BitArray * owner;

        void write(T val)
        {
            const State & state = owner->states[cur];
            T mask0 = state.mask0;//createMask(state.shift1);
            T mask1 = state.mask1;//createMask(state.shift2);
            toWrite |= (val & mask0) << state.shift0;
            if (state.inc) {
                data[0] = toWrite;
                ++data;
                if (state.shift1 == sizeof(T) * 8) {
                    toWrite = 0;
                }
                else {
                    toWrite = (val >> state.shift1) & mask1;
                }
            }
            cur = state.next;
        }

        void finish()
        {
            data[0] = toWrite;
        }
    };

    void init(size_t bitWidth)
    {
        numBits = bitWidth;
        states.clear();
        shift_t currentShift = 0;
        resultMask = createMask(bitWidth);
        do {
            // Extract or write bitWidth bits from currentShift of the
            // 2 * TBITS bits in the two words

            shift_t firstWordBits
                = std::min<shift_t>(TBITS - currentShift, bitWidth);
            shift_t secondWordBits = bitWidth - firstWordBits;

            State state;
            state.shift0 = currentShift;
            state.shift1 = firstWordBits;
            //state.shift2 = secondWordBits;
            state.mask0 = createMask(firstWordBits);
            state.mask1 = createMask(secondWordBits);
            state.inc = firstWordBits + currentShift == TBITS;

            currentShift = (currentShift + bitWidth) % TBITS;

            state.next = (currentShift == 0 ? 0 : states.size() + 1);
            
            states.push_back(state);
            
        } while (currentShift != 0);
    }

    Reader getReader(const T * data)
    {
        return Reader(this, data);
    }

    Writer getWriter(T * data)
    {
        data[0] = 0;
        return Writer(this, data);
    }

    size_t numWordsToAllocate(size_t numEntries) const
    {
        // Return enough to hold the array, plus an extra guard word
        return ((numEntries * numBits) + TBITS - 1) / TBITS + 1;
    }
};


} // namespace MLDB
