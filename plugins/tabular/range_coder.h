#pragma once

//Entropy Coding Source code
//By Sachin Garg, 2006
//
//Includes range coder based upon the carry-less implementation 
//by Dmitry Subbotin, and arithmetic coder based upon Mark Nelson's
//DDJ code.
// 
//Modified to use 64-bit variables for improved performance.
//32-bit reference implementations also included.
//
//For details:
//http://www.sachingarg.com/compression/entropy_coding/64bit
//
//
// Redistribution and use in source and binary forms, with or without modification, are permitted provided that the following conditions are met:
//
//   1. Redistributions of source code must retain the above copyright notice, this list of conditions and the following disclaimer.
//   2. Redistributions in binary form must reproduce the above copyright notice, this list of conditions and the following disclaimer in the documentation and/or other materials provided with the distribution.
//   3. The name of the author may not be used to endorse or promote products derived from this software without specific prior written permission.
//
// THIS SOFTWARE IS PROVIDED BY THE AUTHOR ''AS IS'' AND ANY EXPRESS OR IMPLIED WARRANTIES, INCLUDING, BUT not LIMITED TO, THE IMPLIED WARRANTIES OF MERCHANTABILITY AND FITNESS FOR A PARTICULAR PURPOSE ARE DISCLAIMED. IN false EVENT SHALL THE AUTHOR BE LIABLE FOR ANY DIRECT, INDIRECT, INCIDENTAL, SPECIAL, EXEMPLARY, OR CONSEQUENTIAL DAMAGES (INCLUDING, BUT not LIMITED TO, PROCUREMENT OF SUBSTITUTE GOODS OR SERVICES; LOSS OF USE, DATA, OR PROFITS; OR BUSINESS INTERRUPTION) HOWEVER CAUSED AND ON ANY THEORY OF LIABILITY, WHETHER IN CONTRACT, STRICT LIABILITY, OR TORT (INCLUDING NEGLIGENCE OR OTHERWISE) ARISING IN ANY WAY OUT OF THE USE OF THIS SOFTWARE, EVEN IF ADVISED OF THE POSSIBILITY OF SUCH DAMAGE.#include "stdx/define.h"

/*	Code for range coding, derived from public domain work by Dmitry Subbotin
    Modified to use 64-bit integer maths, for increased precision

    note :	Cannot be used at full 'capacity' as the interface still takes uint32_t parameters (not uint64_t)
            This is done to maintain uniformity in interface across all entropy coders, feel free to 
            change this.
    
    author : Sachin Garg
*/

//Entropy Coding Source code
//By Sachin Garg, 2006
//
//Includes range coder based upon the carry-less implementation 
//by Dmitry Subbotin, and arithmetic coder based upon Mark Nelson's
//DDJ code.
// 
//Modified to use 64-bit variables for improved performance.
//32-bit reference implementations also included.
//
//For details:
//http://www.sachingarg.com/compression/entropy_coding/64bit


#include <iostream>
#include <cstdint>
#include <functional>
#include "mldb/base/exc_assert.h"

struct RangeCoder64 {
    static constexpr uint64_t Top = 1ULL << 56;
    static constexpr uint64_t Bottom = 1ULL << 48;
    static constexpr uint64_t MaxRange = Bottom;

    uint64_t Low = 0;
    // When Range is zero, it means 2^64
    uint64_t Range = 0;  //std::numeric_limits<uint64_t>::max();

    void reduceRange(uint32_t TotalRange)
    {
        using namespace std;
        //cerr << "reduceRange: before Low " << hex << Low << " Range " << Range << " totalRange " << TotalRange << dec << endl;
        if (Low == 0 && Range == 0) {
            // Initial state; range is actually 2^64 not zero...
            // First divide into 2^63, then shift and finally adjust the last digit
            Range = (((1ULL) << 63) / TotalRange) << 1;

            //cerr << "fixup: " << hex << ((Range + 1) * TotalRange) << endl;

            // If multiplication doesn't overflow or overflows to zero, we know that Range + 1 is a better
            // approximation than Range to 2^64 / TotalRange
            if (((Range + 1) * TotalRange) == 0 || ((Range + 1) * TotalRange) > Range)
                Range += 1; 
        }
        else {
            Range /= TotalRange;
        }
        //cerr << "reduceRange: after Low " << hex << Low << " Range " << Range << dec << endl;
    }
};

struct RangeEncoder64: public RangeCoder64 {

    template<typename WriteFn>
    void updateRange(WriteFn && write, uint32_t SymbolLow,uint32_t SymbolHigh,uint32_t TotalRange)
    {
        using namespace std;
        //cerr << "before: " << hex << Low << " to " << Low + Range << endl;
        reduceRange(TotalRange);
        Low += SymbolLow*Range;
        Range *= (SymbolHigh-SymbolLow);
        //cerr << "after: " << hex << Low << " to " << Low + Range << endl;
        
        while ((Low ^ (Low+Range))<Top || Range<Bottom && ((Range= -Low & (Bottom-1)),1)) {
            //cerr << "writing char " << std::hex << (Low >> 56) << " with range up to " << ((Low + Range)>>56) << std::dec << endl;
            write(Low>>56);

            Range<<=8;
            Low<<=8;
        }
    }
    
    template<typename WriteFn>
    void flush(WriteFn && write, bool debug = false)
    {
        flushEof(write, 1, 1, debug);
    }

    // Flush the currently accumulated output.  This will ensure that the EOF can be detected by
    // the decoder.  The EOF condition is:
    // EITHER the range starts and ends unambiguously in the range [0-eofHigh),
    // OR     the same character could have been decoded by writing a lower point, but the one
    //        in the stream was purposefully made higher 
    template<typename WriteFn>
    void flushEof(WriteFn && write, uint32_t eofHigh, uint32_t maxRange, bool debug = false)
    {
        using namespace std;
        //debug = true;

        uint64_t Eof = 0;

        if (debug)
            std::cerr << "flushing EOF ..." << hex << eofHigh << " " << maxRange << dec << std::endl;
        for(int_fast32_t i=0;i<8;i++) {
            using namespace std;
            int cLow = Low >> 56;
            int cHigh = (Low + Range) >> 56;

            if (Low + Range == 0) {
                // if Low + Range == 2^64, we are using the full range
                cHigh = 256;
            }

            if (debug)
                cerr << std::hex << "i = " << i << " low = " << Low << " high = " << Low + Range << " Range " << Range << " Eof " << Eof << " cLow " << cLow << " cHigh " << cHigh << std::dec << endl;

            // Check that what's left over is unambiguously in the eof range from 0 to EofHigh.
            // If not, write another byte.
            if (debug) {
                cerr << std::hex << "cLow = " << cLow << " cHigh = " << cHigh << endl;
                cerr << hex << "rangeLow " << (((Low >> 32) * maxRange) >> 32)  << dec << endl;
                cerr << hex << "rangeHigh " << ((((Low + Range) >> 32) * maxRange) >> 32) << dec << endl;
            }

            // Character to write, if it can be the last one
            int toWrite = -1;

            // If our low has all trailing zeros, we can just write it.  We fit right into the
            // lowest bits.
            if (Low << 8 == 0 && cLow < cHigh) {
                toWrite = cLow; // not unambigous for the decoder
            }
            // Otherwise, if our high does not have all trailing zeros, we can just write it
            else if (cLow == cHigh - 1 && cHigh != 256 && (Low + Range) << 8 > 0) {
                toWrite = cHigh;
            } 
            // Otherwise, if there is a character in between them then just write it
            else if (cLow < cHigh - 1) {
                toWrite = cLow + 1;
            }

            if (toWrite != -1) {
                // Is there a possibility to avoid writing this byte?
                // It must be detectable from the bytes before


                // Look at where we are compared to low
                uint64_t pos1 = ((uint64_t)toWrite << 56);
                uint64_t pos2 = pos1 - Low;
                uint64_t pos3 = pos2 >> 32;
                uint64_t pos4 = Range >> 32;
                uint64_t pos5 = pos3 * maxRange;
                uint64_t pos6 = pos5 / pos4;

                if (debug) {
                    cerr << hex << " pos1 " << pos1 << " pos2 " << pos2 << " pos3 " << pos3 << " pos4 " << pos4 << " pos5 " << pos5 << " pos6 " << pos6 << dec << endl;
                }

                uint32_t position = pos6;//((((((uint64_t)toWrite << 56) - Low) >> 32) * maxRange / (Range >> 32)) >> 32);

                if (debug) {
                    //cerr << "toWrite - Low = " << hex << (((uint64_t)toWrite << 56) - Low) << dec << endl;
                    //cerr << "Range = " << hex << Range << dec << endl;
                    //cerr << "toWrite - Low * maxRange = " << hex << ((((((uint64_t)toWrite << 56) - Low) >> 32) * maxRange) >> 32) << dec << endl;
                    cerr << "position = " << hex << position << " of " << maxRange << dec << endl;
                }

                //position = 0;

                if (position < eofHigh || maxRange == 1) {
                    //cerr << "no extra byte needed as position is " << position << endl;
                    if (debug)
                        cerr << "cHigh - toWrite = " << cHigh - toWrite << endl;
                    write(toWrite);
                    break;
                }
                //cerr << "Extra byte needed as position is " << position << " of " << eofHigh << " cLow " << cLow << " toWrite " << toWrite << " cHigh " << cHigh << endl;
                //if (toWrite < cHigh - 1) cerr << "**** could save a byte" << endl;
            }
            write(cLow);
            Low<<=8;
            Range<<=8;

            if (Low + Range < Low) {
                //cerr << "**** fixup range" << endl;
                Range = -Low;  // we want the full range, which is represented as Low + Range = 0
            }

            if (Range == 0)
                break;
        }
    }

    // WriteFn : void write(char c): write the given character
    // EncodeRangeFn: tuple<uint32_t, uint32_t, uint32_t> encodeRange(int c): get the current range for
    // character c, possibly updating it.  Return the low, high and maxRange.  Should also be
    // callable for EOF.
    template<typename WriteFn, typename EncodeRangeFn>
    void encodeOne(WriteFn && write, int c,
                const EncodeRangeFn & encodeRange,
                bool debug = false)
    {
        auto [low, high, maxRange] = encodeRange(c);
        if (debug)
            std::cerr << "encoding char " << std::hex << (int)c << " into range " << low << " -> " << high << " of " << maxRange << std::dec << std::endl;
        ExcAssert(low < high);
        ExcAssert(low >= 0);
        ExcAssert(high <= maxRange);

        updateRange(write, low, high, maxRange);
        if (debug)
            std::cerr << "current code range is " << std::hex << Low << " -> " << Low + Range << std::dec << std::endl;
    }
};

struct RangeDecoder64: public RangeCoder64 {
    uint64_t Code = 0;
    uint64_t Eof = 0;

    template<typename ReadFn>
    void start(ReadFn && read, bool debug = false)
    {
        using namespace std;
        for(int_fast32_t i=0;i<8;i++) {
            int c = read();
            bool eof = c == EOF;
            if (eof) {
                c = 0;
            }
            if (debug)
                cerr << "read char " << hex << c << " eof " << eof << dec << endl;
            Code = (Code << 8) | c;
            Eof = (Eof << 8) | (eof ? 0xff : 0);
        }

        if (debug)
            std::cerr << "start(): Code is " << std::hex << Code << std::dec << std::endl;
    }

    std::tuple<uint32_t, uint32_t> getCurrentCount(uint32_t TotalRange, bool debug = false)
    {
        using namespace std;
        if (debug)
            cerr << "getCurrentCount: Code " << hex << Code << " Low " << Low << " Range " << Range << " Eof " << Eof << dec << endl;
        reduceRange(TotalRange);
        if (debug)
            cerr << "getCurrentCount 2: Code " << hex << Code << " Low " << Low << " Range " << Range << " Eof " << Eof << dec << endl;
        uint32_t result = (Code-Low)/Range;
        uint32_t resultLow = ((Code & ~Eof) - Low) / Range;
        uint32_t resultHigh = ((Code | Eof) - Low) / Range;

        if (debug)
            cerr << hex << "result " << result << " resultLow " << resultLow << " resultHigh " << resultHigh << dec << endl;

        return { resultLow, resultHigh };
    }

    template<typename ReadFn>
    bool removeRange(ReadFn && read, uint32_t SymbolLow, uint32_t SymbolHigh, uint32_t /* TotalRange */, bool /* debug */ = false)
    {
        using namespace std;
        Low += SymbolLow*Range;
        Range *= SymbolHigh-SymbolLow;

        while ((Low ^ Low+Range)<Top || Range<Bottom && ((Range= -Low & Bottom-1),1)) {
            int c = read();
            bool eof = c == EOF;
            if (eof)
                c = 0;
            Code = Code<<8 | (unsigned char)c;
            Eof = Eof << 8 | (eof ? 0xff : 0x0);
            Range<<=8;
            Low<<=8;
        }
        return Eof >> 56 == 0;
    }

    // returns: decoded code point, plus a bool saying if it's possibly possible to decode any more
    template<typename ReadFn, typename DecodeRangeFn>
    std::pair<int, bool>
    decodeOne(ReadFn && read,
              DecodeRangeFn && decodeRange, uint32_t totalRange, bool debug = false)
    {
        using namespace std;

        if (debug)
            cerr << hex << "Code " << Code << " Low " << Low << " High " << Low + Range << " totalRange " << totalRange << " Eof " << Eof << dec << endl;
        auto [countLow, countHigh] = getCurrentCount(totalRange, debug);
        if (debug || countLow > totalRange)
            cerr << "countLow " << hex << countLow << " countHigh " << countHigh << dec << endl;

        if (countLow > totalRange) {
            return { EOF, false };
        }

        ExcAssert(countHigh >= countLow);
        ExcAssert(countLow <= totalRange);

        auto [c, low, high, maxRange] = decodeRange(countLow);

        ExcAssert(low <= countLow);

        if (debug)
            cerr << "found character " << hex << (int)c << " from " << low << " to " << high << " of " << maxRange << dec << endl;

        //if (countHigh >= high) {
        //    // it's ambiguous whether we're in this character or another.  So we simply
        //    // finish decoding, using this fact as an implicit EOF marker.
        //    return { c, false };
        //}

        ExcAssert(low <= countLow);
        //ExcAssert(high >= countHigh);

        bool maybeMore = removeRange(read, low, high, maxRange, debug);

        return { c, maybeMore };
    }
};
