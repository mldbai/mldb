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
#include "mldb/arch/format.h"
#include "mldb/arch/ansi.h"

namespace MLDB {

struct RangeCoder64 {
    static constexpr uint64_t Top = 1ULL << 56;
    static constexpr uint64_t Bottom = 1ULL << 48;
    static constexpr uint64_t MaxRange = Bottom;

    // Trace structure that traces what happens in the encoding and decoding phases
    struct TraceEntry {
        int n;            /// Index of character
        int c;            /// Character in the decoded character set
        uint32_t Symbol;
        uint32_t SymbolLow;
        uint32_t SymbolHigh;
        uint32_t SymbolRange;

        uint64_t Low;
        uint64_t Range;
        uint64_t Code;
        uint64_t Eof;
        std::string encoded;  /// Characters it encodes into

        std::pair<uint64_t, bool> calcHigh() const
        {
            uint64_t result = 0;
            bool carry = __builtin_add_overflow(Low, Range, &result);
            return { result, carry };
        }

        static void printHeader()
        {
            using namespace std;
            cerr << ansi::cyan << ansi::underline << format("Num Low                High               Range              Code               EOF                ChHx ChInt   Low  High   Rng Encoded In -> Out") << ansi::reset << endl;
        }

        void print(std::function<std::string (int)> printCh) const
        {
            using namespace std;

            auto [High, carry] = calcHigh();

            cerr << ansi::cyan << format("%03d %05x.%012llx %05x.%012llx %05x.%012llx %05x.%012llx %05x.%012llx %04x %5i %05x %05x %05x %05x",
                                        n,
                                        whole(Low), frac(Low),
                                        whole(High, carry), frac(High),
                                        whole(Range, carry), frac(Range),
                                        whole(Code), frac(Code),
                                        whole(Eof), frac(Eof),
                                        (uint16_t)c, c, SymbolLow, Symbol, SymbolHigh, SymbolRange)
                << ansi::reset;
            cerr << " " << printCh(c) << " ->";
            for (unsigned char c: encoded) {
                cerr << format(" %02x", c);
            }
            cerr << endl;
        }
    };

    uint64_t Low = 0;
    // When Range is zero, it means 2^64
    uint64_t Range = 0;//std::numeric_limits<uint64_t>::max();

    // Used for decoder only
    uint64_t Code = 0;
    uint64_t Eof = 0;

    std::pair<uint64_t, bool> calcHigh() const
    {
        uint64_t result = 0;
        bool carry = __builtin_add_overflow(Low, Range, &result);
        return { result, carry };
    }

    void reduceRange(uint32_t TotalRange, bool debug)
    {
        using namespace std;
        //cerr << "reduceRange: before Low " << hex << Low << " Range " << Range << " totalRange " << TotalRange << dec << endl;
        if (MLDB_UNLIKELY(Low == 0 && Range == 0)) {
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

    // Calculates if we need to output a character.  May also fix up the range if we're out of
    // precision, so this is non-const. 
    bool outputReady(bool debug)
    {
        using namespace std;

        // We output a character if:
        // 1.  None of the bits differ between Low and High in the highest order byte.  This is
        //     calculated by xoring the two of them, and checking if the result is less than Top
        //     (if it's greater or equal than top, than one of the bits is different).  Equivalent
        //     to checking if Low >> 56 == (Low + Range) >> 56.
        // 2.  We have encoded a very unlikely character, so that we've underflowed our range.  In
        //     that case we need to output some characters, even if there is some entropy left over,
        //     to avoid underflowing the calculation.  This is a rare occurrance.
        //     (PS I'm not sure that I understand this, so please take with a grain of salt).
        //auto [High,carry] = calcHigh();
        //auto LowXorHigh = (Low ^ (Low + Range));
        //cerr << "Low ^ (Low + Range) = " << format("%05x.%012llx", whole(LowXorHigh, carry), frac(LowXorHigh)) << endl;
        if ((Low ^ (Low+Range))<Top) {
            if (debug)
                cerr << ansi::red << "writing char due to unambiguous output" << ansi::reset << endl;
            return true;
        }
        else if (Range<Bottom) {
            if (debug)
                cerr << ansi::red << "writing char to maintain precision" << ansi::reset << endl;
            Range= -Low & (Bottom-1);
            return true;
        }
        else return false;

        return (Low ^ (Low+Range))<Top || Range<Bottom && ((Range= -Low & (Bottom-1)),true);
    }

    static uint32_t whole(uint64_t x, bool carry = false) { return (x >> 48) + (carry ? 0x10000 : 0x0); };
    static unsigned long long frac(uint64_t x) { return x & ((1ULL << 48) - 1); };


    void printHeader() const
    {
        using namespace std;
        cerr << ansi::cyan << ansi::underline << format("Low                High               Range              Code               EOF") << ansi::reset << endl;
    }

    void printState() const
    {
        using namespace std;

        auto [High, carry] = calcHigh();

        cerr << ansi::cyan << format("%05x.%012llx %05x.%012llx %05x.%012llx %05x.%012llx %05x.%012llx",
                                    whole(Low), frac(Low),
                                    whole(High, carry), frac(High),
                                    whole(Range, carry), frac(Range),
                                    whole(Code), frac(Code),
                                    whole(Eof), frac(Eof))
             << ansi::reset << endl;
    }
};

struct RangeEncoder64: public RangeCoder64 {

    template<typename WriteFn>
    void updateRange(WriteFn && write, uint32_t SymbolLow,uint32_t SymbolHigh,uint32_t TotalRange, bool debug, std::vector<TraceEntry> * trace = nullptr)
    {
        using namespace std;
        if (debug)
            cerr << ansi::red
                 << format("reducing range for symbol %04x to %04x of %05x", SymbolLow, SymbolHigh, TotalRange)
                 << ansi::reset << endl;

        reduceRange(TotalRange, debug);
        Low += SymbolLow*Range;
        Range *= (SymbolHigh-SymbolLow);

        if (debug)
            printState();

        if (trace) {
            ExcAssert(!trace->empty());
        }

        //cerr << "after: " << hex << Low << " to " << Low + Range << endl;
        while (outputReady(debug)) {
            if (trace) {
                trace->back().encoded.push_back(Low>>56);
            }
            write(Low>>56);
            Range<<=8;
            Low<<=8;

            if (debug)
                printState();
        }

        if (trace) {
            trace->back().Low = Low;
            trace->back().Range = Range;
            trace->back().Code = 0;
            trace->back().Eof = 0;
        }
    }

#if 0    
    template<typename WriteFn>
    void flush(WriteFn && write, bool debug = false)
    {
        flushEof(write, 1, 1, debug);
    }
#endif

    // Flush the currently accumulated output.  This will ensure that the EOF can be detected by
    // the decoder.  The EOF condition is:
    // EITHER the range starts and ends unambiguously in the range [0-eofHigh),
    // OR     the same character could have been decoded by writing a lower point, but the one
    //        in the stream was purposefully made higher 
    template<typename WriteFn>
    void flushEof(WriteFn && write, uint32_t eofHigh, uint32_t maxRange, bool debug = false, std::vector<TraceEntry> * trace = nullptr)
    {
        // We have up to 64 bits of entropy accumulated.  We need to output the shortest sequence
        // that ensures we're between Low and High (Low + Range), in other words we look for
        // opportunities to use trailing zeros.

        using namespace std;
        //debug = true;

        //uint64_t Eof = 0;

#if 0
        auto [High, carry] = calcHigh();

        while (Low > 0) {
            uint64_t candidate = Low & ((1ULL << 56)-1);
            if (candidate < Low && candidate < (255ULL << 56)) {
                candidate += (1 << 56);
            }
            ExcAssert(candidate >= Low);

            if (!carry && candidate >= High) {

            }

            while (candidate) {
                write(candidate >> 56);
                candidate << 8;
            }

            uint32_t cLow = Low >> 56;
            uint32_t cHigh = High >> 56;
            if (cLow + 1 < cHigh) {
                write(cLow + 1);
                return;
            }
            write(cLow);
            Low << 8;

            if 
        }
#endif

        if (debug) {
            std::cerr << ansi::red << "flushing EOF ... EOF is 0 to " << hex << eofHigh << " of " << maxRange << dec << ansi::reset << std::endl;
            printState();
        }
        for (int_fast32_t i=0;i<8;i++) {
            using namespace std;

            if (trace) {
                TraceEntry entry;
                entry.n = 990 + i;
                entry.c = -1;
                entry.Code = 0;
                entry.Eof = Eof;
                entry.Low = Low;
                entry.Range = Range;
                entry.Symbol = 0;
                entry.SymbolLow = 0;
                entry.SymbolHigh = 0;
                entry.SymbolRange = 0;
                trace->push_back(entry);
            }

            auto [High, carry] = calcHigh();

            int cLow = Low >> 56;
            int cHigh = carry ? 256 : High >> 56;

            if (debug) {
                cerr << ansi::red << format("flush EOF: i = %d cLow = %02x cHigh = %02x", i, cLow, cHigh) << ansi::reset << endl;
                printState();
            }

#if 1
            // Check that what's left over is unambiguously in the eof range from 0 to EofHigh.
            // If not, write another byte.
            //if (debug) {
            //    cerr << std::hex << "cLow = " << cLow << " cHigh = " << cHigh << endl;
            //    cerr << hex << "rangeLow " << (((Low >> 32) * maxRange) >> 32)  << dec << endl;
            //    cerr << hex << "rangeHigh " << ((((Low + Range) >> 32) * maxRange) >> 32) << dec << endl;
            //}

            // Character to write, if it can be the last one
            int toWrite = -1;

            // If our low has all trailing zeros, we can just write it.  We fit right into the
            // lowest bits.
            if (Low << 8 == 0 && cLow < cHigh) {
                if (debug)
                    cerr << ansi::red << "writing due to low having all trailing zeros" << ansi::reset << endl;
                toWrite = cLow; // not unambigous for the decoder
            }
            // Otherwise, if our high does not have all trailing zeros, we can just write it
            else if (cLow == cHigh - 1 && cHigh != 256 && (Low + Range) << 8 > 0) {
                if (debug)
                    cerr << ansi::red << "writing due to high not having all trailing zeros" << ansi::reset << endl;
                toWrite = cHigh;
            } 
            // Otherwise, if there is a character in between them then just write it
            else if (cLow < cHigh - 1) {
                if (debug)
                    cerr << ansi::red << "writing due to multiple possible characters" << ansi::reset << endl;
                
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

                //if (debug) {
                //    cerr << hex << " pos1 " << pos1 << " pos2 " << pos2 << " pos3 " << pos3 << " pos4 " << pos4 << " pos5 " << pos5 << " pos6 " << pos6 << dec << endl;
                //}

                uint32_t position = pos6;//((((((uint64_t)toWrite << 56) - Low) >> 32) * maxRange / (Range >> 32)) >> 32);

                if (debug) {
                    //cerr << "toWrite - Low = " << hex << (((uint64_t)toWrite << 56) - Low) << dec << endl;
                    //cerr << "Range = " << hex << Range << dec << endl;
                    //cerr << "toWrite - Low * maxRange = " << hex << ((((((uint64_t)toWrite << 56) - Low) >> 32) * maxRange) >> 32) << dec << endl;
                    cerr << ansi::red << format("position: %04x of %04x; EOF: %04x", position, maxRange, eofHigh) << ansi::reset << endl;
                    //cerr << "position = " << hex << position << " of " << maxRange << dec << endl;
                }

                //position = 0;

                if (position < eofHigh || maxRange == 1) {
                    //cerr << "no extra byte needed as position is " << position << endl;
                    if (debug)
                        cerr << "cHigh - toWrite = " << cHigh - toWrite << endl;
                    if (trace)
                        trace->back().encoded.push_back(toWrite);
                    write(toWrite);
                    break;
                }
                //cerr << "Extra byte needed as position is " << position << " of " << eofHigh << " cLow " << cLow << " toWrite " << toWrite << " cHigh " << cHigh << endl;
                //if (toWrite < cHigh - 1) cerr << "**** could save a byte" << endl;
            }
#endif
            if (debug)
                cerr << ansi::red << "writing default case" << ansi::reset << endl;
            if (trace) {
                trace->back().encoded.push_back(cLow);
            }
            write(cLow);
            Low<<=8;
            Range<<=8;

            //if (Low + Range < Low) {
            //    cerr << "**** fixup range" << endl;
            //    Range = -Low;  // we want the full range, which is represented as Low + Range = 0
            //}

            if (Low == 0 && Range == 0)
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
                bool debug = false,
                std::vector<TraceEntry> * trace = nullptr)
    {
        auto [SymbolLow, SymbolHigh, TotalRange] = encodeRange(c);

        ExcAssert(SymbolLow < SymbolHigh);
        ExcAssert(SymbolLow >= 0);
        ExcAssert(SymbolHigh <= TotalRange);

        using namespace std;
        if (debug)
            cerr << ansi::magenta << format("encoding code point %04x into %04x to %04x of %04x", (unsigned)c, (unsigned)SymbolLow, (unsigned)SymbolHigh, (unsigned)TotalRange) << ansi::reset << endl;

        if (trace) {
            TraceEntry entry;
            entry.n = trace->size();
            entry.c = c;
            entry.Symbol = 0;
            entry.SymbolLow = SymbolLow;
            entry.SymbolHigh = SymbolHigh;
            entry.SymbolRange = TotalRange;
            trace->push_back(entry);
        }

        updateRange(write, SymbolLow, SymbolHigh, TotalRange, debug, trace);

        return;
    }
};

struct RangeDecoder64: public RangeCoder64 {

    template<typename ReadFn>
    void start(ReadFn && read, bool debug = false, std::vector<TraceEntry> * trace = nullptr)
    {
        using namespace std;
        std::string tracedEncoded;
        for(int_fast32_t i=0;i<8;i++) {
            int c = read();
            bool eof = c == EOF;
            if (eof) {
                c = 0;
            }
            if (trace)
                tracedEncoded.push_back(c);
            if (debug)
                cerr << ansi::yellow << "read char " << hex << c << " eof " << eof << dec << ansi::reset << endl;
            Code = (Code << 8) | c;
            Eof = (Eof << 8) | (eof ? 0xff : 0);
        }

        if (trace) {
            TraceEntry entry;
            std::memset(&entry, 0, sizeof(entry));
            entry.n = trace->size();
            entry.c = -1;
            entry.Code = Code;
            entry.Eof = Eof;
            entry.encoded = std::move(tracedEncoded);
            trace->push_back(entry);
        }

        if (debug) {
            cerr << ansi::red << "finished starting" << ansi::reset << endl;
            printState();
            //std::cerr << "start(): Code is " << std::hex << Code << std::dec << std::endl;
        }
    }

    std::tuple<uint32_t, uint32_t> getCurrentCount(uint32_t TotalRange, bool debug = false)
    {
        using namespace std;
        reduceRange(TotalRange, debug);
        if (debug) {
            printState();
            cerr << "getCurrentCount 2: Code " << hex << Code << " Low " << Low << " Range " << Range << " Eof " << Eof << dec << endl;
        }

        uint32_t result = (Code-Low)/Range;
        uint32_t resultLow = ((Code & ~Eof) - Low) / Range;
        uint32_t resultHigh = ((Code | Eof) - Low) / Range;

        if (debug)
            cerr << hex << "result " << result << " resultLow " << resultLow << " resultHigh " << resultHigh << dec << endl;

        return { resultLow, resultHigh };
    }

    template<typename ReadFn>
    bool removeRange(ReadFn && read, uint32_t SymbolLow, uint32_t SymbolHigh, uint32_t /* TotalRange */,
                     bool debug = false, std::vector<TraceEntry> * trace = nullptr)
    {
        using namespace std;
        Low += SymbolLow*Range;
        Range *= SymbolHigh-SymbolLow;

        std::string tracedEncoded;

        while (outputReady(debug)) {
            int c = read();
            bool eof = c == EOF;
            if (eof)
                c = 0;
            if (trace)
                tracedEncoded.push_back(c);
            Code = Code<<8 | (unsigned char)c;
            Eof = Eof << 8 | (eof ? 0xff : 0x0);
            Range<<=8;
            Low<<=8;
        }

        if (trace) {
            ExcAssert(trace->size() > 0);
            auto & entry = trace->back();
            entry.encoded = std::move(tracedEncoded);
            entry.Low = Low;
            entry.Range = Range;
            entry.Code = Code;
            entry.Eof = Eof;
        }

        return Eof >> 56 == 0;
    }

    // returns: decoded code point, plus a bool saying if it's possibly possible to decode any more
    template<typename ReadFn, typename DecodeRangeFn>
    std::pair<int, bool>
    decodeOne(ReadFn && read,
              DecodeRangeFn && decodeRange, uint32_t totalRange, bool debug = false,
              std::vector<TraceEntry> * trace = nullptr)
    {
        using namespace std;

        //if (debug)
        //    cerr << hex << "Code " << Code << " Low " << Low << " High " << Low + Range << " totalRange " << totalRange << " Eof " << Eof << dec << endl;
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

        if (trace) {
            TraceEntry entry;
            entry.c = c;
            entry.Symbol = countLow;
            entry.SymbolLow = low;
            entry.SymbolHigh = high;
            entry.SymbolRange = maxRange;
            entry.Low = Low;
            entry.Range = Range;
            entry.Code = Code;
            entry.Eof = Eof;
            entry.n = trace->size();
            trace->push_back(entry);
        }

        if (debug) {
            cerr << ansi::magenta << format("decoding code point %04x from %04x to %04x of %04x", (unsigned)c, (unsigned)low, (unsigned)high, (unsigned)maxRange) << ansi::reset << endl;
            cerr << ansi::yellow << format("  --> writing character %02x (%c)", (unsigned)c, isprint(c) ? c : '?') << ansi::reset << endl;
        }

        if (c == EOF)
            return { 0, false };

#if 0
        // Could this character have been encoded by writing a lower point?  If so, we're
        // at the EOF
        if (countLow >= 256 && countLow - 256 >= low) {
            // One point lower would have decoded to the same character, so EOF
            return { c, false /* more */ };
        }

        //if (countHigh >= high) {
        //    // it's ambiguous whether we're in this character or another.  So we simply
        //    // finish decoding, using this fact as an implicit EOF marker.
        //    return { c, false };
        //}
#endif

        ExcAssert(low <= countLow);
        //ExcAssert(high >= countHigh);

        bool maybeMore = removeRange(read, low, high, maxRange, debug, trace);

        return { c, maybeMore };
    }
};

} // namespace MLDB
