/* range_coder_test.cc                                              -*- C++ -*-
   Jeremy Barnes, 6 November 2015
   Copyright (c) 2015 mldb.ai inc.  All rights reserved.

   This file is part of MLDB. Copyright 2015 mldb.ai inc. All rights reserved.
*/

#include "catch2/catch_all.hpp"
#include "mldb/vfs/filter_streams.h"
#include "mldb/utils/memusage.h"
#include "mldb/plugins/tabular/range_coder.h"
#include "mldb/arch/format.h"
#include "mldb/arch/ansi.h"

using namespace std;
using namespace MLDB;

#if 1
TEST_CASE("test identity")
{
    uint32_t totalRange = 65536;
    uint32_t eofHi = 256;

    auto encodeRange = [&] (char16_t c) -> std::tuple<uint32_t, uint32_t, uint32_t>
    {
        if (c < 0 || c >= 256)
            MLDB_THROW_LOGIC_ERROR();
        return { c * 256, (c+1) * 256, totalRange };
    };

    auto decodeRange = [&] (uint32_t c) -> std::tuple<uint16_t, uint32_t, uint32_t, uint32_t>
    {
        cerr << "decodeRange " << c << endl;
        c /= 256;
        auto [lo, hi, rng] = encodeRange(c);
        return { c, lo, hi, rng };
    };

    auto runTest = [&] (const std::string & s)
    {
        bool debug = false;
        cerr << endl << ansi::bold << "encoding: s = " << s << ansi::reset << endl;

        std::string result;

        auto writeChar = [&] (char8_t c)
        {
            cerr << ansi::yellow << format("--> writing character %02x (%c)", (unsigned)c, isprint(c) ? c : '?') << ansi::reset << endl;
            result.push_back(c);
        };

        RangeEncoder64 encoder;
        encoder.printHeader();
        encoder.printState();

        for (char c: s) {
            encoder.encodeOne(writeChar, c, encodeRange, debug);
        }

        encoder.flushEof(writeChar, eofHi, totalRange, debug);

        cerr << "encoded " << s.size() << " characters into " << result.size() << ":" << endl;
        cerr << "orig:";
        for (unsigned char c: s) { cerr << format(" %02x", (unsigned)c); }
        cerr << endl << "enc: ";
        for (unsigned char c: result) { cerr << format(" %02x", (unsigned)c); }
        cerr << endl;

        cerr << endl << ansi::bold << "decoding: s = " << s << ansi::reset << endl;

        int n = 0;
        auto readChar = [&] () -> int
        {
            if (n == result.size())
                return EOF;
            return result[n++];
        };

        RangeDecoder64 decoder;

        std::string decoded;

        decoder.start(readChar);
        bool more = true;
        int c = -1;
        while (more) {
            std::tie(c, more) = decoder.decodeOne(readChar, decodeRange, totalRange, debug);
            cerr << "got char " << c << " more " << more << endl;
            if (c == 0 || c == EOF) {
                //CHECK(!more);  // later...
                break;
            }
            decoded.push_back(c);
        }

        CHECK(s == decoded);

        // For this test only, the encoded version should be the same as the original
        // after we remove any trailing zeros (which we can provide in decoding trivially
        // by allowing excess reads to simply return zero).
        while (!result.empty() && result.back() == 0) {
            result.pop_back();
            --n;
        }
        CHECK(s.size() == result.size());
        CHECK(s == result);

        // Validate that all characters were used
        CHECK(n == result.size());
    };

    std::vector<std::string> tests = {
        "",
        " ",
        "hello",
        "dog cat bird"
    };

    for (auto & test: tests) {
        SECTION(test) {
            runTest(test);
        }
    }
}
#endif

#if 1
TEST_CASE("test rare characters")
{
    // Each character is mapped to one single point on the 65536 possible points, meaning that
    // the vast majority of the encoding space is lost.  This should encode a string into
    // another string that is double the length.
    uint32_t totalRange = 65536;
    uint32_t eofHi = 1;

    auto encodeRange = [&] (char16_t c) -> std::tuple<uint32_t, uint32_t, uint32_t>
    {
        if (c < 0 || c >= 256)
            MLDB_THROW_LOGIC_ERROR();
        return { c, (c == 255 ? totalRange : c + 1), totalRange };
    };

    auto decodeRange = [&] (uint32_t c) -> std::tuple<uint16_t, uint32_t, uint32_t, uint32_t>
    {
        ExcAssertLess(c, 256);
        auto [lo, hi, rng] = encodeRange(c);
        return { c, lo, hi, rng };
    };

    auto runTest = [&] (const std::string & s)
    {
        bool debug = false;
        cerr << endl << ansi::bold << "s = " << s << ansi::reset << endl;

        std::string result;

        auto writeChar = [&] (char8_t c)
        {
            cerr << ansi::yellow << format("--> writing character %02x (%c)", (unsigned)c, isprint(c) ? c : '?') << ansi::reset << endl;
            result.push_back(c);
        };

        RangeEncoder64 encoder;
        encoder.printHeader();
        encoder.printState();

        for (char c: s) {
            encoder.encodeOne(writeChar, c, encodeRange, debug);
        }

        encoder.flushEof(writeChar, eofHi, totalRange, debug);

        cerr << "encoded " << s.size() << " characters into " << result.size() << ":" << endl;
        cerr << "orig:";
        for (unsigned char c: s) { cerr << format(" %02x", (unsigned)c); }
        cerr << endl << "enc: ";
        for (unsigned char c: result) { cerr << format(" %02x", (unsigned)c); }
        cerr << endl;

        int n = 0;
        auto readChar = [&] () -> int
        {
            if (n == result.size())
                return EOF;
            return result[n++];
        };

        RangeDecoder64 decoder;

        std::string decoded;

        decoder.start(readChar);
        bool more = true;
        int c = -1;
        while (more) {
            std::tie(c, more) = decoder.decodeOne(readChar, decodeRange, totalRange, debug);
            cerr << "got char " << c << " more " << more << endl;
            if (c == 0 || c == EOF) {
                //CHECK(!more);  // later...
                break;
            }
            decoded.push_back(c);
        }

        CHECK(s == decoded);

        // Remove any trailing zeros (which we can provide in decoding trivially
        // by allowing excess reads to simply return zero).
        while (!result.empty() && result.back() == 0) {
            result.pop_back();
            --n;
        }

        // Should be double the length, with each char separated with a null
        REQUIRE(s.size() * 2 == result.size());
        for (size_t i = 0;  i < s.size();  ++i) {
            CHECK(s[i] == result[i * 2 + 1]);
        }

        // Validate that all characters were used
        CHECK(n == result.size());
    };

    std::vector<std::string> tests = {
        "",
        " ",
        "hello",
        "dog cat bird"
    };

    for (auto & test: tests) {
        SECTION(test) {
            runTest(test);
        }
    }
}
#endif

#if 1
TEST_CASE("test EOF")
{
    std::vector<char16_t> ranges;

    uint32_t totalRange = 65536;
    uint32_t mid = 1;  // boundary between EOF (0) and '1'

    auto encodeRange = [&] (char16_t c) -> std::tuple<uint32_t, uint32_t, uint32_t>
    {
        if (c == 0)
            return { 0, mid, totalRange };
        else if (c == '1')
            return { mid, totalRange, totalRange };
        else MLDB_THROW_LOGIC_ERROR();
    };

    auto decodeRange = [&] (uint32_t c) -> std::tuple<uint16_t, uint32_t, uint32_t, uint32_t>
    {
        cerr << "decodeRange " << c << endl;
        if (c < mid)
            return { 0, 0, mid, totalRange };
        else return { '1', mid, totalRange, totalRange };
    };

    for (size_t len: { 0, 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 16, /*32, 64, 128, 256, 512, 1024, 2048, 4096, 8192, 16386, 32768*/}) {
        cerr << endl << endl;
        bool debug = false;
        std::string s(len, '1');

        cerr << endl << ansi::bold << "encoding: s = " << s << ansi::reset << endl;

        std::string result;

        auto writeChar = [&] (char8_t c)
        {
            cerr << ansi::yellow << format("--> writing character %02x (%c)", (unsigned)c, isprint(c) ? c : '?') << ansi::reset << endl;
            result.push_back(c);
        };

        RangeEncoder64 encoder;
        encoder.printHeader();
        encoder.printState();

        for (char c: s) {
            encoder.encodeOne(writeChar, c, encodeRange, debug);
        }

        encoder.flushEof(writeChar, mid, totalRange, debug);

        cerr << "encoded " << s.size() << " characters into " << result.size() << ":" << endl;
        cerr << "orig:";
        for (unsigned char c: s) { cerr << format(" %02x", (unsigned)c); }
        cerr << endl << "enc: ";
        for (unsigned char c: result) { cerr << format(" %02x", (unsigned)c); }
        cerr << endl;

        cerr << endl << ansi::bold << "decoding: s = " << s << ansi::reset << endl;

        int n = 0;
        auto readChar = [&] () -> int
        {
            //cerr << "readChar: n = " << n << " of " << result.size() << endl;
            //if (n < result.size())
            //    cerr << "result[n] = " << (int)result[n] << endl;
            if (n == result.size())
                return EOF;
            return (unsigned char)result[n++];
        };

        RangeDecoder64 decoder;
        decoder.printHeader();

        decoder.start(readChar, debug);
        decoder.printState();

        std::string decoded;

        bool more = true;
        int c = -1;
        while (more) {
            std::tie(c, more) = decoder.decodeOne(readChar, decodeRange, totalRange, debug);
            cerr << ansi::yellow << format("--> got character %02x (%c) more %d", (unsigned)c, isprint(c) ? c : '?', more) << ansi::reset << endl;
            //cerr << "got char " << c << " more " << more << endl;
            if (c == 0 || c == EOF) {
                //CHECK(!more);  // later...
                break;
            }
            decoded.push_back(c);
        }

        CHECK(s == decoded);

        // Validate that all characters were used
        CHECK(n == result.size());
    }

}
#endif