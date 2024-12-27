/* range_coder_test.cc                                              -*- C++ -*-
   Jeremy Barnes, 6 November 2015
   Copyright (c) 2015 mldb.ai inc.  All rights reserved.

   This file is part of MLDB. Copyright 2015 mldb.ai inc. All rights reserved.
*/

#include "catch2/catch_all.hpp"
#include "mldb/vfs/filter_streams.h"
#include "mldb/utils/memusage.h"
#include "mldb/plugins/tabular/string_table.h"
#include "mldb/types/value_description.h"
#include "mldb/types/map_description.h"
#include "mldb/types/vector_description.h"
#include "mldb/types/pair_description.h"
#include "mldb/arch/format.h"
#include "mldb/arch/ansi.h"


using namespace std;
using namespace MLDB;


template<typename T>
void loadField(T & val, std::istream & stream)
{
    std::string line;
    std::getline(stream, line);
    val = jsonDecodeStr<T>(line);
}

struct TestCase {
    SuffixEncoder suffixEncoder;
    SuffixDecoder suffixDecoder;
    EntropyEncoderDecoder entropyCoder;
    std::vector<std::string> testCases;
};

TestCase load(const std::string & filename)
{
    cerr << "loading test from " << filename << endl;
    std::ifstream stream(filename);
    stream.exceptions(ios_base::failbit | ios_base::badbit);

    TestCase result;

    std::vector<std::pair<std::string, int> > prefixes;
    loadField(prefixes, stream);
    REQUIRE(prefixes.size() > 0);
    CHECK(prefixes[0].first == "");
    CHECK(prefixes[0].second == 0);
    prefixes[0].first = { '\0' };

    std::map<std::string, uint16_t> prefixMap(prefixes.begin(), prefixes.end());

    result.suffixEncoder.initialize(prefixMap);

    std::vector<std::string> codeTable;
    codeTable.reserve(prefixes.size() + 1);
    size_t numCharacters = 0;
    for (auto [str, codepoint]: prefixes) {
        if (codeTable.size() <= codepoint)
            codeTable.resize(codepoint + 1);
        codeTable.at(codepoint) = str;
        numCharacters += str.size();
        //cerr << "code " << codepoint << " str " << str << endl;
    }
    cerr << "codeTable.size() = " << codeTable.size()
         << " prefixes.size() = " << prefixes.size() << endl;
    result.suffixDecoder.charToPrefix.reserve(codeTable.size(), numCharacters);

    for (size_t i = 0;  i < codeTable.size();  ++i) {
        ExcAssertEqual(result.suffixDecoder.charToPrefix.add(codeTable[i]), i);
    }

    loadField(result.entropyCoder.capitalizationCodes.codeRanges, stream);
    loadField(result.entropyCoder.characterCodes.codeRanges, stream);
    loadField(result.testCases, stream);

    return result;
}

static const std::string testDir = "mldb/plugins/tabular/testing/fixtures/";
//static const std::string testDir = "./";

TEST_CASE("replay")
{
    for (std::string filename: {
            "st1/encode-decode-test-1.txt", /*"st1/encode-decode-test-2.txt", "st1/encode-decode-test-3.txt", "st1/encode-decode-test-4.txt",*/
            /*"st2/encode-decode-test-1.txt", "st2/encode-decode-test-2.txt", "st2/encode-decode-test-3.txt", "st2/encode-decode-test-4.txt",
            "st2/encode-decode-test-5.txt", "st2/encode-decode-test-6.txt", "st2/encode-decode-test-7.txt",*/
             }) {

        SECTION(filename) {
            TestCase testCase = load(testDir + filename);

            for (auto & s: testCase.testCases) {
                SECTION("without suffix encoding") {
                    cerr << endl << ansi::bold << "encoding: s = " << s << ansi::reset << endl;
                    bool debug = false;
                    auto cap = getCapStyle(s);
                    std::string storage;
                    auto capEnc = encodeWithCapStyle(s, cap, storage);
                    std::u16string str16(capEnc.begin(), capEnc.end());

                    std::vector<RangeCoder64::TraceEntry> trace;

                    std::string encoded = testCase.entropyCoder.encode(str16, cap, debug, &trace);
                    cerr << "encoded " << s.size() << " chars into " << encoded.size() << endl;

                    bool dumpTrace = false;

                    if (dumpTrace) {
                        RangeCoder64::TraceEntry::printHeader();
                        for (auto & e: trace) {
                            e.print();
                        }
                    }

                    cerr << endl << ansi::bold << "decoding: s = " << s << ansi::reset << endl;

                    std::vector<RangeCoder64::TraceEntry> trace2;

                    auto [capDecoded, dec16] = testCase.entropyCoder.decode(encoded, debug, &trace2);

                    if (dumpTrace) {
                        RangeCoder64::TraceEntry::printHeader();
                        for (auto & e: trace2) {
                            e.print();
                        }
                    }

                    CHECK(capDecoded == cap);
                    std::string narrowed(dec16.begin(), dec16.end());

                    std::string storage2;
                    auto capDec = decodeWithCapStyle(narrowed, capDecoded, storage2);

                    CHECK(str16.length() == dec16.length());
                    CHECK(Utf8String(s) == Utf8String(capDec));
                }

#if 1
                SECTION("with suffix encoding") {
                    cerr << endl << ansi::bold << "encoding: s = " << s << ansi::reset << endl;
                    bool debug = false;

                    auto cap = getCapStyle(s);
                    std::string storage;
                    auto capEnc = encodeWithCapStyle(s, cap, storage);


                    auto suffixEnc = testCase.suffixEncoder.encode(capEnc, nullptr, debug);

                    std::string encoded = testCase.entropyCoder.encode(suffixEnc, cap, debug);
                    cerr << "encoded " << s.size() << " chars into " << encoded.size() << endl;

                    cerr << endl << ansi::bold << "decoding: s = " << s << ansi::reset << endl;
                    auto [capDecoded, dec16] = testCase.entropyCoder.decode(encoded, debug);

                    CHECK(capDecoded == cap);

                    std::string suffixDec = testCase.suffixDecoder.decode(dec16, debug);

                    std::string storage2;
                    auto capDec = decodeWithCapStyle(suffixDec, capDecoded, storage2);

                    CHECK(suffixEnc.length() == dec16.length());
                    CHECK(suffixEnc == dec16);
                    CHECK(s.length() == capDec.length());
                    CHECK(Utf8String(s) == Utf8String(capDec));
                }
#endif
            }
        }
    }
}
