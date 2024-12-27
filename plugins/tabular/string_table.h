/* string_table.h                                              -*- C++ -*-
   Jeremy Barnes, 6 November 2015
   Copyright (c) 2015 mldb.ai inc.  All rights reserved.

   This file is part of MLDB. Copyright 2015 mldb.ai inc. All rights reserved.
*/

#pragma once

#include <vector>
#include <string>
#include "mldb/utils/memusage.h"
#include <compare>
#include <iostream>
#include <numeric>
#include "range_coder.h"
#include "string_table_iterator.h"
#include "prefix_trie.h"
#include "mldb/base/exc_assert.h"
#include <mutex>
#include <iomanip>
#include <cassert>

namespace std {

inline std::ostream & operator << (std::ostream & stream, const std::u16string_view & str)
{
    using namespace std;
    for (auto c: str) {
        if (isascii(c))
            stream << (char)c;
        else
            stream << "<" << setw(4) << hex << (int)c << dec << setw(0) << ">";
    }
    return stream;
}

} // namespace std

namespace MLDB {

template<typename Char = char, typename String = std::string, typename StringView = std::string_view>
struct StringTableT {

    StringTableT()
    {
        mem.reserve(131072);
        offsets.push_back(0);
    }

    String mem;
    std::vector<uint32_t> offsets;

    void reserve(size_t numStrings, size_t numCharacters)
    {
        offsets.reserve(numStrings + 1);
        mem.reserve(numCharacters);
    }

    int add(const StringView & view)
    {
        mem.append(view);
        offsets.push_back(mem.size());
        return offsets.size() - 2;
    }

    StringView get(int i) const
    {
        int start = offsets.at(i);
        int end = offsets.at(i + 1);

        int dist = end - start;

        return StringView(mem.data() + start, dist);
    }

    size_t size() const
    {
        return offsets.size() - 1;
    }

    size_t characters() const
    {
        return mem.size();
    }

    using Iterator = StringTableIterator<StringTableT, StringView>;

    Iterator begin() const
    {
        return {this, 0};
    }

    Iterator end() const
    {
        return {this, size()};
    }

    size_t memUsageIndirect(const MemUsageOptions & opt) const
    {
        return memUsageIndirectMany(opt, mem, offsets);
    }
};

using StringTable = StringTableT<char, std::string, std::string_view>;
using StringTable16 = StringTableT<char16_t, std::u16string, std::u16string_view>;

extern template struct StringTableT<char, std::string, std::string_view>;
extern template struct StringTableT<char16_t, std::u16string, std::u16string_view>;

// Also look at capitalization styles (initial, none, all, mixed)
enum CapStyle {
    TOUPPER_FIRST = 0,
    TOUPPER_ALL = 1,
    IDENTITY = 2,
    DONT_CARE __attribute__((__unused__)) = 3,
    NUM_CAP_STYLES __attribute__((__unused__))
};

CapStyle getCapStyle(std::string_view str);

std::string_view encodeWithCapStyle(std::string_view str, CapStyle style, std::string & storage);

std::string_view decodeWithCapStyle(std::string_view str, CapStyle style, std::string & storage);

struct CapStyleDecoder {
    std::string_view decode(std::string_view str, CapStyle capStyle, std::string & storage) const
    {
        return decodeWithCapStyle(str, capStyle, storage);
    }
};

struct CapStyleEncoder {
    std::string_view encode(std::string_view str, CapStyle capStyle, std::string & storage) const
    {
        return encodeWithCapStyle(str, capStyle, storage);
    }
};

struct Lz4Codec {
    static std::string encode(std::string_view str);
    static std::string decode(std::string_view encoded);
};

template<typename Base>
struct SuffixDecoderImpl: public Base {

    using Base::charToPrefix;

    std::string decode(std::u16string_view encoded, bool debug = false) const
    {
        using namespace std;
        std::string result;
        for (uint16_t c: encoded) {
            if (debug)
                cerr << "decoding char " << hex << c << " as " << charToPrefix.get(c) << dec << endl;
            result += charToPrefix.get(c);
        }
        return result;
    }

    size_t memUsageIndirect(const MemUsageOptions & opt) const
    {
        return memUsageIndirectMany(opt, charToPrefix);
    }
};

struct SuffixDecoderBase {
    StringTable charToPrefix;
};

struct SuffixDecoder: public SuffixDecoderImpl<SuffixDecoderBase> {};

extern template struct SuffixDecoderImpl<SuffixDecoderBase>;

struct SuffixEncoder {

    void initialize(const std::map<std::string, uint16_t> & retainedPrefixes);

    CharPrefixTrie trie;
    std::vector<uint16_t> vals;

    struct Cache {
        std::unordered_map<std::string, std::u16string> prefixCache;
        std::mutex prefixCacheMutex;
    };

    std::u16string encode(std::string_view str, Cache * cache, bool debug = false) const;

    size_t memUsageIndirect(const MemUsageOptions & opt) const;
};

template<typename Base>
struct CharacterRangeTableImpl: public Base {
    
    using Base::codeRanges;
    
    auto decodeRange(uint32_t i, bool debug = false) const
    {
        ExcAssert(i >= 0 && i <= codeRanges.at(codeRanges.size() - 1));
        auto pos = std::lower_bound(codeRanges.begin(), codeRanges.end(), i);
        ExcAssert(pos != codeRanges.end());
        size_t idx = pos - codeRanges.begin();

        if (*pos != i || idx == codeRanges.size() - 1) {
            idx -= 1;
        }

        if (idx < 0 || idx > codeRanges.size() /*|| debug*/) {
            using namespace std;
            cerr << "i = " << i << endl;
            cerr << hex << "idx = " << idx << " *pos = " << *pos << dec << endl;
            cerr << "code " << codeRanges.at(idx) << " next " << codeRanges.at(idx + 1) << endl;
        }
        ExcAssert(idx >= 0 && idx < codeRanges.size());
        return std::make_tuple(idx, codeRanges.at(idx), codeRanges.at(idx + 1), range());
    }

    auto encodeRange(uint32_t i, bool debug = false) const
    {
        using namespace std;
        //cerr << "encodeRange " << i << " of " << codeRanges.size() << endl;
        ExcAssert(i >= 0);
        ExcAssert(i < codeRanges.size());
        if (debug) {
            //cerr << "getting range for " << hex << i << ": " << codeRanges[i] << "-" << codeRanges[i + 1] << " of " << range();
            //if (i > 0) cerr << "  prev " << codeRanges[i - 1];
            //if (i < codeRanges.size() - 1) cerr << "  next " << codeRanges[i + 2] << endl;
            //cerr << dec << endl;
        }

        auto [idx, lo, hi, rng] = decodeRange(codeRanges[i]);
        auto [idx2, lo2, hi2, rng2] = decodeRange(codeRanges[i + 1] - 1);
        ExcAssert(idx == i);
        ExcAssert(lo == codeRanges[i]);
        ExcAssert(hi == codeRanges[i + 1]);
        ExcAssert(rng == range());
        ExcAssert(idx2 == i);
        ExcAssert(lo2 == codeRanges[i]);
        ExcAssert(hi2 == codeRanges[i + 1]);
        ExcAssert(rng2 == range());

        return std::make_tuple(codeRanges[i], codeRanges[i + 1], range());
    }

    uint32_t range() const { return codeRanges.empty() ? 0 : codeRanges.at(codeRanges.size() - 1); }

    size_t memUsageIndirect(const MemUsageOptions & opt) const
    {
        return memUsageIndirectMany(opt, codeRanges);
    }
};

struct CharacterRangeTableBase {
    std::vector<uint32_t> codeRanges;
};

struct CharacterRangeTable: public CharacterRangeTableImpl<CharacterRangeTableBase> {};

extern template struct CharacterRangeTableImpl<CharacterRangeTableBase>;

CharacterRangeTable create_character_table(const std::vector<int> & charFrequencies, int start = 0);

template<class Base>
struct EntropyEncoderDecoderImpl: public Base {

    using Base::capitalizationCodes;
    using Base::characterCodes;

    std::pair<CapStyle, std::u16string>
    decode(const std::string_view & str, bool debug = false,
           std::vector<RangeCoder64::TraceEntry> * trace = nullptr) const
    {
        using namespace std;
        if (str.empty())
            return { IDENTITY, {} };

        std::u16string decoded;
        decoded.reserve(64);

        RangeDecoder64 decoder;

        size_t readPos = 0;
        auto readChar = [&] () -> int
        {
            if (readPos >= str.length()) {
                return EOF;
            }
            return (unsigned char)str[readPos++];
        };

        decoder.start(readChar, debug, trace);

        auto decodeRangeCap = [&] (int i)
        {
            return capitalizationCodes.decodeRange(i, debug);
        };

        auto decodeRangeChar = [&] (int i)
        {
            if (i == EOF)
                i = 0;
            return characterCodes.decodeRange(i, debug);
        };

        CapStyle capStyle = (CapStyle)decoder.decodeOne(readChar, decodeRangeCap, capitalizationCodes.range(), debug).first;

        for (;;) {
            //debug = decoded.size() > 300;
            auto [c, maybeMore] =  decoder.decodeOne(readChar, decodeRangeChar, characterCodes.range(), debug, trace);
            if (c == 0 || c == EOF)
                break;
            decoded.push_back(c);
            if (!maybeMore)
                break;
        }

        if (debug)
            cerr << "finished decoding with range " << std::hex << decoder.Low << " to " << decoder.Low + decoder.Range << std::dec << endl;

        return { capStyle, std::move(decoded) };
    }

    std::string
    encode(std::u16string_view str, CapStyle capStyle, bool debug = false,
           std::vector<RangeCoder64::TraceEntry> * trace = nullptr) const
    {
        using namespace std;
        
        std::string encoded;
        if (str.empty())
            return encoded;

        encoded.reserve(str.length());

        RangeEncoder64 encoder;
    
        auto writeChar = [&] (unsigned char c)
        {
            using namespace std;
            if (debug)
                std::cerr << "writing char " << std::hex << (int)(unsigned char)c << std::dec << endl;
            encoded.push_back((unsigned char)c);
        };

        auto encodeRangeCap = [&] (int i)
        {
            return capitalizationCodes.encodeRange(i, debug);
        };

        auto encodeRangeChar = [&] (int i)
        {
            if (i == EOF)
                i = 0;
            return characterCodes.encodeRange(i, debug);
        };

        encoder.encodeOne(writeChar, capStyle, encodeRangeCap, debug, trace);
        for (uint16_t c: str) {
            encoder.encodeOne(writeChar, c, encodeRangeChar, debug, trace);
        }

        // First try to encode WITHOUT the EOF character.  Sometimes we are able to know we're done
        // by the fact that there are no more input characters.

        // Ending it:
        // 1.  An unambiguous EOF written to the stream
        // 2.  A possible EOF, with no more characters to read
        if (false) {
#if 0
            std::string shortEncoded = encoded;
            RangeEncoder64 encoder2 = encoder;
            encoder2.flushEof(writeChar, characterCodes.codeRanges.at(1), characterCodes.range(), debug);
            shortEncoded.swap(encoded);
            encoder.encodeOne(writeChar, EOF, encodeRangeChar, debug);
            encoder.flush(writeChar, debug);

            if (encoded.length() > shortEncoded.length()) {
                using namespace std;
                cerr << "EOF character added a byte for " << str << " from " << shortEncoded.length() << " to " << encoded.length() << endl;
            }

            // Does the short encoded allow us to recover the string?
            auto [shortDecodedCap, shortDecoded] = decode(shortEncoded, debug);

            if (shortDecodedCap == capStyle && shortDecoded == str && shortEncoded.length() < encoded.length()) {
                // we don't need to write the EOF character
                cerr << "Saved a byte for " << str << " from " << shortEncoded.length() << " to " << encoded.length() << endl;
                return encoded;
            }
#endif
        }
        else {
            //encoder.encodeOne(writeChar, EOF, encodeRangeChar, debug);
            //encoder.flush(writeChar, debug);
            encoder.flushEof(writeChar, characterCodes.codeRanges.at(1), characterCodes.range(), debug, trace);
        }

        return encoded; //Lz4Codec::encode(encoded);
    }

    size_t memUsageIndirect(const MemUsageOptions & opt) const
    {
        return memUsageIndirectMany(opt, characterCodes, capitalizationCodes);
    }
};

struct EntropyEncoderDecoderBase {
    CharacterRangeTable characterCodes;
    CharacterRangeTable capitalizationCodes;
};

struct EntropyEncoderDecoder: public EntropyEncoderDecoderImpl<EntropyEncoderDecoderBase> {};

extern template struct EntropyEncoderDecoderImpl<EntropyEncoderDecoderBase>;

template<typename Base>
struct OptimizedStringTableImpl: public Base {

    using Base::encoded;
    using Base::entropyDecoder;
    using Base::suffixDecoder;
    using Base::capDecoder;

    std::string get(int index) const
    {
        std::string_view raw = this->encoded.get(index);
        std::string storage1, storage2, storage3;
        auto [capStyle, entropyDecoded] = entropyDecoder.decode(raw);
        std::string suffixDecoded = suffixDecoder.decode(entropyDecoded);
        std::string_view result = capDecoder.decode(suffixDecoded, capStyle, storage3);
        if (storage3.data() != result.data())
            storage3 = result;
        return storage3;
    }

    struct {
        uint32_t version:8;
        uint32_t unused:24;
    } md = {0,0};

    size_t size() const
    {
        return this->encoded.size();
    }

    using Iterator = StringTableIterator<OptimizedStringTableImpl, std::string>;

    Iterator begin() const
    {
        return {this, 0};
    }

    Iterator end() const
    {
        return {this, size()};
    }

    size_t memUsageIndirect(const MemUsageOptions & opt) const
    {
        return memUsageIndirectMany(opt, encoded, capDecoder, suffixDecoder, entropyDecoder);
    }
};

struct OptimizedStringTableBase {
    StringTable encoded;
    CapStyleDecoder capDecoder;
    SuffixDecoder suffixDecoder;
    EntropyEncoderDecoder entropyDecoder;
};

struct OptimizedStringTable: public OptimizedStringTableImpl<OptimizedStringTableBase> {};

extern template struct OptimizedStringTableImpl<OptimizedStringTableBase>;

OptimizedStringTable optimize_string_table(const StringTable & table);

} // namespace MLDB
