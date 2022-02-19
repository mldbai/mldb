/* string_table.cc                                              -*- C++ -*-
   Jeremy Barnes, 6 November 2015
   Copyright (c) 2015 mldb.ai inc.  All rights reserved.

   This file is part of MLDB. Copyright 2015 mldb.ai inc. All rights reserved.
*/

#include "string_table.h"
#include "suffix_array.h"
#include "mldb/base/exc_assert.h"
#include <sstream>
#include <array>

namespace MLDB {

template struct StringTableT<char, std::string, std::string_view>;
template struct StringTableT<char16_t, std::u16string, std::u16string_view>;

CapStyle getCapStyle(std::string_view str)
{
    if (str.empty())
        return IDENTITY;
    
    int numUpper = 0;
    int numLower = 0;

    for (char c: str) {
        if (isupper(c))
            ++numUpper;
        else if (islower(c))
            ++numLower;
    }

    if (numUpper == 0 && numLower == 0)
        return DONT_CARE;

    if (numUpper > 0 && numLower == 0)
        return TOUPPER_ALL;

    if (isupper(str[0]) && numUpper == 1)
        return TOUPPER_FIRST;

    return IDENTITY;
};

std::string_view encodeWithCapStyle(std::string_view str, CapStyle style, std::string & storage)
{
    switch (style) {
        case TOUPPER_ALL:
            storage = str;
            for (auto & c: storage)
                c = tolower(c);
            return storage;
        case TOUPPER_FIRST:
            storage = str;
            storage[0] = tolower(storage.at(0));
            return storage;
        default:
            return str;
    }
    ExcAssert(false);
};

std::string_view decodeWithCapStyle(std::string_view str, CapStyle style, std::string & storage)
{
    switch (style) {
        case TOUPPER_ALL:
            storage = str;
            for (auto & c: storage)
                c = toupper(c);
            return storage;
        case TOUPPER_FIRST:
            storage = str;
            storage[0] = toupper(storage.at(0));
            return storage;
        default:
            return str;
    }
    ExcAssert(false);
};

struct CapStyleFilter {
    std::array<int, NUM_CAP_STYLES> alphaCounts;
    StringTable strings;
    std::vector<CapStyle> capStyles;
    CapStyleDecoder decoder;
    CapStyle dontCare = DONT_CARE;  // where we map the don't care value to another

    CapStyleFilter() = default;

    CapStyleFilter(const StringTable & table)
    {
        initialize(table);
    }

    void initialize(const StringTable & table)
    {
        std::fill(alphaCounts.begin(), alphaCounts.end(), 0);

        strings.reserve(table.size(), table.characters());

        for (auto str: table) {
            CapStyle style = getCapStyle(str);
            ++alphaCounts[style];
            capStyles.push_back(style);
            std::string storage;
            strings.add(encodeWithCapStyle(str, style, storage));
        }

        using namespace std;
        cerr << "cap counts" << endl;
        cerr << "  TOUPPER_FIRST: " << alphaCounts[TOUPPER_FIRST] << endl;
        cerr << "  TOUPPER_ALL: " << alphaCounts[TOUPPER_ALL] << endl;
        cerr << "  IDENTITY: " << alphaCounts[IDENTITY] << endl;
        cerr << "  DONT_CARE: " << alphaCounts[DONT_CARE] << endl;

        // The DONT_CARE class maps to the one with the highest count
        dontCare = CapStyle(std::max_element(alphaCounts.begin(), alphaCounts.begin() + DONT_CARE - 1) - alphaCounts.begin());
        alphaCounts[dontCare] += alphaCounts[DONT_CARE];
        alphaCounts[DONT_CARE] = 0;

        // Replace with the don't care style
        for (CapStyle & style: capStyles) {
            if (style == DONT_CARE)
                style = dontCare;
        }
    }
};

std::u16string SuffixEncoder::encode(std::string_view str, bool debug) const
{
    using namespace std;

    if (debug)
        cerr << "encodeWithPrefixes " << str << endl;

    // Divide and conquer.  Look at all ways we can split into two, take the shortest one
    if (str.length() <= 1)
        return u16string(str.begin(), str.end());
    
    u16string best;

    // Does it match one of our prefixes?
    {
        auto it = prefixes.find(std::string(str));
        if (it != prefixes.end()) {
            return { it->second };
        }   
    }

    std::string cacheKey(str.begin(), str.end());
    {
        std::unique_lock<std::mutex> guard(prefixCacheMutex);
        auto it = prefixCache.find(cacheKey);
        if (it != prefixCache.end()) {
            return it->second;
        }
    }

    for (size_t position = 1;  position < str.length();  ++position) {

        //cerr << "splitting " << str << " into " << str.substr(0, position) << " and " << str.substr(position) << endl;

        u16string first = encode(str.substr(0, position), debug);
        u16string rest = encode(str.substr(position), debug);

        //cerr << "str " << str << " position " << position << " first " << first << " rest " << rest << endl;

        if (best.empty() || first.length() + rest.length() < best.length()) {
            best = first;
            best += rest;
        }
    }

    std::unique_lock<std::mutex> guard(prefixCacheMutex);
    prefixCache[cacheKey] = best;

    return best;
}

size_t SuffixEncoder::memUsageIndirect(const MemUsageOptions & opt) const
{
    return memUsageIndirectMany(opt, prefixes);
}

template struct SuffixDecoderImpl<SuffixDecoderBase>;

struct SuffixFilter {

    SuffixFilter() = default;

    SuffixFilter(const StringTable & strings)
    {
        initialize(strings);
    }

    static std::string_view commonPrefix(const std::string_view & s1, const std::string_view & s2)
    {
        //cerr << "common prefix between " << s1.substr(0, 50) << " and " << s2.substr(0, 50) << endl;
        auto l = std::min(s1.length(), s2.length());
        //cerr << "l = " << l << endl;
        for (size_t i = 0; i < l;  ++i) {
            //cerr << "i = " << i << " c1 = " << s1[i] << " c2 = " << s2[i] << endl;
            if (s1[i] != s2[i])
                return s1.substr(0, i);
        }
        return s1.substr(0,l);
    }

    std::map<std::string_view, int> countPrefixes(const MultiSuffixArray & suffixes)
    {
        using namespace std;

        using PrefixMap = std::map<std::string_view, int>;
        PrefixMap prefixCounts;
        std::vector<int> prefixStarts;  // position at which each prefix started

        auto calcPrefixes = [&] (std::string_view prefix, int index)
        {
            // If our prefix is shorter, some prefixes ended so we write out their results
            //cerr << "prefixStarts.length() = " << prefixStarts.size() << " prefix " << prefix << endl;

            while (prefixStarts.size() > prefix.length()) {
                //cerr << "  adding " << index - prefixStarts.back() << " to " << prefix.substr(0, prefixStarts.size()) << endl;
                prefixCounts[prefix.substr(0, prefixStarts.size())] += index - prefixStarts.back();
                prefixStarts.pop_back();
            }

            // And any increased prefix length starts here
            prefixStarts.resize(prefix.length(), index);
        };

        std::string_view lastStr;

        for (size_t i = 0;  i < suffixes.size();  ++i) {
            auto [str,index,start] = suffixes.at(i);
            std::string_view prefix = commonPrefix(str, lastStr);
            calcPrefixes(prefix, i);
            lastStr = str;
        }

        // Write out the last of the prefixes
        calcPrefixes("", suffixes.size());
    
        return prefixCounts;
    }

    std::pair<std::map<std::string, char16_t>,
              std::vector<std::string>>
    scorePrefixes(const StringTable & table,
                  const MultiSuffixArray & suffixes,
                  const std::map<std::string_view, int> & prefixCounts, bool trace = false)
    {
        using namespace std;

        // How many characters do we have in total
        size_t totalCharacters = suffixes.str.size() - suffixes.offsets.size();

        std::vector<size_t> characterCounts(256);

        for (unsigned char c: suffixes.str) {
            characterCounts[c] += 1;
        }

        std::vector<double> characterPriors(256);
        for (int c = 0;  c < 256;  ++c) {
            characterPriors[c] = 1.0 * characterCounts[c]  / totalCharacters;
            if (characterCounts[c] > 0) {
               cerr << "character " << (char)c << " has prior " << characterPriors[c] << endl;
            }
        }

        // How many characters we could save using this prefix
        auto scorePrefix = [&] (std::string_view prefix, int count) -> double
        {
            if (prefix == "")
                return -INFINITY;
            //cerr << "scoring prefix " << prefix << endl;

            // probability of this prefix?
            double prefixProbability = 1.0 * count / totalCharacters;

            // prior probability of this prefix?
            double priorProbability = 1.0;
            for (size_t i = 0;  i < prefix.length();  ++i) {
                priorProbability = priorProbability * characterPriors[(unsigned char)prefix[0]];
            }

            ExcAssert(priorProbability != 0);

            // Ratio
            double probabilityRatio = prefixProbability / priorProbability;

            // How many bits saved?  Instead of encoding each character individually,
            // we encode the single character here, saving a number of bits per character.
            double bitsSaved = count * log2(probabilityRatio);

            //cerr << "probabilityRatio " << probabilityRatio << " bits per " << log2(probabilityRatio) << " bitsSaved " << bitsSaved << endl;

            return bitsSaved;
        };

        cerr << "scoring " << prefixCounts.size() << " prefixes" << endl;
        std::vector<std::tuple<double, std::string_view, int> > prefixScores;
        
        for (auto & [prefix,count]: prefixCounts) {
            prefixScores.emplace_back(scorePrefix(prefix, count), prefix, count);
        }

        std::sort(prefixScores.rbegin(), prefixScores.rend());

#if 0
        for (int i = 0;  i < prefixScores.size() && i < 200;  ++i) {
            auto & [score, prefix, count] = prefixScores[i];
            if (trace) {
                cerr << "prefix " << i << ": " << score << " " << prefix << " " << count << endl;
            }
        }
#endif

        std::map<std::string, char16_t> retainedPrefixes;
        std::vector<std::string> charToPrefix;

        // First 256 code points map directly onto the same byte, unless the byte is not present in which
        // case we record it as a gap and replace it with something better

        std::vector<size_t> gaps;
        uint16_t maxCode = 0;

        for (unsigned i = 0;  i < 256;  ++i) {
            if (i == 0 || characterCounts[i] > 0) {
                while (charToPrefix.size() < i) {
                    gaps.push_back(charToPrefix.size());
                    charToPrefix.push_back("");
                }
                ExcAssert(charToPrefix.size() == i);
                charToPrefix.emplace_back(std::string({(char)i}));
                maxCode = i;
            }
        }

        // Now add as many prefixes as it makes sense to add.  Note that adding a prefix
        // is a double edged sword: it saves bits, but may interfere with others if it
        // overhaps, and adding a new prefix incurs a cost: we also have to
        // - Encode an extra 16 bits in the ranges table
        // - Encode the prefix itself in the string table, with an offset of around 16 bits and its bytes

        // Retained prefixes fit into unused characters (for now)
        size_t gapNumber = 0;
        for (size_t i = 0;  i < prefixScores.size() && maxCode < 1024;  ++i) {
            auto & [score, prefix, count] = prefixScores[i];
            if (score < 32 + 8 * prefix.length()) {
                // Not worth adding this prefix
                continue;
            }
            uint16_t code = (gapNumber < gaps.size() ? gaps[gapNumber++] : charToPrefix.size());
            maxCode = std::max<uint32_t>(maxCode, code);

            if (trace || true) {
                cerr << "  retained prefix " << prefix << " with score " << score << " at code point " << code << endl;
            }

            retainedPrefixes[std::string(prefix)] = code;
            if (code < charToPrefix.size()) {
                charToPrefix[code] = prefix;
            }
            else {
                ExcAssert(code == charToPrefix.size());
                charToPrefix.emplace_back(prefix);
            }

            ExcAssert(charToPrefix.at(code) == prefix);
        }

        cerr << "maxCode = " << maxCode << " charToPrefix.size() = " << charToPrefix.size() << endl;
        if (charToPrefix.size() > maxCode + 1) {
            charToPrefix.resize(maxCode + 1);
        }

        // Now let's see how may times they are actually used
        SuffixEncoder encoder;
        encoder.prefixes = retainedPrefixes;

        std::vector<int> realCounts(charToPrefix.size() + 1);

        for (std::string_view s: table) {
            auto encoded = encoder.encode(s);
            for (uint16_t c: encoded) {
                if (c >= realCounts.size()) {
                    cerr << "character " << c << " size " << realCounts.size() << endl;
                }
                realCounts.at(c) += 1;
            }
        }

        // Now only keep the ones that really help
        for (size_t i = 0;  i < retainedPrefixes.size();  ++i) {
            std::string_view prefix = charToPrefix[i];
            if (i == 0 || prefix.empty())
                continue;
            int expectedCount;
            if (prefix.length() == 1) {
                expectedCount = characterCounts[(unsigned char)prefix[0]];
            }
            else {
                auto it = prefixCounts.find(prefix);
                if (it == prefixCounts.end()) {
                    cerr << "didn't find prefix '" << prefix << "' of length " << prefix.length() << " first char " << (int)prefix[0] << " at " << i << endl;
                }
                ExcAssert(it != prefixCounts.end());
                expectedCount = it->second;
            }
            cerr << "prefix " << i << " " << prefix << " expected count " << expectedCount << " real count " << realCounts[i] << endl;
        }


        return { retainedPrefixes, charToPrefix };
    }

    void initialize(const StringTable & table)
    {
        using namespace std;
        cerr << "creating suffix array on " << table.mem.size() << " entries" << endl;
        auto started = std::chrono::steady_clock::now();

        MultiSuffixArray suffixes(table.begin(), table.end());

        auto ended = std::chrono::steady_clock::now();
        std::chrono::duration<double> elapsed_seconds = ended-started;
        std::cerr << "elapsed time: " << elapsed_seconds.count() << endl;

        auto prefixCounts = countPrefixes(suffixes);

        auto [retainedPrefixes, charToPrefixVector] = scorePrefixes(table, suffixes, prefixCounts);

        StringTable charToPrefix;
        for (auto && s: charToPrefixVector) {
            charToPrefix.add(s);
        }

        encoder.prefixes = retainedPrefixes;
        decoder = { { { charToPrefix } } };
    
        strings.reserve(table.size(), table.characters());

        for (std::string_view s: table) {
            auto encoded = encoder.encode(s);
            strings.add(encoded);
        }
    }

    StringTable16 strings;
    SuffixEncoder encoder;
    SuffixDecoder decoder;
};

template struct CharacterRangeTableImpl<CharacterRangeTableBase>;

CharacterRangeTable create_character_table(const std::vector<int> & charFrequencies, int start)
{
    using namespace std;
    
    int totalCount = std::accumulate(charFrequencies.begin(), charFrequencies.end(), 0);

    CharacterRangeTable result;

#if 1
    std::vector<std::pair<int, char> > sortedCharFrequencies;
    for (size_t i = 0;  i < charFrequencies.size();  ++i) {
        if (charFrequencies[i] <= 1) continue;
        sortedCharFrequencies.emplace_back(charFrequencies[i], i);
    }
    std::sort(sortedCharFrequencies.rbegin(), sortedCharFrequencies.rend());

    double totalBits = 0.0;
    cerr << "frequencies of " << sortedCharFrequencies.size() << " characters: " << endl;
    for (auto & [freq, ch] : sortedCharFrequencies) {
        double prob = 1.0 * freq / totalCount;
        double bits = -log2(prob) * freq;// - (1-prob) * log2(1-prob);
        totalBits += bits;
        cerr << "character " << ch << " " << (uint16_t)ch << " freq " << freq << " prob " << 100.0 * prob << "% bits " << bits << endl;
    }
    cerr << "totalBits = " << totalBits << endl;
#endif

    std::vector<uint32_t> characterCodes(charFrequencies.size() + 1);

    int accumCount = 0;

    for (size_t i = 0;  i < charFrequencies.size();  ++i) {
        int myChars = charFrequencies[i];
        accumCount += myChars;
        int mine = start + 1 + i + 1.0 * (65535 - charFrequencies.size() - start + 1) * accumCount / totalCount;
        characterCodes.at(i + 1) = mine;
        if (i != 0) {
            ExcAssert(characterCodes.at(i) < characterCodes.at(i + 1));
        }
        //cerr << "i = " << i << " myChars = " << myChars << " mine " << mine << endl;
    }

    cerr << "characterCodes.size() = " << characterCodes.size() << endl;
    cerr << "characterCodes.back() = " << characterCodes.back() << endl;

    ExcAssert(characterCodes.back() == 65536);

    result.codeRanges = std::move(characterCodes);
    return result;
}

template struct EntropyEncoderDecoderImpl<EntropyEncoderDecoderBase>;

struct EntropyCodeFilter {

    EntropyCodeFilter() = default;

    EntropyCodeFilter(const StringTable16 & table, const std::vector<CapStyle> & capStyles)
    {
        initialize(table, capStyles);
    }

    void initialize(const StringTable16 & table, const std::vector<CapStyle> & capStyles)
    {
        // Get character frequencies, needed to understand encoding entropy
        std::vector<int> charFrequencies(1);

        // Frequency of EOF is in charFrequencies[0]; each entry has one EOF entry
        charFrequencies[0] = table.size();

        for (auto s: table) {
            for (uint16_t c: s) {
                if (c >= charFrequencies.size()) {
                    charFrequencies.resize(c + 1);
                }
                charFrequencies.at(c) += 1;
            }
        }

        std::vector<int> capitalizationFrequencies(CapStyle::NUM_CAP_STYLES);

        for (CapStyle s: capStyles) {
            capitalizationFrequencies[s] += 1;
        }

        encoder.characterCodes = decoder.characterCodes = create_character_table(charFrequencies, 0 /*256*/);
        encoder.capitalizationCodes = decoder.capitalizationCodes = create_character_table(capitalizationFrequencies);

        // Reset to uniform probabilities for now
        //for (int i = 0;  i <= 256;  ++i) {
        //    characterCodes[i] = i == 0 ? 0 : 1 + i * 256;
        //}
        //characterCodes[256] = 65536;

        //for (int i = 0;  i <= 256;  ++i) {
        //    characterCodes[i] = i * 256;
        //}

        ExcAssert(table.size() == capStyles.size());

        for (size_t i = 0;  i < table.size();  ++i) {
            std::string storage;
            auto encoded = encoder.encode(table.get(i), capStyles[i]);
            strings.add(encoded);
        }
    }

    StringTable strings;
    EntropyEncoderDecoder encoder, decoder;
};

OptimizedStringTable optimize_string_table(const StringTable & table)
{
    using namespace std;

    // 1.  Analyze the capitalization, creating decapitalized strings
    //     and removing redundancy from capitalization patterns
    CapStyleFilter capFilter(table);

    const std::vector<CapStyle> & capStyles = capFilter.capStyles;
    const StringTable & decapitalizedStrings = capFilter.strings;

    ExcAssert(decapitalizedStrings.size() == table.size());
    ExcAssert(capStyles.size() == table.size());

    // 2.  Analyze the suffixes in the decapitalized strings, to
    //     remove redundancy from character sequences.
    SuffixFilter suffixFilter(decapitalizedStrings);

    const StringTable16 & compactedStrings = suffixFilter.strings;

    ExcAssert(compactedStrings.size() == table.size());

    // 3.  Use a range encoder to remove redundancy from having differing
    //     character patterns
    EntropyCodeFilter entropyFilter(compactedStrings, capStyles);

    OptimizedStringTable result;
    result.encoded = entropyFilter.strings;
    result.capDecoder = capFilter.decoder;
    result.suffixDecoder = suffixFilter.decoder;
    result.entropyDecoder = entropyFilter.decoder;

    cerr << "Encoding results: input   size      " << table.characters() << endl;
    cerr << "                  cap enc size      " << decapitalizedStrings.characters() << endl;
    cerr << "                  suffix enc size   " << compactedStrings.characters() << endl;
    cerr << "                  final size        " << entropyFilter.strings.characters() << endl;
    cerr << "                  memusage before   " << memUsage(table) << endl;
    cerr << "                  memusage after    " << memUsage(result) << endl;
    cerr << "                    encoded.mem     " << memUsage(result.encoded.mem) << endl;
    cerr << "                    encoded.offsets " << memUsage(result.encoded.offsets) << endl;
    cerr << "                    capEncoder      " << memUsage(result.capDecoder) << endl;
    cerr << "                    suffixDecoder   " << memUsage(result.suffixDecoder) << endl;
    cerr << "                    entropyDecoder  " << memUsage(result.entropyDecoder) << endl;
    
    // Test that all of the strings are faithfully decoded
    for (size_t i = 0;  i < table.size();  ++i) {
        auto s1 = table.get(i);
        auto s2 = result.get(i);

        if (s1 != s2) {
            
            auto dump = [] (auto str) -> std::string
            {
                std::ostringstream s;
                s << str << "\t";
                for (auto c: str) {
                    s << hex << setw(2 * sizeof(c)) << (unsigned)c << setw(0) << " " << dec;
                }
                return s.str();
            };

            std::string storage;
            cerr << "error on string " << i << ":" << endl;
            cerr << "original      = " << s1 << endl;
            cerr << "capEncoded    = " << dump(decapitalizedStrings.get(i)) << endl;
            cerr << "prefixDecoded = " << dump(suffixFilter.decoder.decode(compactedStrings.get(i))) << endl;
            cerr << "prefixEncoded = " << dump(compactedStrings.get(i)) << endl;
            cerr << "entropyDecoded= " << dump(entropyFilter.decoder.decode(entropyFilter.strings.get(i)).second) << endl;
            cerr << "fullyEncoded  = " << dump(entropyFilter.strings.get(i)) << endl;
            cerr << "reconstituted = " << dump(s2) << endl;
            cerr << endl;
            cerr << "------ encoding" << endl;
            entropyFilter.encoder.encode(compactedStrings.get(i), capStyles[i], true /* debug */);
            cerr << endl;
            cerr << "------ decoding" << endl;
            entropyFilter.decoder.decode(entropyFilter.strings.get(i), true /* debug */);
            ExcAssert(false);
        }
    }
 
    return result;
}

template struct OptimizedStringTableImpl<OptimizedStringTableBase>;

} // namespace MLDB
