/* suffix_array.cc                                              -*- C++ -*-
   Jeremy Barnes, 6 November 2015
   Copyright (c) 2015 mldb.ai inc.  All rights reserved.

   This file is part of MLDB. Copyright 2015 mldb.ai inc. All rights reserved.
*/

#include "suffix_array.h"
#include "mldb/ext/sais.h"
#include "mldb/arch/endian.h"
#include <bit>
#include <map>
#include <array>

using namespace std;

namespace MLDB {

void SuffixArray::construct(std::string_view str)
{
    // Number of characters in the suffix table
    size_t n = str.size();
    this->str = str;
    suffixes.resize(n);

    int result = saisxx((const unsigned char *)str.data(), suffixes.data(), (int)n, 256);
    if (result != 0) {
        std::cerr << "sais result " << result << std::endl;
        MLDB_THROW_RUNTIME_ERROR("Suffix array construction failed");
    }
}

void MultiSuffixArray::finish_construct()
{        
    using namespace std;
    
    auto offsetToStringIndex = [this] (int offset) -> int
    {
        ExcAssert(offset >= 0);
        ExcAssert(offset < offsets.back());
        ExcAssert(!offsets.empty());
        int pos = std::upper_bound(offsets.begin() + 1, offsets.end(), offset) - offsets.begin() - 1;
        //using namespace std;
        //cerr << "offset " << offset << " pos " << pos << " offsets[pos] " << offsets[pos] << " offsets[pos + 1] " << offsets[pos + 1] << endl;
        ExcAssert(offset >= offsets[pos]);
        ExcAssert(offset < offsets[pos + 1]);
        return pos;
    };

    //cerr << "constructing array" << endl;

    // Construct the sorted suffix array
    array.construct(str);

    //cerr << "array.size() = " << array.size() << endl;

    size_t totalLength = str.length();
    size_t numStrings = offsets.size() - 1;

    // Now get our iteration entries.  This is not the same as the suffix entries,
    // as we skip those that correspond just to nulls.  We also store some extra
    // information.
    entries.reserve(totalLength - numStrings);

    // For each entry, figure out in which string it's embedded
    for (auto sit = array.begin(), send = array.end();  sit != send;  ++sit) {

        // This suffix starts at which offset in the giant concatenated string?
        int offset = SuffixArray::offset(sit);

        char c = str[offset];

        //cerr << "offset " << offset << " c " << (int)c << endl;

        // This one points to the null terminator of a string, we can skip it
        if (c == 0)
            continue;

        // Which string does it belong to?
        int stringNumber = offsetToStringIndex(offset);

        // Where does that string start?
        int stringStart = offsets[stringNumber];

        // Where does that string end?
        //int stringEnd = offsets[stringNumber + 1];

        //cerr << "offset " << offset << " stringNumber " << stringNumber << " start " << stringStart <<" end " << stringEnd << endl;

        // Write our entry
        entries.emplace_back(stringNumber, offset - stringStart);
    }
}

size_t commonPrefixLength2(const char * p1, const char * p2, size_t maxLen)
{
    // Narrow down for the last 8 bytes
    for (auto i = 0; i < maxLen;  ++i) {
        if (p1[i] != p2[i]) {
            return i;
        }
    }

    return maxLen;
}

static size_t commonPrefixLength(const char * p1, const char * p2, size_t maxLen)
{
    auto n = 0;

    auto l = maxLen;
    auto lp1 = (const uint64_t *)p1;
    auto lp2 = (const uint64_t *)p2;

    // 64 bits at a time
    for (; l >= 8;  l-=8, n+=8, ++lp1, ++lp2) {
        auto xx = *lp1 ^ *lp2;
        if (xx == 0)
            continue;
#if (__BYTE_ORDER__ == __ORDER_LITTLE_ENDIAN__)
        return n + (std::countr_zero(xx) / 8);
#else
        return n + 8 - (std::countl_zero(xx) / 8);
#endif
    }

    // Narrow down for the last 8 bytes
    for (auto i = n; i < maxLen;  ++i) {
        if (p1[i] != p2[i]) {
            return i;
        }
    }

    return maxLen;
}

static std::string_view
commonPrefix(const std::string_view & s1, const std::string_view & s2, uint32_t maxLen)
{
    //cerr << "common prefix between " << s1.substr(0, 50) << " and " << s2.substr(0, 50) << endl;
    auto l = std::min<size_t>(std::min(s1.length(), s2.length()), maxLen);
    auto pl = commonPrefixLength(s1.data(), s2.data(), l);
    //auto pl2 = commonPrefixLength2(s1.data(), s2.data(), l);
    //ExcAssertEqual(pl, pl2);
    return s1.substr(0,pl);
}

template<typename Char, typename Value>
struct Trie {
    static constexpr size_t N = 1ULL << (8 * sizeof(Char));
    using char_type = Char;
    using value_type = Value;

    template<typename Seq>
    Value & operator [] (const Seq & seq)
    {
        auto [val, inserted] = root_.get(seq.begin(), seq.end());
        if (inserted)
            size_ += 1;
        return *val;
    }

    size_t size() const { return size_; }

private:
    struct Node {
        std::array<Node *, N> children = { nullptr };
        Value val = Value();

        ~Node()
        {
            for (auto & c: children)
                delete c;
        }

        template<typename It>
        std::tuple<Value *, bool> get(It first, It last)
        {
            if (first == last)
                return {&val, false};
            Node * & n = children[*first++];
            if (!n) {
                n = new Node();
            }
            auto [val, inserted] = n->get(first, last);
            return { val, true };
        }
    };

    Node root_;
    size_t size_ = 0;
};

std::vector<std::pair<std::string_view, uint32_t>>
countPrefixes(const MultiSuffixArray & suffixes, uint32_t maxLen)
{
    using namespace std;

    std::vector<std::pair<std::string_view, uint32_t>> prefixCounts;

    std::vector<int> prefixStarts;  // index in the suffix array at which prefix of each length started

    size_t numInsertions = 0;

    auto calcPrefixes = [&] (std::string_view prefix, std::string_view lastStr, int index)
    {
        // If our prefix is shorter, some prefixes ended so we write out their results
        //cerr << "prefixStarts.length() = " << prefixStarts.size() << " prefix " << prefix << " length " << prefix.length() << endl;

        bool first = true;
        while (prefixStarts.size() > prefix.length()) {
            auto n = index - prefixStarts.back() + 1;
            if (first || n > 2) {
                //cerr << "  adding " << n << " to " << lastStr.substr(0, prefixStarts.size()) << endl;
                prefixCounts.emplace_back(lastStr.substr(0, prefixStarts.size()), n);
                ++numInsertions;
            }
            first = false;
            prefixStarts.pop_back();
        }

        // And any increased prefix length starts here
        prefixStarts.resize(prefix.length(), index);
    };

    std::string_view lastStr;

    //cerr << "suffixes.size() = " << suffixes.size() << endl;

    for (size_t i = 0;  i < suffixes.size();  ++i) {
        auto [str,index,start] = suffixes.at(i);
        std::string_view prefix = commonPrefix(str, lastStr, maxLen);
        //cerr << "i = " << i << " commonPrefix = " << prefix << endl;
        calcPrefixes(prefix, lastStr, i);
        lastStr = str;
    }

    // Write out the last of the prefixes
    calcPrefixes("", lastStr, suffixes.size());

    //cerr << "numInsertions = " << numInsertions << " prefixCounts.size() = " << prefixCounts.size() << endl;

    return prefixCounts;
}


} // namespace MLDB
