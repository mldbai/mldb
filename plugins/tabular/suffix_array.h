/* suffix_array.h                                               -*- C++ -*-
   Jeremy Barnes, 6 November 2015
   Copyright (c) 2015 mldb.ai inc.  All rights reserved.

   This file is part of MLDB. Copyright 2015 mldb.ai inc. All rights reserved.
*/

#pragma once

#include "string_table_iterator.h"
#include <string_view>
#include <iostream>
#include <vector>
#include "mldb/base/exc_assert.h"

namespace MLDB {

struct SuffixArray {
    SuffixArray() = default;
    SuffixArray(std::string_view str)
    {
        construct(str);
    }

    void construct(std::string_view str);

    std::string_view str;
    std::vector<int> suffixes;

    std::string_view at(size_t n) const
    {
        auto offset = suffixes.at(n);
        return std::string_view(str.data() + offset, size() - offset);
    }

    size_t size() const
    {
        return str.size();
    }

    using Iterator = StringTableIterator<const SuffixArray, std::string_view>;

    Iterator begin() const
    {
        return {this, 0};
    }

    Iterator end() const
    {
        return {this, 0};
    }

    static size_t offset(Iterator it)
    {
        return it.owner->suffixes.at(it.position());
    }
};

struct MultiSuffixArray {
    MultiSuffixArray() = default;

    template<typename ForwardIterator>
    MultiSuffixArray(ForwardIterator begin, ForwardIterator end)
    {
        construct(begin, end);
    }

    template<typename ForwardIterator>
    void construct(ForwardIterator begin, ForwardIterator end)
    {
        using namespace std;
        // First, concatenate all of the strings with null separators so they sort properly
        str.clear();
        offsets.clear();

        std::vector<std::pair<int, int> > startToString;

        size_t numStrings = std::distance(begin, end);

        offsets.reserve(numStrings + 1);
        offsets.push_back(0);

        size_t totalLength = 0;
        // Calculate the total length
        for (auto it = begin;  it != end;  ++it) {
            auto&& s = *it;
            //using namespace std;
            //cerr << "s = " << s << " s.length() = " << s.length() << " strlen(s.data()) = " << strlen(s.data()) << endl;
            totalLength += s.length() + 1;  // +1 for the null terminator
            offsets.push_back(totalLength);
        }

        str.reserve(totalLength);

        for (auto it = begin;  it != end;  ++it) {
            auto&& s = *it;
            //cerr << "appending " << s << endl;
            str.append(s, 0, s.length());
            str.append({'\0'});
        }

        ExcAssert(str.length() == totalLength);
        ExcAssert(offsets.size() == numStrings + 1);

        finish_construct();
    }

    void finish_construct();

    std::string str;
    SuffixArray array;
    std::vector<int> offsets;  //< Offset in string table for each one
    std::vector<std::pair<int, int>> entries;  // (string number, start offset) for each string

    std::tuple<std::string_view, int, int> at(size_t n) const
    {
        auto [stringNumber, startOffset] = entries.at(n);
        int stringOffset = offsets[stringNumber];
        int stringLength = offsets[stringNumber + 1] - stringOffset - 1;
        std::string_view view(str.data() + stringOffset + startOffset, stringLength - startOffset);
        return std::make_tuple(view, stringNumber, startOffset);
    }

    size_t size() const
    {
        return entries.size();
    }

    // STL compatible iterator that allows us to iterate over the sorted substrings in the
    // suffix array
    using Iterator = StringTableIterator<const MultiSuffixArray, std::tuple<std::string_view, int, int> >;

    Iterator begin() const
    {
        return {this, 0};
    }

    Iterator end() const
    {
        return {this, size()};
    }
};

} // namespace MLDB

