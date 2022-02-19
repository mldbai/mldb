/* suffix_array.cc                                              -*- C++ -*-
   Jeremy Barnes, 6 November 2015
   Copyright (c) 2015 mldb.ai inc.  All rights reserved.

   This file is part of MLDB. Copyright 2015 mldb.ai inc. All rights reserved.
*/

#include "suffix_array.h"
#include "mldb/ext/sais.h"

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

    // Construct the sorted suffix array
    array.construct(str);

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

} // namespace MLDB
