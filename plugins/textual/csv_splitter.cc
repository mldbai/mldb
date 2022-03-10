/** for_each_line.cc
    Jeremy Barnes, 29 November 2013
    Copyright (c) 2013 mldb.ai inc.  All rights reserved.
    This file is part of MLDB. Copyright 2015 mldb.ai inc. All rights reserved.

*/

#include "csv_splitter.h"
#include <iostream>
#include "mldb/types/annotated_exception.h"
#include "mldb/types/basic_value_descriptions.h"
#include "mldb/base/exc_assert.h"

using namespace std;

namespace MLDB {

CsvLineEncoding parseCsvLineEncoding(const std::string & encodingStr_)
{
    std::string encodingStr;
    for (auto & c: encodingStr_)
        encodingStr += tolower(c);

    CsvLineEncoding encoding;
    if (encodingStr == "us-ascii" || encodingStr == "ascii") {
        encoding = ASCII;
    }
    else if (encodingStr == "utf-8" || encodingStr == "utf8") {
        encoding = UTF8;
    }
    else if (encodingStr == "latin1" || encodingStr == "iso8859-1")
        encoding = LATIN1;
    else throw AnnotatedException(400, "Unknown encoding '" + encodingStr_
                                   + "'for import.text parser: accepted are "
                                   "'us-ascii', 'ascii', 'utf-8', 'utf8', "
                                   "'latin1', 'iso8859-1'",
                                   "encoding", encodingStr);
    return encoding;
}

std::pair<const char *, CSVSplitterState>
CSVSplitter::
nextBlockT(const char * block1, size_t n1, const char * block2, size_t n2,
            bool noMoreData,
            const CSVSplitterState & state) const
{
    const char * p = block1;
    const char * e = block1 + n1;
    int blockNum = 1;

    //cerr << "looking for next block with " << n << " characters" << endl;

    //cerr << "block1 = " << (const void *)block1 << endl;
    //cerr << "n1 = " << n1 << endl;
    //cerr << "block2 = " << (const void *)block2 << endl;
    //cerr << "n2 = " << n2 << endl;

    bool has8bit = false;
    bool atEnd = n1 == 0 && n2 == 0;
    int c = (atEnd ? -1 : *p);

    auto use_next_block = [&] () -> bool
    {
        if (blockNum != 1 || !block2 || n2 == 0)
            return false;
        cerr << "new block" << endl;
        if (p != e) {
            cerr << "blockNum = " << blockNum << endl;
            cerr << "p = " << (const void *)p << endl;
            cerr << "e = " << (const void *)e << endl;
            cerr << "block1 = " << (const void *)block1 << endl;
            cerr << "n1 = " << n1 << endl;
            cerr << "block2 = " << (const void *)block2 << endl;
            cerr << "n2 = " << n2 << endl;
            cerr << "has8bit = " << has8bit << endl;
        }
        ExcAssert(p == e);
        p = block2;
        e = block2 + n2;
        return true;
    };

    auto next = [&] () -> bool
    {
        ExcAssert(!atEnd);
        ++p;
        if (MLDB_UNLIKELY(p == e)) {
            use_next_block();
            atEnd = (p == e);
        }
        c = atEnd ? -1 : *(unsigned char *)p;
        return atEnd;
    };

    auto skip = [&] (auto n) -> bool
    {
        if (MLDB_LIKELY(n < (e - p))) {
            p += n;
            c = *p;
            return true;
        }

        while (n > 0) {
            if (atEnd)
                return false;

            // Skip all but one
            size_t toSkip = std::min<size_t>(n, (e - p - 1));
            p += toSkip;
            n -= toSkip;

            // Skip the last
            if (next())
                n -= 1;
        }

        return n == 0;
    };

    auto peek = [&] () -> int
    {
        return c;
    };

    auto char_8bit = [&] (int c)
    {
        ExcAssert(c >= 0 && c <= 255);
        return c & 128;
    };

    auto find_next_match = [&] (auto && match) -> char
    {
        if (MLDB_LIKELY(!has8bit)) {
            do {
                for (; !atEnd;  next()) {
                    ExcAssert(c != -1);
                    if (match(c)) {
                        auto res = c;
                        next();
                        return res;
                    }
                    if (char_8bit(c) && encoding == CsvLineEncoding::UTF8) {
                        has8bit = true;
                        break;
                    }
                }
            } while (!atEnd && !has8bit);
        }
        if (MLDB_UNLIKELY(has8bit)) {
            // 8 bit characters means UTF-8 encodings

            for (; !atEnd; /* no inc */) {
                //cerr << "doing " << (int)*p << " with " << (e - p) << " chars left" << endl;
                if (char_8bit(c)) {
                    auto charlen = utf8::internal::sequence_length(p);
                    if (!skip(charlen))
                        return 0;  // ends in middle of utf-8 character
                }
                else {
                    if (match(*p))
                        return *p++;
                    ++p;
                }
            }
        }
        return 0;
    };

    auto find_next_of_1 = [&] (char tofind) -> char
    {
        if (MLDB_LIKELY(encoding == CsvLineEncoding::ASCII)) {
            while (!atEnd) {
                auto p2 = (const char *)memchr(p, tofind, e - p);
                size_t n = p2 ? p2 - p : e - p;
                bool success = skip(n);
                ExcAssert(success);
                if (c == tofind) {
                    next();
                    return tofind;
                }
                else {
                    return 0;
                }
            }
        }

        auto match = [tofind] (char cin) { return cin == tofind; };
        return find_next_match(match);
    };

    auto find_next_of_2 = [&] (char c1, char c2) -> char
    {
        if (MLDB_LIKELY(encoding == CsvLineEncoding::ASCII)) {
            for (; !atEnd;  next()) {
                if (c == c1 || c == c2) {
                    auto result = c;
                    next();
                    return result;
                }
            }
            return 0;
        }

        auto match = [c1,c2] (char cin) { return cin == c1 || cin == c2; };
        return find_next_match(match);
    };

    auto truncated = [] () -> std::pair<const char *, CSVSplitterState>
    {
        //cerr << "truncated" << endl;
        return { nullptr, {} };
    };

    while (!atEnd) {
        unsigned char found = find_next_of_2('\n', quoteChar);
        //cerr << "found char " << (int)found << (unsigned char)found << " with " << (e - p) << " left" << endl;
        if (found == quoteChar) {
            //cerr << "  in quote; *p = " << (int)*p << endl;
            // end of string means we're truncated
            int c2 = peek();

            //cerr << "in quote; c2 = " << c2 << endl;

            if (c2 == -1)
                return truncated();
            else if (c2 == quoteChar) {
                next();
                continue;  // double quote char means literal quote
            }

            // We're in a string; continue until we get a non-doubled quote
            for (;;) {
                if (allowMultiLine) {
                    found = find_next_of_1(quoteChar);
                }
                else {
                    found = find_next_of_2('\n', quoteChar);
                    if (found == '\n') {
                        // Newline in quoted field without allowMultiLine
                        // Will eventually result in an error, but let the parser
                        // deal with that
                        return { p, {} };
                    }
                }

                if (found != quoteChar)
                    return truncated();

                // Double quote
                if (peek() == quoteChar) {
                    next();
                    continue;
                }
                
                // Finished the string
                break;
            }
        }
        else if (found == '\n') {
            //cerr << "*** FINISHED LINE OUTSIDE OF STRING with " << (e - p) << " chars remaining" << endl;
            //cerr << string(current, p - 1) << endl;
            //cerr << "  *(p-1) = " << (int)(*(p-1)) << endl;

            return { p, {} };
        }
        else break;
    }

    return truncated();
}

std::span<const char>
CSVSplitter::
fixupBlock(std::span<const char> block) const
{
    return NewlineSplitter::removeNewlines(block);
}

} // namespace MLDB