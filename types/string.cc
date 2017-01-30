// This file is part of MLDB. Copyright 2015 mldb.ai inc. All rights reserved.

/* string.cc
   Sunil Rottoo, 27 April 2012
   Copyright (c) 2012 mldb.ai inc.  All rights reserved.

*/

#include "string.h"
#include "mldb/ext/jsoncpp/json.h"
#include <iostream>
#include "mldb/arch/exception.h"
#include <unicode/unistr.h>
#include "mldb/base/exc_assert.h"
#include <boost/algorithm/string.hpp>
#include <boost/locale.hpp>
#include "mldb/arch/demangle.h"
#include <cxxabi.h>

using namespace std;


namespace MLDB {


/*****************************************************************************/
/* UTF8STRING                                                                */
/****************************************************************************/

Utf8String
Utf8String::fromLatin1(const std::string & lat1Str)
{
    size_t bufferSize = lat1Str.size();
    const char *inBuf = lat1Str.c_str();
    string utf8Str(bufferSize * 4, '.');

    auto iter = utf8Str.begin();
    auto start = iter;
    for (size_t i = 0; i < bufferSize; i++) {
        uint32_t cp(inBuf[i] & 0xff);
        iter = utf8::append(cp, iter);
    }
    utf8Str.resize(iter-start);

    // No need to check for valid code points, since already done before
    return Utf8String(std::move(utf8Str), false /* check */);
}

Utf8String
Utf8String::fromLatin1(std::string && lat1Str)
{
    // Special case: all ASCII characters
    bool allAscii = true;
    for (auto & c: lat1Str) {
        if (c >= ' ' && c < 127)
            continue;
        allAscii = false;
        break;
    }

    if (allAscii)
        return Utf8String(std::move(lat1Str), false /* check */);

    size_t bufferSize = lat1Str.size();
    const char *inBuf = lat1Str.c_str();
    string utf8Str(bufferSize * 4, '.');

    auto iter = utf8Str.begin();
    auto start = iter;
    for (size_t i = 0; i < bufferSize; i++) {
        uint32_t cp(inBuf[i] & 0xff);
        iter = utf8::append(cp, iter);
    }
    utf8Str.resize(iter-start);

    // No need to check for valid code points, since already done before
    return Utf8String(std::move(utf8Str), false /* check */);
}

Utf8String::Utf8String(const Utf8String & str, size_t startAt, ssize_t endAt)
{
    auto it = str.begin(), end = str.end();
    while (it != end && startAt--)
        ++it;
    if (endAt == -1) {
        data_ = string(it.base(), str.data_.end());
        return;
    }

    auto fin = it;
    while (fin != end && endAt--)
        ++fin;

    data_ = string(it, fin);
}

Utf8String::Utf8String(const string & in, bool check)
    : data_(in)
{
    if (check)
        doCheck();
}

Utf8String::Utf8String(const char *start, size_t len, bool check)
    :data_(start, len)
{
    if (check)
        doCheck();
}

Utf8String::Utf8String(string && in, bool check)
    : data_(std::move(in))
{
    if (check)
        doCheck();
}

Utf8String::Utf8String(const char * in, bool check)
    : data_(in)
{
    if (check)
        doCheck();
}

Utf8String::Utf8String(const std::basic_string<char32_t> & str)
{
    // TODO: less inefficient way of doing it...

    Utf8String result;
    for (auto & c: str)
        result += c;
    
    *this = std::move(result);
}

Utf8String::Utf8String(const wchar_t * str)
{
    // TODO: less inefficient way of doing it...

    Utf8String result;
    for (; *str; ++str)
        result += *str;
    
    *this = std::move(result);
}


Utf8String::Utf8String(const_iterator first, const const_iterator & last)
    : data_(first.base(), last.base())
{
    // No need to check, since it comes from an Utf8String where it must
    // have been checked already.
}

void
Utf8String::
doCheck() const
{
    // Check if we find an invalid encoding
    string::const_iterator end_it = utf8::find_invalid(data_.begin(), data_.end());
    if (end_it != data_.end())
        {
            throw MLDB::Exception("Invalid sequence within utf-8 string");
        }
}

Utf8String::iterator
Utf8String::begin()
{
    return iterator(data_.begin(), data_.begin(), data_.end()) ;
}

Utf8String::iterator
Utf8String::end()
{
    return iterator(data_.end(), data_.begin(), data_.end()) ;
}

Utf8String::const_iterator
Utf8String::begin() const
{
    return const_iterator(data_.begin(), data_.begin(), data_.end()) ;
}

Utf8String::const_iterator
Utf8String::end() const
{
    return const_iterator(data_.end(), data_.begin(), data_.end()) ;
}

Utf8String &Utf8String::operator+=(const Utf8String &utf8str)
{
    data_ += utf8str.data_;
    return *this;
}

Utf8String& Utf8String::operator += (char32_t ch)
{
    char buf[16];  // shouldn't need more than 5
    char * p = buf;

    p = utf8::append(ch, p);

    data_.append(buf, p - buf);

    return *this;
}


std::ostream & operator << (std::ostream & stream, const Utf8String & str)
{
    stream << string(str.rawData(), str.rawLength()) ;
    return stream;
}

string Utf8String::extractAscii() const
{
    string s;
    for(auto it = begin(); it != end(); it++) {
        int c = *it;
        if (c >= ' ' && c < 127) {
            s += c;
        } else {
            s += '?';
        }
    }
    return s;
}

bool
Utf8String::isAscii() const
{
    for(auto it = begin(); it != end(); it++) {
        int c = *it;
        if (c < 0 || c >= 128)
            return false;
    }
    return true;
}

size_t Utf8String::length() const
{
    return std::distance(begin(), end());
}

void
Utf8String::
reserve(size_t capacity)
{
    data_.reserve(capacity);
}

size_t
Utf8String::
capacity() const
{
    return data_.capacity();
}

Utf8String::const_iterator
Utf8String::
find(int c) const
{
    return std::find(begin(), end(), c);
}

Utf8String::const_iterator
Utf8String::
find(const char * s) const
{
    return find(Utf8String(s));
}

Utf8String::const_iterator
Utf8String::
find(const wchar_t * s) const
{
    return find(Utf8String(s));
}

Utf8String::const_iterator
Utf8String::
find(const std::string & s) const
{
    return find(Utf8String(s));
}

Utf8String::const_iterator
Utf8String::
find(const Utf8String & s) const
{
    return std::search(begin(), end(),
                       s.begin(), s.end());
}

Utf8String::iterator
Utf8String::
find(int c)
{
    return std::find(begin(), end(), c);
}

Utf8String::iterator
Utf8String::
find(const char * s)
{
    return find(Utf8String(s));
}

Utf8String::iterator
Utf8String::
find(const wchar_t * s)
{
    return find(Utf8String(s));
}

Utf8String::iterator
Utf8String::
find(const std::string & s)
{
    return find(Utf8String(s));
}

Utf8String::iterator
Utf8String::
find(const Utf8String & s)
{
    return std::search(begin(), end(),
                       s.begin(), s.end());
}

Utf8String::const_iterator
Utf8String::
rfind(int c) const
{
    if (empty())
        return end();

    for (auto it = std::prev(end()), beg = begin(); it != beg;  --it) {
        //cerr << "checking " << (char)*it << " at position " << std::distance(begin(), it) << endl;

        if (*it == c)
            return it;
    }

    if (*begin() == c)
        return begin();

    return end();
}

Utf8String::const_iterator
Utf8String::
rfind(const char * s) const
{
    return rfind(Utf8String(s));
}

Utf8String::const_iterator
Utf8String::
rfind(const wchar_t * s) const
{
    return rfind(Utf8String(s));
}

Utf8String::const_iterator
Utf8String::
rfind(const std::string & s) const
{
    return rfind(Utf8String(s));
}

Utf8String::const_iterator
Utf8String::
rfind(const Utf8String & s) const
{
    return const_cast<Utf8String &>(*this).rfind(s);
}

Utf8String::iterator
Utf8String::
rfind(int c)
{
    if (empty())
        return end();

    for (auto it = std::prev(end()), beg = begin(); it != beg;  --it) {
        //cerr << "checking " << (char)*it << " at position " << std::distance(begin(), it) << endl;

        if (*it == c)
            return it;
    }

    if (*begin() == c)
        return begin();

    return end();
}

Utf8String::iterator
Utf8String::
rfind(const char * s)
{
    return rfind(Utf8String(s));
}

Utf8String::iterator
Utf8String::
rfind(const wchar_t * s)
{
    return rfind(Utf8String(s));
}

Utf8String::iterator
Utf8String::
rfind(const std::string & s)
{
    return rfind(Utf8String(s));
}

Utf8String::iterator
Utf8String::
rfind(const Utf8String & s)
{
    if (s.empty() || empty())
        return end();

    int pivot = *s.begin();

    // Point to the first place it could match
    auto it = end();
    for (size_t i = 0;  i < s.length();  ++i) {
        if (it == begin())
            return end();
        --it;
    }
    
    // Check each character for a match
    while (1) {
        // Is there a match here?

        if (*it == pivot) {
            // potential match.  Look forwards to see if the whole thing matches
            bool found = true;
            auto itb = std::next(it);
            for (auto it2 = std::next(s.begin()), end2 = s.end();  it2 != end2 && found;
                 ++it2, ++itb) {
                found = *it2 == *itb;
            }
            if (found)
                return it;
        }

        if (it == begin())
            return end();  // not found

        --it;
    }
}

bool Utf8String::startsWith(const Utf8String & prefix) const
{
    auto it1 = begin(), end1 = end();
    auto it2 = prefix.begin(), end2 = prefix.end();

    while (it1 != end1 && it2 != end2 && *it1 == *it2) {
        ++it1;
        ++it2;
    }

    return it2 == end2;
}

bool Utf8String::startsWith(const char * prefix) const
{
    return startsWith(Utf8String(prefix));
}

bool Utf8String::startsWith(const std::string & prefix) const
{
    return startsWith(Utf8String(prefix));
}

bool Utf8String::endsWith(const Utf8String & suffix) const
{
    if (suffix.empty())
        return true;
    if (empty())
        return false;

    auto it1 = boost::prior(end()), start1 = begin();
    auto it2 = boost::prior(suffix.end()), start2 = suffix.begin();
    
    for (;;) {
        if (*it1 != *it2)
            return false;
        if (it2 == start2)
            return true;
        if (it1 == start1)
            return false;
        --it1;
        --it2;
    }
}

bool Utf8String::endsWith(const char * prefix) const
{
    return endsWith(Utf8String(prefix));
}

bool Utf8String::endsWith(const std::string & suffix) const
{
    return endsWith(Utf8String(suffix));
}

bool
Utf8String::
removePrefix(const Utf8String & prefix)
{
    auto it1 = begin(), end1 = end();
    auto it2 = prefix.begin(), end2 = prefix.end();

    while (it1 != end1 && it2 != end2 && *it1 == *it2) {
        ++it1;
        ++it2;
    }

    if (it2 == end2) {
        data_.erase(data_.begin(), it1.base());
        return true;
    }

    return false;
}

bool
Utf8String::
removeSuffix(const Utf8String & suffix)
{
    if (suffix.empty())
        return true;
    if (empty())
        return false;

    auto it1 = boost::prior(end()), start1 = begin();
    auto it2 = boost::prior(suffix.end()), start2 = suffix.begin();
    
    for (;;) {
        if (*it1 != *it2)
            return false;
        if (it2 == start2) {
            data_.erase(it1.base(), data_.end());
            return true;
        }
        if (it1 == start1)
            return false;
        --it1;
        --it2;
    }
}

void Utf8String::replace(ssize_t startIndex, ssize_t endIndex,
                         const Utf8String & replaceWith)
{
    std::basic_string<char32_t> str(begin(), end());
    std::basic_string<char32_t> replaceWith2(replaceWith.begin(), replaceWith.end());

    str.replace(startIndex, endIndex, replaceWith2);

    Utf8String result(str);
    *this = std::move(result);
}

void
Utf8String::
erase(iterator first, iterator last)
{
    // Assuming it's a valid range, we can perform the operation on the
    // underlying string
    ExcAssert(first.base() >= data_.begin());
    ExcAssert(first.base() <= data_.end());
    ExcAssert(last.base() >= data_.begin());
    ExcAssert(last.base() <= data_.end());
    ExcAssert(first.base() < last.base() || first.base() == last.base());
    data_.erase(first.base(), last.base());
}

Utf8String
Utf8String::
toLower() const
{
    return boost::locale::to_lower(data_);
}

Utf8String
Utf8String::
toUpper() const
{
    return boost::locale::to_upper(data_);
}

Utf8String::iterator
Utf8String::
wrapIterator(std::string::iterator it)
{
    return iterator(it, data_.begin(), data_.end());
}

Utf8String::const_iterator
Utf8String::
wrapIterator(std::string::const_iterator it) const
{
    return const_iterator(it, data_.begin(), data_.end());
}


/*****************************************************************************/
/* UTF32STRING                                                                */
/****************************************************************************/

Utf32String Utf32String::fromLatin1(const std::string &str) {
    std::u32string u32str;
    for (auto c: str) {
        u32str.push_back(static_cast<char32_t>(static_cast<uint8_t>(c)));
    }

    Utf32String ret;
    ret.data_ = u32str;
    return ret;
}

Utf32String Utf32String::fromUtf8(const Utf8String &str) {
    return Utf32String(str.rawString());
}

string Utf32String::extractAscii() const {
    string ascii;
    for (auto c: data_) {
        if ((c & 0x80) == 0)
            ascii += c;
        else
            ascii += '?';
    }

    return ascii;
}

Utf32String operator+(Utf32String lhs, const Utf32String &rhs) {
    return lhs += rhs;
}


std::ostream & operator << (std::ostream & stream, const Utf32String & str)
{
    return stream;
}

namespace {

struct AtInit {
    AtInit()
    {
        boost::locale::generator gen;
        locale loc=gen("en_US.UTF-8"); 
        locale::global(loc); 
        cout.imbue(loc);
        cerr.imbue(loc);
    }

} atInit;

} // file scope

Utf8String getUtf8ExceptionString()
{
    return getExceptionString();
}

} // namespace MLDB
