/* string.cc
   Sunil Rottoo, 27 April 2012
   Copyright (c) 2012 mldb.ai inc.  All rights reserved.
   This file is part of MLDB. Copyright 2015 mldb.ai inc. All rights reserved.

*/

#include "string.h"
#include "mldb/ext/jsoncpp/json.h"
#include <iostream>
#include "mldb/arch/exception.h"
#include <unicode/unistr.h>
#include "mldb/base/exc_assert.h"
#include "mldb/utils/split.h"
#include <locale>
#include "mldb/arch/demangle.h"
#include <cxxabi.h>
#include <unicode/unistr.h>
#include <clocale>
#include <algorithm>
#include <filesystem>

using namespace std;


namespace MLDB {

// Put this here, since it needs Utf8String to work
Exception::Exception(const Utf8String & msg)
    : message(msg.rawString())
{
}

Exception::
Exception(int errnum, const Utf8String & msg, const char * function)
: Exception(errnum, msg.rawString(), function)
{
}


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

Utf8String::operator std_filesystem_path() const
{
    return std_filesystem_path(data_);
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

template<typename InputIterator>
void
Utf8String::
init_from_range(InputIterator start, InputIterator end, bool check)
{
    constexpr size_t size_of_char = sizeof(typename std::iterator_traits<InputIterator>::value_type);
    if constexpr (size_of_char == 1) {
        init_from_range_u8(start, end, check);
    } else if constexpr (size_of_char == 2) {
        init_from_range_u16(start, end, check);
    } else if constexpr (size_of_char == 4) {
        init_from_range_u32(start, end, check);
    } else {
        throw MLDB::Exception("Unsupported character size: " + std::to_string(size_of_char));
    }
}

template<typename Char>
void
Utf8String::
init_from_null_terminated(const Char * start, bool check)
{
    constexpr size_t size_of_char = sizeof(*start);
    if constexpr (size_of_char == 1) {
        init_from_null_terminated_u8(start, check);
    } else if constexpr (size_of_char == 2) {
        init_from_null_terminated_u16(start, check);
    } else if constexpr (size_of_char == 4) {
        init_from_null_terminated_u32(start, check);
    } else {
        throw MLDB::Exception("Unsupported character size: " + std::to_string(size_of_char));
    }
}

template<typename InputIterator>
void
Utf8String::
init_from_range_u8(InputIterator start, InputIterator end, bool check)
{
    data_ = std::string(start, end);
    if (check) doCheck();
}

template<typename Char>
void
Utf8String::
init_from_range_u8(const Char * start, const Char * end, bool check)
{
    data_ = string((const char *)start, (const char *)end);
    if (check) doCheck();
}

template<typename Char>
void
Utf8String::
init_from_null_terminated_u8(const Char * start, bool check)
{
    static_assert(sizeof(Char) == 1, "Char must be 1 byte");
    data_ = std::string((const char *)start);
    if (check) doCheck();
}

template<typename InputIterator>
void
Utf8String::
inefficient_init_from_range_wide(InputIterator start, InputIterator end, bool check)
{
    // TODO: less inefficient way of doing it...

    Utf8String result;
    for (auto it = start; it != end; ++it)
        result += *it;
    
    *this = std::move(result);

    // No need to check, we did it one character at a time
}

template<typename Char>
void
Utf8String::
inefficient_init_from_null_terminated_wide(const Char * start, bool check)
{
    // TODO: less inefficient way of doing it...

    Utf8String result;
    for (; *start; ++start)
        result += *start;

    *this = std::move(result);

    // no need to check, we did it one character at a time
}

template<typename InputIterator>
void
Utf8String::
init_from_range_u16(InputIterator start, InputIterator end, bool check)
{
    inefficient_init_from_range_wide(start, end, check);
}

template<typename Char>
void
Utf8String::
init_from_range_u16(const Char * start, const Char * end, bool check)
{
    inefficient_init_from_range_wide(start, end, check);
}

template<typename Char>
void
Utf8String::
init_from_null_terminated_u16(const Char * start, bool check)
{
    static_assert(sizeof(Char) == 2, "Char must be 2 bytes");
    inefficient_init_from_null_terminated_wide(start, check);
}

template<typename InputIterator>
void
Utf8String::
init_from_range_u32(InputIterator start, InputIterator end, bool check)
{
    inefficient_init_from_range_wide(start, end, check);
}

template<typename Char>
void
Utf8String::
init_from_range_u32(const Char * start, const Char * end, bool check)
{
    static_assert(sizeof(Char) == 4, "Char must be 4 bytes");
    inefficient_init_from_range_wide(start, end, check);
}

template<typename Char>
void
Utf8String::
init_from_null_terminated_u32(const Char * start, bool check)
{
    static_assert(sizeof(Char) == 4, "Char must be 4 bytes");
    inefficient_init_from_null_terminated_wide(start, check);
}

Utf8String::Utf8String(const char *start, size_t len, bool check)
    :data_(start, len)
{
    if (check)
        doCheck();
}

Utf8String::Utf8String(const char *start, const char * end, bool check)
    :data_(start, end)
{
    if (check)
        doCheck();
}

Utf8String::Utf8String(std::string str, bool check)
    : data_(std::move(str))
{
    if (check)
        doCheck();
}

Utf8String::Utf8String(const std::wstring & str, bool check)
{
    init_from_range(str.begin(), str.end(), check);
}

Utf8String::Utf8String(const std::u8string & str, bool check)
{
    init_from_range(str.begin(), str.end(), check);
}

Utf8String::Utf8String(const std::u16string & str, bool check)
{
    init_from_range(str.begin(), str.end(), check);
}

Utf8String::Utf8String(const std::u32string & str, bool check)
{
    init_from_range(str.begin(), str.end(), check);
}

Utf8String::Utf8String(const char * in, bool check)
{
    init_from_null_terminated(in, check);
}

Utf8String::Utf8String(const char8_t *start, size_t len, bool check)
{
    init_from_range(start, start + len, check);
}

Utf8String::Utf8String(const char8_t *start, const char8_t * end, bool check)
{
    init_from_range(start, end, check);
}

Utf8String::Utf8String(const char8_t * in, bool check)
{
    init_from_null_terminated(in, check);
}

Utf8String::Utf8String(const wchar_t * in, bool check)
{
    init_from_null_terminated(in, check);
}

Utf8String::Utf8String(const char16_t *start, bool check)
{
    init_from_null_terminated(start, check);
}

Utf8String::Utf8String(const char32_t *start, bool check)
{
    init_from_null_terminated(start, check);
}

Utf8String::Utf8String(std::string_view str, bool check)
{
    init_from_range(str.begin(), str.end(), check);
}

Utf8String::Utf8String(std::wstring_view str, bool check)
{
    init_from_range(str.begin(), str.end(), check);
}

Utf8String::Utf8String(std::u8string_view str, bool check)
{
    init_from_range(str.begin(), str.end(), check);
}

Utf8String::Utf8String(std::u16string_view str, bool check)
{
    init_from_range(str.begin(), str.end(), check);
}

Utf8String::Utf8String(std::u32string_view str, bool check)
{
    init_from_range(str.begin(), str.end(), check);
}

Utf8String::Utf8String(std::span<char> str, bool check)
{
    init_from_range(str.begin(), str.end(), check);
}

Utf8String::Utf8String(std::span<wchar_t> str, bool check)
{
    init_from_range(str.begin(), str.end(), check);
}

Utf8String::Utf8String(std::span<char8_t> str, bool check)
{
    init_from_range(str.begin(), str.end(), check);
}

Utf8String::Utf8String(std::span<char16_t> str, bool check)
{
    init_from_range(str.begin(), str.end(), check);
}

Utf8String::Utf8String(std::span<char32_t> str, bool check)
{
    init_from_range(str.begin(), str.end(), check);
}

Utf8String::Utf8String(const const_iterator & first, const const_iterator & last, bool /* check */)
    : data_(first.base(), last.base())
{
    // No need to check, since it comes from an Utf8String where it must
    // have been checked already.
}

Utf8String::Utf8String(const iterator & first, const iterator & last, bool /* check */)
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
            throw MLDB::Exception("Invalid sequence within utf-8 string: pos "
                                  + std::to_string(end_it - data_.begin())
                                  + " chars: " + std::to_string((int)*end_it));
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

Utf8String::reverse_iterator
Utf8String::rbegin()
{
    return std::make_reverse_iterator(end());
}

Utf8String::reverse_iterator
Utf8String::rend()
{
    return std::make_reverse_iterator(begin());
}

Utf8String::const_reverse_iterator
Utf8String::rbegin() const
{
    return std::make_reverse_iterator(end());
}

Utf8String::const_reverse_iterator
Utf8String::rend() const
{
    return std::make_reverse_iterator(begin());
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
            throw MLDB::Exception("Not an ASCII string: " + std::to_string(c));
            //s += '?';
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

std::string
Utf8String::
uriEncode() const
{
    string result;
    for (unsigned c: data_) {

        if (isalnum(c) || c == '-' || c == '_' || c == '.' || c == '~' || c == '/')
            result += c;
        else result += MLDB::format("%%%02X", c);
    }

    return result;
}

std::string
Utf8String::
stealAsciiString()
{
    if (isAscii()) {
        return stealRawString();
    }
    else throw MLDB::Exception("Not an ASCII string");
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
find(int c, const_iterator start) const
{
    return std::find(start, end(), c);
}

Utf8String::const_iterator
Utf8String::
find(const char * s, const_iterator start) const
{
    return find(Utf8String(s), start);
}

Utf8String::const_iterator
Utf8String::
find(const wchar_t * s, const_iterator start) const
{
    return find(Utf8String(s), start);
}

Utf8String::const_iterator
Utf8String::
find(const std::string & s, const_iterator start) const
{
    return find(Utf8String(s), start);
}

Utf8String::const_iterator
Utf8String::
find(const Utf8String & s, const_iterator start) const
{
    // For now, shortcut through the pointers
    const char * startPtr = start.base().base();
    const char * endPtr = data_.end().base();
    const char * sPtr = s.rawData();
    const char * sEnd = sPtr + s.rawLength();

    const char * result = std::search(startPtr, endPtr, sPtr, sEnd);
    if (result == endPtr)
        return end();

    return const_iterator(data_.begin() + (result - rawData()), data_.begin(), data_.end());
}

Utf8String::iterator
Utf8String::
find(int c, iterator start)
{
    return std::find(start, end(), c);
}

Utf8String::iterator
Utf8String::
find(const char * s, iterator start)
{
    return find(Utf8String(s), start);
}

Utf8String::iterator
Utf8String::
find(const wchar_t * s, iterator start)
{
    return find(Utf8String(s), start);
}

Utf8String::iterator
Utf8String::
find(const std::string & s, iterator start)
{
    return find(Utf8String(s), start);
}

Utf8String::iterator
Utf8String::
find(const Utf8String & s, iterator start)
{
    // For now, shortcut through the pointers
    char * startPtr = start.base().base();
    char * endPtr = data_.end().base();
    const char * sPtr = s.rawData();
    const char * sEnd = sPtr + s.rawLength();

    char * result = std::search(startPtr, endPtr, sPtr, sEnd);
    if (result == endPtr)
        return end();

    return iterator(data_.begin() + (result - rawData()), data_.begin(), data_.end());
}

Utf8String::const_iterator
Utf8String::
rfind(int c, const_iterator start) const
{
    if (empty())
        return end();

    for (auto it = std::prev(start), beg = begin(); it != beg;  --it) {
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
rfind(const char * s, const_iterator start) const
{
    return rfind(Utf8String(s), start);
}

Utf8String::const_iterator
Utf8String::
rfind(const wchar_t * s, const_iterator start) const
{
    return rfind(Utf8String(s));
}

Utf8String::const_iterator
Utf8String::
rfind(const std::string & s, const_iterator start) const
{
    return rfind(Utf8String(s), start);
}

namespace { // file scope

template<typename String, typename StringIterator>
auto do_rfind(String && haystack, const Utf8String & s, StringIterator start) -> decltype(declval<String>().end())
{
    if (s.empty() || start == haystack.begin())
        return haystack.end();

    int pivot = *s.begin();

    // Point to the first place it could match
    auto it = start;
    for (size_t i = 0;  i < s.length();  ++i) {
        if (it == haystack.begin())
            return haystack.end();
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

        if (it == haystack.begin())
            return haystack.end();  // not found

        --it;
    }
}

} // file scope

Utf8String::const_iterator
Utf8String::
rfind(const Utf8String & s, const_iterator start) const
{
    return do_rfind(*this, s, start);
}

Utf8String::iterator
Utf8String::
rfind(int c, iterator start)
{
    if (empty())
        return end();

    for (auto it = std::prev(start), beg = begin(); it != beg;  --it) {
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
rfind(const char * s, iterator start)
{
    return rfind(Utf8String(s), start);
}

Utf8String::iterator
Utf8String::
rfind(const wchar_t * s, iterator start)
{
    return rfind(Utf8String(s), start);
}

Utf8String::iterator
Utf8String::
rfind(const std::string & s, iterator start)
{
    return rfind(Utf8String(s), start);
}

Utf8String::iterator
Utf8String::
rfind(const Utf8String & s, iterator start)
{
    return do_rfind(*this, s, start);
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

    auto it1 = std::prev(end()), start1 = begin();
    auto it2 = std::prev(suffix.end()), start2 = suffix.begin();
    
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

    auto it1 = std::prev(end()), start1 = begin();
    auto it2 = std::prev(suffix.end()), start2 = suffix.begin();
    
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

    Utf8String result(std::u32string_view{str});
    *this = std::move(result);
}

void
Utf8String::
erase(iterator first, iterator last)
{
    // Assuming it's a valid range, we can perform the operation on the
    // underlying string
    ExcCheck(first.base() >= data_.begin(), "erase: first is off-the-front");
    ExcCheck(first.base() <= data_.end(), "erase: first is past-the-end");
    ExcCheck(last.base() >= data_.begin(), "erase: last is off-the-front");
    ExcCheck(last.base() <= data_.end(), "erase: last is past-the-end");
    ExcCheck(first.base() < last.base() || first.base() == last.base(), "erase: first is after last");
    data_.erase(first.base(), last.base());
}

Utf8String
Utf8String::
toLower() const
{
    // TODO: less copying
    icu::UnicodeString us(data_.c_str());
    us.toLower();
    Utf8String result;
    us.toUTF8String(result.data_);
    return result;
}

Utf8String
Utf8String::
toUpper() const
{
    // TODO: less copying
    icu::UnicodeString us(data_.c_str());
    us.toUpper();
    Utf8String result;
    us.toUTF8String(result.data_);
    return result;
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
        std::setlocale(LC_ALL, "en_US.UTF-8");
        std::locale loc("en_US.UTF-8");
        cout.imbue(loc);
        cerr.imbue(loc);
    }

} atInit;

} // file scope

Utf8String getUtf8ExceptionString()
{
    return getExceptionString();
}

void to_lower(Utf8String & s)
{
    s = s.toLower();
}

} // namespace MLDB
