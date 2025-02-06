/* string.h                                                          -*- C++ -*-
   Sunil Rottoo, 27 April 2012
   Copyright (c) 20102 mldb.ai inc.  All rights reserved.

   This file is part of MLDB. Copyright 2015 mldb.ai inc. All rights reserved.

   Basic classes for dealing with string including internationalisation
*/

#pragma once

#include <string>
#include <string_view>
#include <span>
#include "mldb/ext/utfcpp/source/utf8.h"
#include "mldb/compiler/compiler.h"
#include "mldb/compiler/filesystem_fwd.h"


namespace MLDB {


/*****************************************************************************/
/* Utf8String                                                               */
/*****************************************************************************/

namespace JS {
struct JSValue;
} // namespace JS


class Utf8String
{
public:
    using size_type = size_t;
    using value_type = char32_t;

    struct iterator: utf8::iterator<std::string::iterator> {
        using base_iterator = utf8::iterator<std::string::iterator>;
        using base_iterator::iterator;
        iterator(base_iterator it): base_iterator(it) {}
        iterator operator += (ssize_t n) { if (n < 0) return operator -= (-n); for (size_t i = 0; i < n; ++i) ++(*this); return *this; }
        iterator operator -= (ssize_t n) { if (n < 0) return operator += (-n); for (size_t i = 0; i < n; ++i) --(*this); return *this; }
        iterator operator + (ssize_t n) { iterator result = *this; result += n; return result; }
        iterator operator - (ssize_t n) { iterator result = *this; result -= n; return result; }
    };

    struct const_iterator: utf8::iterator<std::string::const_iterator> {
        using base_iterator = utf8::iterator<std::string::const_iterator>;
        using base_iterator::iterator;
        const_iterator(base_iterator it): base_iterator(it) {}
        const_iterator operator += (ssize_t n) { if (n < 0) return operator -= (-n); for (size_t i = 0; i < n; ++i) ++(*this); return *this; }
        const_iterator operator -= (ssize_t n) { if (n < 0) return operator += (-n); for (size_t i = 0; i < n; ++i) --(*this); return *this; }
        const_iterator operator + (ssize_t n) { const_iterator result = *this; result += n; return result; }
        const_iterator operator - (ssize_t n) { const_iterator result = *this; result -= n; return result; }
    };

    using const_reverse_iterator = std::reverse_iterator<const_iterator>;
    using reverse_iterator = std::reverse_iterator<iterator>;

    static Utf8String fromLatin1(const std::string & lat1Str);
    static Utf8String fromLatin1(std::string && lat1Str);

    operator std::basic_string_view<char8_t>() const { return {(const char8_t *)data_.data(), data_.length()}; }
    operator std_filesystem_path() const;
    Utf8String(const std_filesystem_path & path, bool check = true);

    /** Allow default construction of an empty string. */
    Utf8String()
    {
    }

    /** Move constructor. */
    Utf8String(Utf8String && str) noexcept
        : data_(std::move(str.data_))
    {
    }

    /** Copy constructor. */
    Utf8String(const Utf8String & str)
        : data_(str.data_)
    {
    }

    /** Copy part of another string, by index. */
    Utf8String(const Utf8String & str, size_t startAt, ssize_t endAt = -1);

    /**
     * Take possession of a utf8-encoded string.
     * @param input A string that contains utf8-encoded characters
     * @param check If true we will make sure that the string contains valid
     * utf-8 characters and will throw an exception if invalid characters are found
     */

    Utf8String(const char * in, bool check=true);
    
    Utf8String(const char *start, size_t len, bool check=true);

    Utf8String(const char *start, const char * end, bool check=true);

    Utf8String(std::string str, bool check = true);
    Utf8String(const std::wstring & str, bool check = true);
    Utf8String(const std::u8string & str, bool check = true);
    Utf8String(const std::u16string & str, bool check = true);
    Utf8String(const std::u32string & str, bool check = true);

    Utf8String(std::string_view str, bool check = true);
    Utf8String(std::wstring_view str, bool check = true);
    Utf8String(std::u8string_view str, bool check = true);
    Utf8String(std::u16string_view str, bool check = true);
    Utf8String(std::u32string_view str, bool check = true);

    Utf8String(std::span<char> str, bool check = true);
    Utf8String(std::span<wchar_t> str, bool check = true);
    Utf8String(std::span<char8_t> str, bool check = true);
    Utf8String(std::span<char16_t> str, bool check = true);
    Utf8String(std::span<char32_t> str, bool check = true);
    
    Utf8String(const char8_t * in, bool check=true);
    Utf8String(const wchar_t * in, bool check=true);
    Utf8String(const char16_t *start, bool check=true);
    Utf8String(const char32_t *start, bool check=true);

    Utf8String(const char8_t *start, size_t len, bool check=true);

    Utf8String(const char8_t *start, const char8_t * end, bool check=true);

    Utf8String(const const_iterator & first, const const_iterator & last, bool /* check */ = false);

    Utf8String(const iterator & first, const iterator & last, bool /* check */ = false);

    template<typename InputIterator>
    Utf8String(InputIterator start, InputIterator end, bool /* check */ = false)
    {
        // TODO: there are better ways...
        Utf8String result;
        for (auto it = start; it != end; ++it)
            result += *it;
        swap(result);
    }

    Utf8String & operator=(Utf8String && str) noexcept
    {
        Utf8String newMe(std::move(str));
        swap(newMe);
        return *this;
    }

    Utf8String & operator=(const Utf8String & str)
    {
        Utf8String newMe(str);
        swap(newMe);
        return *this;
    }

    Utf8String & operator=(const std::string &str)
    {
    	data_ = str;
    	return *this;
    }

    Utf8String & operator=(std::string &&str)
    {
    	data_ = std::move(str);
    	return *this;
    }

    Utf8String & operator=(const char * str)
    {
    	data_ = str;
    	return *this;
    }

    void swap(Utf8String & other)
    {
        data_.swap(other.data_);
    }

    bool empty() const
    {
        return data_.empty();
    }

    iterator begin();
    iterator end();
    reverse_iterator rbegin();
    reverse_iterator rend();

    const_iterator begin() const;
    const_iterator end() const;
    const_reverse_iterator rbegin() const;
    const_reverse_iterator rend() const ;

    Utf8String&  operator+=(const std::string& str)
    {
        data_+=str;
    	return *this;
    }

    Utf8String&  operator+=(const char * str)
    {
        data_+=str;
    	return *this;
    }

    Utf8String &operator+=(const Utf8String &utf8str);

    Utf8String& operator += (char32_t ch);

    template<typename It>
    Utf8String& append(It first, It last, bool check=true)
    {
        Utf8String toAppend(first, last, check);
        data_.append(toAppend.data_);
        return *this;
    }

    template<typename It>
    Utf8String& insert(iterator where, It first, It last, bool check=true)
    {
        Utf8String toInsert(first, last, check);
        data_.insert(where.base(), toInsert.data_.begin(), toInsert.data_.end());
        return *this;
    }

    /*
     * Returns access to the underlying representation - unsafe
     */
    std::string stealRawString() { return std::move(data_); }
    std::string stealAsciiString();
    const std::string & rawString() const { return data_; }
    const std::string & utf8String() const { return data_; }

    // Data but not necessarily null terminated
    const char * rawData() const { return data_.c_str(); }
    std::string_view rawView() const { return {data_.data(), data_.length()}; }

    // Null terminated string of utf8 encoded charadcters
    const char * c_str() const { return data_.c_str(); }

    size_t rawLength() const { return data_.length() ; }

    bool startsWith(const Utf8String & prefix) const;
    bool startsWith(const char * prefix) const;
    bool startsWith(const char32_t * prefix) const;
    bool startsWith(const std::string & prefix) const;

    bool endsWith(const Utf8String & suffix) const;
    bool endsWith(const char * suffix) const;
    bool endsWith(const char32_t * suffix) const;
    bool endsWith(const std::string & suffix) const;

    /** Checks for a prefix or suffix match and, if it does match, removes
        the prefix/suffix from the string (mutate operation).
    */
    bool removePrefix(const Utf8String & prefix);
    bool removeSuffix(const Utf8String & prefix);    

    /** Replaces the characters at the given range with a new
        string.  Note that this is an expensive operation as
        the string needs to be iterated through from the
        beginning to find the character indexes.
    */
    void replace(ssize_t startIndex, ssize_t endIndex,
                 const Utf8String & with);

    /** Erase a subset of the string. */
    void erase(iterator first, iterator last);

    size_t length() const;

    /** Reserve the given amount of space. This is in bytes for the UTF-8
        encoded string, not capacity; in other words to reserve space
        for string x use x.rawLength() not x.length().
    */
    void reserve(size_t capacity);

    /** How much space is reserved?  This is in bytes for the UTF-8
        encoded string, not capacity; in other words to reserve space
        for string x use x.rawLength() not x.length().
    */
    size_t capacity() const;

    const_iterator find(int c) const { return find(c, begin()); }
    const_iterator find(const char * s) const { return find(s, begin()); }
    const_iterator find(const wchar_t * s) const { return find(s, begin()); }
    const_iterator find(const std::string & s) const { return find(s, begin()); }
    const_iterator find(const Utf8String & s) const { return find(s, begin()); }

    const_iterator find(int c, const_iterator start) const;
    const_iterator find(const char * s, const_iterator start) const;
    const_iterator find(const wchar_t * s, const_iterator start) const;
    const_iterator find(const std::string & s, const_iterator start) const;
    const_iterator find(const Utf8String & s, const_iterator start) const;

    iterator find(int c) { return find(c, begin()); }
    iterator find(const char * s) { return find(s, begin()); }
    iterator find(const wchar_t * s) { return find(s, begin()); }
    iterator find(const std::string & s) { return find(s, begin()); }
    iterator find(const Utf8String & s) { return find(s, begin()); }

    iterator find(int c, iterator start);
    iterator find(const char * s, iterator start);
    iterator find(const wchar_t * s, iterator start);
    iterator find(const std::string & s, iterator start);
    iterator find(const Utf8String & s, iterator start);

    const_iterator rfind(int c) const { return rfind(c, end()); }
    const_iterator rfind(const char * s) const { return rfind(s, end()); }
    const_iterator rfind(const wchar_t * s) const { return rfind(s, end()); }
    const_iterator rfind(const std::string & s) const { return rfind(s, end()); }
    const_iterator rfind(const Utf8String & s) const { return rfind(s, end()); }

    const_iterator rfind(int c, const_iterator start) const;
    const_iterator rfind(const char * s, const_iterator start) const;
    const_iterator rfind(const wchar_t * s, const_iterator start) const;
    const_iterator rfind(const std::string & s, const_iterator start) const;
    const_iterator rfind(const Utf8String & s, const_iterator start) const;

    iterator rfind(int c) { return rfind(c, end()); }
    iterator rfind(const char * s) { return rfind(s, end()); }
    iterator rfind(const wchar_t * s) { return rfind(s, end()); }
    iterator rfind(const std::string & s) { return rfind(s, end()); }
    iterator rfind(const Utf8String & s) { return rfind(s, end()); }

    iterator rfind(int c, iterator start);
    iterator rfind(const char * s, iterator start);
    iterator rfind(const wchar_t * s, iterator start);
    iterator rfind(const std::string & s, iterator start);
    iterator rfind(const Utf8String & s, iterator start);

    iterator wrapIterator(std::string::iterator it);
    const_iterator wrapIterator(std::string::const_iterator it) const;

    std::string extractAscii() const;
    std::string uriEncode() const;
    bool isAscii() const;

    /** Return a lowercase version of this string. */
    Utf8String toLower() const;

    /** Return an uppercase version of this string. */
    Utf8String toUpper() const;

    bool operator == (const Utf8String & other) const
    {
        return data_ == other.data_;
    }

    bool operator == (const char * other) const
    {
        return data_ == other;
    }

    bool operator == (const std::string & other) const
    {
        return data_ == other;
    }
    
    bool operator != (const Utf8String & other) const
    {
        return data_ != other.data_;
    }

    bool operator != (const std::string & other) const
    {
        return data_ != other;
    }

    bool operator != (const char * other) const
    {
        return data_ != other;
    }

    bool operator < (const Utf8String & other) const
    {
        return data_ < other.data_;
    }

    bool operator < (const char * other) const
    {
        return data_ < other;
    }

    bool operator < (const std::string & other) const
    {
        return data_ < other;
    }

    bool operator >= (const Utf8String & other) const
    {
        return data_ >= other.data_;
    }

    bool operator >= (const char * other) const
    {
        return data_ >= other;
    }

    bool operator >= (const std::string & other) const
    {
        return data_ >= other;
    }

private:
    /** Check for invalid code points in the string. */
    void doCheck() const;

    template<typename InputIterator>
    void init_from_range(InputIterator start, InputIterator end, bool check);
    template<typename Char>
    void init_from_null_terminated(const Char * start, bool check);

    template<typename InputIterator>
    void init_from_range_u8(InputIterator start, InputIterator end, bool check);
    template<typename Char>
    void init_from_range_u8(const Char * start, const Char * end, bool check);
    template<typename Char>
    void init_from_null_terminated_u8(const Char * start, bool check);

    template<typename InputIterator>
    void init_from_range_u16(InputIterator start, InputIterator end, bool check);
    template<typename Char>
    void init_from_range_u16(const Char * start, const Char * end, bool check);
    template<typename Char>
    void init_from_null_terminated_u16(const Char * start, bool check);

    template<typename InputIterator>
    void init_from_range_u32(InputIterator start, InputIterator end, bool check);
    template<typename Char>
    void init_from_range_u32(const Char * start, const Char * end, bool check);
    template<typename Char>
    void init_from_null_terminated_u32(const Char * start, bool check);

    template<typename InputIterator>
    void inefficient_init_from_range_wide(InputIterator start, InputIterator end, bool check);
    template<typename Char>
    void inefficient_init_from_null_terminated_wide(const Char * start, bool check);

    std::string data_; // original utf8-encoded string
};

inline bool operator == (const char * str1, const Utf8String & s2)
{
    return s2 == str1;
}

inline bool operator == (const std::string & str1, const Utf8String & s2)
{
    return s2 == str1;
}

inline bool operator != (const char * str1, const Utf8String & s2)
{
    return s2 != str1;
}

inline bool operator != (const std::string & str1, const Utf8String & s2)
{
    return s2 != str1;
}

inline bool operator < (const char * str1, const Utf8String & s2)
{
    return s2 >= str1;
}

inline bool operator < (const std::string & str1, const Utf8String & s2)
{
    return s2 >= str1;
}

inline Utf8String operator + (Utf8String str1, const Utf8String & str2)
{
    return str1 += str2;
}

inline Utf8String operator + (Utf8String str1, const char * str2)
{
    return str1 += str2;
}

inline Utf8String operator + (Utf8String str1, const std::string & str2)
{
    return str1 += str2;
}

inline Utf8String operator + (const char * str1, const Utf8String & str2)
{
    Utf8String result(str1);
    result += str2;
    return result;
}

inline Utf8String operator + (const std::string & str1, const Utf8String & str2)
{
    Utf8String result(str1);
    result += str2;
    return result;
}

inline Utf8String operator + (Utf8String str1, char32_t ch)
{
    return str1 += ch;
}

inline Utf8String operator + (char32_t ch, const Utf8String & str1)
{
    Utf8String result;
    result += ch;
    result += str1;
    return result;
}

inline void swap(Utf8String & s1, Utf8String & s2)
{
    s1.swap(s2);
}

inline Utf8String lowercase(const Utf8String & str)
{
    return str.toLower();
}

std::ostream & operator << (std::ostream & stream, const Utf8String & str);

inline size_t size(const Utf8String & str)
{
    return str.length();
}

inline auto begin(Utf8String & str) -> decltype(str.begin())
{
    return str.begin();
}

inline auto end(Utf8String & str) -> decltype(str.end())
{
    return str.end();
}

inline Utf8String to_lower(const Utf8String & str)
{
    return str.toLower();
}

inline Utf8String to_upper(const Utf8String & str)
{
    return str.toUpper();
}

#define MLDB_UTF8STRING_FREE_FUNCTION(ret, name, method, type) \
    inline ret name(const Utf8String & str1, type str2) { return str1.method(str2); }

MLDB_UTF8STRING_FREE_FUNCTION(bool, starts_with, startsWith, const Utf8String &);
MLDB_UTF8STRING_FREE_FUNCTION(bool, starts_with, startsWith, const char *);
MLDB_UTF8STRING_FREE_FUNCTION(bool, starts_with, startsWith, const std::string &);
MLDB_UTF8STRING_FREE_FUNCTION(bool, ends_with, endsWith, const Utf8String &);
MLDB_UTF8STRING_FREE_FUNCTION(bool, ends_with, endsWith, const char *);
MLDB_UTF8STRING_FREE_FUNCTION(bool, ends_with, endsWith, const std::string &);

#undef MLDB_UTF8STRING_FREE_FUNCTION

// Free functions for array limits
inline Utf8String::const_iterator arr_begin(const Utf8String & str) { return str.begin(); }
inline Utf8String::const_iterator arr_end(const Utf8String & str) { return str.end(); }
inline Utf8String::iterator arr_begin(Utf8String & str) { return str.begin(); }
inline Utf8String::iterator arr_end(Utf8String & str) { return str.end(); }
inline Utf8String::const_reverse_iterator arr_rbegin(const Utf8String & str) { return str.rbegin(); }
inline Utf8String::const_reverse_iterator arr_rend(const Utf8String & str) { return str.rend(); }
inline Utf8String::reverse_iterator arr_rbegin(Utf8String & str) { return str.rbegin(); }
inline Utf8String::reverse_iterator arr_rend(Utf8String & str) { return str.rend(); }
inline size_t arr_size(const Utf8String & str) { return str.length(); }


/*****************************************************************************/
/* UTF32 STRING                                                              */
/*****************************************************************************/

class Utf32String {
public:
    typedef std::u32string::iterator iterator;
    typedef std::u32string::const_iterator const_iterator;

    static Utf32String fromLatin1(const std::string &str);
    static Utf32String fromUtf8(const Utf8String &utf8Str);

    Utf32String()
    {
    }

    explicit Utf32String(const std::string &str) {
        utf8::utf8to32(std::begin(str), std::end(str), std::back_inserter(data_));
    }

    Utf32String(const Utf32String &other)
    : data_(other.data_)
    {
    }
    Utf32String(Utf32String &&other) noexcept
    {
        *this = std::move(other);
    }

    Utf32String &operator=(const Utf32String &other)
    {
        Utf32String newMe(other);
        swap(newMe);
        return *this;
    }

    Utf32String &operator=(Utf32String &&other) noexcept
    {
        data_ = std::move(other.data_);
        return *this;
    }

    Utf32String &operator=(const std::string &other) {
        utf8::utf8to32(std::begin(other), std::end(other), std::back_inserter(data_));
        return *this;
    }

    void swap(Utf32String &other)
    {
        std::swap(data_, other.data_);
    }

    bool empty() const {
        return data_.empty();
    }

    iterator begin() {
        return data_.begin();
    }

    const_iterator begin() const {
        return data_.begin();
    }

    iterator end() {
        return data_.end();
    }

    const_iterator end() const {
        return data_.end();
    }

    std::u32string rawString() const {
        return data_;
    }

    std::string utf8String() const {
        std::string utf8Str;
        utf8::utf32to8(std::begin(data_), std::end(data_), std::back_inserter(utf8Str));
        return utf8Str;
    }

    const char32_t *rawData() const {
        return data_.c_str();
    }

    const size_t rawLength() const {
        return data_.length();
    }

    Utf32String &operator+=(const std::string &other) {
        std::u32string u32other;
        utf8::utf8to32(std::begin(other), std::end(other), std::back_inserter(u32other));
        data_ += u32other;

        return *this;
    }

    Utf32String &operator+=(const Utf32String &other) {
        data_ += other.data_;

        return *this;
    }

    bool operator==(const Utf32String &other) const {
        return data_ == other.data_;
    }

    bool operator!=(const Utf32String &other) const {
        return !operator==(other);
    }

    bool operator<(const Utf32String &other) const {
        return data_ < other.data_;
    }

    std::string extractAscii() const;


private:
    std::u32string data_;
};

Utf32String operator+(Utf32String lhs, const Utf32String &rhs);

std::ostream & operator << (std::ostream & stream, const Utf32String & str);

typedef Utf8String UnicodeString;

Utf8String getUtf8ExceptionString();

// This is a helper function to allow us to pass a Utf8String to a printf
// Called in arch/format.h
MLDB_ALWAYS_INLINE const char * forwardForPrintf(const Utf8String & s)
{
    return s.c_str();
}

} // namespace MLDB

namespace std {

template<>
struct hash<MLDB::Utf8String> {
    size_t operator()(const MLDB::Utf8String & str) const
    {
        return std::hash<std::string>()(str.rawString());
    }
};

} // namespace std
