/* string.h                                                          -*- C++ -*-
   Sunil Rottoo, 27 April 2012
   Copyright (c) 20102 mldb.ai inc.  All rights reserved.

   This file is part of MLDB. Copyright 2015 mldb.ai inc. All rights reserved.

   Basic classes for dealing with string including internationalisation
*/

#pragma once

#include <string>
#include "mldb/ext/utf8cpp/source/utf8.h"


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
    typedef utf8::iterator<std::string::const_iterator> const_iterator;
    typedef utf8::iterator<std::string::iterator> iterator;

    static Utf8String fromLatin1(const std::string & lat1Str);
    static Utf8String fromLatin1(std::string && lat1Str);

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

    /** Construct from wide string constant. */
    Utf8String(const wchar_t * str);

    /**
     * Take possession of a utf8-encoded string.
     * @param input A string that contains utf8-encoded characters
     * @param check If true we will make sure that the string contains valid
     * utf-8 characters and will throw an exception if invalid characters are found
     */
    Utf8String(const std::string &in, bool check=true) ;

    Utf8String(std::string &&in, bool check=true) ;

    Utf8String(const char * in, bool check=true) ;
    
    Utf8String(const char *start, size_t len, bool check=true);

    Utf8String(const std::basic_string<char32_t> & str);

    Utf8String(const_iterator first, const const_iterator & last);

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

    const_iterator begin() const;
    const_iterator end() const ;

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

    /*
     * Returns access to the underlying representation - unsafe
     */
    std::string stealRawString() const { return std::move(data_); }
    const std::string & rawString() const { return data_; }
    const std::string & utf8String() const { return data_; }
    const char * rawData() const { return data_.c_str(); }
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

    const_iterator find(int c) const;
    const_iterator find(const char * s) const;
    const_iterator find(const wchar_t * s) const;
    const_iterator find(const std::string & s) const;
    const_iterator find(const Utf8String & s) const;

    iterator find(int c);
    iterator find(const char * s);
    iterator find(const wchar_t * s);
    iterator find(const std::string & s);
    iterator find(const Utf8String & s);

    const_iterator rfind(int c) const;
    const_iterator rfind(const char * s) const;
    const_iterator rfind(const wchar_t * s) const;
    const_iterator rfind(const std::string & s) const;
    const_iterator rfind(const Utf8String & s) const;

    iterator rfind(int c);
    iterator rfind(const char * s);
    iterator rfind(const wchar_t * s);
    iterator rfind(const std::string & s);
    iterator rfind(const Utf8String & s);

    iterator wrapIterator(std::string::iterator it);
    const_iterator wrapIterator(std::string::const_iterator it) const;

    std::string extractAscii() const;
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

inline void swap(Utf8String & s1, Utf8String & s2)
{
    s1.swap(s2);
}

inline Utf8String lowercase(const Utf8String & str)
{
    return str.toLower();
}

std::ostream & operator << (std::ostream & stream, const Utf8String & str);


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

} // namespace MLDB

namespace std {

template<typename T> struct hash;

template<>
struct hash<MLDB::Utf8String>
    : public std::unary_function<MLDB::Utf8String, size_t>
{
    size_t operator()(const MLDB::Utf8String & str) const
    {
        return std::hash<std::string>()(str.rawString());
    }
};

} // namespace std
