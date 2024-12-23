/* url.h                                                           -*- C++ -*-
   Jeremy Barnes, 16 March 2012
   Copyright (c) 2012 mldb.ai inc.  All rights reserved.
   This file is part of MLDB. Copyright 2015 mldb.ai inc. All rights reserved.

   URL class.
*/

#pragma once

#include <string>
#include <memory>
#include "value_description_fwd.h"
#include "string.h"

namespace MLDB {

struct Url {
    Url();
    explicit Url(std::string s);
    explicit Url(const char * s);
    explicit Url(Utf8String s);
    explicit Url(const Utf32String & s);
    ~Url();

    void init(Utf8String s);

    Utf8String toUtf8String() const;
    Utf8String toString() const;  // should be Utf8 by default
    Utf8String toDecodedUtf8String() const;
    Utf8String toDecodedString() const;
    std::u8string toDecodedU8String() const;
    std::string toEncodedAsciiString() const; // does percent encoding; for use in HTTP headers

    const char * c_str() const;

    bool valid() const;
    bool empty() const;

    const Utf8String & original() const { return original_; }

    Utf8String canonical() const;
    std::string scheme() const;
    Utf8String username() const;
    Utf8String password() const;
    std::string asciiHost() const;
    Utf8String host() const;
    bool hostIsIpAddress() const;
    bool domainMatches(const std::string & str) const;
    int port() const;
    Utf8String path() const;
    std::string asciiPath() const;
    Utf8String query() const;
    std::string asciiQuery() const;
    Utf8String fragment() const;

    static Utf8String decodeUri(Utf8String str);

    // See https://developer.mozilla.org/en-US/docs/Web/JavaScript/Reference/Global_Objects/encodeURI
    static Utf8String encodeUri(const Utf8String & str);
    static std::string encodeUri(const std::string & str);
    static std::string encodeUri(const char * str);

private:
    class State;

    std::shared_ptr<State> state_;
    Utf8String original_;
};

inline std::ostream & operator << (std::ostream & stream, const Url & url)
{
    return stream << url.toString();
}

/** Reach inside the URL's value description, and set its documentation
    URI to the given string.
*/
void setUrlDocumentationUri(const std::string & newUri);

PREDECLARE_VALUE_DESCRIPTION(Url);

} // namespace MLDB
