
// This file is part of MLDB. Copyright 2015 mldb.ai inc. All rights reserved.

/* url.h                                                           -*- C++ -*-
   Jeremy Barnes, 16 March 2012
   Copyright (c) 2012 mldb.ai inc.  All rights reserved.

   URL class.
*/

#pragma once

#define TOLERATE_URL_BAD_ENCODING 1
#include <string>
#include <memory>
#include "value_description_fwd.h"
#include "string.h"

class GURL;

namespace MLDB {

struct Url {
    Url();
    explicit Url(const std::string & s);
    explicit Url(const char * s);
    explicit Url(const Utf8String & s);
    explicit Url(const Utf32String & s);
    ~Url();

    void init(std::string s);

    Utf8String toUtf8String() const;
    std::string toString() const;  // should be Utf8 by default
    Utf8String toDecodedUtf8String() const;
    std::string toDecodedString() const;

    const char * c_str() const;

    bool valid() const;
    bool empty() const;

    std::string canonical() const;
    std::string scheme() const;
    std::string username() const;
    std::string password() const;
    std::string host() const;
    bool hostIsIpAddress() const;
    bool domainMatches(const std::string & str) const;
    int port() const;
    std::string path() const;
    std::string query() const;

    uint64_t urlHash();
    uint64_t hostHash();

    std::shared_ptr<GURL> url;
    std::string original;

    static Utf8String decodeUri(Utf8String str);

    // See https://developer.mozilla.org/en-US/docs/Web/JavaScript/Reference/Global_Objects/encodeURI
    static Utf8String encodeUri(const Utf8String & str);
    static std::string encodeUri(const std::string & str);
    static std::string encodeUri(const char * str);
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
