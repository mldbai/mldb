// This file is part of MLDB. Copyright 2015 Datacratic. All rights reserved.

/* url.h                                                           -*- C++ -*-
   Jeremy Barnes, 16 March 2012
   Copyright (c) 2012 Datacratic.  All rights reserved.

   URL class.
*/

#pragma once

#include <string>
#include <memory>
#include "value_description_fwd.h"
#include "string.h"

class GURL;

namespace Datacratic {

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
};

inline std::ostream & operator << (std::ostream & stream, const Url & url)
{
    return stream << url.toString();
}

extern std::string urlDocumentationUri;

/** Reach inside the URL's value description, and set its documentation
    URI to the given string.
*/
void setUrlDocumentationUri(const std::string & newUri);

PREDECLARE_VALUE_DESCRIPTION(Url);

} // namespace Datacratic
