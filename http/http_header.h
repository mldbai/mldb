/* http_header.h                                                 -*- C++ -*-
   Jeremy Barnes, 18 February 2011
   Copyright (c) 2011 mldb.ai inc.  All rights reserved.

   This file is part of MLDB. Copyright 2015 mldb.ai inc. All rights reserved.

   http header parsing class.
*/

#pragma once

#include <string>
#include <map>
#include <iostream>
#include <vector>
#include "mldb/types/string.h"
#include "mldb/types/value_description_fwd.h"

namespace MLDB {

/*****************************************************************************/
/* REST PARAMS                                                               */
/*****************************************************************************/

struct RestParams
    : public std::vector<std::pair<Utf8String, Utf8String> > {
    RestParams()
    {
    }

    RestParams(const std::vector<std::pair<Utf8String, Utf8String> > & vec)
        : std::vector<std::pair<Utf8String, Utf8String> >(vec)
    {
    }

    RestParams(std::initializer_list<std::pair<Utf8String, Utf8String> > l)
        : std::vector<std::pair<Utf8String, Utf8String> >(l.begin(), l.end())
    {
    }

    template<typename It>
    RestParams(It first, It last)
        : std::vector<std::pair<Utf8String, Utf8String> >(first, last)
    {
    }

    RestParams(const std::map<Utf8String, Utf8String> & params)
        : std::vector<std::pair<Utf8String, Utf8String> >
          (params.begin(), params.end())
    {
    }

    RestParams(const std::map<std::string, std::string> & params)
        : std::vector<std::pair<Utf8String, Utf8String> >
          (params.begin(), params.end())
    {
    }

    bool hasValue(const Utf8String & key) const;

    /** Return the value of the given key.  Throws an exception if it's not
        found.
    */
    Utf8String getValue(const Utf8String & key) const;

    std::string uriEscaped() const;

    /** Convert to pure ASCII representation for HTTP headers, etc. */
    operator std::vector<std::pair<std::string, std::string> > () const;
};

PREDECLARE_VALUE_DESCRIPTION(RestParams);


/*****************************************************************************/
/* HTTP HEADER                                                               */
/*****************************************************************************/

/** Header for an HTTP request.  Just a place to dump the data. */

struct HttpHeader {
    HttpHeader()
        : contentLength(-1), isChunked(false)
    {
    }

    void swap(HttpHeader & other);

    void parse(const std::string & headerAndData, bool checkBodyLength = true);

    std::string verb;       // GET, PUT, etc
    std::string resource;   // after the get
    std::string version;    // after the get

    int responseCode() const;  // for responses; parses it out of the "version" field

    RestParams queryParams; // Query parameters pulled out of the URL

    // These headers are automatically pulled out
    std::string contentType;
    int64_t contentLength;
    bool isChunked;

    // The rest of the headers are here
    std::map<std::string, std::string> headers;

    const std::string & getHeader(const std::string & key) const;

    const std::string & tryGetHeader(const std::string & key) const noexcept;

    // If some portion of the data is known, it's put in here
    std::string knownData;
};

std::ostream & operator << (std::ostream & stream, const HttpHeader & header);

/** Returns the reason phrase for the given code.
    See http://www.w3.org/Protocols/rfc2616/rfc2616-sec6.html
*/
const std::string & getResponseReasonPhrase(int code);

DECLARE_STRUCTURE_DESCRIPTION(HttpHeader);

} // namespace MLDB
