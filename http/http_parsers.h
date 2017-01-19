// This file is part of MLDB. Copyright 2015 mldb.ai inc. All rights reserved.

/* http_parsers.h                                                  -*- C++ -*-
   Wolfgang Sourdeau, January 2014
   Copyright (c) 2014 mldb.ai inc.  All rights reserved.

   This module contains classes meant to parse http requests
   (HttpRequestParser) and responses (HttpResponseParser), with a callback
   interface. Those classes are meant the reduce the number of allocations and
   are completely contents agnostic.
*/

#pragma once

#include <functional>
#include <memory>
#include <string>


namespace MLDB {

/****************************************************************************/
/* HTTP PARSER                                                              */
/****************************************************************************/

struct HttpParser {
    /* Type of callback used when to report a header-line, including the
     * header key and the value. */
    typedef std::function<void (const char *, size_t)> OnHeader;

    /* Type of callback used when receiving an "Expect: 100-continue" header.
     */
    typedef std::function<bool ()> OnExpect100Continue;

    /* Type of callback used when to report a chunk of the response body. Only
       invoked when the body is larger than 0 byte. */
    typedef std::function<void (const char *, size_t)> OnData;

    /* Type of callback used when to report the end of a response */
    typedef std::function<void (bool)> OnDone;

    /* structure to hold the temporary state of the parser used when "feed" is
       invoked */
    struct BufferState;

    HttpParser()
        noexcept
    {
        clear();
    }

    /* Feed the parsing with a 0-ended data chunk. Slightly slower than the
       explicitly sized version, but useful for testing. Avoid in production
       code. */
    void feed(const char * data);

    /* Feed the parsing with a data chunk of a specied size. */
    void feed(const char * data, size_t size);

    /* XXX */
    virtual bool parseFirstLine(BufferState & state) = 0;

    /* Returns the number of bytes remaining to parse from the body, as
     * specified by the "Content-Length" header. */
    uint64_t remainingBody()
        const
    {
        return remainingBody_;
    }

    bool useChunkedEncoding()
        const
    {
        return useChunkedEncoding_;
    }

    bool requireClose()
        const
    {
        return requireClose_;
    }

    OnHeader onHeader;
    OnExpect100Continue onExpect100Continue;
    OnData onData;
    OnDone onDone;

private:
    void clear() noexcept;

    bool parseHeaders(BufferState & state);
    bool parseBody(BufferState & state);
    bool parseChunkedBody(BufferState & state);
    bool parseBlockBody(BufferState & state);

    void handleHeader(const char * data, size_t dataSize);
    void finalizeParsing();

    bool expectBody_;

    int stage_;
    std::string buffer_;

    uint64_t remainingBody_;
    bool expect100Continue_;
    bool useChunkedEncoding_;
    bool requireClose_;
};


/****************************************************************************/
/* HTTP RESPONSE PARSER                                                     */
/****************************************************************************/

/* HttpResponseParser offers a very fast and memory efficient HTTP/1.1
 * response parser. It provides a callback-based interface which enables
 * on-the-fly response processing.
 */

struct HttpResponseParser : public HttpParser {
    /* Type of callback used when a response is starting, passing the HTTP
     * version in use as well as the HTTP response code as parameters */
    typedef std::function<void (const std::string &, int)> OnResponseStart;

    /* Indicates whether to expect a body during the parsing of the next
       response. */
    void setExpectBody(bool expBody)
    { expectBody_ = expBody; }

    virtual bool parseFirstLine(BufferState & state);

    OnResponseStart onResponseStart;

private:
    bool parseStatusLine(BufferState & state);

    bool expectBody_;
};


/****************************************************************************/
/* HTTP REQUEST PARSER                                                      */
/****************************************************************************/

/* HttpRequestParser offers a very fast and memory efficient HTTP/1.1
 * request parser, similarly to HttpResponseParser.
 */

struct HttpRequestParser : public HttpParser {
    /* Type of callback used when a request is starting, passing the HTTP
     * version in use as well as the HTTP request code as parameters */
    typedef std::function<void (const char *, size_t,
                                const char *, size_t,
                                const char *, size_t)> OnRequestStart;

    virtual bool parseFirstLine(BufferState & state);

    OnRequestStart onRequestStart;

private:
    bool parseRequestLine(BufferState & state);

    bool expectBody_;
};

} // namespace MLDB
