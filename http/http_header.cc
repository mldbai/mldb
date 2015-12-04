// This file is part of MLDB. Copyright 2015 Datacratic. All rights reserved.

/* http_header.cc
   Jeremy Barnes, 18 February 2010
   Copyright (c) 2010 Datacratic.  All rights reserved.

*/

#include "http_header.h"
#include "mldb/base/parse_context.h"
#include "mldb/jml/db/persistent.h"
#include "mldb/jml/utils/vector_utils.h"
#include "http_exception.h"
#include "mldb/types/structure_description.h"
#include "mldb/types/map_description.h"
#include "mldb/types/vector_description.h"
#include "mldb/types/pair_description.h"

using namespace std;
using namespace ML;


namespace Datacratic {


/*****************************************************************************/
/* REST PARAMS                                                               */
/*****************************************************************************/

std::string
RestParams::
uriEscaped() const
{
    auto urlEscape = [] (const Utf8String & str)
        {
            string result;
            for (char c: str.rawString()) {
                if (isalnum(c) || c == '-' || c == '_' || c == '.' || c == '~')
                    result += c;
                else result += ML::format("%%%02x", c);
            }
            return result;
        };
        
    std::string uri;

    for (unsigned i = 0;  i < size();  ++i) {
        if (i == 0)
            uri += "?";
        else uri += "&";
        uri += urlEscape((*this)[i].first)
            + "=" + urlEscape((*this)[i].second);
    }

    return uri;
}

bool
RestParams::
hasValue(const Utf8String & key) const
{
    for (auto & kv: *this)
        if (kv.first == key)
            return true;
    return false;
}

Utf8String
RestParams::
getValue(const Utf8String & key) const
{
    for (auto & kv: *this)
        if (kv.first == key)
            return kv.second;
    throw HttpReturnException(400, "key " + key + " not found in RestParams");
}

RestParams::
operator std::vector<std::pair<std::string, std::string> > () const
{
    std::vector<std::pair<std::string, std::string> > result;
    for (auto & v: *this)
        result.emplace_back(v.first.rawString(), v.second.rawString());
    return result;
}

struct RestParamsDescription: public ValueDescriptionT<RestParams> {
    virtual void parseJsonTyped(RestParams * val,
                                JsonParsingContext & context) const
    {
        throw ML::Exception("Can't parse JSON for RestParams");
    }

    virtual void printJsonTyped(const RestParams * val,
                                JsonPrintingContext & context) const
    {
        context.writeJson(jsonEncode((const vector<pair<string, string> > &)(*val)));
    }

    virtual bool isDefaultTyped(const RestParams * val) const
    {
        return val->empty();
    }
};

DEFINE_VALUE_DESCRIPTION(RestParams, RestParamsDescription);


/*****************************************************************************/
/* HTTP HEADER                                                               */
/*****************************************************************************/

void
HttpHeader::
swap(HttpHeader & other)
{
    verb.swap(other.verb);
    resource.swap(other.resource);
    contentType.swap(other.contentType);
    std::swap(contentLength, other.contentLength);
    headers.swap(other.headers);
    knownData.swap(other.knownData);
    std::swap(isChunked, other.isChunked);
    std::swap(version, other.version);
}

namespace {

std::string
expectUrlEncodedString(ML::Parse_Context & context,
                       string delimiters)
{
    string result;
    while (context) {
        char c = *context;
        for (unsigned i = 0;  i < delimiters.length();  ++i)
            if (c == delimiters[i])
                return result;
        
        ++context;

        if (c == '%') {
#if 0
            auto hexToInt = [&] (int c)
                {
                    if (isdigit(c))
                        return c - '0';
                    if (isalpha(c)) {
                        c = tolower(c);
                        if (c >= 'a' && c <= 'f')
                            return c + 10 - 'a';
                        context.exception("invalid hex character");
                    }
                };
#endif        

            char s[3] = { *context++, *context++, 0 };
            char * endptr;
            int code = strtol(s, &endptr, 16);
            if (endptr != s + 2) {
                cerr << "s = " << (void *)s << endl;
                cerr << "endptr = " << (void *)endptr << endl;
                context.exception("invalid url encoded character: " + string(s));
            }
            result += code;
        }
        else if (c == '+') {
            result += ' ';
        }
        else {
            result += c;
        }
    }

    return result;
}

} // file scope

void
HttpHeader::
parse(const std::string & headerAndData, bool checkBodyLength)
{
    try {
        HttpHeader parsed;

        // Parse http
        ML::Parse_Context context("request header",
                                  headerAndData.c_str(),
                                  headerAndData.c_str()
                                      + headerAndData.length());

        parsed.verb = context.expect_text(" \n");
        context.expect_literal(' ');
        parsed.resource = context.expect_text(" ?");
        if (context.match_literal('?')) {
            do {
                string key = expectUrlEncodedString(context, "=& ");
                if (context.match_literal('=')) {
                    string value = expectUrlEncodedString(context, "& ");
                    queryParams.push_back(make_pair(key, value));
                } else {
                    queryParams.push_back(make_pair(key, ""));
                }
            } while (context.match_literal('&'));
        }
        context.expect_literal(' ');
        parsed.version = context.expect_text('\r');
        context.expect_eol();

        while (!context.match_literal("\r\n")) {
            string name = context.expect_text("\r\n:");
            for (auto & c: name)
                c = tolower(c);
            //cerr << "name = " << name << endl;
            context.expect_literal(':');
            context.match_whitespace();
            if (name == "content-length") {
                parsed.contentLength = context.expect_long_long();
                //cerr << "******* set cntentLength " << parsed.contentLength
                //     << endl;
            }
            else if (name == "content-type")
                parsed.contentType = context.expect_text('\r');
            else if (name == "transfer-encoding") {
                string transferEncoding
                    = context.expect_text('\r');
                for (auto & c: transferEncoding)
                    c = tolower(c);

                if (transferEncoding != "chunked")
                    throw ML::Exception("unknown transfer-encoding");
                parsed.isChunked = true;
            }
            else {
                string value = context.expect_text('\r');
                parsed.headers[name] = value;
            }
            context.expect_eol();
        }

        // The rest of the data is the body
        const char * content_start
            = headerAndData.c_str() + context.get_offset();

        parsed.knownData
            = string(content_start,
                     headerAndData.c_str() + headerAndData.length());

        if (checkBodyLength && (parsed.contentLength != -1)
            && ((int)parsed.knownData.length() > (int)parsed.contentLength)) {
            cerr << "got double packet: got content length " << parsed.knownData.length()
                 << " wanted " << parsed.contentLength << endl;
#if 1            
            context.exception(format("too much data for content length: "
                                     "%d > %d for data \"%s\"",
                                     (int)parsed.knownData.length(),
                                     (int)parsed.contentLength,
                                     headerAndData.c_str()));
#endif
            parsed.knownData.resize(parsed.contentLength);
        }
        
        swap(parsed);
    }
    catch (const std::exception & exc) {
        cerr << "error parsing http header: " << exc.what() << endl;
        cerr << headerAndData << endl;
        throw;
    }
}

int HttpHeader::responseCode() const
{
    return std::stoi(resource);
}

std::ostream & operator << (std::ostream & stream, const HttpHeader & header)
{
    stream << header.verb << " " << header.resource
           << header.queryParams.uriEscaped();
    stream << " HTTP/1.1\r\n"
           << "Content-Type: " << header.contentType << "\r\n";
    if (header.isChunked)
        stream << "Transfer-Encoding: chunked\r\n";
    else if (header.contentLength != -1)
        stream << "Content-Length: " << header.contentLength << "\r\n";
    for (auto it = header.headers.begin(), end = header.headers.end();
         it != end;  ++it) {
        stream << it->first << ": " << it->second << "\r\n";
    }
    stream << "\r\n";
    return stream;
}

std::string getResponseReasonPhrase(int code)
{
    switch (code) {
    case 100: return "Continue";
    case 101: return "Switching Protocols";
    case 200: return "OK";
    case 201: return "Created";
    case 202: return "Accepted";
    case 203: return "Non-Authoritative Information";
    case 204: return "No Content";
    case 205: return "Reset Content";
    case 206: return "Partial Content";
    case 300: return "Multiple Choices";
    case 301: return "Moved Permanently";
    case 302: return "Found";
    case 303: return "See Other";
    case 304: return "Not Modified";
    case 305: return "Use Proxy";
    case 307: return "Temporary Redirect";
    case 400: return "Bad Request";
    case 401: return "Unauthorized";
    case 402: return "Payment Required";
    case 403: return "Forbidden";
    case 404: return "Not Found";
    case 405: return "Method Not Allowed";
    case 406: return "Not Acceptable";
    case 407: return "Proxy Authentication Required";
    case 408: return "Request Time-out";
    case 409: return "Conflict";
    case 410: return "Gone";
    case 411: return "Length Required";
    case 412: return "Precondition Failed";
    case 413: return "Request Entity Too Large";
    case 414: return "Request-URI Too Large";
    case 415: return "Unsupported Media Type";
    case 416: return "Requested range not satisfiable";
    case 417: return "Expectation Failed";
    case 500: return "Internal Server Error";
    case 501: return "Not Implemented";
    case 502: return "Bad Gateway";
    case 503: return "Service Unavailable";
    case 504: return "Gateway Time-out";
    case 505: return "HTTP Version not supported";
    default:
        return ML::format("unknown response code %d", code);
    }
}

DEFINE_STRUCTURE_DESCRIPTION(HttpHeader);

HttpHeaderDescription::
HttpHeaderDescription()
{
    addField("verb", &HttpHeader::verb,
             "Verb to be performed");
    addField("resource", &HttpHeader::resource,
             "Resource to perform on");
    addField("version", &HttpHeader::version,
             "Version of the HTTP protocol");
    addField("queryParams", &HttpHeader::queryParams,
             "Query parameters");
    addField("contentType", &HttpHeader::contentType,
             "Content-type header");
    addField("contentLength", &HttpHeader::contentLength,
             "Content length header", (int64_t)-1);
    addField("isChunked", &HttpHeader::isChunked,
             "Using chunked transfer encoding", false);
    addField("headers", &HttpHeader::headers,
             "Headers of request");
}

} // namespace Datacratic
