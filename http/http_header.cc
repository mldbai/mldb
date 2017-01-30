// This file is part of MLDB. Copyright 2015 mldb.ai inc. All rights reserved.

/* http_header.cc
   Jeremy Barnes, 18 February 2010
   Copyright (c) 2010 mldb.ai inc.  All rights reserved.

*/

#include "http_header.h"
#include "mldb/base/parse_context.h"
#include "http_exception.h"
#include "mldb/types/structure_description.h"
#include "mldb/types/map_description.h"
#include "mldb/types/vector_description.h"
#include "mldb/types/pair_description.h"

using namespace std;


namespace {

const string Continue("Continue");
const string SwitchingProtocols("Switching Protocols");
const string OK("OK");
const string Created("Created");
const string Accepted("Accepted");
const string NonAuthoritativeInformation("Non-Authoritative Information");
const string NoContent("No Content");
const string ResetContent("Reset Content");
const string PartialContent("Partial Content");
const string MultipleChoices("Multiple Choices");
const string MovedPermanently("Moved Permanently");
const string Found("Found");
const string SeeOther("See Other");
const string NotModified("Not Modified");
const string UseProxy("Use Proxy");
const string TemporaryRedirect("Temporary Redirect");
const string BadRequest("Bad Request");
const string Unauthorized("Unauthorized");
const string PaymentRequired("Payment Required");
const string Forbidden("Forbidden");
const string NotFound("Not Found");
const string MethodNotAllowed("Method Not Allowed");
const string NotAcceptable("Not Acceptable");
const string ProxyAuthenticationRequired("Proxy Authentication Required");
const string RequestTimeout("Request Time-out");
const string Conflict("Conflict");
const string Gone("Gone");
const string LengthRequired("Length Required");
const string PreconditionFailed("Precondition Failed");
const string RequestEntityTooLarge("Request Entity Too Large");
const string RequestURITooLarge("Request-URI Too Large");
const string UnsupportedMediaType("Unsupported Media Type");
const string RequestedRangeNotSatisfiable("Requested range not satisfiable");
const string ExpectationFailed("Expectation Failed");
const string InternalServerError("Internal Server Error");
const string NotImplemented("Not Implemented");
const string BadGateway("Bad Gateway");
const string ServiceUnavailable("Service Unavailable");
const string GatewayTimeout("Gateway Time-out");
const string HTTPVersionNotSupported("HTTP Version not supported");
const string UnknownResponseCode("Unknown response code");

}


namespace MLDB {


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
                else result += MLDB::format("%%%02x", c);
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
        throw MLDB::Exception("Can't parse JSON for RestParams");
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
expectUrlEncodedString(ParseContext & context,
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
        ParseContext context("request header",
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
                    throw MLDB::Exception("unknown transfer-encoding");
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

const string & getResponseReasonPhrase(int code)
{
    switch (code) {
    case 100: return Continue;
    case 101: return SwitchingProtocols;
    case 200: return OK;
    case 201: return Created;
    case 202: return Accepted;
    case 203: return NonAuthoritativeInformation;
    case 204: return NoContent;
    case 205: return ResetContent;
    case 206: return PartialContent;
    case 300: return MultipleChoices;
    case 301: return MovedPermanently;
    case 302: return Found;
    case 303: return SeeOther;
    case 304: return NotModified;
    case 305: return UseProxy;
    case 307: return TemporaryRedirect;
    case 400: return BadRequest;
    case 401: return Unauthorized;
    case 402: return PaymentRequired;
    case 403: return Forbidden;
    case 404: return NotFound;
    case 405: return MethodNotAllowed;
    case 406: return NotAcceptable;
    case 407: return ProxyAuthenticationRequired;
    case 408: return RequestTimeout;
    case 409: return Conflict;
    case 410: return Gone;
    case 411: return LengthRequired;
    case 412: return PreconditionFailed;
    case 413: return RequestEntityTooLarge;
    case 414: return RequestURITooLarge;
    case 415: return UnsupportedMediaType;
    case 416: return RequestedRangeNotSatisfiable;
    case 417: return ExpectationFailed;
    case 500: return InternalServerError;
    case 501: return NotImplemented;
    case 502: return BadGateway;
    case 503: return ServiceUnavailable;
    case 504: return GatewayTimeout;
    case 505: return HTTPVersionNotSupported;
    default:
      return UnknownResponseCode;
    }
}

const std::string &
HttpHeader::
getHeader(const std::string & key) const
{
    auto it = headers.find(key);
    if (it == headers.end())
        throw MLDB::Exception("couldn't find header " + key);
    return it->second;
}

const std::string &
HttpHeader::
tryGetHeader(const std::string & key) const noexcept
{
    static const std::string NONE;
    auto it = headers.find(key);
    if (it == headers.end())
        return NONE;
    return it->second;
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

} // namespace MLDB
