// This file is part of MLDB. Copyright 2015 mldb.ai inc. All rights reserved.

#define BOOST_TEST_MAIN
#define BOOST_TEST_DYN_LINK

#include <iostream>
#include <boost/test/unit_test.hpp>

#include "mldb/http/http_parsers.h"
#include "mldb/http/http_header.h"
#include "mldb/utils/testing/print_utils.h"
#include "mldb/types/value_description.h"
#include "mldb/base/parse_context.h"
#include "mldb/ext/jsoncpp/json.h"


using namespace std;
using namespace MLDB;


#if 1
/* This test does progressive testing of the HttpResponseParser by sending
 * only a certain amount of bytes to check for all potential parsing faults,
 * for each step (top line, headers, body, response restart, ...). */
BOOST_AUTO_TEST_CASE( http_response_parser_test )
{
    string statusLine;
    vector<string> headers;
    string body;
    bool done;
    bool shouldClose;

    HttpResponseParser parser;
    parser.onResponseStart = [&] (const string & httpVersion, int code) {
        cerr << "response start\n";
        statusLine = httpVersion + "/" + to_string(code);
        headers.clear();
        body.clear();
        shouldClose = false;
        done = false;
    };
    parser.onHeader = [&] (const char * data, size_t size) {
        // cerr << "header: " + string(data, size) + "\n";
        headers.emplace_back(data, size);
    };
    parser.onData = [&] (const char * data, size_t size) {
        // cerr << "data\n";
        body.append(data, size);
    };
    parser.onDone = [&] (bool doClose) {
        shouldClose = doClose;
        done = true;
    };

    /* status line */
    parser.feed("HTTP/1.");
    BOOST_CHECK_EQUAL(statusLine, "");
    parser.feed("1 200 Th");
    BOOST_CHECK_EQUAL(statusLine, "");
    parser.feed("is is ");
    BOOST_CHECK_EQUAL(statusLine, "");
    parser.feed("some blabla\r");
    BOOST_CHECK_EQUAL(statusLine, "");
    parser.feed("\n");
    BOOST_CHECK_EQUAL(statusLine, "HTTP/1.1/200");

    /* headers */
    parser.feed("Head");
    BOOST_CHECK_EQUAL(headers.size(), 0);
    parser.feed("er1: value1\r\nHeader2: value2");
    BOOST_CHECK_EQUAL(headers.size(), 1);
    BOOST_CHECK_EQUAL(headers[0], "Header1: value1\r\n");
    parser.feed("\r");
    BOOST_CHECK_EQUAL(headers.size(), 1);

    /* Headers require line ending + 1 character, since the latter requires
     * testing for multiline headers. */
    parser.feed("\n");
    BOOST_CHECK_EQUAL(headers.size(), 1);
    parser.feed("H");
    BOOST_CHECK_EQUAL(headers.size(), 2);
    BOOST_CHECK_EQUAL(headers[1], "Header2: value2\r\n");
    parser.feed("ead");
    BOOST_CHECK_EQUAL(headers.size(), 2);
    parser.feed("er3: Val3\r\nC");
    BOOST_CHECK_EQUAL(headers.size(), 3);
    BOOST_CHECK_EQUAL(headers[2], "Header3: Val3\r\n");
    parser.feed("ontent-Length: 10\r\n\r");
    parser.feed("\n");
    BOOST_CHECK_EQUAL(headers.size(), 5);
    BOOST_CHECK_EQUAL(headers[3], "Content-Length: 10\r\n");
    BOOST_CHECK_EQUAL(headers[4], "\r\n");
    BOOST_CHECK_EQUAL(parser.remainingBody(), 10);

    /* body */
    parser.feed("0123");
    parser.feed("456");
    parser.feed("789");
    BOOST_CHECK_EQUAL(body, "0123456789");
    BOOST_CHECK_EQUAL(done, true);

    /* one full response and a partial one without body */
    parser.feed("HTTP/1.1 204 No content\r\n"
                "MyHeader: my value1\r\n\r\nHTTP");

    BOOST_CHECK_EQUAL(statusLine, "HTTP/1.1/204");
    BOOST_CHECK_EQUAL(headers.size(), 2);
    BOOST_CHECK_EQUAL(headers[0], "MyHeader: my value1\r\n");
    BOOST_CHECK_EQUAL(headers[1], "\r\n");
    BOOST_CHECK_EQUAL(body, "");
    BOOST_CHECK_EQUAL(done, true);
    BOOST_CHECK_EQUAL(parser.remainingBody(), 0);

    parser.feed("/1.1 666 The number of the beast\r\n"
                "Connection: close\r\n"
                "Header: value\r\n\r\n");
    BOOST_CHECK_EQUAL(statusLine, "HTTP/1.1/666");
    BOOST_CHECK_EQUAL(headers.size(), 3);
    BOOST_CHECK_EQUAL(headers[0], "Connection: close\r\n");
    BOOST_CHECK_EQUAL(headers[1], "Header: value\r\n");
    BOOST_CHECK_EQUAL(headers[2], "\r\n");
    BOOST_CHECK_EQUAL(body, "");
    BOOST_CHECK_EQUAL(done, true);
    BOOST_CHECK_EQUAL(shouldClose, true);
    BOOST_CHECK_EQUAL(parser.remainingBody(), 0);

    /* 2 full reponses with body */
    const char * payload = ("HTTP/1.1 200 This is some blabla\r\n"
                            "Header1: value1\r\n"
                            "Header2: value2\r\n"
                            "Content-Type: text/plain\r\n"
                            "Content-Length: 10\r\n"
                            "\r\n"
                            "0123456789");
    parser.feed(payload);
    BOOST_CHECK_EQUAL(body, "0123456789");
    BOOST_CHECK_EQUAL(done, true);
    parser.feed(payload);
    BOOST_CHECK_EQUAL(body, "0123456789");
    BOOST_CHECK_EQUAL(done, true);
    BOOST_CHECK_EQUAL(shouldClose, false);
}
#endif

#if 1
/* Ensures that multiline headers are correctly parsed. */
BOOST_AUTO_TEST_CASE( http_parser_multiline_header_test )
{
    vector<string> headers;

    HttpResponseParser parser;
    parser.onResponseStart = [&] (const string & httpVersion, int code) {
        headers.clear();
    };
    parser.onHeader = [&] (const char * data, size_t size) {
        headers.emplace_back(data, size);
    };

    parser.feed("HTTP/1.1 200 This is some blabla\r\n");

    parser.feed("Header1: value1\r\nH");
    BOOST_CHECK_EQUAL(headers.size(), 1);
    BOOST_CHECK_EQUAL(headers[0], "Header1: value1\r\n");

    parser.feed("eader2: value2\r\n  with another line\r\nH");
    BOOST_CHECK_EQUAL(headers.size(), 2);
    BOOST_CHECK_EQUAL(headers[1], "Header2: value2 with another line\r\n");
    parser.feed("eader3: Val3\r\n\t with tab\r\n  and space\r\nH");
    BOOST_CHECK_EQUAL(headers.size(), 3);
    BOOST_CHECK_EQUAL(headers[2], "Header3: Val3 with tab and space\r\n");
    parser.feed("eader4: Value4\r\n \r\n\r\n");
    BOOST_CHECK_EQUAL(headers.size(), 5);
    BOOST_CHECK_EQUAL(headers[3], "Header4: Value4\r\n");
    BOOST_CHECK_EQUAL(headers[4], "\r\n");
}
#endif

#if 1
/* Ensures that chunked encoding is well supported. */
BOOST_AUTO_TEST_CASE( http_parser_chunked_encoding_test )
{
    /* missing error tests:
       - invalid hex value for chunk length
       - excessive chunk size (> chunk length)
       - content-length and content-coding are mutually exclusive
       - no chunk after last-chunk
    */

    HttpResponseParser parser;

    string chunkA = randomString(0xa);
    string chunk20 = randomString(0x20);
    string chunk100 = randomString(0x100);

    int numResponses(0);
    parser.onResponseStart = [&] (const string & httpVersion, int code) {
        numResponses++;
    };

    vector<string> bodyChunks;
    parser.onData = [&] (const char * data, size_t size) {
        bodyChunks.emplace_back(data, size);
    };

    parser.feed("HTTP/1.1 200 This is some blabla\r\n"
                "Header1: value1\r\n"
                "Transfer-Encoding: chunked\r\n"
                "\r\n");
    BOOST_CHECK_EQUAL(numResponses, 1);

    string feedData = "a\r\n" + chunkA + "\r\n";
    parser.feed(feedData.c_str(), feedData.size());
    BOOST_CHECK_EQUAL(bodyChunks.size(), 1);
    BOOST_CHECK_EQUAL(bodyChunks[0], chunkA);

    feedData = "A;someext\r\n" + chunkA + "\r\n";
    parser.feed(feedData.c_str(), feedData.size());
    BOOST_CHECK_EQUAL(bodyChunks.size(), 2);
    BOOST_CHECK_EQUAL(bodyChunks[1], chunkA);

    feedData = "20;someext\r\n" + chunk20 + "\r\n";
    parser.feed(feedData.c_str(), feedData.size());
    BOOST_CHECK_EQUAL(bodyChunks.size(), 3);
    BOOST_CHECK_EQUAL(bodyChunks[2], chunk20);

    feedData = "100;otherext=value\r\n" + chunk100 + "\r\n";
    parser.feed(feedData.c_str(), feedData.size());
    BOOST_CHECK_EQUAL(bodyChunks.size(), 4);
    BOOST_CHECK_EQUAL(bodyChunks[3], chunk100);

    feedData = "0000\r\n\r\n";
    parser.feed(feedData.c_str(), feedData.size());
    BOOST_CHECK_EQUAL(bodyChunks.size(), 4);

    BOOST_CHECK_EQUAL(numResponses, 1);

    /* another response can be fed */
    parser.feed("HTTP/1.1 200 This is some blabla\r\n"
                "Header1: value1\r\n"
                "Transfer-Encoding: chunked\r\n"
                "\r\n");
    BOOST_CHECK_EQUAL(numResponses, 2);

    /* we now test chunks of multiple chunks */
    bodyChunks.clear();

    feedData = ("20\r\n" + chunk20 + "\r\n"
                "20\r\n" + chunk20 + "\r\n"
                "20\r\n" + chunk20 + "\r\n"
                "0\r\n\r\n");
    parser.feed(feedData.c_str(), feedData.size());
    BOOST_CHECK_EQUAL(bodyChunks.size(), 3);
    BOOST_CHECK_EQUAL(bodyChunks[0], chunk20);
    BOOST_CHECK_EQUAL(bodyChunks[1], chunk20);
    BOOST_CHECK_EQUAL(bodyChunks[2], chunk20);

    /* yet another response can be fed */
    parser.feed("HTTP/1.1 200 This is some blabla\r\n"
                "Header1: value1\r\n"
                "Transfer-Encoding: chunked\r\n"
                "\r\n");
    BOOST_CHECK_EQUAL(numResponses, 3);
}
#endif

string badRequest = 
"HTTP/1.1 123 BLAH\r\n"
"Host: xxxxxxxx_backend\r\n"
"Content-Length: 699\r\n"
"Accept: */*\r\n"
"Accept-Encoding: deflate, gzip\r\n"
"X-Forwarded-For: 12.34.56.78\r\n"
"x-openrtb-version: 2.2\r\n"
"User-Agent: xx_http_client\r\n"
"Content-Type: application/json\r\n"
"\r\n"
"{\"id\":\"xxxxxxxxxxxxxxxxxxxx\",\"imp\":[{\"id\":\"1\",\"displaymanager\":\"xxxxxxxx\",\"instl\":0,\"video\":{\"startdelay\":0,\"mimes\":[\"video\\/mp4\",\"video\\/x-flv\",\"application\\/x-shockwave-flash\",\"image\\/jpeg\",\"image\\/png\",\"image\\/gif\"],\"h\":400,\"w\":710,\"protocols\":[2,5],\"minduration\":0,\"maxduration\":180,\"linearity\":1,\"minbitrate\":256,\"playbackmethod\":[3],\"api\":[1,2],\"ext\":{\"adtype\":3}},\"secure\":0}],\"site\":{\"id\":\"98809\",\"domain\":\"xxxxxxxxxxxxxx.com\",\"page\":\"http%3A%2F%2Fxxxxxxxxxxxxxx.com%2F\",\"publisher\":{\"id\":\"98765\",\"domain\":\"xxxxxxxxxxxxxx.com\"},\"content\":{\"context\":\"6\",\"livestream\":0}},\"regs\":{\"coppa\":0},\"device\":{\"ua\":\"Mozilla\\/5.0 (Macintosh; Intel Mac OS X 10_10_4) AppleWebKit\\/600.7.12 (KHTML, like Ge";

BOOST_AUTO_TEST_CASE( test_bad_request )
{
    HttpResponseParser parser;
    parser.feed(badRequest.c_str());
}

BOOST_AUTO_TEST_CASE( test_bad_request2 )
{
    HttpHeader header;
    header.parse(badRequest);
    MLDB_TRACE_EXCEPTIONS(false);
    BOOST_CHECK_THROW(jsonDecodeStr<Json::Value>(header.knownData), ParseContext::Exception);
    BOOST_CHECK_THROW(Json::Value val2 = Json::parse(header.knownData), Json::Exception);
}

#if 1
/* Test the behaviour of the parser in the presence of the "Expect:
 * 100-continue" header. */
BOOST_AUTO_TEST_CASE( http_request_100_continue_test )
{
    const char requestHeaders[] = ("POST /pouet HTTP/1.1\r\n"
                                   "Content-Type: some/type\r\n"
                                   "Expect: 100-continue\r\n"
                                   "Content-Length: 16\r\n"
                                   "\r\n");
    const char requestBody[] = "abc 123 abcd 123";

    set<string> headers;
    string body;
    bool done(false);

    HttpRequestParser parser;
    parser.onRequestStart = [&] (const char * methodData, size_t methodSize,
                                 const char * urlData, size_t urlSize,
                                 const char * versionData, size_t vSize) {
        headers.clear();
        body.clear();
        done = false;
    };
    parser.onHeader = [&] (const char * data, size_t size) {
        headers.insert(string(data, size));
    };
    parser.onData = [&] (const char * data, size_t size) {
        body.append(data, size);
    };
    parser.onDone = [&] (bool doClose) {
        done = true;
    };

    /* first test: without a on100Continue callback */
    parser.feed(requestHeaders);
    BOOST_CHECK(headers.count("Expect: 100-continue\r\n") > 0);
    BOOST_CHECK(headers.count("Content-Length: 16\r\n") > 0);
    BOOST_CHECK(body.empty());
    BOOST_CHECK(!done);
    parser.feed(requestBody);
    BOOST_CHECK_EQUAL(body, string(requestBody));
    BOOST_CHECK(done);

    /* second test: with a on100Continue callback that returns true */
    auto on100ContinueTrue = [] () {
        return true;
    };
    parser.onExpect100Continue = on100ContinueTrue;
    parser.feed(requestHeaders);
    BOOST_CHECK(headers.count("Expect: 100-continue\r\n") == 0);
    BOOST_CHECK(headers.count("Content-Length: 16\r\n") > 0);
    BOOST_CHECK(!done);
    parser.feed(requestBody);
    BOOST_CHECK_EQUAL(body, string(requestBody));
    BOOST_CHECK(done);

    /* third test: with a on100Continue callback that returns false */
    auto on100ContinueFalse = [] () {
        return false;
    };
    parser.onExpect100Continue = on100ContinueFalse;
    parser.feed(requestHeaders);
    BOOST_CHECK(headers.count("Expect: 100-continue\r\n") == 0);
    BOOST_CHECK(headers.count("Content-Length: 16\r\n") > 0);
    BOOST_CHECK(done);

    /* we get an exception because the parser expects a valid request first
       line after the rejection of the expectation. */
    MLDB_TRACE_EXCEPTIONS(false);
    BOOST_CHECK_THROW(parser.feed(requestBody), MLDB::Exception);
}
#endif

#if 1
/* The HttpResponse and HttpRequestParsers share most of their code, which is
 * tested above. This test here thus is limited to the parsing of a request
 * line. */
BOOST_AUTO_TEST_CASE( http_request_parser_test )
{
    string statusLine;
    vector<string> headers;
    string body;
    bool done;
    bool shouldClose;

    HttpRequestParser parser;
    parser.onRequestStart = [&] (const char * methodData, size_t methodSize,
                                 const char * urlData, size_t urlSize,
                                 const char * versionData, size_t versionSize) {
        cerr << "request start\n";
        statusLine = (string(methodData, methodSize)
                      + "|" + string(urlData, urlSize)
                      + "|" + string(versionData, versionSize));
        headers.clear();
        body.clear();
        shouldClose = false;
        done = false;
    };
    parser.onHeader = [&] (const char * data, size_t size) {
        // cerr << "header: " + string(data, size) + "\n";
        headers.emplace_back(data, size);
    };
    parser.onData = [&] (const char * data, size_t size) {
        // cerr << "data\n";
        body.append(data, size);
    };
    parser.onDone = [&] (bool doClose) {
        shouldClose = doClose;
        done = true;
    };

    /* status line */
    parser.feed("GE");
    BOOST_CHECK_EQUAL(statusLine, "");
    parser.feed("T /poiltruc?bla");
    BOOST_CHECK_EQUAL(statusLine, "");
    parser.feed("blabla HTTP/1.1");
    BOOST_CHECK_EQUAL(statusLine, "");
    parser.feed("\r");
    BOOST_CHECK_EQUAL(statusLine, "");
    parser.feed("\n");
    BOOST_CHECK_EQUAL(statusLine, "GET|/poiltruc?blablabla|HTTP/1.1");
}
#endif
