/* http_parsers.h                                                  -*- C++ -*-
   Wolfgang Sourdeau, January 2014
   Copyright (c) 2014 mldb.ai inc.  All rights reserved.

   This file is part of MLDB. Copyright 2015 mldb.ai inc. All rights reserved.
*/

#include <string.h>
#include <strings.h>
#include <iostream>
#include "base/exc_assert.h"
#include "mldb/arch/exception.h"
#include "mldb/base/parse_context.h"

#include "http_parsers.h"

using namespace std;
using namespace MLDB;


/****************************************************************************/
/* HTTP PARSER :: BUFFER STATE                                              */
/****************************************************************************/

struct HttpParser::BufferState : public ParseContext {
    BufferState(const char * start, size_t length, bool fromBuffer);

    /* skip as many characters as possible until character "c" is found */
    bool skip_to_char(char c, bool throwOnEol);

    const char * start()
        const
        {
            return start_;
        }

    const char * get_offset_ptr()
        const
        {
            return start_ + get_offset();
        }

    bool from_buffer()
        const
        {
            return fromBuffer_;
        }

private:
    const char * start_;
    bool fromBuffer_;
};


namespace {

/* Parse an integer stored in the chars between "start" and "end",
   where all characters are expected to be strict digits. The name is inspired
   from "atoi" with the "n" indicating that it is reading only from a numbered
   set of bytes. Base 10 can be negative. */
int
antoi(const char * start, const char * end, int base = 10)
{
    int result(0);
    bool neg = false;
    if (*start == '-') {
        if (base == 10) {
            neg = true;
        }
        else {
            throw MLDB::Exception("Cannot negate non base 10");
        }
        start++;
    }
    else if (*start == '+') {
        start++;
    }

    for (const char * ptr = start; ptr < end; ptr++) {
        int digit;
        if (*ptr >= '0' and *ptr <= '9') {
            digit = *ptr - '0';
        }
        else if (*ptr >= 'A' and *ptr <= 'F') {
            digit = *ptr - 'A' + 10;
        }
        else if (*ptr >= 'a' and *ptr <= 'f') {
            digit = *ptr - 'a' + 10;
        }
        else {
            throw MLDB::Exception("expected digit");
        }
        if (digit > base) {
            intptr_t offset = ptr - start;
            throw MLDB::Exception("digit '%c' (%d) exceeds base '%d'"
                                " at offset '%d'",
                                *ptr, digit, base, offset);
        }
        result = result * base + digit;
    }

    if (neg) {
        return result * -1;
    }

    return result;
}

HttpParser::BufferState
prepareParsing(const char * bufferData, size_t bufferSize, string & buffer)
{
    const char * data;
    size_t dataSize;
    bool fromBuffer;

    if (buffer.size() > 0) {
        buffer.append(bufferData, bufferSize);
        data = buffer.c_str();
        dataSize = buffer.size();
        fromBuffer = true;
    }
    else {
        data = bufferData;
        dataSize = bufferSize;
        fromBuffer = false;
    }

    return HttpParser::BufferState(data, dataSize, fromBuffer);
}

} // local namespace

/****************************************************************************/
/* HTTP PARSER                                                              */
/****************************************************************************/

void
HttpParser::
clear()
    noexcept
{
    expectBody_ = true;
    stage_ = 0;
    buffer_.clear();
    expect100Continue_ = false;
    remainingBody_ = 0;
    useChunkedEncoding_ = false;
    requireClose_ = false;
}

void
HttpParser::
feed(const char * bufferData)
{
    // cerr << "feed: /" + ML::hexify_string(string(bufferData)) + "/\n";
    feed(bufferData, strlen(bufferData));
}

void
HttpParser::
feed(const char * bufferData, size_t bufferSize)
{
    // std::cerr << ("data: /"
    //          + ML::hexify_string(string(bufferData, bufferSize))
    //          + "/\n");
    auto state = prepareParsing(bufferData, bufferSize, buffer_);
    // cerr << ("state: " + to_string(stage_)
    //          + "; dataSize: " + to_string(dataSize) + "\n");

    /* We loop as long as there are bytes available for parsing and as long as
       the parsing stages change. */
    bool stageDone(true);
    while (stageDone && state.readahead_available() > 0) {
        if (stage_ == 0) {
            stageDone = parseFirstLine(state);
            if (stageDone) {
                stage_ = 1;
            }
        }
        else if (stage_ == 1) {
            stageDone = parseHeaders(state);
            if (stageDone) {
                if (expect100Continue_) {
                    /* The setting of expect100Continue_ is conditional on the
                       definition of onExpect100Continue. */
                    ExcAssert(onExpect100Continue);
                    if (!onExpect100Continue()) {
                        expectBody_ = false;
                    }
                }
                if (!expectBody_
                    || (remainingBody_ == 0 && !useChunkedEncoding_)) {
                    finalizeParsing();
                    stage_ = 0;
                }
                else {
                    stage_ = 2;
                }
            }
        }
        else if (stage_ == 2) {
            stageDone = parseBody(state);
            if (stageDone) {
                finalizeParsing();
                stage_ = 0;
            }
        }
    }

    size_t remaining = state.readahead_available();
    if (remaining > 0) {
        if (state.get_offset() > 0 || !state.from_buffer()) {
            buffer_.assign(state.get_offset_ptr(), remaining);
        }
    }
    else if (state.from_buffer()) {
        buffer_.clear();
    }
}

bool
HttpParser::
parseHeaders(BufferState & state)
{
    string multiline;
    unsigned int numLines(0);

    std::unique_ptr<ParseContext::Revert_Token> token;

    token.reset(new ParseContext::Revert_Token(state));

    /* header line parsing */
    while (*state != '\r' || numLines > 0) {
        size_t lineOff = state.get_offset();
        if (numLines == 0) {
            if (!state.skip_to_char(':', true)) {
                return false;
            }
        }
        if (!state.skip_to_char('\r', false)) {
            return false;
        }
        if (state.readahead_available() < 3) {
            return false;
        }
        state++;
        state.expect_literal('\n');

        /* does the next line starts with a space or a tab? */
        if (*state == ' ' || *state == '\t') {
            multiline.append(state.start() + lineOff, state.get_offset() - lineOff - 2);
            numLines++;
            state++;
        }
        else {
            if (numLines == 0) {
                handleHeader(state.start() + lineOff, state.get_offset() - lineOff);
            }
            else {
                multiline.append(state.start() + lineOff,
                                 state.get_offset() - lineOff);
                handleHeader(multiline.c_str(), multiline.size());
                multiline.clear();
                numLines = 0;
            }
            token->ignore();
            token.reset(new ParseContext::Revert_Token(state));
        }
    }
    if (state.get_offset() + 1 == state.total_buffered()) {
        return false;
    }
    state++;
    state.expect_literal('\n');
    token->ignore();

    if (onHeader) {
        onHeader("\r\n", 2);
    }

    return true;
}

void
HttpParser::
handleHeader(const char * data, size_t dataSize)
{
    size_t ptr(0);
    bool skipHeader(!onHeader);

    auto skipToChar = [&] (char c) {
        while (ptr < dataSize) {
            if (data[ptr] == c)
                return true;
            ptr++;
        }

        return false;
    };
    auto skipChar = [&] (char c) {
        while (ptr < dataSize && data[ptr] == c) {
            ptr++;
        }
    };
    auto matchString = [&] (const char * testString, size_t len) {
        bool result;
        if (dataSize >= (ptr + len)
            && ::strncasecmp(data + ptr, testString, len) == 0) {
            ptr += len;
            result = true;
        }
        else {
            result = false;
        }
        return result;
    };

    auto skipToValue = [&] () {
        skipChar(' ');
        skipToChar(':');
        ptr++;
        skipChar(' ');
    };

    if (matchString("Connection", 10)) {
        skipToValue();
        if (matchString("close", 5)) {
            requireClose_ = true;
        }
    }
    else if (matchString("Content-Length", 14)) {
        skipToValue();
        remainingBody_ = antoi(data + ptr, data + dataSize - 2);
    }
    else if (matchString("Transfer-Encoding", 15)) {
        skipToValue();
        if (matchString("chunked", 7)) {
            useChunkedEncoding_ = true;
        }
    }
    else if (matchString("Expect", 6)) {
        skipToValue();
        if (matchString("100-continue", 12)) {
            if (onExpect100Continue) {
                expect100Continue_ = true;
                skipHeader = true;
            }
        }
    }

    if (!skipHeader) {
        onHeader(data, dataSize);
    }
}

bool
HttpParser::
parseBody(BufferState & state)
{
    return (useChunkedEncoding_
            ? parseChunkedBody(state)
            : parseBlockBody(state));
}

bool
HttpParser::
parseChunkedBody(BufferState & state)
{
    int chunkSize(-1);

    /* we loop as long as there are chunks to process */
    while (chunkSize != 0) {
        ParseContext::Revert_Token token(state);
        const char * sizeStart = state.get_offset_ptr();
        if (!state.skip_to_char('\r', false)) {
            return false;
        }

        if (state.readahead_available() < 2) {
            return false;
        }

        const char * sizeEnd = state.get_offset_ptr();

        /* look for ';' and adjust sizeEnd in consequence */
        for (const char * ptr = sizeStart; ptr < sizeEnd; ptr++) {
            if (*ptr == ';') {
                sizeEnd = ptr;
                break;
            }
        }

        chunkSize = antoi(sizeStart, sizeEnd, 16);

        state += 2;
        if (state.readahead_available() < chunkSize + 2) {
            return false;
        }

        if (onData && chunkSize > 0) {
            onData(state.get_offset_ptr(), chunkSize);
        }
        state += chunkSize + 2;
        token.ignore();
    }

    return true;
}

bool
HttpParser::
parseBlockBody(BufferState & state)
{
    ParseContext::Revert_Token token(state);

    size_t chunkSize = min<size_t>(state.readahead_available(), remainingBody_);
    // cerr << "toSend: " + to_string(chunkSize) + "\n";
    // cerr << "received body: /" + string(data, chunkSize) + "/\n";
    if (onData && chunkSize > 0) {
        onData(state.get_offset_ptr(), chunkSize);
    }
    state += chunkSize;
    remainingBody_ -= chunkSize;
    token.ignore();

    return (remainingBody_ == 0);
}

void
HttpParser::
finalizeParsing()
{
    if (onDone) {
        onDone(requireClose_);
    }
    clear();
}


/****************************************************************************/
/* HTTP PARSER :: BUFFER STATE                                              */
/****************************************************************************/

HttpParser::BufferState::
BufferState(const char * start, size_t length, bool fromBuffer)
    : ParseContext(ParseContext::CONSOLE, start, start + length),
      start_(start), fromBuffer_(fromBuffer)
{
}

bool
HttpParser::BufferState::
skip_to_char(char c, bool throwOnEol)
{
    while (!eof()) {
        if (operator *() == c) {
            return true;
        }
        else if (throwOnEol && match_eol(false)) {
            throw MLDB::Exception("unexpected end of line");
        }
        operator ++();
    }

    return false;
}


/****************************************************************************/
/* HTTP RESPONSE PARSER                                                     */
/****************************************************************************/

bool
HttpResponseParser::
parseFirstLine(BufferState & state)
{
    /* status line parsing */

    /* sizeof("HTTP/X.X XXX ") */
    if (state.readahead_available() < 16) {
        return false;
    }

    ParseContext::Revert_Token token(state);
    if (!state.match_literal_str("HTTP/", 5)) {
        throw MLDB::Exception("version must start with 'HTTP/'");
    }

    if (!state.skip_to_char(' ', true)) {
        /* post-version ' ' not found even though size is sufficient */
        throw MLDB::Exception("version too long");
    }
    size_t versionEnd = state.get_offset();

    state++;
    const char * codeStartPtr = state.get_offset_ptr();
    if (!state.skip_to_char(' ', true)) {
        /* post-code ' ' not found even though size is sufficient */
        throw MLDB::Exception("code too long");
    }
    const char * codeEndPtr = state.get_offset_ptr();
    int code = antoi(codeStartPtr, codeEndPtr);

    /* we skip the whole "reason" string */
    if (!state.skip_to_char('\r', false)) {
        return false;
    }
    state++;
    if (state.readahead_available() == 0) {
        return false;
    }
    state.expect_literal('\n');
    token.ignore();

    if (onResponseStart) {
        onResponseStart(std::string(state.start(), versionEnd), code);
    }

    return true;
}


/****************************************************************************/
/* HTTP REQUEST PARSER                                                      */
/****************************************************************************/

bool
HttpRequestParser::
parseFirstLine(BufferState & state)
{
    /* request line parsing */

    ParseContext::Revert_Token token(state);
    size_t methodStart = state.get_offset();
    if (!state.skip_to_char(' ', true)) {
        return false;
    }
    size_t methodSize = state.get_offset() - methodStart;
    state++;

    size_t urlStart = state.get_offset();
    if (!state.skip_to_char(' ', true)) {
        return false;
    }
    size_t urlSize = state.get_offset() - urlStart;
    state++;

    if (!state.match_literal_str("HTTP/", 5)) {
        throw MLDB::Exception("version must start with 'HTTP/'");
    }
    size_t versionStart = urlStart + urlSize + 1;
    /* we skip the whole "reason" string */
    if (!state.skip_to_char('\r', false)) {
        return false;
    }
    size_t versionSize = state.get_offset() - versionStart;
    state++;

    if (state.readahead_available() == 0) {
        return false;
    }
    state.expect_literal('\n');
    token.ignore();

    if (onRequestStart) {
        onRequestStart(state.start() + methodStart, methodSize,
                       state.start() + urlStart, urlSize,
                       state.start() + versionStart, versionSize);
    }

    return true;
}
