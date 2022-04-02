/* lisp_parsing.cc                                                  -*- C++ -*-
   Jeremy Barnes, 22 February 2013
   Copyright (c) 2013 mldb.ai inc.  All rights reserved.

   This file is part of MLDB. Copyright 2015 mldb.ai inc. All rights reserved.
*/

#include "lisp_parsing.h"
#include "mldb/base/scope.h"

using namespace std;

namespace MLDB {

// in json_parsing.cc
int getEscapedJsonCharacterPointUtf8(ParseContext & context);

namespace Lisp {

std::optional<PathElement>
match_rule_name(ParseContext & context)
{
    std::string segment;
    segment.reserve(18);
    context.skip_whitespace();
    if (!context) {
        return nullopt;
    }
    else {
        char c = *context;
        // Rules start with a capital letter or an @
        if (!isupper(c) && c != '@')
            return nullopt;
        segment += c;  c = *(++context);
        while (isalpha(c) || c == '_' || (!segment.empty() && isnumber(c))) {
            segment += c;
            ++context;
            if (context.eof())
                break;
            c = *context;
        }
        if (segment.empty())
            return nullopt;
        else return PathElement(segment);
    }
}

static bool isid(int c)
{
    return !(c == '(' || c == ')' || c == '\'' || c == '"' || c == ':'
          || c == ';' || c == ',' || c == '['  || c == ']' || c == '{'
          || c == '}' || c == '.' || isspace(c));
}

// Current character is included if include is true
std::optional<PathElement>
match_rest_of_name(ParseContext & context, bool includeFirst, ParseContext::Revert_Token * token)
{
    std::string segment;
    segment.reserve(18);

    //cerr << "rest of name: first = " << *context << endl;

    if (includeFirst)
        segment += *context;
    ++context;

    while (context) {
        char c = *context;
        //cerr << "rest of name: next = " << *context << endl;
        if (!isid(c))
            break;
        segment += c;
        ++context;
    }
    if (segment.empty())
        return nullopt;
    if (token)
        token->ignore();
    return PathElement(segment);
}

std::optional<PathElement>
match_variable_name(ParseContext & context)
{
    ParseContext::Revert_Token token(context);
    context.skip_whitespace();
    if (!context) {
        return nullopt;
    }

    char c = *context;
    // Variables start with a lowercase letter or an $
    if (!islower(c) && c != '$')
        return nullopt;
    return match_rest_of_name(context, c != '$', &token);
}

std::optional<PathElement>
match_symbol_name(ParseContext & context)
{
    ParseContext::Revert_Token token(context);
    context.skip_whitespace();
    if (!context)
        return nullopt;
    char c = *context;
    if (isdigit(c) || !isid(c))
        return nullopt;
    return match_rest_of_name(context, true, &token);
}

static constexpr std::array operators{'+', '-', '*', '/', '%'};

std::optional<PathElement>
match_operator_name(ParseContext & context)
{
    ParseContext::Revert_Token token(context);
    context.skip_whitespace();
    for (char op: operators) {
        if (context.match_literal(op)) {
            token.ignore();
            return PathElement(std::string(1, op));
        }
    }
    return nullopt;
}

optional<Utf8String> match_delimited_string(ParseContext & context, char delim)
{
    ParseContext::Revert_Token token(context);

    context.skip_whitespace();

    if (!context.match_literal(delim))
        return nullopt;

    try {
        char internalBuffer[4096];

        char * buffer = internalBuffer;
        size_t bufferSize = 4096;
        size_t pos = 0;
        Scope_Exit(if (buffer != internalBuffer) delete[] buffer);

        // Keep expanding until it fits
        while (!context.match_literal(delim)) {
            if (context.eof())
                return nullopt;

            // We need up to 4 characters to add a new UTF-8 code point
            if (pos >= bufferSize - 4) {
                size_t newBufferSize = bufferSize * 8;
                char * newBuffer = new char[newBufferSize];
                std::copy(buffer, buffer + bufferSize, newBuffer);
                if (buffer != internalBuffer)
                    delete[] buffer;
                buffer = newBuffer;
                bufferSize = newBufferSize;
            }

            int c = *context;
            
            //cerr << "c = " << c << " " << (char)c << endl;

            if (c < 0 || c > 127) {
                // Unicode
                c = context.expect_utf8_code_point();

                // 3.  Write the decoded character to the buffer
                char * p1 = buffer + pos;
                char * p2 = p1;
                pos += utf8::append(c, p2) - p1;

                continue;
            }
            ++context;

            if (c == '\\') {
                c = getEscapedJsonCharacterPointUtf8(context);
            }

            if (c < ' ' || c >= 127) {
                char * p1 = buffer + pos;
                char * p2 = p1;
                pos += utf8::append(c, p2) - p1;
            }
            else buffer[pos++] = c;
        }

        Utf8String result(string(buffer, buffer + pos));
        
        token.ignore();
        return result;
    } catch (const MLDB::Exception & exc) {
        return nullopt;
    }
}

} // namespace Lisp
} // namespace MLDB
