/* json_stream.cc                                                  -*- C++ -*-
   Jeremy Barnes, 22 February 2013
   Copyright (c) 2013 mldb.ai inc.  All rights reserved.

   This file is part of MLDB. Copyright 2015 mldb.ai inc. All rights reserved.
*/

#include "json_stream.h"
#include "mldb/arch/exception.h"
#include "mldb/types/path.h"
#include "mldb/types/value_description.h"
#include <optional>


using namespace std;


namespace MLDB {

const Json::Value ArrayJsonStreamParsingContext::DUMMY;

namespace {

Utf8String
stringRender(const Json::Value & val)
{
    using std::to_string;

    switch (val.type()) {
    case Json::nullValue:    return "";
    case Json::intValue:     return to_string(val.asInt());
    case Json::uintValue:    return to_string(val.asUInt());
    case Json::realValue:    return to_string(val.asDouble());
    case Json::booleanValue: return to_string(val.asBool());
    case Json::stringValue:  return val.asStringUtf8();
    case Json::arrayValue: {
        Utf8String str;
        for (auto & v: val) {
            if (!str.empty())
                str += " ";
            str += stringRender(v);
        }
        return str;
    }
    default:
        throw MLDB::Exception("can't render value " + val.toString() + " as string");
    }
}

} // file scope

struct JsonApplyEach: public JsonStreamProcessor {

    std::vector<std::shared_ptr<JsonStreamProcessor>> processors;

    virtual void process(JsonStreamParsingContext & in, JsonStreamPrintingContext & out) const override
    {
        auto position = in.savePosition();

        for (auto & processor: processors) {
            in.restorePosition(position);
            processor->process(in, out);
        }
    }
};

struct JsonApplyChain: public JsonStreamProcessor {

    std::vector<std::shared_ptr<JsonStreamProcessor>> processors;

    virtual void process(JsonStreamParsingContext & in, JsonStreamPrintingContext & out) const override
    {
        MLDB_THROW_UNIMPLEMENTED("apply chain");
    }
};


struct JsonTransformer: public JsonStreamProcessor {

    virtual Json::Value transform(const Json::Value & input) const
    {
        MLDB_THROW_UNIMPLEMENTED_ON_THIS("Need to override one of the tranform() methods");
    }

    virtual void transform(JsonParsingContext & in, JsonPrintingContext & out) const
    {
        auto val = in.expectJson();
        out.writeJson(transform(val));
    }

    virtual void process(JsonStreamParsingContext & in, JsonStreamPrintingContext & out) const override
    {
        //cerr << "process" << endl;
        while (auto current = in.current()) {
            transform(**current, out.current());
            in.next();
            out.next();
        }
        out.finish();
    }
};

struct PathJsonStreamProcessor: public JsonTransformer {
    PathJsonStreamProcessor(Path path)
        : path(path)
    {
    }

    Path path;

    virtual void transform(JsonParsingContext & in, JsonPrintingContext & out) const
    {
        MLDB_THROW_UNIMPLEMENTED("extract path");
    }

    virtual void process(JsonStreamParsingContext & in, JsonStreamPrintingContext & out) const override
    {
        if (path.empty()) {
            out.take(in);
        }
        else {
            JsonTransformer::process(in, out);
        }
    }
};

struct LiteralJsonStreamProcessor: public JsonTransformer {
    LiteralJsonStreamProcessor(Json::Value literal)
        : literal(std::move(literal))
    {
    }

    Json::Value literal;

    virtual void transform(JsonParsingContext & in, JsonPrintingContext & out) const override
    {
        out.writeJson(literal);
    }
};

struct ConcatenationStreamProcessor: public JsonTransformer {
    ConcatenationStreamProcessor(std::vector<std::shared_ptr<JsonTransformer>> elements)
        : elements(std::move(elements))
    {
    }

    std::vector<std::shared_ptr<JsonTransformer>> elements;

    virtual void transform(JsonParsingContext & in, JsonPrintingContext & out) const override
    {
        Utf8String result;
        for (auto & el: elements) {
            Json::Value elVal;
            StructuredJsonPrintingContext elOut(elVal);
            el->transform(in, elOut);
            result += stringRender(elVal);
        }
        out.writeStringUtf8(std::move(result));
    }
};

struct StructureJsonStreamProcessor: public JsonTransformer {
    StructureJsonStreamProcessor(std::vector<std::pair<PathElement, std::shared_ptr<JsonTransformer>>> members)
        : members(std::move(members))
    {
    }

    std::vector<std::pair<PathElement, std::shared_ptr<JsonTransformer>>> members;

    virtual void transform(JsonParsingContext & in, JsonPrintingContext & out) const override
    {
        out.startObject();
        for (auto & [name, value]: members) {
            out.startMember(name.toUtf8String());
            value->transform(in, out);
        }
        out.endObject();
    }
};

struct JsonArithmetic: public JsonTransformer {
    JsonArithmetic(std::shared_ptr<JsonTransformer> lhs,
                   std::shared_ptr<JsonTransformer> rhs)
        : lhs(std::move(lhs)), rhs(std::move(rhs))
    {
    }

    std::shared_ptr<JsonTransformer> lhs, rhs;

    virtual Json::Value apply(Json::Value l, Json::Value r) const = 0;

    virtual void transform(JsonParsingContext & in, JsonPrintingContext & out) const override
    {
        Json::Value l, r;
        StructuredJsonPrintingContext cl(l), cr(r);
        lhs->transform(in, cl);
        rhs->transform(in, cr);
        Json::Value res = apply(std::move(l), std::move(r));
        out.writeJson(std::move(res));
    }
};

struct JsonAddition: public JsonArithmetic {
    using JsonArithmetic::JsonArithmetic;

    virtual Json::Value apply(Json::Value lhs, Json::Value rhs) const
    {
        if (lhs.isNumeric() && rhs.isNumeric()) {
            if (lhs.isDouble() || rhs.isDouble()) {
                return lhs.asDouble() + rhs.asDouble();
            }
            return lhs.asInt() + rhs.asInt();
        }
        else if (lhs.isString() || rhs.isString()) {
            return stringRender(lhs) + stringRender(rhs);
        }
        else if (lhs.isArray() && rhs.isArray()) {
            Json::Value result = lhs;
            for (auto && v: rhs)
                result.append(v);
            return result;
        }
        else return stringRender(lhs) + stringRender(rhs);
    }
};

struct JsonSubtraction: public JsonArithmetic {
    using JsonArithmetic::JsonArithmetic;

    virtual Json::Value apply(Json::Value lhs, Json::Value rhs) const
    {
        if (lhs.isNumeric() && rhs.isNumeric()) {
            if (lhs.isDouble() || rhs.isDouble()) {
                return lhs.asDouble() + rhs.asDouble();
            }
            return lhs.asInt() - rhs.asInt();
        }
        else throw MLDB::Exception("don't know how to subtract %s and %s",
                                 lhs.toString().c_str(),
                                 rhs.toString().c_str());
    }
};

struct JsonMultiplication: public JsonArithmetic {
    using JsonArithmetic::JsonArithmetic;

    virtual Json::Value apply(Json::Value lhs, Json::Value rhs) const
    {
        if (lhs.isNumeric() && rhs.isNumeric()) {
            if (lhs.isDouble() || rhs.isDouble()) {
                return lhs.asDouble() * rhs.asDouble();
            }
            return lhs.asInt() * rhs.asInt();
        }
        else throw MLDB::Exception("don't know how to multiply %s by %s",
                                 lhs.toString().c_str(),
                                 rhs.toString().c_str());
    }
};

struct JsonDivision: public JsonArithmetic {
    using JsonArithmetic::JsonArithmetic;

    virtual Json::Value apply(Json::Value lhs, Json::Value rhs) const
    {
        if (lhs.isNumeric() && rhs.isNumeric()) {
            if (lhs.isDouble() || rhs.isDouble()) {
                if (rhs.asDouble() == 0.0) {
                    throw MLDB::Exception("divide by zero");
                }
                return lhs.asDouble() / rhs.asDouble();
            }
            if (rhs.asInt() == 0) {
                throw MLDB::Exception("divide by zero");
            }
            return lhs.asInt() / rhs.asInt();
        }
        else throw MLDB::Exception("don't know how to divide %s by %s",
                                 lhs.toString().c_str(),
                                 rhs.toString().c_str());
    }
};

struct JsonModulus: public JsonArithmetic {
    using JsonArithmetic::JsonArithmetic;

    virtual Json::Value apply(Json::Value lhs, Json::Value rhs) const
    {
        if (lhs.isNumeric() && rhs.isNumeric()) {
            if (rhs.asInt() == 0) {
                throw MLDB::Exception("divide by zero in modulus");
            }
            return lhs.asInt() % rhs.asInt();
        }
        else throw MLDB::Exception("don't know how to take %s modulo %s",
                                 lhs.toString().c_str(),
                                 rhs.toString().c_str());
    }
};

struct JsonFormatter: public JsonTransformer {
    JsonFormatter(Utf8String formatter)
        : formatter(std::move(formatter))
    {
    }

    Utf8String formatter;

    virtual void transform(JsonParsingContext & in, JsonPrintingContext & out) const override
    {
        MLDB_THROW_UNIMPLEMENTED(("formatter @" + formatter).rawString());
    }
};

std::optional<Utf8String> match_identifier(ParseContext & context)
{
    Utf8String name;
    name.reserve(18);
    char c = *context;
    while (isalpha(c) || c == '_' || (!name.empty() && isnumber(c))) {
        name += c;
        ++context;
        if (context.eof())
            break;
        c = *context;
    }
    if (name.empty())
        return nullopt;
    else return std::move(name);
}

Utf8String expect_identifier(ParseContext & context)
{
    auto result = match_identifier(context);
    if (!result)
        context.exception("expected identifier");
    return *result;
}

std::optional<PathElement> match_path_segment(ParseContext & context)
{
    std::string segment;
    segment.reserve(18);
    if (context.match_literal('"')) {
        // quoted segment name
        // TODO: embedded quotes in name
        segment += context.expect_text("\"");
        context.expect_literal('\"');
        return PathElement(segment);
    }
    else {
        char c = *context;
        while (isalpha(c) || c == '_' || (!segment.empty() && isnumber(c))) {
            segment += c;
            ++context;
            if (context.eof())
                break;
            c = *context;
        }
        if (segment.empty())
            return {};
        else return PathElement(segment);
    }
}

std::optional<Path> match_path(ParseContext & context)
{
    //cerr << "match_path: *context = " << *context << (int)*context << endl;
    context.skip_whitespace();
    if (context.eof() || *context != '.')
        return {};
    
    PathBuilder builder;
    bool first = true;
    while (context.match_literal('.')) {
        if (first && context.eof())
            break;
        auto segment = match_path_segment(context);
        if (!segment) {
            context.exception("empty path name");
        }
        builder.add(*segment);
        first = false;
        context.skip_whitespace();
    }
    //cerr << "extracting path" << endl;
    return builder.extract();
}

int getEscapedJsonCharacterPointUtf8(ParseContext & context);  // in json_parsing.cc

std::shared_ptr<JsonTransformer> createJqTransformer(ParseContext & context, int minPrecedence = 0);

std::shared_ptr<JsonTransformer> expectStringTemplateUtf8(ParseContext & context)
{
    std::vector<std::shared_ptr<JsonTransformer>> result;

    skipJsonWhitespace(context);
    context.expect_literal('"');

    char internalBuffer[4096];

    char * buffer = internalBuffer;
    size_t bufferSize = 4096;
    size_t pos = 0;

    auto pushResult = [&] ()
    {
        Utf8String str(string(buffer, buffer + pos));
        result.emplace_back(std::make_shared<LiteralJsonStreamProcessor>(str));
        pos = 0;
    };

    // Keep expanding until it fits
    while (!context.match_literal('"')) {
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
            c = utf8::unchecked::next(context);

            char * p1 = buffer + pos;
            char * p2 = p1;
            pos += utf8::append(c, p2) - p1;

            continue;
        }
        ++context;

        if (c == '\\') {
            if (*context == '(') {
                ++context;
                // Interpolated string... we parse out an expression
                // inter\("pol" + "ation")" -> "interpolation"
                pushResult();
                auto interpolation = createJqTransformer(context);
                result.emplace_back(std::move(interpolation));
                context.skip_whitespace();
                context.expect_literal(')');
                continue;
            }
            c = getEscapedJsonCharacterPointUtf8(context);
        }

        if (c < ' ' || c >= 127) {
            char * p1 = buffer + pos;
            char * p2 = p1;
            pos += utf8::append(c, p2) - p1;
        }
        else buffer[pos++] = c;
    }

    if (pos != 0 || result.empty())
        pushResult();

    if (buffer != internalBuffer)
        delete[] buffer;

    if (result.size() == 1) {

    }

    return std::make_shared<ConcatenationStreamProcessor>(std::move(result));
}

std::shared_ptr<JsonTransformer> createJqTransformer(ParseContext & context, int minPrecedence)
{
    std::shared_ptr<JsonTransformer> result;
    context.skip_whitespace();
    if (*context == '.') {
        auto path = match_path(context);
        result = std::make_shared<PathJsonStreamProcessor>(std::move(*path));
    }
    else if (context.match_literal('{')) {
        context.skip_whitespace();
        std::vector<std::pair<PathElement, std::shared_ptr<JsonTransformer>>> members;
        while (!context.match_literal('}')) {
            context.skip_whitespace();
            auto key = match_path_segment(context);
            if (!key)
                context.exception("expected structure member name");
            context.skip_whitespace();
            context.expect_literal(':');
            context.skip_whitespace();
            auto value = createJqTransformer(context);
            members.emplace_back(std::move(*key), std::move(value));
            if (context.match_literal(','))
                continue;
            context.expect_literal('}', "expected } to close structure");
            break;
        }
        result = std::make_shared<StructureJsonStreamProcessor>(std::move(members));
    }
    else if (*context == '"') {
        result = expectStringTemplateUtf8(context);
    }
    else if (context.match_literal('(')) {
        // Parenthesis
        result = createJqTransformer(context, 0 /* precedence */);
        context.skip_whitespace();
        context.expect_literal(')');
    }
    else if (*context == '{') {
        MLDB_THROW_UNIMPLEMENTED("arrays");
    }
    else if (*context == '$') {
        MLDB_THROW_UNIMPLEMENTED("variables");
    }
    else if (context.match_literal('@')) {
        auto formatter = expect_identifier(context);
        result = std::make_shared<JsonFormatter>(formatter);
    }
    else {
        auto literal = expectJson(context);
        result = std::make_shared<LiteralJsonStreamProcessor>(std::move(literal));
    }

    context.skip_whitespace();

    while (context) {
        if (minPrecedence <= 10 && context.match_literal('+')) {
            auto rhs = createJqTransformer(context, 10);
            result = std::make_shared<JsonAddition>(result, rhs);
        }
        else if (minPrecedence <= 10 && context.match_literal('-')) {
            auto rhs = createJqTransformer(context, 10);
            result = std::make_shared<JsonSubtraction>(result, rhs);
        }
        else if (minPrecedence <= 20 && context.match_literal('*')) {
            auto rhs = createJqTransformer(context, 20);
            result = std::make_shared<JsonMultiplication>(result, rhs);
        }
        else if (minPrecedence <= 20 && context.match_literal('/')) {
            auto rhs = createJqTransformer(context, 20);
            result = std::make_shared<JsonDivision>(result, rhs);
        }
        else if (minPrecedence <= 20 && context.match_literal('%')) {
            auto rhs = createJqTransformer(context, 20);
            result = std::make_shared<JsonModulus>(result, rhs);
        }
        else if (minPrecedence <= 1 && *context == ',') {
            std::vector<std::shared_ptr<JsonTransformer>> clauses;
            while (context.match_literal(',')) {
                auto clause = createJqTransformer(context, 2 /* minPrecedence */);
                clauses.emplace_back(std::move(clause));
                context.skip_whitespace();
            }
            result = std::make_shared<JsonApplyEach>(std::move(clauses));
        }
        else break;
    }

    return result;
}

std::shared_ptr<JsonStreamProcessor> createJqStreamProcessor(ParseContext & context)
{
    std::shared_ptr<JsonTransformer> result = createJqTransformer(context);

    context.skip_whitespace();
    
    context.expect_eof();
    if (result)
        return result;

    context.exception("don't know how to parse from here");
}

std::shared_ptr<JsonStreamProcessor> createJqStreamProcessor(const std::string & program)
{
    ParseContext context(program, program.c_str(), program.length());

    return createJqStreamProcessor(context);
}

} // namespace MLDB

