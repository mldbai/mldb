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

    virtual Path getPath(JsonStreamParsingContext & in)
    {
        ArrayJsonStreamPrintingContext out;
        process(in, out);
        ExcAssertEqual(out.values().size(), 1);
        return Path::parse(out.values()[0].asStringUtf8());
    }

    virtual Path getPath(JsonParsingContext & in)
    {
        Json::Value val;
        StructuredJsonPrintingContext out(val);
        transform(in, out);
        return Path::parse(val.asStringUtf8());
    }
};

struct SubscriptExpression {
    virtual ~SubscriptExpression() = default;
    virtual Utf8String toLisp() const = 0;
    // TODO: execution
};

struct AllRange: public SubscriptExpression {
    virtual Utf8String toLisp() const
    {
        return "(all)";
    }
};

struct SteppedRange: public SubscriptExpression {

    SteppedRange(std::shared_ptr<JsonTransformer> start,
                 std::optional<std::shared_ptr<JsonTransformer>> step,
                 std::shared_ptr<JsonTransformer> end)
        : start(std::move(start)), step(std::move(step)), end(std::move(end))
    {
    }

    std::shared_ptr<JsonTransformer> start;
    std::optional<std::shared_ptr<JsonTransformer>> step;
    std::shared_ptr<JsonTransformer> end;

    virtual Utf8String toLisp() const
    {
        Utf8String result = "(step " + start->toLisp() + " ";
        if (step)
            result += " " + (*step)->toLisp();
        result += " " + end->toLisp() + ")";
        return result;
    }
};

struct SingleItemRange: public SubscriptExpression {

    SingleItemRange(std::shared_ptr<JsonTransformer> expr)
        : expr(std::move(expr))
    {
    }

    std::shared_ptr<JsonTransformer> expr;

    virtual Utf8String toLisp() const
    {
        Utf8String result = "(item " + expr->toLisp() + ")";
        return result;
    }
};

struct JsonApplyEach: public JsonStreamProcessor {

    JsonApplyEach(std::vector<std::shared_ptr<JsonStreamProcessor>> processors)
        : processors(std::move(processors))
    {        
    }

    std::vector<std::shared_ptr<JsonStreamProcessor>> processors;

    virtual void process(JsonStreamParsingContext & in, JsonStreamPrintingContext & out) const override
    {
        auto position = in.savePosition();

        for (auto & processor: processors) {
            in.restorePosition(position);
            processor->process(in, out);
        }
    }

    virtual Utf8String toLisp() const override
    {
        Utf8String result = "(each";
        for (auto & processor: processors) {
            result += " " + processor->toLisp();
        }
        result += ")";
        return result;
    }
};

struct JsonApplyChain: public JsonStreamProcessor {

    JsonApplyChain(std::vector<std::shared_ptr<JsonStreamProcessor>> processors)
        : processors(std::move(processors))
    {        
    }

    std::vector<std::shared_ptr<JsonStreamProcessor>> processors;

    virtual void process(JsonStreamParsingContext & in, JsonStreamPrintingContext & out) const override
    {
        MLDB_THROW_UNIMPLEMENTED("apply chain");
    }

    virtual Utf8String toLisp() const override
    {
        Utf8String result = "(chain";
        for (auto & processor: processors) {
            result += " " + processor->toLisp();
        }
        result += ")";
        return result;
    }
};

struct LiteralPathElement: public SubscriptExpression {
    LiteralPathElement(PathElement path)
        : path(path)
    {
    }

    PathElement path;

    virtual void enter(JsonParsingContext & context) const
    {
        MLDB_THROW_UNIMPLEMENTED();
    }

    virtual Utf8String toLisp() const
    {
        return "." + path.toUtf8String();
    }
};

struct MaybeLiteralPathElement: public SubscriptExpression {
    MaybeLiteralPathElement(PathElement path)
        : path(path)
    {
    }

    PathElement path;

    virtual void enter(JsonParsingContext & context) const
    {
        MLDB_THROW_UNIMPLEMENTED();
    }

    virtual Utf8String toLisp() const
    {
        return "(maybe ."+ path.toUtf8String() + ")";
    }
};

#if 0
struct ExpressionPathElement: public SubscriptExpression {
    ExpressionPathElement(std::shared_ptr<JsonTransformer> expr)
        : expr(expr)
    {
    }

    std::shared_ptr<JsonTransformer> expr;

    virtual void enter(JsonParsingContext & context) const
    {
        MLDB_THROW_UNIMPLEMENTED();
    }

    virtual Utf8String toLisp() const
    {
        return expr->toLisp();
    }
};
#endif

struct PathExpression: public JsonTransformer {
    virtual Path getPath(JsonStreamParsingContext & in) = 0;
    virtual Path getPath(JsonParsingContext & in) = 0;
};

struct StructuredPathExpression: public PathExpression {
    StructuredPathExpression(std::vector<std::shared_ptr<SubscriptExpression>> elements)
        : elements(std::move(elements))
    {
    }

    std::vector<std::shared_ptr<SubscriptExpression>> elements;

    virtual Path getPath(JsonStreamParsingContext & in)
    {
        MLDB_THROW_UNIMPLEMENTED();
    }

    virtual Path getPath(JsonParsingContext & in)
    {
        MLDB_THROW_UNIMPLEMENTED();
    }

    virtual void transform(JsonParsingContext & in, JsonPrintingContext & out) const override
    {
        MLDB_THROW_UNIMPLEMENTED();
    }

    virtual Utf8String toLisp() const override
    {

        Utf8String result = "(path";
        for (auto & el: elements)
            result += " " + el->toLisp();
        result += ")";
        return result;
    }
};

struct EvaluatePathExpression: public PathExpression {
    EvaluatePathExpression(std::shared_ptr<JsonTransformer> expr)
        : expr(std::move(expr))
    {
    }

    std::shared_ptr<JsonTransformer> expr;

    virtual Path getPath(JsonStreamParsingContext & in)
    {
        MLDB_THROW_UNIMPLEMENTED();
    }

    virtual Path getPath(JsonParsingContext & in)
    {
        MLDB_THROW_UNIMPLEMENTED();
    }

    virtual void transform(JsonParsingContext & in, JsonPrintingContext & out) const override
    {
        MLDB_THROW_UNIMPLEMENTED();
    }

    virtual Utf8String toLisp() const override
    {
        Utf8String result = "(topath " + expr->toLisp() + ")";
        return result;
    }
};


struct LiteralPath: public PathExpression {
    LiteralPath(Path literal)
        : literal(std::move(literal))
    {
    }

    Path literal;

    virtual Path getPath(JsonStreamParsingContext & in)
    {
        return literal;
    }

    virtual Path getPath(JsonParsingContext & in)
    {
        return literal;
    }

    virtual void transform(JsonParsingContext & in, JsonPrintingContext & out) const override
    {
        out.writeJson(literal.toUtf8String());
    }

    virtual Utf8String toLisp() const override
    {
        Utf8String result = "." + literal.toUtf8String();
        return result;
    }
};


struct PathJsonStreamProcessor: public JsonTransformer {
    PathJsonStreamProcessor(std::shared_ptr<PathExpression> path)
        : path(path)
    {
    }

    std::shared_ptr<PathExpression> path;

    virtual void transform(JsonParsingContext & in, JsonPrintingContext & out) const
    {
        MLDB_THROW_UNIMPLEMENTED("extract path");
    }

    virtual void process(JsonStreamParsingContext & in, JsonStreamPrintingContext & out) const override
    {
        MLDB_THROW_UNIMPLEMENTED("extract path");
#if 0
        if (path.empty()) {
            out.take(in);
        }
        else {
            JsonTransformer::process(in, out);
        }
#endif
    }

    virtual Utf8String toLisp() const override
    {
        Utf8String result = "(path " + path->toLisp() + ")";
        return result;
    }
};

struct IterateJsonStreamProcessor: public PathJsonStreamProcessor {
    IterateJsonStreamProcessor(std::shared_ptr<PathExpression> path,
                               std::shared_ptr<SubscriptExpression> range)
        : PathJsonStreamProcessor(std::move(path)), range(std::move(range))
    {
    }

    std::shared_ptr<SubscriptExpression> range;

    virtual void process(JsonStreamParsingContext & in, JsonStreamPrintingContext & out) const override
    {
        MLDB_THROW_UNIMPLEMENTED("extract path");
#if 0
        if (path.empty()) {
            while (auto currento = in.current()) {
                auto & current = **currento;

                auto onElement = [&] ()
                {
                    out.take(current);                  
                };

                if (current.isArray()) {
                    current.forEachElement(onElement);
                }
                else if (current.isObject()) {
                    current.forEachElement(onElement);
                }
                else {
                    onElement();
                }
            }
        }
        else {
            MLDB_THROW_UNIMPLEMENTED("extract path");
        }
#endif
    }

    virtual Utf8String toLisp() const override
    {
        Utf8String result = "(iterate " + path->toLisp() + " " + range->toLisp() + ")";
        return result;
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

    virtual Utf8String toLisp() const override
    {
        Utf8String result = "(literal " + literal.toStringNoNewLineUtf8() + ")";
        return result;
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

    virtual Utf8String toLisp() const override
    {
        Utf8String result = "(concat";
        for (auto & el: elements)
            result += " " + el->toLisp();
        result += ")";
        return result;
    }
};

struct StructureJsonStreamProcessor: public JsonTransformer {
    StructureJsonStreamProcessor(std::vector<std::pair<std::shared_ptr<PathExpression>, std::shared_ptr<JsonTransformer>>> members)
        : members(std::move(members))
    {
    }

    std::vector<std::pair<std::shared_ptr<PathExpression>, std::shared_ptr<JsonTransformer>>> members;

    virtual void transform(JsonParsingContext & in, JsonPrintingContext & out) const override
    {
        out.startObject();
        for (auto & [name, value]: members) {
            out.startMember(name->getPath(in).toUtf8String());
            value->transform(in, out);
        }
        out.endObject();
    }

    virtual Utf8String toLisp() const override
    {
        Utf8String result = "(struct";
        for (auto & [key, value]: members)
            result += " (member " + key->toLisp() + " " + value->toLisp() + ")";
        result += ")";
        return result;
    }
};

struct ArrayJsonStreamProcessor: public JsonStreamProcessor {
    ArrayJsonStreamProcessor(std::vector<std::shared_ptr<JsonStreamProcessor>> elements)
        : elements(std::move(elements))
    {
    }

    std::vector<std::shared_ptr<JsonStreamProcessor>> elements;

    virtual void process(JsonStreamParsingContext & in, JsonStreamPrintingContext & outc) const override
    {
        auto & out = outc.current();
        out.startArray();
        while (auto current = in.current()) {
            auto & value = **current;
            out.newArrayElement();
            out.writeJson(value.expectJson());
        }
        out.endArray();
    }

#if 0
    virtual void transform(JsonParsingContext & in, JsonPrintingContext & out) const override
    {
        out.startArray();
        for (auto & value: elements) {
            out.newArrayElement();
            value->transform(in, out);
        }
        out.endArray();
    }
#endif

    virtual Utf8String toLisp() const override
    {
        Utf8String result = "(array";
        for (auto & value: elements)
            result += " " + value->toLisp();
        result += ")";
        return result;
    }
};

struct RecurseJsonStreamProcessor: public JsonStreamProcessor {
    virtual void process(JsonStreamParsingContext & in, JsonStreamPrintingContext & outc) const override
    {
        MLDB_THROW_UNIMPLEMENTED();
    }

    virtual Utf8String toLisp() const override
    {
        Utf8String result = "(recurse)";
        return result;
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

    virtual std::string op() const = 0;

    virtual Utf8String toLisp() const override
    {
        Utf8String result = "(" + op() + " " + lhs->toLisp() + " " + rhs->toLisp() + ")";
        return result;
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

    virtual std::string op() const
    {
        return "+";
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

    virtual std::string op() const
    {
        return "-";
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

    virtual std::string op() const
    {
        return "*";
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

    virtual std::string op() const
    {
        return "/";
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

    virtual std::string op() const
    {
        return "%";
    }
};

struct JsonFormatter: public JsonTransformer {
    JsonFormatter(Utf8String formatter, std::vector<std::shared_ptr<JsonTransformer>> args)
        : formatter(std::move(formatter)), args(std::move(args))
    {
    }

    Utf8String formatter;
    std::vector<std::shared_ptr<JsonTransformer>> args;

    virtual void transform(JsonParsingContext & in, JsonPrintingContext & out) const override
    {
        MLDB_THROW_UNIMPLEMENTED(("formatter @" + formatter).rawString());
    }

    virtual Utf8String toLisp() const override
    {
        Utf8String result = "(format " + formatter;
        for (auto & arg: args) {
            result += " " + arg->toLisp();
        }
        result += ")";
        return result;
    }
};

struct JsonApplyFunction: public JsonTransformer {
    JsonApplyFunction(Utf8String functionName, std::vector<std::shared_ptr<JsonTransformer>> args)
        : functionName(std::move(functionName)), args(std::move(args))
    {
    }

    Utf8String functionName;
    std::vector<std::shared_ptr<JsonTransformer>> args;

    virtual void transform(JsonParsingContext & in, JsonPrintingContext & out) const override
    {
        MLDB_THROW_UNIMPLEMENTED(("call " + functionName).rawString());
    }

    virtual Utf8String toLisp() const override
    {
        Utf8String result = "(call " + functionName;
        for (auto & arg: args) {
            result += " " + arg->toLisp();
        }
        result += ")";
        return result;
    }
};

struct JsonTryCatch: public JsonStreamProcessor {
    JsonTryCatch(std::shared_ptr<JsonStreamProcessor> tryExpr,
                 std::shared_ptr<JsonStreamProcessor> catchExpr)
        : tryExpr(std::move(tryExpr)), catchExpr(std::move(catchExpr))
    {
    }

    std::shared_ptr<JsonStreamProcessor> tryExpr;
    std::shared_ptr<JsonStreamProcessor> catchExpr;

    virtual Utf8String toLisp() const override
    {
        Utf8String result = "(try " + tryExpr->toLisp() + " " + catchExpr->toLisp() + ")";
        return result;
    }
};

std::shared_ptr<JsonTransformer> createJqTransformer(ParseContext & context, int minPrecedence = 0);
std::optional<shared_ptr<JsonTransformer>>
matchJqTransformer(ParseContext & context, int minPrecedence);
std::optional<std::shared_ptr<SubscriptExpression>> matchSubscriptExpression(ParseContext & context);


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

std::optional<PathElement> match_path_element_literal(ParseContext & context)
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

std::optional<std::shared_ptr<SubscriptExpression>> match_path_element_expression(ParseContext & context)
{
    ParseContext::Revert_Token token(context);
    context.skip_whitespace();
    auto element = match_path_element_literal(context);
    if (element) {
        context.skip_whitespace();
        bool optional = context.match_literal('?');
        token.ignore();
        if (optional)
            return std::make_shared<MaybeLiteralPathElement>(*element);
        else
            return std::make_shared<LiteralPathElement>(*element);
    }
    else if (context.match_literal('[')) {
        auto range = matchSubscriptExpression(context);
        if (!range)
            context.exception("expected range expression");
        context.skip_whitespace();
        context.expect_literal(']', "expected end of path range expression");
        return std::move(*range);

#if 0
        context.skip_whitespace();
        if (context.match_literal(']'))
            return nullopt;  // it's a take all expression
        auto expr = createJqTransformer(context, 0 /* precedence */);
        if (!expr)
            context.exception("expected jq path expression");
        context.skip_whitespace();
        context.expect_literal(']', "expected end of path element");
        token.ignore();
        return std::make_shared<ExpressionPathElement>(expr);
#endif
    }
    return nullopt;
}

std::optional<std::shared_ptr<PathExpression>> match_path(ParseContext & context)
{
    std::vector<std::shared_ptr<JsonTransformer>> elements;

    //cerr << "match_path: *context = " << *context << (int)*context << endl;
    context.skip_whitespace();
    if (context.eof() || *context != '.')
        return nullopt;
    
    std::vector<std::shared_ptr<SubscriptExpression>> segments;
    bool first = true;
    while (context.match_literal('.')) {
        if (first && context.eof())
            break;
        auto segment = match_path_element_expression(context);
        if (!segment) {
            break;
            context.exception("empty path name");
        }
        segments.emplace_back(*segment);
        first = false;
        context.skip_whitespace();
    }

    return std::make_shared<StructuredPathExpression>(std::move(segments));
}

int getEscapedJsonCharacterPointUtf8(ParseContext & context);  // in json_parsing.cc

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

std::shared_ptr<JsonStreamProcessor> createJqStreamProcessor(ParseContext & context, int minPrecedence);

std::optional<std::shared_ptr<SubscriptExpression>>
matchSubscriptExpression(ParseContext & context)
{
    auto start = matchJqTransformer(context, 0 /* minPrecedence */);
    if (!start)
        return std::make_shared<AllRange>();
    context.skip_whitespace();
    if (context.match_literal(':')) {
        auto stepOrEnd = matchJqTransformer(context, 0 /* minPrecedence */);
        if (!stepOrEnd)
            context.exception("expected step/end of range");
        if (context.match_literal(':')) {
            auto end = matchJqTransformer(context, 0 /* minPrecedence */);
            if (!end)
                context.exception("expected end of range");
            return std::make_shared<SteppedRange>(*start, *stepOrEnd, *end);
        }
        else {
            return std::make_shared<SteppedRange>(*start, nullopt, *stepOrEnd);
        }
    }
    else {
        return std::make_shared<SingleItemRange>(*start);
    }
}

std::optional<shared_ptr<JsonTransformer>>
matchJqTransformer(ParseContext & context, int minPrecedence)
{
    ParseContext::Revert_Token token(context);

    std::shared_ptr<JsonTransformer> result;
    context.skip_whitespace();
    if (*context == '.') {
        auto path = match_path(context);
        if (context.match_literal("[")) {
            auto range = matchSubscriptExpression(context);
            if (!range)
                context.exception("Expected iteration range");
            context.skip_whitespace();
            context.expect_literal(']', "expected end of iteration range");
            result = std::make_shared<IterateJsonStreamProcessor>(std::move(*path), std::move(*range));
        }
        else {
            result = std::make_shared<PathJsonStreamProcessor>(*path);
        }
        context.skip_whitespace();
        if (context.match_literal('?')) {
            /// TODO...
        }
    }
    else if (context.match_literal('{')) {
        context.skip_whitespace();
        std::vector<std::pair<std::shared_ptr<PathExpression>, std::shared_ptr<JsonTransformer>>> members;
        while (!context.match_literal('}')) {
            context.skip_whitespace();
            std::shared_ptr<PathExpression> key;
            std::shared_ptr<JsonTransformer> value;
            if (context.match_literal('(')) {
                // key expression
                auto keyExpr = createJqTransformer(context);
                ExcAssert(keyExpr);
                key = std::make_shared<EvaluatePathExpression>(keyExpr);
                context.skip_whitespace();
                context.expect_literal(')');
            }
            else {
                auto keyo = match_path_element_literal(context);
                if (!keyo)
                    context.exception("expected structure member name");
                std::vector<std::shared_ptr<SubscriptExpression>> elements;
                elements.emplace_back(std::make_shared<LiteralPathElement>(*keyo));
                key = std::make_shared<StructuredPathExpression>(elements);
            }
            context.skip_whitespace();
            if (context.match_literal(':')) {
                context.skip_whitespace();
                value = createJqTransformer(context);
                ExcAssert(value);
            }
            else {
                value = std::make_shared<PathJsonStreamProcessor>(key);
            }
            members.emplace_back(std::move(key), std::move(value));
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
#if 0
    else if (context.match_literal('(')) {
        // Parenthesis
        result = createJqTransformer(context, 0 /* precedence */);
        context.skip_whitespace();
        context.expect_literal(')');
    }
    else if (context.match_literal('[')) {
        context.skip_whitespace();
        std::vector<std::shared_ptr<JsonTransformer>> elements;
        while (!context.match_literal(']')) {
            context.skip_whitespace();
            auto value = createJqTransformer(context);
            elements.emplace_back(std::move(value));
            if (context.match_literal(','))
                continue;
            context.expect_literal(']', "expected ] to close array");
            break;
        }
        result = std::make_shared<ArrayJsonStreamProcessor>(std::move(elements));
    }
#endif
    else if (*context == '$') {
        MLDB_THROW_UNIMPLEMENTED("variables");
    }
    else if (context && (*context == '@' || *context == '_' || isalpha(*context))) {
        bool isFormatter = context.match_literal('@');
        auto name = expect_identifier(context);
        if (name == "null") {
            result = std::make_shared<LiteralJsonStreamProcessor>(Json::nullValue);
        }
        else if (name == "true") {
            result = std::make_shared<LiteralJsonStreamProcessor>(Json::Value(true));
        }
        else if (name == "false") {
            result = std::make_shared<LiteralJsonStreamProcessor>(Json::Value(false));
        }
        else if (name == "try") {
            auto tryExpr = matchJqTransformer(context, minPrecedence);
            if (!tryExpr)
                context.exception("Expected try expression");
            context.skip_whitespace();
            auto catchExpr = matchJqTransformer(context, minPrecedence);
            if (!catchExpr)
                context.exception("Expected catch expression");
            result = std::make_shared<JsonTryCatch>(std::move(*tryExpr), std::move(*catchExpr));
        }
        else {
            std::vector<std::shared_ptr<JsonTransformer>> args;
            while (context && context.match_whitespace()) {
                auto arg = matchJqTransformer(context, minPrecedence);
                if (!arg)
                    break;
                args.emplace_back(std::move(*arg));
            }
            if (isFormatter) {
                result = std::make_shared<JsonFormatter>(name, std::move(args));
            }
            else {
                result = std::make_shared<JsonApplyFunction>(name, std::move(args));
            }
        }
    }
    else {
        ParseContext::Revert_Token token(context);
        try {
            MLDB_TRACE_EXCEPTIONS(false);
            auto literal = expectJson(context);
            result = std::make_shared<LiteralJsonStreamProcessor>(std::move(literal));
            token.ignore();
        }
        MLDB_CATCH_ALL {
            return nullopt;
        }
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
        else break;
    }

    token.ignore();
    return result;
}

std::shared_ptr<JsonTransformer>
createJqTransformer(ParseContext & context, int minPrecedence)
{
    auto matched = matchJqTransformer(context, minPrecedence);
    if (!matched.has_value()) {
        context.exception("expected jq transformer expression");
    }
    return std::move(*matched);
}


std::shared_ptr<JsonStreamProcessor> createJqStreamProcessor(ParseContext & context, int minPrecedence)
{
    std::shared_ptr<JsonStreamProcessor> result;
    context.skip_whitespace();

    if (context.match_literal('(')) {
        // Parenthesis
        result = createJqStreamProcessor(context, 0 /* precedence */);
        context.skip_whitespace();
        context.expect_literal(')');
    }
    else if (context.match_literal('[')) {
        context.skip_whitespace();
        if (context.match_literal("..")) {
            context.skip_whitespace();
            context.expect_literal(']', "expected end of recurse operator ..");
            result = std::make_shared<RecurseJsonStreamProcessor>();
        }
        else { 
            std::vector<std::shared_ptr<JsonStreamProcessor>> elements;
            while (!context.match_literal(']')) {
                context.skip_whitespace();
                auto value = createJqStreamProcessor(context, 0 /* precedence */);
                elements.emplace_back(std::move(value));
                if (context.match_literal(','))
                    continue;
                context.expect_literal(']', "expected ] to close array");
                break;
            }
            result = std::make_shared<ArrayJsonStreamProcessor>(std::move(elements));
        }
    }
    else {
        result = createJqTransformer(context, minPrecedence);
    }

    context.skip_whitespace();

    while (context) {
        if (minPrecedence <= 2 && *context == ',') {
            std::vector<std::shared_ptr<JsonStreamProcessor>> clauses = { result };
            while (context.match_literal(',')) {
                auto clause = createJqStreamProcessor(context, 3 /* minPrecedence */);
                clauses.emplace_back(std::move(clause));
                context.skip_whitespace();
            }
            result = std::make_shared<JsonApplyEach>(std::move(clauses));
        }
        else if (minPrecedence <= 1 && *context == '|') {
            std::vector<std::shared_ptr<JsonStreamProcessor>> clauses = { result };
            while (context.match_literal('|')) {
                auto clause = createJqStreamProcessor(context, 2 /* minPrecedence */);
                clauses.emplace_back(std::move(clause));
                context.skip_whitespace();
            }
            result = std::make_shared<JsonApplyChain>(std::move(clauses));
        }
        else break;
    }

    return result;
}

std::shared_ptr<JsonStreamProcessor> createJqStreamProcessor(ParseContext & context)
{
    std::shared_ptr<JsonStreamProcessor> result = createJqStreamProcessor(context, 0);

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

