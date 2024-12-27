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

void
JsonStreamProcessor::
process(JsonStreamParsingContext & in, JsonStreamPrintingContext & out) const
{
    MLDB_THROW_UNIMPLEMENTED_ON_THIS("subclass needs to override process()");
}

void
JsonStreamProcessor::
transform(JsonParsingContext & in, JsonPrintingContext & out) const
{
    MLDB_THROW_UNIMPLEMENTED_ON_THIS("subclass needs to override transform");
}

#if 0
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
#endif

struct SubscriptExpression: public JsonStreamProcessor {
    virtual ~SubscriptExpression() = default;
    virtual Utf8String toLisp() const = 0;
    virtual Path getPath() const
    {
        MLDB_THROW_UNIMPLEMENTED_ON_THIS("This class of subscript expression cannot generate paths");
    }
    // TODO: execution
};

struct MaybeSubscript: public SubscriptExpression {

    MaybeSubscript(std::shared_ptr<JsonStreamProcessor> expr)
        : expr(expr)
    {
    }

    std::shared_ptr<JsonStreamProcessor> expr;

    virtual ~MaybeSubscript() = default;
    virtual Utf8String toLisp() const
    {
        return "(maybe " + expr->toLisp() + ")";
    }
};

struct AllRange: public SubscriptExpression {
    virtual Utf8String toLisp() const
    {
        return "(all)";
    }
};

struct SteppedRange: public SubscriptExpression {

    SteppedRange(std::shared_ptr<JsonStreamProcessor> start,
                 std::optional<std::shared_ptr<JsonStreamProcessor>> step,
                 std::shared_ptr<JsonStreamProcessor> end)
        : start(std::move(start)), step(std::move(step)), end(std::move(end))
    {
    }

    std::shared_ptr<JsonStreamProcessor> start;
    std::optional<std::shared_ptr<JsonStreamProcessor>> step;
    std::shared_ptr<JsonStreamProcessor> end;

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

    SingleItemRange(std::shared_ptr<JsonStreamProcessor> expr)
        : expr(std::move(expr))
    {
    }

    std::shared_ptr<JsonStreamProcessor> expr;

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

struct JsonStreamIdentity: public JsonStreamProcessor {

    virtual void process(JsonStreamParsingContext & in, JsonStreamPrintingContext & out) const override
    {
        MLDB_THROW_UNIMPLEMENTED("apply identity");
    }

    virtual Utf8String toLisp() const override
    {
        Utf8String result = "(identity)";
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

#if 0
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
#endif

#if 0
struct ExpressionPathElement: public SubscriptExpression {
    ExpressionPathElement(std::shared_ptr<JsonStreamProcessor> expr)
        : expr(expr)
    {
    }

    std::shared_ptr<JsonStreamProcessor> expr;

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

#if 0
struct PathExpression: public JsonStreamProcessor {
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
#endif

struct EvaluatePathExpression: public SubscriptExpression {
    EvaluatePathExpression(std::shared_ptr<JsonStreamProcessor> expr)
        : expr(std::move(expr))
    {
    }

    std::shared_ptr<JsonStreamProcessor> expr;

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

#if 0
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
#endif

#if 0
struct PathJsonStreamProcessor: public JsonStreamProcessor {
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
            JsonStreamProcessor::process(in, out);
        }
#endif
    }

    virtual Utf8String toLisp() const override
    {
        Utf8String result = "(path " + path->toLisp() + ")";
        return result;
    }
};
#endif

#if 0
struct IterateJsonStreamProcessor: public JsonStreamProcessor {
    IterateJsonStreamProcessor(std::shared_ptr<SubscriptExpression> range)
        : range(std::move(range))
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
        Utf8String result = "(iterate " + range->toLisp() + ")";
        return result;
    }
};
#endif

struct LiteralJsonStreamProcessor: public JsonStreamProcessor {
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

// Always returns nothing
struct JsonNone: public JsonStreamProcessor {
    virtual void transform(JsonParsingContext & in, JsonPrintingContext & out) const override
    {
        // Do nothing
    }

    virtual Utf8String toLisp() const override
    {
        Utf8String result = "(none)";
        return result;
    }
};

struct JsonRange: public JsonStreamProcessor {

    JsonRange(std::vector<std::shared_ptr<JsonStreamProcessor>> args_)
        : args(std::move(args_))
    {
        if (args.size() < 1 || args.size() > 3) {
            throw MLDB::Exception("invalid number of range arguments");
        }
    }

    std::vector<std::shared_ptr<JsonStreamProcessor>> args;

    virtual Utf8String toLisp() const override
    {
        Utf8String result = "(range";
        for (auto & arg: args)
            result += " " + arg->toLisp();
        result += ")";
        return result;
    }
};

struct JsonWhile: public JsonStreamProcessor {

    JsonWhile(std::vector<std::shared_ptr<JsonStreamProcessor>> args_)
        : args(std::move(args_))
    {
        if (args.size() != 2) {
            throw MLDB::Exception("invalid number of while arguments");
        }
    }

    std::vector<std::shared_ptr<JsonStreamProcessor>> args;

    virtual Utf8String toLisp() const override
    {
        Utf8String result = "(while";
        for (auto & arg: args)
            result += " " + arg->toLisp();
        result += ")";
        return result;
    }
};

struct ConcatenationStreamProcessor: public JsonStreamProcessor {
    ConcatenationStreamProcessor(std::vector<std::shared_ptr<JsonStreamProcessor>> elements)
        : elements(std::move(elements))
    {
    }

    std::vector<std::shared_ptr<JsonStreamProcessor>> elements;

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

struct StructureJsonStreamProcessor: public JsonStreamProcessor {
    StructureJsonStreamProcessor(std::vector<std::pair<std::shared_ptr<SubscriptExpression>, std::shared_ptr<JsonStreamProcessor>>> members)
        : members(std::move(members))
    {
    }

    std::vector<std::pair<std::shared_ptr<SubscriptExpression>, std::shared_ptr<JsonStreamProcessor>>> members;

    virtual void transform(JsonParsingContext & in, JsonPrintingContext & out) const override
    {
        MLDB_THROW_UNIMPLEMENTED();
#if 0        
        out.startObject();
        for (auto & [name, value]: members) {
            out.startMember(name->getPath(in).toUtf8String());
            value->transform(in, out);
        }
        out.endObject();
#endif
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

struct JsonArithmetic: public JsonStreamProcessor {
    JsonArithmetic(std::shared_ptr<JsonStreamProcessor> lhs,
                   std::shared_ptr<JsonStreamProcessor> rhs)
        : lhs(std::move(lhs)), rhs(std::move(rhs))
    {
    }

    std::shared_ptr<JsonStreamProcessor> lhs, rhs;

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

struct JsonAssign: public JsonStreamProcessor {
    JsonAssign(std::shared_ptr<JsonStreamProcessor> lhs,
               std::shared_ptr<JsonStreamProcessor> rhs)
        : lhs(std::move(lhs)), rhs(std::move(rhs))
    {
    }

    std::shared_ptr<JsonStreamProcessor> lhs;
    std::shared_ptr<JsonStreamProcessor> rhs;

    Utf8String toLisp() const override
    {
        return "(assign " + lhs->toLisp() + " " + rhs->toLisp() + ")";
    }
};

struct JsonSubscript: public JsonStreamProcessor {
    JsonSubscript(std::shared_ptr<JsonStreamProcessor> lhs,
                  std::shared_ptr<JsonStreamProcessor> rhs)
        : lhs(std::move(lhs)), rhs(std::move(rhs))
    {
    }

    std::shared_ptr<JsonStreamProcessor> lhs;
    std::shared_ptr<JsonStreamProcessor> rhs;

    Utf8String toLisp() const override
    {
        return "(subscript " + lhs->toLisp() + " " + rhs->toLisp() + ")";
    }
};

struct JsonFormatter: public JsonStreamProcessor {
    JsonFormatter(Utf8String formatter, std::vector<std::shared_ptr<JsonStreamProcessor>> args)
        : formatter(std::move(formatter)), args(std::move(args))
    {
    }

    Utf8String formatter;
    std::vector<std::shared_ptr<JsonStreamProcessor>> args;

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

struct JsonApplyFunction: public JsonStreamProcessor {
    JsonApplyFunction(Utf8String functionName, std::vector<std::shared_ptr<JsonStreamProcessor>> args)
        : functionName(std::move(functionName)), args(std::move(args))
    {
    }

    Utf8String functionName;
    std::vector<std::shared_ptr<JsonStreamProcessor>> args;

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

std::optional<std::shared_ptr<SubscriptExpression>> matchSubscriptExpression(ParseContext & context);
std::shared_ptr<JsonStreamProcessor> createJqStreamProcessor(ParseContext & context, int minPrecedence);
std::optional<shared_ptr<JsonStreamProcessor>>
matchJqStreamProcessor(ParseContext & context, int minPrecedence);



std::optional<Utf8String> match_identifier(ParseContext & context)
{
    Utf8String name;
    name.reserve(18);
    char c = *context;
    while (isalpha(c) || c == '_' || (!name.empty() && isdigit(c))) {
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
    context.skip_whitespace();
    if (context.match_literal('"')) {
        // quoted segment name
        // TODO: embedded quotes in name
        segment += context.expect_text("\"");
        context.expect_literal('\"');
        return PathElement(segment);
    }
    else if (!context) {
        return nullopt;
    }
    else {
        char c = *context;
        while (isalpha(c) || c == '_' || (!segment.empty() && isdigit(c))) {
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

#if 1
std::optional<std::shared_ptr<SubscriptExpression>>
match_path_element_expression(ParseContext & context)
{
    ParseContext::Revert_Token token(context);
    context.skip_whitespace();
    std::shared_ptr<SubscriptExpression> result;
    auto element = match_path_element_literal(context);
    if (element) {
        token.ignore();
        result = std::make_shared<LiteralPathElement>(*element);
    }
    else if (context.match_literal('[')) {
        auto range = matchSubscriptExpression(context);
        if (!range)
            context.exception("expected range expression");
        context.skip_whitespace();
        context.expect_literal(']', "expected end of path range expression");
        result = std::move(*range);
    }
    else {
        return nullopt;
    }
    ExcAssert(result);
    context.skip_whitespace();
    token.ignore();
    return std::move(result);
}
#endif

#if 0
std::optional<std::shared_ptr<PathExpression>> match_path(ParseContext & context)
{
    std::vector<std::shared_ptr<JsonStreamProcessor>> elements;

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
#endif

int getEscapedJsonCharacterPointUtf8(ParseContext & context);  // in json_parsing.cc

std::shared_ptr<JsonStreamProcessor> expectStringTemplateUtf8(ParseContext & context)
{
    std::vector<std::shared_ptr<JsonStreamProcessor>> result;

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
            c = context.expect_utf8_code_point();

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
                auto interpolation = createJqStreamProcessor(context, 0);
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

std::optional<std::shared_ptr<SubscriptExpression>>
matchSubscriptExpression(ParseContext & context)
{
    auto start = matchJqStreamProcessor(context, 0 /* minPrecedence */);
    if (!start)
        return std::make_shared<AllRange>();
    context.skip_whitespace();
    if (context.match_literal(':')) {
        auto stepOrEnd = matchJqStreamProcessor(context, 0 /* minPrecedence */);
        if (!stepOrEnd)
            context.exception("expected step/end of range");
        if (context.match_literal(':')) {
            auto end = matchJqStreamProcessor(context, 0 /* minPrecedence */);
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

std::optional<shared_ptr<JsonStreamProcessor>>
matchJqPathExpression(ParseContext & context)
{
    shared_ptr<JsonStreamProcessor> result;

    context.skip_whitespace();
    if (context.eof()) {
        return nullopt;
    }
    if (context.match_literal('[') || context.match_literal(".[")) {
        auto range = matchSubscriptExpression(context);
        if (!range)
            context.exception("Expected iteration range");
        context.skip_whitespace();
        context.expect_literal(']', "expected end of iteration range");
        result = *range;
    }
    else if (context.match_literal('.')) {
        auto path = match_path_element_expression(context);
        if (path)
            result = *path;
        else {
            result = std::make_shared<JsonStreamIdentity>();
        }
    }
    else {
        return nullopt;
    }
    ExcAssert(result);

    context.skip_whitespace();
    if (context.match_literal('?')) {
        result = std::make_shared<MaybeSubscript>(std::move(result));
        context.skip_whitespace();
    }

    auto next = matchJqPathExpression(context);
    if (next) {
        return std::make_shared<JsonApplyChain>(vector{result, *next});
    }
    else return result;
}

std::optional<shared_ptr<JsonStreamProcessor>>
matchJqStreamProcessor(ParseContext & context, int minPrecedence)
{
    ParseContext::Revert_Token token(context);

    std::shared_ptr<JsonStreamProcessor> result;
    context.skip_whitespace();
    if (*context == '.') {
        auto expr = matchJqPathExpression(context);
        ExcAssert(expr);
        result = *expr;
    }
    else if (context.match_literal('{')) {
        context.skip_whitespace();
        std::vector<std::pair<std::shared_ptr<SubscriptExpression>, std::shared_ptr<JsonStreamProcessor>>> members;
        while (!context.match_literal('}')) {
            context.skip_whitespace();
            std::shared_ptr<SubscriptExpression> key;
            std::shared_ptr<JsonStreamProcessor> value;
            if (context.match_literal('(')) {
                // key expression
                auto keyExpr = createJqStreamProcessor(context, 0);
                ExcAssert(keyExpr);
                key = std::make_shared<EvaluatePathExpression>(keyExpr);
                context.skip_whitespace();
                context.expect_literal(')');
            }
            else {
                auto keyo = match_path_element_literal(context);
                if (!keyo)
                    context.exception("expected structure member name");
                key = std::make_shared<LiteralPathElement>(*keyo);
            }
            context.skip_whitespace();
            //cerr << "after name " << key->toLisp() << ", c = " << *context << endl;
            if (context.match_literal(':')) {
                context.skip_whitespace();
                // Need minPrecedence of 3 so comma isn't matched
                value = createJqStreamProcessor(context, 3 /* minPrecedence */);
                ExcAssert(value);
            }
            else {
                value = key;  // when used in this context, will extract the same thing as its name
            }
            //cerr << "got member " << key->toLisp() << " = " << value->toLisp() << endl;
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
    else if (context.match_literal('(')) {
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
            auto tryExpr = matchJqStreamProcessor(context, minPrecedence);
            if (!tryExpr)
                context.exception("Expected try expression");
            context.skip_whitespace();
            context.expect_literal("catch");
            auto catchExpr = matchJqStreamProcessor(context, minPrecedence);
            if (!catchExpr)
                context.exception("Expected catch expression");
            result = std::make_shared<JsonTryCatch>(std::move(*tryExpr), std::move(*catchExpr));
        }
        else if (name == "catch") {
            return nullopt;
        }
        else if (name == "range" || name == "while") {
            context.skip_whitespace();
            context.expect_literal('(');
            std::vector<std::shared_ptr<JsonStreamProcessor>> args;
            do {
                auto arg = matchJqStreamProcessor(context, 0 /* minPrecedence */);
                if (!arg)
                    context.exception("expected range argument");
                args.emplace_back(std::move(*arg));
                context.skip_whitespace();
            } while (args.size() < 3 && context.match_literal(';'));
            context.expect_literal(')', "expected end of range arguments");
            if (args.size() < 1 || args.size() > 3)
                context.exception("range should have one to three ;-separated arugments");

            if (name == "range")
                result = std::make_shared<JsonRange>(std::move(args));
            else if (name == "while")
                result = std::make_shared<JsonWhile>(std::move(args));
            else context.exception("logic error in range/while");
        }
        else {
            std::vector<std::shared_ptr<JsonStreamProcessor>> args;
            while (context && context.match_whitespace()) {
                auto arg = matchJqStreamProcessor(context, minPrecedence);
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
            auto rhs = createJqStreamProcessor(context, 10);
            result = std::make_shared<JsonAddition>(result, rhs);
        }
        else if (minPrecedence <= 10 && context.match_literal('-')) {
            auto rhs = createJqStreamProcessor(context, 10);
            result = std::make_shared<JsonSubtraction>(result, rhs);
        }
        else if (minPrecedence <= 20 && context.match_literal('*')) {
            auto rhs = createJqStreamProcessor(context, 20);
            result = std::make_shared<JsonMultiplication>(result, rhs);
        }
        else if (minPrecedence <= 20 && context.match_literal('/')) {
            auto rhs = createJqStreamProcessor(context, 20);
            result = std::make_shared<JsonDivision>(result, rhs);
        }
        else if (minPrecedence <= 20 && context.match_literal('%')) {
            auto rhs = createJqStreamProcessor(context, 20);
            result = std::make_shared<JsonModulus>(result, rhs);
        }
        else if (minPrecedence <= 4 && context.match_literal('=')) {
            //auto lhs = dynamic_pointer_cast<SubscriptExpression>(result);
            // Later...
            //if (!lhs)
            //    context.exception(("Attempt to assign to non-lvalue " + result->toLisp()).rawString());
            auto rhs = createJqStreamProcessor(context, 4);
            result = std::make_shared<JsonAssign>(result, rhs);
        }
        else if (minPrecedence <= 2 && *context == ',') {
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
        else if (*context == '[') {
            context.expect_literal('[');
            auto subscripts = matchJqStreamProcessor(context, 0 /* min precedence */);
            if (!subscripts)
                subscripts = std::make_shared<JsonNone>();
            context.skip_whitespace();
            context.expect_literal(']');
            result = std::make_shared<JsonSubscript>(result, *subscripts);
        }
        else break;
    }

    token.ignore();
    return result;
}

std::shared_ptr<JsonStreamProcessor>
createJqStreamProcessor(ParseContext & context, int minPrecedence)
{
    auto matched = matchJqStreamProcessor(context, minPrecedence);
    if (!matched.has_value()) {
        context.exception("expected jq transformer expression");
    }
    return std::move(*matched);
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

