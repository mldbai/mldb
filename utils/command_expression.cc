// This file is part of MLDB. Copyright 2015 mldb.ai inc. All rights reserved.

/* command_template.cc
   Jeremy Barnes, 27 August 2013
   Copyright (c) 2013 mldb.ai inc.  All rights reserved.

   Template to allow command lines to be created by subsituting values.
*/

#include "command_expression.h"
#include "command_expression_impl.h"
#include "mldb/base/parse_context.h"
#include "mldb/base/exc_assert.h"
//#include "mldb/jml/utils/json_parsing.h"
#include "mldb/jml/utils/csv.h"
#include <mutex>
#include <thread>
#include "mldb/types/periodic_utils.h"
#include "mldb/types/structure_description.h"
#include "mldb/types/vector_description.h"
#include "mldb/utils/json_utils.h"
#include <boost/lexical_cast.hpp>
#include <boost/range/adaptor/reversed.hpp>


using namespace std;
using namespace ML;


namespace MLDB {

namespace PluginCommand {

namespace {

Json::Value dateFormat(const std::vector<Json::Value> & params)
{
    Date date = jsonDecode<Date>(params[1]);

    //cerr << "params[0] = " << params[0] << endl;
    //cerr << "params[0].type() = " << params[0].type() << endl;

    std::string format = params.at(0).asString();
    return filenameFor(date, format);
}

Json::Value dateFormatIso8601(const std::vector<Json::Value> & params)
{
    Date date = jsonDecode(params.at(0), (Date *)0);
    return date.printIso8601();
}

Json::Value join(const std::vector<Json::Value> & params)
{
    string result;
    const Json::Value & arr = params.at(0);
    string separator = params.at(1).asString();

    for (unsigned i = 0;  i < arr.size();  ++i) {
        if (i != 0)
            result += separator;
        result += stringRender(arr[i]);
    }
            
    return result;
}

std::vector<Date> iterDates(Date startDate, Date endDate,
                            TimePeriod step)
{
    if (endDate < startDate)
        throw MLDB::Exception("end date less than start date");
    if (step.number <= 0)
        throw MLDB::Exception("time interval goes the wrong way");

    vector<Date> result;
    for (Date d = startDate;  d < endDate;  d += step) {
        if (result.size() > 100000)
            throw MLDB::Exception("list of dates is too long");
        result.push_back(d);
    }

    return result;
}

Date dateAdd(Date input, TimePeriod period)
{
    return input + period;
}

Json::Value splitPath(const std::vector<Json::Value> & params)
{
    string name = params[0].asString();

    Json::Value result;
    auto pos = name.find("://");
    int start = 0;
    if (pos != string::npos) {
        result["scheme"] = string(name, 0, pos);
        start = pos + 3;

        pos = name.find('/', start);
        if (pos == string::npos) {
            result["entity"] = "";
        }
        else {
            result["entity"] = string(name, start, pos - start);
            start = pos;
        }
    }
    else {
        result["scheme"] = "";
        result["entity"] = "";
        start = 0;
    }

    string filename(name, start);
    result["dirpath"] = filename;

    string file;

    auto lastSlash = filename.rfind('/');
    if (lastSlash == string::npos) {
        result["directory"] = "";
        result["file"] = file = filename;
    }
    else {
        result["directory"] = string(filename, 0, lastSlash);
        result["file"] = file = string(filename, lastSlash + 1);
    }

    auto firstPeriod = file.find('.');
    if (firstPeriod == string::npos) {
        result["extension"] = "";
        result["basename"] = file;
    }
    else {
        result["extension"] = string(file, firstPeriod + 1);
        result["basename"] = string(file, 0, firstPeriod);
    }

    //cerr << "parsed " << name << " into " << result << endl;
    
    return result;
}

Json::Value basename(const std::vector<Json::Value> & params)
{
    return splitPath(params)["basename"];
}

Json::Value csv(const std::vector<Json::Value> & params)
{
    std::string result;
    for (unsigned i = 0;  i < params.size();  ++i) {
        if (i != 0)
            result += ',';
        if (params[i].isArray()) {
            for (unsigned j = 0;  j < params[i].size();  ++j) {
                if (j != 0)
                    result += ',';
                result += csv_escape(stringRender(params[i][j]));
            }
        }
        else result += csv_escape(stringRender(params[i]));
    }
    return result;
}

Json::Value tsv(const std::vector<Json::Value> & params)
{
    std::string result;
    for (unsigned i = 0;  i < params.size();  ++i) {
        if (i != 0)
            result += '\t';
        if (params[i].isArray()) {
            for (unsigned j = 0;  j < params[i].size();  ++j) {
                if (j != 0)
                    result += '\t';
                result += stringRender(params[i][j]);
            }
        }
        else result += stringRender(params[i]);
    }
    return result;
}

Json::Value hash(const std::vector<Json::Value> & params)
{
    if (params.size() != 1)
        throw MLDB::Exception("hash() function takes exactly one argument");
    return jsonHash(params[0]);
}

} // file scope


/*****************************************************************************/
/* COMMAND EXPRESSION VARIABLES                                              */
/*****************************************************************************/

const CommandExpressionVariables *
CommandExpressionVariables::
builtins()
{
    static CommandExpressionVariables * value = 0;
    static std::mutex lock;

    if (value)
        return value;
    std::unique_lock<std::mutex> guard(lock);
    if (value)
        return value;

    value = new CommandExpressionVariables(nullptr);
    value->addFunction("dateFormat", dateFormat);
    value->addFunction("dateFormatIso8601", dateFormatIso8601);
    value->addFunction("join", join);
    value->addFunction("splitPath", splitPath);
    value->addFunction("basename", basename);
    value->addFunction("csv", csv);
    value->addFunction("tsv", tsv);
    value->addBoundFunction("iterDates", iterDates);
    value->addBoundFunction("dateAdd", dateAdd);
    value->addFunction("min", jsonMinVector);
    value->addFunction("max", jsonMaxVector);
    value->addFunction("flatten", flatten);
    value->addFunction("hash", hash);

    return value;
}

Json::Value
CommandExpressionVariables::
applyFunction(const std::string & functionName,
              const std::vector<Json::Value> & functionArgs) const
{
    //using namespace std;
    //cerr << functionName << "(" << functionArgs << ")" << endl;

    auto it = functions.find(functionName);
    if (it == functions.end()) {
        if (outer)
            return outer->applyFunction(functionName, functionArgs);
        else {
            using namespace std;
            cerr << "function " << functionName << " is not registered"
                 << endl;
            throw MLDB::Exception("function " + functionName + " is not registered");
        }
    }
    try {
        return it->second(functionArgs);
    } catch (const std::exception & exc) {
        throw MLDB::Exception("error trying to apply %s to args %s: %s",
                            functionName.c_str(),
                            boost::lexical_cast<std::string>(functionArgs).c_str(),
                            exc.what());
    }
}


/*****************************************************************************/
/* COMMAND EXPRESSION                                                        */
/*****************************************************************************/


std::shared_ptr<CommandExpression>
CommandExpression::
parse(const std::string & val)
{
    ParseContext context(val, val.c_str(), val.c_str() + val.length());
    
    std::shared_ptr<CommandExpression> res;
    if (context.match_literal("%!"))
        res = parseArgumentExpression(context);
    else res = parseExpression(context, false /* stopOnWhitespace */);

    context.expect_eof();

    return res;
}

std::shared_ptr<CommandExpression>
CommandExpression::
parseArgumentExpression(const std::string & val)
{
    ParseContext context(val, val.c_str(), val.c_str() + val.length());
    
    auto res = parseArgumentExpression(context);
    
    context.expect_eof();
    
    return res;
}

std::shared_ptr<CommandExpression>
CommandExpression::
parse(const std::vector<std::string> & vals)
{
    std::shared_ptr<ArrayExpression> expr(new ArrayExpression());

    for (auto & s: vals)
        expr->clauses.push_back(parse(s));

    return expr;
}

std::shared_ptr<CommandExpression>
CommandExpression::
parseExpression(ParseContext & context, bool stopOnWhitespace)
{
    // Default is concat... we stop when we have a percent

    std::string current;

    std::shared_ptr<ConcatExpression> expr(new ConcatExpression());

    ParseContext::Hold_Token token(context);

    auto addContext = [&] (std::shared_ptr<CommandExpression> expr)
        {
            expr->surfaceForm = token.captured();
            return expr;
        };

    auto flush = [&] ()
        {
            if (!current.empty()) {
                expr->clauses.push_back(std::make_shared<LiteralExpression>(current));
                current = "";
            }
        };

    while (context) {

        if (stopOnWhitespace) {
            while (context && *context != '%' && !isspace(*context))
                current += *context++;
            if (!context || isspace(*context))
                break;
        }
        else {
            while (context && *context != '%')
                current += *context++;
            if (!context) break;
        }

        *context++;

        if (!context || isspace(*context)) {
            current += "%";
            continue;
        }

        if (context.match_literal('{')) {
            flush();

            expr->clauses.push_back(parseArgumentExpression(context));

            context.expect_literal('}');
        }
        else {
            // %% is a percent sign
            if (context.match_literal("%")) {
                current += "%";
                continue;
            }
            flush();
            // % something is the variable of that name
            expr->clauses.push_back(std::make_shared<VariableExpression>(string(1, *context++)));
        }
    }

    flush();

    return addContext(expr);
}

std::shared_ptr<CommandExpression>
CommandExpression::
parseArgumentExpression(ParseContext & context)
{
    ParseContext::Hold_Token token(context);

    auto addContext = [&] (std::shared_ptr<CommandExpression> expr)
        {
            expr->surfaceForm = token.captured();
            return expr;
        };

    context.skip_whitespace();

    std::shared_ptr<CommandExpression> result;

    if (context.match_literal('\'')) {
        // string literal
        string value;
        while (*context != '\'')
            value.push_back(*context++);
        context.expect_literal('\'');
        result = std::make_shared<LiteralExpression>(value);
    }
    else if (*context == '\"') {
        // string literal
        std::string value = expectJsonStringAscii(context);
        result = std::make_shared<LiteralExpression>(value);
    }
    else if (context.match_literal('[')) {
        // Inline array expression
        //cerr << "inline array" << endl;
        std::vector<std::shared_ptr<CommandExpression> > elements;
        context.skip_whitespace();
        if (!context.match_literal(']')) {
            while (*context != ']') {
                context.skip_whitespace();
                elements.push_back(parseArgumentExpression(context));
                context.skip_whitespace();
                if (!context.match_literal(','))
                    break;
            }

            context.expect_literal(']', "expected array close");
        }
        result = std::make_shared<InlineArrayExpression>(elements);
    }
    else if (context.match_literal('{')) {
        // Inline object expression
        std::vector<std::pair<std::shared_ptr<CommandExpression>,
                              std::shared_ptr<CommandExpression> > > elements;
        context.skip_whitespace();
        if (!context.match_literal('}')) {
            while (*context != '}') {
                context.skip_whitespace();
                auto kexpr = parseArgumentExpression(context);
                context.expect_literal(':', "expected colon after object key");
                context.skip_whitespace();
                auto vexpr = parseArgumentExpression(context);
                context.skip_whitespace();
                
                elements.push_back(make_pair(kexpr, vexpr));

                if (!context.match_literal(','))
                    break;
            }

            context.expect_literal('}', "expected object close");
        }
        result = std::make_shared<ObjectExpression>(elements);
    }
    else if (context.match_literal('%')) {
        context.exception("nested expression literals not yet supported");
    }
    else if (context.match_literal("map ")) {
        //cerr << "map expression" << endl;
        context.skip_whitespace();

        // Extract entity names

        std::vector<MapExpression::IterExpression> expressions;

        for (;;) {
            string entityName;

            while (isalnum(*context) || *context == '_')
                entityName += *context++;
            context.skip_whitespace();
            context.expect_literal(':', ("expected colon after map variable name " + entityName).c_str());
            context.skip_whitespace();

            auto iterExpr = parseArgumentExpression(context);

            expressions.push_back({entityName, iterExpr});

            context.skip_whitespace();
            if (context.match_literal(',')) {
                context.skip_whitespace();
                continue;
            }
            context.expect_literal("->", "expected arrow after map iter expr");
            break;
        }

        context.skip_whitespace();
        auto applyExpr = parseArgumentExpression(context);

        result = std::make_shared<MapExpression>(expressions, applyExpr);
    }
    else if (isalpha(*context) || *context == '_') {
        // Must be a variable or argument name

        string entityName;
        bool isFunction = false;
        vector<shared_ptr<CommandExpression> > args;

        while (isalnum(*context) || *context == '_')
            entityName += *context++;

        if (*context == ' ' && false) {
            context.skip_whitespace();
            // Expression with space separated args
            isFunction = true;
            args.push_back(parseArgumentExpression(context));
            context.skip_whitespace();
            if (!context.match_literal(',')) {
                context.expect_literal(')');
            }
        }
        else if (context.match_literal('(')) {
            // Expression with arguments in paranthesis

            isFunction = true;
                
            while (!context.match_literal(')')) {
                args.push_back(parseArgumentExpression(context));
                context.skip_whitespace();
                if (!context.match_literal(',')) {
                    context.expect_literal(')');
                    break;
                }
            }
        }

        if (isFunction)
            result = std::make_shared<FunctionExpression>(entityName, args);
        else
            result = std::make_shared<VariableExpression>(entityName);

    }
    else result = addContext(std::make_shared<JsonLiteralExpression>(expectJson(context)));
    
    // Now look for operators that modify the output of the previous
    while (true) {
        context.skip_whitespace();

        if (context.match_literal('.')) {
            string fieldName;
            while (isalnum(*context) || *context == '_')
                fieldName += *context++;
        
            result = std::make_shared<ExtractFieldExpression>(fieldName, result);
        }
        else if (context.match_literal('[')) {
            auto element = parseArgumentExpression(context);
            result = std::make_shared<ExtractElementExpression>(element, result);
            context.skip_whitespace();
            context.expect_literal(']');
        }
        else if (context.match_literal('+')) {
            auto rhs = parseArgumentExpression(context);
            result = std::make_shared<PlusExpression>(result, rhs);
        }
        else if (context.match_literal('/')) {
            auto rhs = parseArgumentExpression(context);
            result = std::make_shared<DivideExpression>(result, rhs);
        }
        else break;
    }

    return addContext(result);
}

std::string
argumentRender(const Json::Value & val)
{
    using std::to_string;

    switch (val.type()) {
    case Json::nullValue:    return "";
    case Json::intValue:     return to_string(val.asInt());
    case Json::uintValue:    return to_string(val.asUInt());
    case Json::realValue:    return to_string(val.asDouble());
    case Json::booleanValue: return to_string(val.asBool());
    case Json::stringValue:  return val.asString();
    case Json::arrayValue: {
        std::string str;
        for (auto & v: val) {
            if (!str.empty())
                str += " ";
            str += shellEscape(argumentRender(v));
        }
        return str;
    }
    default:
        throw MLDB::Exception("can't render value " + val.toString() + " as string");
    }
}

std::vector<std::string>
commandRender(const Json::Value & val)
{
    //cerr << "commandRender " << val << endl;

    using std::to_string;

    switch (val.type()) {
    case Json::nullValue:    return { "" };
    case Json::intValue:     return { to_string(val.asInt()) };
    case Json::uintValue:    return { to_string(val.asUInt()) };
    case Json::realValue:    return { to_string(val.asDouble()) };
    case Json::booleanValue: return { to_string(val.asBool()) };
    case Json::stringValue:  return { val.asString() };
    case Json::arrayValue: {
        std::vector<std::string> str;
        for (auto & v: val) {
            str.push_back(argumentRender(v));
        }
        return str;
    }
    default:
        throw MLDB::Exception("can't render value " + val.toString() + " as command");
    }
}

std::string
stringRender(const Json::Value & val)
{
    using std::to_string;

    switch (val.type()) {
    case Json::nullValue:    return "";
    case Json::intValue:     return to_string(val.asInt());
    case Json::uintValue:    return to_string(val.asUInt());
    case Json::realValue:    return to_string(val.asDouble());
    case Json::booleanValue: return to_string(val.asBool());
    case Json::stringValue:  return val.asString();
    case Json::arrayValue: {
        std::string str;
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


/*****************************************************************************/
/* COMMAND TEMPLATE                                                          */
/*****************************************************************************/

void
StringTemplate::
parse(const std::string & command)
{
    ParseContext context(command,
                              command.c_str(), command.c_str() + command.size());

    expr = CommandExpression::parseExpression(context, false /* stop on whitespace */);

    context.expect_eof();
}

std::string
StringTemplate::
operator () (CommandExpressionContext & context) const
{
    return stringRender(expr->apply(context));
}


std::string
StringTemplate::
operator () (const std::initializer_list<std::pair<std::string, std::string> > & vals) const
{
    CommandExpressionContext context(vals);
    return operator () (context);
}


/*****************************************************************************/
/* COMMAND TEMPLATE                                                          */
/*****************************************************************************/

void
CommandTemplate::
parse(const std::string & command)
{
    ParseContext context(command,
                              command.c_str(), command.c_str() + command.size());

    while (context) {
        commandLine.push_back
            (CommandExpression::parseExpression
             (context, true /* stop on whitespace */));
        context.skip_whitespace();
    }
}

void
CommandTemplate::
parse(const std::vector<std::string> & cmdline)
{
    for (unsigned i = 0;  i < cmdline.size();  ++i)
        commandLine.push_back(CommandExpression::parse(cmdline[i]));
}

Command
CommandTemplate::
operator () (CommandExpressionContext & context) const
{
    Command result;

    for (auto & c: commandLine) {
        Json::Value r = c->apply(context);
        //cerr << "result of " << c->surfaceForm << " is " << r << endl;
        vector<string> vals = commandRender(r);
        result.cmdLine.insert(result.cmdLine.end(), vals.begin(), vals.end());
    }

    return result;
}


Command
CommandTemplate::
operator () (const std::initializer_list<std::pair<std::string, std::string> > & vals) const
{
    CommandExpressionContext context(vals);
    return operator () (context);
}

#if 0
struct CommandTemplateDescription
    : public StructureDescriptionImpl<CommandTemplate, CommandTemplateDescription> {

    CommandTemplateDescription();
};

inline CommandTemplateDescription * getDefaultDescription(CommandTemplate*)
{
    return new CommandTemplateDescription();
}
#endif

DEFINE_STRUCTURE_DESCRIPTION(CommandTemplate);

CommandTemplateDescription::
CommandTemplateDescription()
{
    //addField("env", &CommandTemplate::env, "expression to generate the environment");
    addField("commandLine", &CommandTemplate::commandLine, "expression to generate the command line");
}

DEFINE_VALUE_DESCRIPTION_NS(std::shared_ptr<CommandExpression>, CommandExpressionDescription);
DEFINE_VALUE_DESCRIPTION_NS(StringTemplate, StringTemplateDescription);

} // namespace PluginCommand
} // namespace MLDB

