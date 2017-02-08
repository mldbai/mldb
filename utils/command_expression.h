/* command_expression.h                                            -*- C++ -*-
   Jeremy Barnes, 27 August 2013
   This file is part of MLDB. Copyright 2015 mldb.ai inc. All rights reserved.

   Template for a command that can be reconstituted with calculated
   arguments.
*/

#pragma once

#include "mldb/ext/jsoncpp/json.h"
#include <functional>
#include "mldb/arch/exception.h"
#include <memory>
#include "mldb/base/parse_context.h"
#include "mldb/jml/utils/vector_utils.h"
#include "mldb/base/exc_assert.h"
#include "mldb/types/value_description_fwd.h"
#include "command.h"


namespace MLDB {

namespace PluginCommand {

/*****************************************************************************/
/* COMMAND TEMPLATE                                                          */
/*****************************************************************************/

/** Grammar

    Variables can be any JSON value (not just an object or an array)
    Arrays are converted to [1,2,3] when needed as a string
    Objects can not be converted to strings
    

    %{name} is the value of the variable called 'name'
    %{'name'} is 'name' (without quotes)
    %{function(param1,param2,param3)} is the result of applying the given function
*/

struct CommandExpressionVariables;

/*****************************************************************************/
/* COMMAND EXPRESSION VARIABLES                                              */
/*****************************************************************************/

/** This contains the value of variables that are set in a given scope for
    a command expression.

    It can contain a pointer to an outer scope, which will be used as a
    fallback if a value can't be found.
*/

typedef std::function<Json::Value (const std::vector<Json::Value> &)>
    CommandExpressionFunction;

struct CommandExpressionVariables {

    /// Contains the set of build-in functions and global values.  This will
    /// be used as an outer context by default for every context that is
    /// created so that they will always be acessible.
    static const CommandExpressionVariables * builtins();

    CommandExpressionVariables(const CommandExpressionVariables * outer = builtins())
        : outer(outer)
    {
    }
    
    CommandExpressionVariables(const std::initializer_list<std::pair<std::string, std::string> > & vals)
        : outer(builtins()), values(vals.begin(), vals.end())
    {
    }

    CommandExpressionVariables(const CommandExpressionVariables * outer,
                               const std::initializer_list<std::pair<std::string, std::string> > & vals)
        : outer(outer),
          values(vals.begin(), vals.end())
    {
    }

    /// Outer context we look in if we don't find something here
    const CommandExpressionVariables * outer;

    /// Value of local variables
    std::map<std::string, Json::Value> values;

    /// Definition of local functions
    std::map<std::string, CommandExpressionFunction> functions;
    
    bool hasValue(const std::string & name) const
    {
        return values.count(name);
    }

    void setValue(const std::string & key, const Json::Value & val)
    {
        values[key] = val;
    }

    void setValue(const std::string & key, const std::string & val)
    {
        values[key] = val;
    }

    void setValue(const std::string & key, const char * val)
    {
        values[key] = val;
    }

    Json::Value getValue(const std::string & variableName) const
    {
        auto it = values.find(variableName);
        if (it == values.end()) {
            if (outer)
                return outer->getValue(variableName);
            else throw MLDB::Exception("couldn't find value of " + variableName);
        }
        return it->second;
    }
    
    template<typename T>
    void setValue(const std::string & key, const T & val);

    Json::Value applyFunction(const std::string & functionName,
                              const std::vector<Json::Value> & functionArgs) const;

    // TODO: allow functions to be wrapped naturally like this...
    template<typename R>
    std::function<Json::Value (const std::vector<Json::Value> & params)>
    bindFunction(const std::function<R ()> & fn);

    std::function<Json::Value (const std::vector<Json::Value> & params)>
    bindFunction(const std::function<void ()> & fn);

    // TODO: allow functions to be wrapped naturally like this...
    template<typename R, typename A1>
    std::function<Json::Value (const std::vector<Json::Value> & params)>
    bindFunction(const std::function<R (A1)> & fn);

    template<typename A1>
    std::function<Json::Value (const std::vector<Json::Value> & params)>
    bindFunction(const std::function<void (A1)> & fn);

    template<typename R, typename A1, typename A2>
    std::function<Json::Value (const std::vector<Json::Value> & params)>
    bindFunction(const std::function<R (A1, A2)> & fn);

    template<typename A1, typename A2>
    std::function<Json::Value (const std::vector<Json::Value> & params)>
    bindFunction(const std::function<void (A1, A2)> & fn);

    template<typename R, typename A1, typename A2, typename A3>
    std::function<Json::Value (const std::vector<Json::Value> & params)>
    bindFunction(const std::function<R (A1, A2, A3)> & fn);

    template<typename A1, typename A2, typename A3>
    std::function<Json::Value (const std::vector<Json::Value> & params)>
    bindFunction(const std::function<void (A1, A2, A3)> & fn);

    template<typename R, typename A1, typename A2, typename A3, typename A4>
    std::function<Json::Value (const std::vector<Json::Value> & params)>
    bindFunction(const std::function<R (A1, A2, A3, A4)> & fn);

    template<typename A1, typename A2, typename A3, typename A4>
    std::function<Json::Value (const std::vector<Json::Value> & params)>
    bindFunction(const std::function<void (A1, A2, A3, A4)> & fn);

    template<typename R, typename A1, typename A2, typename A3, typename A4,
             typename A5>
    std::function<Json::Value (const std::vector<Json::Value> & params)>
    bindFunction(const std::function<R (A1, A2, A3, A4, A5)> & fn);

    template<typename A1, typename A2, typename A3, typename A4, typename A5>
    std::function<Json::Value (const std::vector<Json::Value> & params)>
    bindFunction(const std::function<void (A1, A2, A3, A4, A5)> & fn);

    template<typename R, typename... Args>
    void addBoundFunction(const std::string & name,
                          const std::function<R (Args...)> & fn)
    {
        addFunction(name, bindFunction(fn));
    }

    template<typename R, typename... Args>
    void addBoundFunction(const std::string & name,
                          R (*fn) (Args...))
    {
        addFunction(name, bindFunction(std::function<R (Args...)>(fn)));
    }

    void addFunction(const std::string & name, const CommandExpressionFunction & function)
    {
        functions[name] = function;
    }
};


/** Renders the given JSON expression as a string. */
std::string stringRender(const Json::Value & val);


/*****************************************************************************/
/* COMMAND EXPRESSION CONTEXT                                                */
/*****************************************************************************/

struct CommandExpressionContext : public CommandExpressionVariables {
    CommandExpressionContext(const CommandExpressionVariables * outer = builtins())
        : CommandExpressionVariables(outer)
    {
    }
    
    CommandExpressionContext(const std::initializer_list<std::pair<std::string, std::string> > & vals)
        : CommandExpressionVariables(vals)
    {
    }
    
    CommandExpressionContext(const CommandExpressionVariables * outer,
                             const std::initializer_list<std::pair<std::string, std::string> > & vals)
        : CommandExpressionVariables(outer, vals)
    {
    }

    template<typename Val>
    void addGlobal(const std::string & key, const Val & val)
    {
        setValue(key, val);
    }
};


/*****************************************************************************/
/* COMMAND EXPRESSION                                                        */
/*****************************************************************************/

/** An expression that generates a command. */

struct CommandExpression {
    virtual ~CommandExpression()
    {
    }
    
    virtual Json::Value apply(const CommandExpressionContext & vars) const
    {
            return applyString(vars);

    }

    virtual std::string applyString(const CommandExpressionContext & vars) const
    {
        try {
            return stringRender(apply(vars));
        } catch (const std::exception & exc) {
            using namespace std;
            cerr << "apply was " << apply(vars) << endl;
            throw;
        }
    }

    // Original form that was parsed to get it... used for printing
    std::string surfaceForm;

    static std::shared_ptr<CommandExpression>
    parseExpression(ParseContext & context, bool stopOnWhitespace = false);

    static std::shared_ptr<CommandExpression>
    parseArgumentExpression(ParseContext & context);

    static std::shared_ptr<CommandExpression>
    parse(const std::string & val);
    
    static std::shared_ptr<CommandExpression>
    parseArgumentExpression(const std::string & expr);
    
    static std::shared_ptr<CommandExpression> parse(const std::vector<std::string> & vals);


};

// Convert to strings and concatenate
struct ConcatExpression : public CommandExpression {

    virtual std::string applyString(const CommandExpressionContext & vars) const
    {
        std::string result;

        for (auto & c: clauses)
            result += c->applyString(vars);
        
        return result;
    }

    std::vector<std::shared_ptr<CommandExpression> > clauses;
};

struct ArrayExpression: public CommandExpression {

    virtual Json::Value apply(const CommandExpressionContext & vars) const
    {
        Json::Value result(Json::arrayValue);

        for (auto & c: clauses)
            result.append(c->apply(vars));
        
        return result;
    }

    std::vector<std::shared_ptr<CommandExpression> > clauses;
};

struct ObjectExpression: public CommandExpression {

    ObjectExpression(std::vector<std::pair<std::shared_ptr<CommandExpression>,
                                           std::shared_ptr<CommandExpression> > > clauses)
        : clauses(std::move(clauses))
    {
    }

    virtual Json::Value apply(const CommandExpressionContext & vars) const
    {
        Json::Value result(Json::objectValue);

        for (auto & c: clauses)
            result[c.first->applyString(vars)] = c.second->apply(vars);
        
        return result;
    }

    std::vector<std::pair<std::shared_ptr<CommandExpression>,
                          std::shared_ptr<CommandExpression> > > clauses;
};

struct LiteralExpression : public CommandExpression {

    LiteralExpression(const std::string & literal = "")
        : literal(literal)
    {
    }

    virtual std::string applyString(const CommandExpressionContext & vars) const
    {
        return literal;
    }

    std::string literal;
};

struct JsonLiteralExpression : public CommandExpression {

    JsonLiteralExpression(const Json::Value & literal = Json::Value())
        : literal(literal)
    {
    }

    virtual Json::Value apply(const CommandExpressionContext & vars) const
    {
        return literal;
    }

    Json::Value literal;
};

struct InlineArrayExpression: public CommandExpression {
    InlineArrayExpression(const std::vector<std::shared_ptr<CommandExpression> > & elements)
        : elements(elements)
    {
    }

    virtual Json::Value apply(const CommandExpressionContext & vars) const
    {
        Json::Value result(Json::arrayValue);
        for (auto & e: elements)
            result.append(e->apply(vars));
        return result;
    }

    std::vector<std::shared_ptr<CommandExpression> > elements;
};

/** Expression that iterates over one or more lists and calls an inner
    expression on the cross product of those expressions.  Eg

    map color: [ 'red', 'green', 'blue' ] -> upperCase(color)
      (returns [ 'RED', 'GREEN', 'BLUE' ])
    map x: [0, 1], y: [0, 1] -> xor(x, y)
      (returns [ 0, 1, 1, 0 ])
    map x: [0, 1] -> map y: [0,1] -> xor(x,y)
      (returns [ [ 0, 1 ], [ 1, 0 ] ])
*/

struct MapExpression: public CommandExpression {
    struct IterExpression {
        std::string variableName;
        std::shared_ptr<CommandExpression> expression;
    };

    MapExpression(const std::vector<IterExpression> & iterExpressions,
                  std::shared_ptr<CommandExpression> outputExpression)
        : iterExpressions(iterExpressions),
          outputExpression(outputExpression)
    {
    }

    virtual Json::Value apply(const CommandExpressionContext & vars) const
    {
        Json::Value result(Json::arrayValue);

        if (iterExpressions.empty())
            return result;

        applyRecursive(0, vars, result);

        return result;
    }

    void applyRecursive(int exprIndex, const CommandExpressionContext & vars,
                        Json::Value & result) const
    {
        auto & expr = iterExpressions.at(exprIndex);
        Json::Value iterResult = expr.expression->apply(vars);

        CommandExpressionContext inner(vars);

        ExcAssert(iterResult.isArray());

        for (auto & el: iterResult) {
            inner.setValue(expr.variableName, el);

            if (exprIndex == iterExpressions.size() - 1)
                result.append(outputExpression->apply(inner));
            else applyRecursive(exprIndex + 1, inner, result);
        }
    }

    std::vector<IterExpression> iterExpressions;
    std::shared_ptr<CommandExpression> outputExpression;
};

struct FunctionExpression : public CommandExpression {

    FunctionExpression(const std::string & functionNameValue,
                       std::vector<std::shared_ptr<CommandExpression> > args)
        : functionNameValue(functionNameValue),
          args(args)
    {
    }

    //std::shared_ptr<CommandExpression> functionName;
    std::string functionNameValue;
    std::vector<std::shared_ptr<CommandExpression> > args;

    virtual Json::Value
    apply(const CommandExpressionContext & context) const
    {
        //std::string functionNameValue = functionName->applyString(context);

        std::vector<Json::Value> argValues;
        for (auto & a: args)
            argValues.push_back(a->apply(context));

        return context.applyFunction(functionNameValue, argValues);
    }
};

struct VariableExpression : public CommandExpression {

    VariableExpression(const std::string & variableName = "")
        : variableName(variableName)
    {
    }

    std::string variableName;

    virtual Json::Value apply(const CommandExpressionContext & vars) const
    {
        return vars.getValue(variableName);
    }
    
};

struct ExtractFieldExpression: public CommandExpression {

    ExtractFieldExpression(const std::string & fieldName,
                           std::shared_ptr<CommandExpression> expr)
        : fieldName(fieldName),
          expr(expr)
    {
    }

    virtual Json::Value apply(const CommandExpressionContext & vars) const
    {
        return expr->apply(vars)[fieldName];
    }

    std::string fieldName;
    std::shared_ptr<CommandExpression> expr;
};

struct ExtractElementExpression: public CommandExpression {

    ExtractElementExpression(std::shared_ptr<CommandExpression> element,
                             std::shared_ptr<CommandExpression> expr)
        : element(element),
          expr(expr)
    {
    }

    virtual Json::Value apply(const CommandExpressionContext & vars) const
    {
        Json::Value index = element->apply(vars);
        if (index.isString())
            return expr->apply(vars)[index.asString()];
        return expr->apply(vars)[index.asInt()];
    }

    std::shared_ptr<CommandExpression> element;
    std::shared_ptr<CommandExpression> expr;
};

struct ArithmeticExpression: public CommandExpression {
    
};

struct BinaryArithmeticExpression: public CommandExpression {
    BinaryArithmeticExpression(std::shared_ptr<CommandExpression> lhs,
                               std::shared_ptr<CommandExpression> rhs)
        : lhs(lhs), rhs(rhs)
    {
    }

    std::shared_ptr<CommandExpression> lhs;
    std::shared_ptr<CommandExpression> rhs;

    virtual Json::Value op(const Json::Value & lhs,
                           const Json::Value & rhs) const = 0;

    virtual Json::Value apply(const CommandExpressionContext & vars) const
    {
        Json::Value l = lhs->apply(vars);
        Json::Value r = rhs->apply(vars);

        return op(l, r);
    }
};

struct PlusExpression: public BinaryArithmeticExpression {

    PlusExpression(std::shared_ptr<CommandExpression> lhs,
                   std::shared_ptr<CommandExpression> rhs)
        : BinaryArithmeticExpression(lhs, rhs)
    {
    }

    virtual Json::Value op(const Json::Value & lhs,
                           const Json::Value & rhs) const
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
        //else throw MLDB::Exception("don't know how to add %s and %s",
        //                         lhs.toString().c_str(),
        //                         rhs.toString().c_str());
    }
};

struct DivideExpression: public BinaryArithmeticExpression {

    DivideExpression(std::shared_ptr<CommandExpression> lhs,
                   std::shared_ptr<CommandExpression> rhs)
        : BinaryArithmeticExpression(lhs, rhs)
    {
    }

    virtual Json::Value op(const Json::Value & lhs,
                           const Json::Value & rhs) const
    {
        if (lhs.isNumeric() && rhs.isNumeric()) {
            if (lhs.isDouble() || rhs.isDouble()) {
                return lhs.asDouble() / rhs.asDouble();
            }
            return lhs.asInt() / rhs.asInt();
        }
        else throw MLDB::Exception("don't know how to divide %s by %s",
                                 lhs.toString().c_str(),
                                 rhs.toString().c_str());
    }
};

PREDECLARE_VALUE_DESCRIPTION(std::shared_ptr<CommandExpression>);

/** Renders the given JSON expression as a shell command. */
std::vector<std::string> commandRender(const Json::Value & val);


/*****************************************************************************/
/* STRING TEMPLATE                                                           */
/*****************************************************************************/

struct StringTemplate {
    StringTemplate(const std::string & val = "")
    {
        parse(val);
    }

    StringTemplate(const char * str)
    {
        parse(str);
    }

    StringTemplate(const std::shared_ptr<CommandExpression> & expr)
        : expr(expr)
    {
        ExcAssert(expr);
    }

    StringTemplate & operator = (const std::string & str)
    {
        parse(str);
        return *this;
    }

    StringTemplate & operator = (const char * str)
    {
        parse(str);
        return *this;
    }

    /** Parse the given string as an expression. */
    void parse(const std::string & command);

    std::string toString() const
    {
        return expr->surfaceForm;
    }

    std::shared_ptr<CommandExpression> expr;

    /** Apply the template to an expression context to return a command. */
    std::string operator () (CommandExpressionContext & context) const;

    /** Apply the template to a list of variables. */
    std::string operator () (const std::initializer_list<std::pair<std::string, std::string> > & vals) const;
};

PREDECLARE_VALUE_DESCRIPTION(StringTemplate);

inline std::ostream & operator << (std::ostream & stream, const StringTemplate & tmpl)
{
    return stream << tmpl.toString();
}


/*****************************************************************************/
/* COMMAND TEMPLATE                                                          */
/*****************************************************************************/

struct CommandTemplate {

    CommandTemplate()
    {
    }

    CommandTemplate(const std::vector<std::string> & args)
    {
        parse(args);
    }

    CommandTemplate(const std::initializer_list<std::string> & args)
    {
        parse(std::vector<std::string>(args));
    }

    CommandTemplate(const std::string & val)
    {
        parse(val);
    }

    void parse(const std::string & command);  /// will be split up by whitespace
    void parse(const std::vector<std::string> & cmdline); /// Pre split

    bool empty()
    {
        return commandLine.empty();
    }
    
    std::vector<std::pair<std::string, StringTemplate > > env;
    std::vector<std::shared_ptr<CommandExpression> > commandLine;
    //StringTemplate resources;

    /** Apply the template to an expression context to return a command. */
    Command operator () (CommandExpressionContext & context) const;

    /** Apply the template to a list of variables. */
    Command operator () (const std::initializer_list<std::pair<std::string, std::string> > & vals) const;
};

DECLARE_STRUCTURE_DESCRIPTION(CommandTemplate);

} // namespace PluginCommand
} // namespace MLDB

