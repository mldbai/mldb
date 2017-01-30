/* command_expression_impl.h                                       -*- C++ -*-
   Jeremy Barnes, 27 August 2013
   Copyright (c) 2013 mldb.ai inc.  All rights reserved.

   This file is part of MLDB. Copyright 2015 mldb.ai inc. All rights reserved.

   Implementation of templates for command expression.
*/

#pragma once

#include "command_expression.h"
#include "mldb/types/value_description.h"

namespace MLDB {

namespace PluginCommand {

template<typename T>
void
CommandExpressionVariables::
setValue(const std::string & key, const T & val)
{
    static auto desc = getDefaultDescriptionSharedT<T>();
    Json::Value jval;
    StructuredJsonPrintingContext context(jval);
    desc->printJsonTyped(&val, context);
    setValue(key, std::move(jval));
}

template<typename R>
std::function<Json::Value (const std::vector<Json::Value> & params)>
CommandExpressionVariables::
bindFunction(const std::function<R ()> & fn)
{
    return [=] (const std::vector<Json::Value> & args)
        {
            if (args.size() != 0)
                throw MLDB::Exception("wrong number of args for function");
            return jsonEncode(fn());
        };
}

inline std::function<Json::Value (const std::vector<Json::Value> & params)>
CommandExpressionVariables::
bindFunction(const std::function<void ()> & fn)
{
    return [=] (const std::vector<Json::Value> & args)
        {
            if (args.size() != 0)
                throw MLDB::Exception("wrong number of args for function");
            fn();
            return Json::Value();
        };
}

// TODO: allow functions to be wrapped naturally like this...
template<typename R, typename A1>
std::function<Json::Value (const std::vector<Json::Value> & params)>
CommandExpressionVariables::
bindFunction(const std::function<R (A1)> & fn)
{
    return [=] (const std::vector<Json::Value> & args)
        {
            if (args.size() != 1)
                throw MLDB::Exception("wrong number of args for function");
            return jsonEncode(fn(jsonDecode<A1>(args[0])));
        };
}

template<typename A1>
std::function<Json::Value (const std::vector<Json::Value> & params)>
CommandExpressionVariables::
bindFunction(const std::function<void (A1)> & fn)
{
    return [=] (const std::vector<Json::Value> & args)
        {
            if (args.size() != 1)
                throw MLDB::Exception("wrong number of args for function");
            fn(jsonDecode<A1>(args[0]));
            return Json::Value();
        };
}

template<typename R, typename A1, typename A2>
std::function<Json::Value (const std::vector<Json::Value> & params)>
CommandExpressionVariables::
bindFunction(const std::function<R (A1, A2)> & fn)
{
    return [=] (const std::vector<Json::Value> & args)
        {
            if (args.size() != 2)
                throw MLDB::Exception("wrong number of args for function");
            return jsonEncode(fn(jsonDecode<A1>(args[0]),
                                 jsonDecode<A2>(args[1])));
        };
}

template<typename A1, typename A2>
std::function<Json::Value (const std::vector<Json::Value> & params)>
CommandExpressionVariables::
bindFunction(const std::function<void (A1, A2)> & fn)
{
    return [=] (const std::vector<Json::Value> & args)
        {
            if (args.size() != 2)
                throw MLDB::Exception("wrong number of args for function");
            fn(jsonDecode<A1>(args[0]),
               jsonDecode<A2>(args[1]));
            return Json::Value();
        };
}

template<typename R, typename A1, typename A2, typename A3>
std::function<Json::Value (const std::vector<Json::Value> & params)>
CommandExpressionVariables::
bindFunction(const std::function<R (A1, A2, A3)> & fn)
{
    return [=] (const std::vector<Json::Value> & args)
        {
            if (args.size() != 3)
                throw MLDB::Exception("wrong number of args for function");
            return jsonEncode(fn(jsonDecode<A1>(args[0]),
                                 jsonDecode<A2>(args[1]),
                                 jsonDecode<A3>(args[2])));
        };
}

template<typename A1, typename A2, typename A3>
std::function<Json::Value (const std::vector<Json::Value> & params)>
CommandExpressionVariables::
bindFunction(const std::function<void (A1, A2, A3)> & fn)
{
    return [=] (const std::vector<Json::Value> & args)
        {
            if (args.size() != 3)
                throw MLDB::Exception("wrong number of args for function");
            fn(jsonDecode<A1>(args[0]),
               jsonDecode<A2>(args[1]),
               jsonDecode<A3>(args[2]));
            return Json::Value();
        };
}

template<typename R, typename A1, typename A2, typename A3, typename A4>
std::function<Json::Value (const std::vector<Json::Value> & params)>
CommandExpressionVariables::
bindFunction(const std::function<R (A1, A2, A3, A4)> & fn)
{
    return [=] (const std::vector<Json::Value> & args)
        {
            if (args.size() != 4)
                throw MLDB::Exception("wrong number of args for function");
            return jsonEncode(fn(jsonDecode<A1>(args[0]),
                                 jsonDecode<A2>(args[1]),
                                 jsonDecode<A3>(args[2]),
                                 jsonDecode<A4>(args[3])));
        };
}

template<typename A1, typename A2, typename A3, typename A4>
std::function<Json::Value (const std::vector<Json::Value> & params)>
CommandExpressionVariables::
bindFunction(const std::function<void (A1, A2, A3, A4)> & fn)
{
    return [=] (const std::vector<Json::Value> & args)
        {
            if (args.size() != 4)
                throw MLDB::Exception("wrong number of args for function");
            fn(jsonDecode<A1>(args[0]),
               jsonDecode<A2>(args[1]),
               jsonDecode<A3>(args[2]),
               jsonDecode<A4>(args[3]));
            return Json::Value();
        };
}

template<typename R, typename A1, typename A2, typename A3, typename A4,
         typename A5>
std::function<Json::Value (const std::vector<Json::Value> & params)>
CommandExpressionVariables::
bindFunction(const std::function<R (A1, A2, A3, A4, A5)> & fn)
{
    return [=] (const std::vector<Json::Value> & args)
        {
            if (args.size() != 5)
                throw MLDB::Exception("wrong number of args for function");
            return jsonEncode(fn(jsonDecode<A1>(args[0]),
                                 jsonDecode<A2>(args[1]),
                                 jsonDecode<A3>(args[2]),
                                 jsonDecode<A4>(args[3]),
                                 jsonDecode<A5>(args[4])));
        };
}

template<typename A1, typename A2, typename A3, typename A4, typename A5>
std::function<Json::Value (const std::vector<Json::Value> & params)>
CommandExpressionVariables::
bindFunction(const std::function<void (A1, A2, A3, A4, A5)> & fn)
{
    return [=] (const std::vector<Json::Value> & args)
        {
            if (args.size() != 4)
                throw MLDB::Exception("wrong number of args for function");
            fn(jsonDecode<A1>(args[0]),
               jsonDecode<A2>(args[1]),
               jsonDecode<A3>(args[2]),
               jsonDecode<A4>(args[3]),
               jsonDecode<A5>(args[4]));
            return Json::Value();
        };
}

struct CommandExpressionDescription
    : public ValueDescriptionT<std::shared_ptr<CommandExpression> > {

    virtual void parseJsonTyped(std::shared_ptr<CommandExpression> * val,
                                JsonParsingContext & context) const
    {
        std::string str = context.expectStringAscii();
        ParseContext pcontext(str, str.c_str(), str.c_str() + str.size());
        *val = CommandExpression::parseExpression(pcontext, false);
    }

    virtual void printJsonTyped(const std::shared_ptr<CommandExpression> * val,
                                JsonPrintingContext & context) const
    {
        context.writeString((*val)->surfaceForm);
    }
};

struct StringTemplateDescription
    : public ValueDescriptionT<StringTemplate> {

    virtual bool isDefaultTyped(const StringTemplate * val) const
    {
        return !val->expr
            || val->expr->surfaceForm.empty();
    }

    virtual void parseJsonTyped(StringTemplate * val,
                                JsonParsingContext & context) const
    {
        std::string str = context.expectStringAscii();
        val->parse(str);
    }

    virtual void printJsonTyped(const StringTemplate * val,
                                JsonPrintingContext & context) const
    {
        context.writeString(val->toString());
    }
};

} // namespace PluginCommand

} // namespace MLDB
