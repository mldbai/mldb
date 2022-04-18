/* lisp_value.h                                                  -*- C++ -*-
   Jeremy Barnes, 22 February 2013
   Copyright (c) 2013 mldb.ai inc.  All rights reserved.

   This file is part of MLDB. Copyright 2015 mldb.ai inc. All rights reserved.
*/

#pragma once

#include "lisp_value.h"
#include "mldb/types/is_visitable.h"
#include <utility>
#include <iostream>

namespace MLDB {
namespace Lisp {

template<typename Visitor, typename... ExtraTypes, typename Value>
auto visit(Visitor && visitor, Value && value) -> typename std::decay_t<Visitor>::return_type;

template<typename Return>
struct Visitor {
    using return_type = Return;
    using visitor_base = Visitor;
};

template<>
struct Visitor<Value> {
    using return_type = Value;
    using visitor_base = Visitor;
};

template<typename Unknown>
struct HandleUnknown {
    using return_type = std::invoke_result_t<Unknown, Value>;
    Unknown unknown;
    HandleUnknown(Unknown&&unknown) : unknown(std::move(unknown)) {}
};

template<typename Unknown, class... Ops>
struct LambdaVisitor
    : HandleUnknown<Unknown>, Ops... {
    using visitor_base = LambdaVisitor;
    using Ops::operator ()...;
    using HandleUnknown<Unknown>::unknown;

    template<typename Arg>
    auto visit(Arg&& arg) -> std::invoke_result_t<LambdaVisitor, Arg>
    {
        return (*this)(std::forward<Arg>(arg));
    }

    template<typename Arg1, typename Arg2>
    auto visit(Arg1&& arg1, Arg2&& arg2) -> std::invoke_result_t<LambdaVisitor, Arg1, Arg2>
    {
        return (*this)(std::forward<Arg1>(arg1), std::forward<Arg2>(arg2));
    }
};

template<typename Unknown, class... Ts>
LambdaVisitor(Unknown, Ts...) -> LambdaVisitor<Unknown, Ts...>;

template<class... Ops>
struct RecursiveLambdaVisitor
    : Ops... {
    using return_type = Value;
    using Ops::operator ()...;

    template<typename Arg>
    auto visit(Arg&& arg) -> std::invoke_result_t<RecursiveLambdaVisitor, Arg>
    {
        return (*this)(std::forward<Arg>(arg));
    }

    template<typename Arg1, typename Arg2>
    auto visit(Arg1&& arg1, Arg2&& arg2) -> std::invoke_result_t<RecursiveLambdaVisitor, Arg1, Arg2>
    {
        return (*this)(std::forward<Arg1>(arg1), std::forward<Arg2>(arg2));
    }

    template<typename ValueIn>
    Value unknown(ValueIn&&value)
    {
        return std::forward<ValueIn>(value);
    }
};
template<class... Ts> RecursiveLambdaVisitor(Ts...) -> RecursiveLambdaVisitor<Ts...>;

template<typename Result>
struct ExceptionOnUnknownReturning {
    using return_type = Result;
    Result unknown(const Value & val) const
    {
        MLDB_THROW_LOGIC_ERROR(val.getErrorMessageString(msg));
    }
    ExceptionOnUnknownReturning(const char * msg) : msg(msg) {}
    const char * msg;
};

template<typename Result>
struct HandleUnknown<ExceptionOnUnknownReturning<Result>>: ExceptionOnUnknownReturning<Result> {
    template<typename T>
    HandleUnknown(T&&arg): ExceptionOnUnknownReturning<Result>(std::forward<T>(arg)) {}
};

template<typename Visitor, typename... ExtraTypes, typename Value>
auto visit(Visitor && visitor, Value && value) -> typename std::decay_t<Visitor>::return_type
{
    using DecayedVisitor = typename std::decay_t<Visitor>;
    using Return = typename DecayedVisitor::return_type;
#define LISP_TRY_VISIT(Type) \
    if constexpr (isVisitable<DecayedVisitor, Return(Value,Type)>()) { \
        if (value.template is<Type>()) { \
            return visitor.visit(value, value.template as<Type>()); \
        }\
    } \
    if constexpr (isVisitable<DecayedVisitor, Return(Type)>()) { \
        if (value.template is<Type>()) { \
            return visitor.visit(value.template as<Type>()); \
        }\
    }
    LISP_TRY_VISIT(List);
    LISP_TRY_VISIT(Symbol);
    LISP_TRY_VISIT(Wildcard);
    LISP_TRY_VISIT(Ellipsis);
    LISP_TRY_VISIT(Null);
    LISP_TRY_VISIT(uint64_t);
    LISP_TRY_VISIT(int64_t);
    LISP_TRY_VISIT(double);
    LISP_TRY_VISIT(bool);
    LISP_TRY_VISIT(Utf8String);
    LISP_TRY_VISIT(Function);
#undef LISP_TRY_VISIT
    return visitor.unknown(value);    
}

template<typename Visitor, typename... ExtraTypes, typename ValueIn>
Value recurse(Visitor && visitor, ValueIn && value)
{
    using DecayedVisitor = typename std::decay_t<Visitor>;
    using Return = typename DecayedVisitor::return_type;
    static_assert(std::is_convertible_v<Return, Value>, "recursive visitor must return value");

    if (value.template is<List>()) {
        auto && list = value.template as<List>();
        List recursed;
        recursed.reserve(list.size());
        for (auto && val: list) {
            recursed.emplace_back(recurse(visitor, val));
        }

        Value result{ value.getContext(), std::move(recursed) };

        if constexpr(isVisitable<DecayedVisitor, Value(Value, List)>()) {
            return visitor.visit(result, result.as<List>());
        }
        if constexpr(isVisitable<DecayedVisitor, Value(List)>()) {
            return visitor.visit(std::move(result.as<List>()));
        }
        if constexpr(isVisitable<DecayedVisitor, Value(Value)>()) {
            return visitor.visit(std::move(result));
        }
        else {
            return result;
        }
    }
    else {
        return visit(std::forward<Visitor>(visitor), std::forward<ValueIn>(value));
    }
}

} // namespace Lisp
} // namespace MLDB

