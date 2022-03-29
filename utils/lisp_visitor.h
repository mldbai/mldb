/* lisp_value.h                                                  -*- C++ -*-
   Jeremy Barnes, 22 February 2013
   Copyright (c) 2013 mldb.ai inc.  All rights reserved.

   This file is part of MLDB. Copyright 2015 mldb.ai inc. All rights reserved.
*/

#pragma once

#include "lisp_value.h"
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

template<typename Visitor, typename Method>
struct IsVisitable {
    static_assert(
        std::integral_constant<Method, false>::value,
        "Second template parameter (Method) needs to be of function type.");
};

// specialization that does the checking

template<typename Visitor, typename Ret, typename... Args>
struct IsVisitable<Visitor, Ret(Args...)> {
private:
    template<typename T>
    static constexpr auto check(T*)
    -> typename
        std::is_convertible<
            decltype( std::declval<T>().visit( std::declval<Args>()... ) ),
            Ret
        >::type;  // attempt to call it and see if the return type is correct

    template<typename>
    static constexpr std::false_type check(...);

    typedef decltype(check<Visitor>(nullptr)) type;

public:
    static constexpr bool value = type::value;
};

template<typename Visitor, typename Method>
constexpr bool isVisitable()
{
    return IsVisitable<Visitor, Method>::value;
}

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

    template<typename ValueIn>
    Value unknown(ValueIn&&value)
    {
        return std::forward<ValueIn>(value);
    }
};
template<class... Ts> RecursiveLambdaVisitor(Ts...) -> RecursiveLambdaVisitor<Ts...>;


template<typename Visitor, typename... ExtraTypes, typename Value>
auto visit(Visitor && visitor, Value && value) -> typename std::decay_t<Visitor>::return_type
{
    using DecayedVisitor = typename std::decay_t<Visitor>;
    using Return = typename DecayedVisitor::return_type;
#define LISP_TRY_VISIT(type) if constexpr (isVisitable<DecayedVisitor, Return(type)>()) { if (value.template is<type>()) { return visitor.visit(value.template as<type>()); }}
    LISP_TRY_VISIT(List);
    LISP_TRY_VISIT(Variable);
    LISP_TRY_VISIT(Function);
    LISP_TRY_VISIT(Symbol);
    LISP_TRY_VISIT(Wildcard);
    LISP_TRY_VISIT(Variable);
    LISP_TRY_VISIT(Null);
    LISP_TRY_VISIT(uint64_t);
    LISP_TRY_VISIT(int64_t);
    LISP_TRY_VISIT(double);
    LISP_TRY_VISIT(bool);
    LISP_TRY_VISIT(Utf8String);
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
        if constexpr(isVisitable<DecayedVisitor, Return(List)>()) {
            return visitor.visit(std::move(recursed));
        }
        else {
            Value result{ value.getContext(), std::move(recursed) };
            return result;
        }
    }
    else {
        return visit(std::forward<Visitor>(visitor), std::forward<ValueIn>(value));
    }
}

} // namespace Lisp
} // namespace MLDB

