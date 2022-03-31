/* json_parsing.h                                                  -*- C++ -*-
   Jeremy Barnes, 22 February 2013
   Copyright (c) 2013 mldb.ai inc.  All rights reserved.

   This file is part of MLDB. Copyright 2015 mldb.ai inc. All rights reserved.
*/

#pragma once

#include "is_visitable.h"
#include "json_parsing.h"
#include "json.h"

namespace MLDB {

template<typename Visitor, typename... ExtraTypes>
auto visit(Visitor && visitor, JsonParsingContext & context) -> typename std::decay_t<Visitor>::return_type;

template<typename Unknown>
struct HandleJsonUnknown {
    using return_type = std::invoke_result_t<Unknown, Json::Value>;
    Unknown unknown;
    HandleJsonUnknown(Unknown&&unknown) : unknown(std::move(unknown)) {}
};

Utf8String to_string(const Json::Value & val);

template<typename Result>
struct ExceptionOnUnknownJsonReturning {
    using return_type = Result;
    Result unknown(const Json::Value & val) const
    {
        context.exception("Encountered unknown JSON construct: " + val.toStringNoNewLineUtf8());
        MLDB_THROW_LOGIC_ERROR();
    }
    ExceptionOnUnknownJsonReturning(JsonParsingContext & context) : context(context) {}
    JsonParsingContext & context;
};

template<typename Result>
struct HandleJsonUnknown<ExceptionOnUnknownJsonReturning<Result>>: ExceptionOnUnknownJsonReturning<Result> {
    template<typename T>
    HandleJsonUnknown(T&&arg): ExceptionOnUnknownJsonReturning<Result>(std::forward<T>(arg)) {}
};

template<typename Unknown, class... Ops>
struct JsonLambdaVisitor
    : HandleJsonUnknown<Unknown>, Ops... {
    using visitor_base = JsonLambdaVisitor;
    using Ops::operator ()...;
    using HandleJsonUnknown<Unknown>::unknown;

    template<typename Arg>
    auto visit(Arg&& arg) -> std::invoke_result_t<JsonLambdaVisitor, Arg>
    {
        return (*this)(std::forward<Arg>(arg));
    }
};

template<typename Unknown, class... Ts>
JsonLambdaVisitor(Unknown, Ts...) -> JsonLambdaVisitor<Unknown, Ts...>;

struct JsonArrayTag {};
struct JsonObjectTag {};
struct JsonNullTag {};
struct JsonMemberTag {};
struct JsonElementTag {};

template<typename Visitor, typename... ExtraTypes>
auto visit(Visitor && visitor, JsonParsingContext & context) -> typename std::decay_t<Visitor>::return_type
{
    using DecayedVisitor = typename std::decay_t<Visitor>;
    using Return = typename DecayedVisitor::return_type;

    if (context.isNull()) {
        if constexpr (isVisitable<DecayedVisitor, Return(JsonNullTag)>()) {
            context.expectNull();
            return visitor.visit(JsonNullTag{});
        }
    }
    else if (context.isBool()) {
        if constexpr (isVisitable<DecayedVisitor, Return(bool)>()) {
            return visitor.visit(context.expectBool());
        }
    }
    else if (context.isNumber()) {
        if constexpr (isVisitable<DecayedVisitor, Return(JsonNumber)>()) {
            return visitor.visit(context.expectNumber());
        }
        if constexpr(isVisitable<DecayedVisitor, Return(uint64_t)>()
                     || isVisitable<DecayedVisitor, Return(int64_t)>()
                     || isVisitable<DecayedVisitor, Return(double)>()) {
            JsonNumber num = context.expectNumber();
            if (num.type == JsonNumber::FLOATING_POINT
                && isVisitable<DecayedVisitor, Return(double)>()) {
                return visitor.visit(num.fp);
            }
            if (num.type == JsonNumber::SIGNED_INT
                && isVisitable<DecayedVisitor, Return(int64_t)>()) {
                return visitor.visit(num.sgn);
            }
            if (num.type == JsonNumber::UNSIGNED_INT
                && isVisitable<DecayedVisitor, Return(uint64_t)>()) {
                return visitor.visit(num.uns);
            }
            return visitor.unknown(num.toJson());
        }
    }
    else if (context.isString()) {
        if constexpr (isVisitable<DecayedVisitor, Return(Utf8String)>()) {
            return visitor.visit(context.expectStringUtf8());
        }
    }
    else if (context.isArray()) {
        if constexpr (isVisitable<DecayedVisitor, Return(JsonArrayTag)>()) {
            return visitor.visit(JsonArrayTag{});
        }
    }
    else if (context.isObject()) {
        if constexpr (isVisitable<DecayedVisitor, Return(JsonObjectTag)>()) {
            return visitor.visit(JsonObjectTag{});
        }
    }

    return visitor.unknown(context.expectJson());    
}

} // namespace MLDB
