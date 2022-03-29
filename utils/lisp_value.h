/* lisp_value.h                                                  -*- C++ -*-
   Jeremy Barnes, 22 February 2013
   Copyright (c) 2013 mldb.ai inc.  All rights reserved.

   This file is part of MLDB. Copyright 2015 mldb.ai inc. All rights reserved.
*/

#pragma once

#include <optional>
#include "mldb/base/parse_context.h"
#include <vector>
#include <memory>
#include "mldb/types/string.h"
#include "mldb/types/path.h"
#include "mldb/types/any.h"

namespace MLDB {
namespace Lisp {

struct CompilationScope;
struct ExecutionScope;
struct Value;
struct CompilationState;


/*******************************************************************************/
/* LISP VALUE                                                                  */
/*******************************************************************************/

struct Variable {
    PathElement var;
};

DECLARE_STRUCTURE_DESCRIPTION(Variable);

struct FunctionHandle;

struct Function {
    Path fn;
    std::shared_ptr<FunctionHandle> handle;
};

DECLARE_STRUCTURE_DESCRIPTION(Function);

struct Symbol {
    PathElement sym;
};

DECLARE_STRUCTURE_DESCRIPTION(Symbol);

struct Null {
};

DECLARE_STRUCTURE_DESCRIPTION(Null);

struct Wildcard {
};

PREDECLARE_VALUE_DESCRIPTION(Wildcard);

struct List: public std::vector<Value> {
    using std::vector<Value>::vector;
    Path functionName() const;

    template<typename T, typename UpdateFn, typename FoldFn>
    T fold(UpdateFn && updater, FoldFn && folder, T before = T(), T between = T(), T after = T()) const
    {
        T result = before;
        for (size_t i = 0, n = size();  i < n;  ++i) {
            if (i != 0) updater(result, between);
            updater(result, folder(at(i)));
        }
        updater(result, after);
        return result;
    }
};

DECLARE_STRUCTURE_DESCRIPTION(List);

struct Context;

struct Value {
    Value();
    Value(const Value & other);
    Value(Value && other);
    Value & operator = (const Value & other);
    Value & operator = (Value && other);
    ~Value();

    Value(Context & context, const Utf8String & str);
    Value(Context & context, Utf8String&& str);
    Value(Context & context, std::string&& str);

    template<typename Int>
    Value(Context & context, Int i, std::enable_if_t<std::is_integral_v<Int> && std::is_signed_v<Int>> * = 0)
        : Value(context, (int64_t)i) {}

    template<typename Int>
    Value(Context & context, Int i, std::enable_if_t<std::is_integral_v<Int> && !std::is_signed_v<Int>> * = 0)
        : Value(context, (uint64_t)i) {}

    Value(Context & context, int64_t i);
    Value(Context & context, uint64_t i);
    Value(Context & context, double d);
    Value(Context & context, bool b);

    Value(Context & context, Symbol sym);
    Value(Context & context, Variable var);
    Value(Context & context, Function fn);
    Value(Context & context, Wildcard);
    Value(Context & context, List list);
    Value(Context & context, Null);

#if 0
    static Value list(PathElement type, std::vector<Value> args);
    static Value var(PathElement name);
    static Value str(Utf8String str);
    static Value sym(PathElement name);
    static Value boolean(bool val);
    static Value i64(int64_t val);
    static Value u64(uint64_t val);
    static Value f64(double val);
    static Value null();
    static Value wildcard();
#endif

    bool operator == (const Value & other) const;
    bool operator != (const Value & other) const = default;

    void toJson(JsonPrintingContext & context) const;
    static Value fromJson(Context & lcontext, JsonParsingContext & pcontext);

    Utf8String print() const;
    static std::optional<Value> match(Context & lcontext, ParseContext & pcontext);
    static Value parse(Context & lcontext, ParseContext & pcontext);
    static Value parse(Context & lcontext, const Utf8String & str);

    // Verify that the context matches the expected, or throw an exception
    void verifyContext(Context * expectedContext) const
    {
        if (expectedContext != context_)
            MLDB_THROW_LOGIC_ERROR("mixed lisp contexts");
    }

    Context & getContext() const
    {
        ExcAssert(context_);
        return *context_;
    }

    template<typename T> bool is() const { return value_.is<T>(); }
    template<typename T> const T & as() const { return value_.as<T>(); }

    PathElement getVariableName() const { return as<Variable>().var; }
private:
    Context * context_;
    Any value_;
};

PREDECLARE_VALUE_DESCRIPTION(Value);

inline std::ostream & operator << (std::ostream & stream, Value val)
{
    return stream << val.print();
}

} // namespace Lisp
} // namespace MLDB
