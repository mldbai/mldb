/* lisp_value.h                                                  -*- C++ -*-
   Jeremy Barnes, 22 February 2013
   Copyright (c) 2013 mldb.ai inc.  All rights reserved.

   This file is part of MLDB. Copyright 2015 mldb.ai inc. All rights reserved.
*/

#pragma once

#include "lisp_fwd.h"
#include <optional>
#include <vector>
#include <memory>
#include <map>
#include "mldb/types/string.h"
#include "mldb/types/path.h"
#include "mldb/types/any.h"

namespace MLDB {
namespace Lisp {

/*******************************************************************************/
/* LISP VALUE                                                                  */
/*******************************************************************************/

struct Symbol {
    PathElement sym;
};

struct Null {
};

struct Wildcard {
};

struct Ellipsis {
};

struct List: public std::vector<Value> {
    using std::vector<Value>::vector;
    Path functionName() const;
    PathElement simpleFunctionName() const;

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

struct Function: public Symbol {
    Function() = default;
    Function(PathElement name, CompiledExpression expr);
    Function(PathElement name, std::shared_ptr<const CompiledExpression> compiled);
    std::shared_ptr<const CompiledExpression> compiled;
};

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
    Value(Context & context, Wildcard);
    Value(Context & context, Ellipsis);
    Value(Context & context, List list);
    Value(Context & context, Null);
    Value(Context & context, Type tp);
    Value(Context & context, Function fn);

    bool operator == (const Value & other) const;
    bool operator != (const Value & other) const = default;

    void toJson(JsonPrintingContext & context) const;
    static Value fromJson(Context & lcontext, JsonParsingContext & pcontext);

    bool hasMetadata() const;
    void addMetadata(Value md);
    Value getMetadata() const;

    int getQuotes() const { return quotes_; }
    void setQuotes(int q) { quotes_ = q; }
    bool isQuoted() const { return quotes_ > 0; }
    void unquote() { if (quotes_) quotes_ -= 1; }
    Value unquoted() const { Value result(*this);  result.unquote(); return result; }

    Utf8String print() const;
    Utf8String asString() const;
    Utf8String getErrorMessageString(const char * msg) const;

    static std::optional<Value> match(Context & lcontext, ParseContext & pcontext);
    static Value parse(Context & lcontext, ParseContext & pcontext);
    static Value parse(Context & lcontext, const Utf8String & str);

    static Value parseAtom(Context & lcontext, ParseContext & pcontext);
    static std::optional<Value> matchAtom(Context & lcontext, ParseContext & pcontext);

    static std::optional<Value>
    matchRecursive(Context & lcontext, ParseContext & pcontext,
                   const std::function<std::optional<Value>(Context &, ParseContext &)> & matchAtom,
                   const std::function<std::optional<Value>(Context &, ParseContext &)> & matchMetadata,
                   const std::function<std::optional<Value>(Context &, ParseContext &)> & recurse);
    static Value
    parseRecursive(Context & lcontext, ParseContext & pcontext,
                   const std::function<Value(Context &, ParseContext &)> & parseAtom,
                   const std::function<Value(Context &, ParseContext &)> & parseMetadata,
                   const std::function<Value(Context &, ParseContext &)> & recurse);

    // Verify that the context matches the expected, or throw an exception
    void verifyContext(Context * expectedContext) const
    {
        if (expectedContext != context_)
            MLDB_THROW_LOGIC_ERROR("mixed lisp contexts");
    }

    // Move this value to a different context
    Value toContext(Context & otherContext) const;

    Context & getContext() const
    {
        ExcAssert(context_);
        return *context_;
    }

    bool isInitialized() const { return context_ != nullptr; }
    bool isUninitialized() const { return context_ == nullptr; }

    template<typename T> const T * cast() const { return is<T>() ? &as<T>() : nullptr; }
    template<typename T> bool is() const { return value_.is<T>(); }
    //template<typename T> T & as() { return value_.as<T>(); }
    template<typename T> const T & as() const { return value_.as<T>(); }
    template<typename T> const T & expect(const char * msg) const
    {
        const T * result = cast<T>();
        if (result) return *result;
        throwUnexpectedValueTypeException(msg, typeid(T));
    }
    
    void throwUnexpectedValueTypeException(const char * msg, const std::type_info & found) const MLDB_NORETURN;

    /// Asserts that the value is a symbol with a single element in its name; returns the name
    PathElement getSymbolName() const;

private:
    Context * context_ = nullptr;
    int quotes_ = 0;   ///< Level to which the value is quoted
    Any value_;
    Any md_;
    friend class ValueDescription;
};

inline std::ostream & operator << (std::ostream & stream, Value val)
{
    return stream << val.print();
}

} // namespace Lisp
} // namespace MLDB
