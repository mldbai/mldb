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
#include <set>
#include <iterator>
#include <compare>
//#include <source_location>
#include "mldb/types/string.h"
#include "mldb/types/path.h"
#include "mldb/types/any.h"

namespace MLDB {
struct ValueDescription;
namespace Lisp {

enum MetadataType {
    SOURCE_LOCATION,    ///< Information on the source location
    TYPE_INFO,          ///< Information on the type of a variable, argument or expression
};

DECLARE_ENUM_DESCRIPTION(MetadataType);

std::string metadataTypeToString(MetadataType tp);
MetadataType stringToMetadataType(std::string_view mdType);
Value metadataTypeToValue(Context & lcontext, MetadataType tp);
MetadataType valueToMetadataType(const Value & mdType);

static const std::set<MetadataType> MD_NONE;
static const std::set<MetadataType> MD_TYPE;
static const std::set<MetadataType> MD_LOC;


/*******************************************************************************/
/* LISP VALUE                                                                  */
/*******************************************************************************/

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
    Value(Context & context, SourceLocation loc);

    bool operator == (const Value & other) const;
    bool operator != (const Value & other) const = default;
    bool operator <  (const Value & other) const;
    bool operator <= (const Value & other) const;
    bool operator >  (const Value & other) const;
    bool operator >= (const Value & other) const;

    void toJson(JsonPrintingContext & context) const;
    static Value fromJson(Context & lcontext, JsonParsingContext & pcontext);

    bool hasMetadata(MetadataType tp) const;
    void addMetadata(MetadataType tp, Value md);
    Value getMetadata(MetadataType tp) const;
    const Value & getExistingMetadata(MetadataType tp) const;
    bool removeMetadata(MetadataType tp);
    bool clearMetadata();
    bool removeMetadataRecursive(MetadataType tp);
    bool clearMetadataRecursive();

    int getQuotes() const { return quotes_; }
    void setQuotes(int q) { quotes_ = q; }
    bool isQuoted() const { return quotes_ > 0; }
    void unquote() { if (quotes_) quotes_ -= 1; }
    Value unquoted() const { Value result(*this);  result.unquote(); return result; }

    Utf8String print(const std::set<MetadataType> & md = MD_NONE) const;
    Utf8String asString() const;
    Utf8String getErrorMessageString(const char * msg) const;

    static std::optional<Value> match(Context & lcontext, ParseContext & pcontext);
    static Value parse(Context & lcontext, ParseContext & pcontext);
    static Value parse(Context & lcontext, const Utf8String & str, const SourceLocation & loc);

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
    const std::span<const std::byte> getBytes() const { return value_.asBytes(); }

    void throwUnexpectedValueTypeException(const char * msg, const std::type_info & found) const MLDB_NORETURN;

    /// Asserts that the value is a symbol with a single element in its name; returns the name
    PathElement getSymbolName() const;

    /// Convert to a boolean for a conditional, according to the rules (basically, an
    /// empty list is false, everything else is true)
    bool truth() const;

    bool isNumeric() const;

    const std::type_info & type() const { return value_.type(); }
    const MLDB::ValueDescription & desc() const { return value_.desc(); }

private:
    Context * context_ = nullptr;
    int quotes_ = 0;   ///< Level to which the value is quoted
    Any value_;
    std::map<MetadataType, Value> md_;
    friend class MLDB::ValueDescription;
};

inline std::ostream & operator << (std::ostream & stream, Value val)
{
    return stream << val.print();
}

struct Symbol {
    PathElement sym;
};

struct Null {
};

struct Wildcard {
};

struct Ellipsis {
};

using ListBuilder = std::vector<Value>;

struct ListHead {
    std::shared_ptr<std::vector<Value>> vals;
    uint32_t start = 0;
    uint32_t end = 0;

    inline size_t nToI(size_t n) const
    {
        ssize_t i = n + start;
        ExcAssertLessEqual(i, end);
        return i;
    }
    
    size_t size() const { return end - start; }
    bool empty() const { return start == end; }
};

// This is a STL-compatible Random Access iterator over a string table.  It enables us
// to use std::lower_bound on StringTable implementations when the strings are sorted.
struct ListIterator {
    const Value * pos = nullptr;

    using iterator_category = std::random_access_iterator_tag;
    using value_type = Value;
    using difference_type = ssize_t;
    using pointer = const Value *;
    using reference = const Value &;

    auto operator <=> (const ListIterator & other) const = default;

    ListIterator & operator++() { pos += 1; return *this; }
    ListIterator & operator += (int n) { pos += n;  return *this; }
    ListIterator & operator -= (int n) { pos -= n;  return *this; }
    ListIterator & operator +  (ssize_t n) { pos += n;  return *this; }
    ListIterator & operator -  (ssize_t n) { pos -= n;  return *this; }

    value_type operator * () const { return *pos; }
    difference_type operator - (const ListIterator & other) const { return pos - other.pos; }
};

struct List { 
    List(ListBuilder vals = {});
    //using std::vector<Value>::vector;
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

    ListBuilder steal();
    ListBuilder steal() const;

    const Value & front() const;
    const Value & back() const;
    const Value & at(size_t n) const;
    const Value & operator [] (size_t n) const;
    size_t size() const;
    bool empty() const;

    using const_iterator = ListIterator;
    
    const_iterator begin() const;
    const_iterator end() const;

    // Return the list with the elements from n *inclusive*
    List tail(size_t n) const;

private:
    ListHead items;  // later... list of these
};

struct Function: public Symbol {
    Function() = default;
    Function(PathElement name, CompiledExpression expr);
    Function(PathElement name, std::shared_ptr<const CompiledExpression> compiled);
    std::shared_ptr<const CompiledExpression> compiled;
};

enum class SourceLocationKind {
    NONE,
    USER_PROVIDED,
    AUTO_GENERATED
};

DECLARE_ENUM_DESCRIPTION(SourceLocationKind);

enum class SourceLocationType {
    NONE,
    FUNCTION,
    MACRO,
    DEFINITION
};

DECLARE_ENUM_DESCRIPTION(SourceLocationType);

struct SourceLocationEntry {
    Utf8String file;
    int line = -1;
    int column = -1;
    SourceLocationKind kind = SourceLocationKind::NONE;
    SourceLocationType type = SourceLocationType::NONE;
    Utf8String name;

    Utf8String print() const;
    static std::optional<SourceLocationEntry> parse(ParseContext & pcontext);
};

DECLARE_STRUCTURE_DESCRIPTION(SourceLocationEntry);

struct SourceLocation {
    std::vector<SourceLocationEntry> locations;

    Utf8String file() const;
    int line() const;
    int column() const;

    Utf8String print() const;
    static SourceLocation parse(const Utf8String & str);
};

DECLARE_STRUCTURE_DESCRIPTION(SourceLocation);

SourceLocation getSourceLocation(const Value & val);
SourceLocation getSourceLocation(const ParseContext & pcontext);
SourceLocation getRawSourceLocation(Utf8String file, int line, int column);
//SourceLocation getSourceLocation(const Value & val, std::source_location loc);
SourceLocation getSourceLocation(const Value & val, Utf8String file, int line, int column, Utf8String function = {});

void addSourceLocation(Value & val, SourceLocation loc);

#define LISP_CREATE_SOURCE_LOCATION(val) \
    getSourceLocation(val, __FILE__, __LINE__, __builtin_COLUMN(), __PRETTY_FUNCTION__);

} // namespace Lisp
} // namespace MLDB
