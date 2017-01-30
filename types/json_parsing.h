/* json_parsing.h                                                  -*- C++ -*-
   Jeremy Barnes, 22 February 2013
   Copyright (c) 2013 mldb.ai inc.  All rights reserved.

   This file is part of MLDB. Copyright 2015 mldb.ai inc. All rights reserved.
*/

#pragma once

#include "mldb/types/string.h"
#include <memory>
#include <vector>
#include <functional>

namespace Json {
struct Value;
} // namespace Json


namespace MLDB {

struct ParseContext;
struct JsonParsingContext;
struct ValueDescription;


/** Representation of a numeric value in JSON.  It's designed to allow
    it to be stored the same way it was written (as an integer versus
    floating point, signed vs unsigned) without losing precision.
*/
struct JsonNumber {
    enum Type {
        NONE,
        UNSIGNED_INT,
        SIGNED_INT,
        FLOATING_POINT
    } type;

    union {
        unsigned long long uns;
        long long sgn;
        double fp;
    };    
};

bool expectJsonBool(ParseContext & context);

/** Expect a JSON number. */
JsonNumber expectJsonNumber(ParseContext & context);

/** Match a JSON number. */
bool matchJsonNumber(ParseContext & context, JsonNumber & num);


/*
 * If non-ascii characters are found an exception is thrown
 */
std::string expectJsonStringAscii(ParseContext & context);

/*
 * If non-ascii characters are found an exception is thrown.
 * Output goes into the given buffer, of the given maximum length.
 * If it doesn't fit, then return zero.
 */
ssize_t expectJsonStringAscii(ParseContext & context, char * buf,
                             size_t maxLength);

/*
 * if non-ascii characters are found we replace them by an ascii character that is supplied
 */
std::string expectJsonStringAsciiPermissive(ParseContext & context, char c);

bool matchJsonString(ParseContext & context, std::string & str);

bool matchJsonNull(ParseContext & context);

void
expectJsonArray(ParseContext & context,
                const std::function<void (int, ParseContext &)> & onEntry);

void
expectJsonObject(ParseContext & context,
                 const std::function<void (const std::string &, ParseContext &)> & onEntry);

/** Expect a Json object and call the given callback.  The keys are assumed
    to be ASCII which means no embedded nulls, and so the key can be passed
    as a const char *.
*/
void
expectJsonObjectAscii(ParseContext & context,
                      const std::function<void (const char *, ParseContext &)> & onEntry);

bool
matchJsonObject(ParseContext & context,
                const std::function<bool (const std::string &, ParseContext &)> & onEntry);

void skipJsonWhitespace(ParseContext & context);

Json::Value expectJson(ParseContext & context);

Json::Value expectJsonAscii(ParseContext & context);



/*****************************************************************************/
/* JSON PATH ENTRY                                                           */
/*****************************************************************************/

/** Internal class used to hold an entry in a JSON path.  It is designed to
    avoid unnecessary memory allocations.
*/

struct JsonPathEntry {

    /// Construct to hold an array index
    JsonPathEntry(int index);
    
    /// Construct to hold an element name.  Key must be UTF-8 encoded.
    JsonPathEntry(const std::string & key);
    
    /// Construct to hold an element name.  Key must be UTF-8 encoded.
    JsonPathEntry(const char * keyPtr);

    /// Move constructor
    JsonPathEntry(JsonPathEntry && other) noexcept;

    /// Move assignment operator
    JsonPathEntry & operator = (JsonPathEntry && other) noexcept;

    ~JsonPathEntry();

    int index;            ///< For an array index, the index, otherwise -1
    std::string * keyStr; ///< Owned string version of the key
    const char * keyPtr;  ///< Pointer to owned const char * of key
    int fieldNumber;      ///< Field number in owning structure

    /// Return the name of the field.  Throws if it's an array index
    std::string fieldName() const;

    /// Return a zero-allocation name of the field.  Throws if it's an array
    /// index.  String is owned by this and reference must not outlive it.
    const char * fieldNamePtr() const;
};

struct JsonPath;
// Defined in json_parsing_impl.h


/*****************************************************************************/
/* JSON PARSING CONTEXT                                                      */
/*****************************************************************************/

/** This is an object used to link a predictive JSON parser with the actual
    JSON it is parsing.  It abstracts away the form of the JSON (string,
    stream, object, etc) from the functionality of the parsing.
*/

struct JsonParsingContext {


    JsonParsingContext();
    ~JsonParsingContext();

    /// How many elements in the path to the current element?
    size_t pathLength() const;

    /// Return path element n from the path
    const JsonPathEntry & pathEntry(int n) const;

    /// Current path inside the object being parsed
    std::unique_ptr<JsonPath> path;

    /// Return the current path in . or [] notation as a string.
    std::string printPath(bool includeLeadingDot = true) const;

    /// Return the outermost field name.  Throws if currently in an array or
    /// at the root.
    std::string fieldName() const;

    /// Return the outermost field name.  Throws if currently in an array or
    /// at the root.  Zero allocation but string is only valid until path is
    /// modified.
    const char * fieldNamePtr() const;

    /// Returns the outermost array index.  Throws if not currently in an
    /// array.
    int fieldNumber() const;

    /// Push an element onto the path
    void pushPath(JsonPathEntry entry, int memberNumber = 0);

    /// Replace the top path element with another
    void replacePath(JsonPathEntry entry);

    /// Pop the top-most element of the path./
    void popPath();

    /// Type of function used to handle unknown fields when parsing a structure.
    typedef std::function<void (const ValueDescription * desc)> OnUnknownField;

    /// Stack of handlers for unknown fields.  Only the top-most element is
    /// used.
    std::vector<OnUnknownField> onUnknownFieldHandlers;

    /// Handle an unknown field in a structure.  The currently parsed type is
    /// provided.x
    void onUnknownField(const ValueDescription * desc = 0);

    /// Format an exception with the current context within the object so that
    /// a reasonable error message can be provided to the user.
    virtual void exception(const std::string & message) const = 0;

    /** Return a string that gives the context of where the parsing is
        at, for example line number and column.
    */
    virtual std::string getContext() const = 0;
    
    /// Expect an integer at the current position and consume and return it.
    /// Throws if not found or overflow.
    virtual int expectInt() = 0;

    /// Expect an unsigned integer at the current position and consume and
    /// return it.  Throws if not found or overflow.
    virtual unsigned int expectUnsignedInt() = 0;

    /// Expect a long signed integer at the current position and consume and
    /// return it.  Throws if not found or overflow.
    virtual long expectLong() = 0;

    /// Expect a long unsigned integer at the current position and consume and
    /// return it.  Throws if not found or overflow.
    virtual unsigned long expectUnsignedLong() = 0;

    /// Expect a long long signed integer at the current position and consume
    /// and return it.  Throws if not found or overflow.
    virtual long long expectLongLong() = 0;
    
    /// Expect a long long unsigned integer at the current position and consume
    /// and return it.  Throws if not found or overflow.
    virtual unsigned long long expectUnsignedLongLong() = 0;

    /// Expect a float at the current position and consume and return it.  Throws
    /// if not found.
    virtual float expectFloat() = 0;

    /// Expect a double at the current position and consume and return it.
    /// Throws if not found.
    virtual double expectDouble() = 0;

    /// Expect a boolean at the current position and consume and return it.
    /// Throws if not found.
    virtual bool expectBool() = 0;

    /// Attempt to match an unsigned long long integer at the current position.
    /// If matched, it is consumed, true is returned and val is overwritten with
    /// its value.  If not matched, then false is returned, val is left alone
    /// and nothing is consumed.
    virtual bool matchUnsignedLongLong(unsigned long long & val) = 0;

    /// Attempt to match an unsigned long integer at the current position.
    /// If matched, it is consumed, true is returned and val is overwritten with
    /// its value.  If not matched, then false is returned, val is left alone
    /// and nothing is consumed.
    virtual bool matchLongLong(long long & val) = 0;

    /// Attempt to match a double at the current position.
    /// If matched, it is consumed, true is returned and val is overwritten with
    /// its value.  If not matched, then false is returned, val is left alone
    /// and nothing is consumed.
    virtual bool matchDouble(double & val) = 0;

    /// Expect an ASCII string at the current position and consume and return
    /// it.  If not found (or the string is not pure ASCII), an exception will
    /// be thrown.
    virtual std::string expectStringAscii() = 0;

    /// Expect an ASCII string at the current position and consume and return
    /// it.  If not found (or the string is not pure ASCII), an exception will
    /// be thrown.
    ///
    /// It will be copied into a user-defined buffer, with the provided
    /// position and length.  If the string is too long to fit in the buffer,
    /// then -1 will be returned.  Otherwise the length of the string in the
    /// buffer is returned.
    virtual ssize_t expectStringAscii(char * value, size_t maxLen) = 0;

    /// Expect an UTF-8 string at the current position and consume and return
    /// it.  If not found, an exception will be thrown.
    virtual Utf8String expectStringUtf8() = 0;

    /// Expect an UTF-8 string at the current position and consume and return
    /// it.  If not found, an exception will be thrown.
    ///
    /// It will be copied into a user-defined buffer, with the provided
    /// position and length.  If the string is too long to fit in the buffer,
    /// then -1 will be returned.  Otherwise the length of the string in the
    /// buffer is returned.
    virtual ssize_t expectStringUtf8(char * value, size_t maxLen) = 0;

    /// Expect and return an arbitrary JSON object at the current position
    /// and return it.  This can only fail due to EOF or if there is a JSON
    /// error.
    virtual Json::Value expectJson() = 0;

    /// Expect a null at the current position and consume it.  Throws if not
    /// found.
    virtual void expectNull() = 0;

    /// Look ahead and determine if the current position contains an object.
    virtual bool isObject() const = 0;

    /// Look ahead and determine if the current position contains a string.
    virtual bool isString() const = 0;

    /// Look ahead and determine if the current position contains an array.
    virtual bool isArray() const = 0;

    /// Look ahead and determine if the current position contains a boolean.
    virtual bool isBool() const = 0;

    /// Look ahead and determine if the current position contains a number.
    virtual bool isNumber() const = 0;

    /// Look ahead and determine if the current position contains a null.
    virtual bool isNull() const = 0;

    /// Look ahead and determine if the current position contains an integer.
    virtual bool isInt() const = 0;

    /// Look ahead and determine if the current position contains an unsigned
    /// integer.
    virtual bool isUnsigned() const = 0;

    /// Skip whatever element we're currently looking at.
    virtual void skip() = 0;

    /** For debugging: print out what is the currently being parsed
        element.  No guarantees about what it actually prints; that
        depends on the .
    */
    virtual std::string printCurrent() = 0;
    
    /// Expect a structure, and for each member call the given function.
    /// The current field name can be obtained from the fieldName()
    /// function.
    virtual void forEachMember(const std::function<void ()> & fn) = 0;

    /// Expect an array, and for each element call the given function.
    /// The current array element number can be obtained by counting or
    /// by asking for path.back().index.
    virtual void forEachElement(const std::function<void ()> & fn) = 0;

    /// Is it at the EOF?
    virtual bool eof() const = 0;

    /// Expect that we are at an EOF position, or throw an exception if not.
    /// Default uses eof() and exception().
    virtual void expectEof() const;
};


/*****************************************************************************/
/* STREAMING JSON PARSING CONTEXT                                            */
/*****************************************************************************/

/** This object allows you to parse a stream (string, file, std::istream)
    containing JSON data into an object without performing an intermediate
    translation into a structured JSON format.  This tends to be a lot
    faster as far fewer memory allocations are required.
*/

struct StreamingJsonParsingContext
    : public JsonParsingContext  {

    /** Default chunk size. */
    enum { DEFAULT_CHUNK_SIZE = 65500 };
    
    StreamingJsonParsingContext();
    ~StreamingJsonParsingContext();

    /// Initialize from a general JML ParseContext object.
    StreamingJsonParsingContext(ParseContext & context);
    
    /** Initialize from a filename, loading the file and uncompressing if
        necessary. */
    StreamingJsonParsingContext(const std::string & filename);
    
    /** Initialize from an istream. */
    StreamingJsonParsingContext(const std::string & filename, std::istream & stream,
                                unsigned line = 1, unsigned col = 1,
                                size_t chunk_size = DEFAULT_CHUNK_SIZE);

    /** Initialize from a memory region. */
    StreamingJsonParsingContext(const std::string & filename, const char * start,
                                const char * finish, unsigned line = 1, unsigned col = 1);
    
    StreamingJsonParsingContext(const std::string & filename, const char * start,
                                size_t length, unsigned line = 1, unsigned col = 1);
    
    /// Initialize from a general JML ParseContext object.
    void init(ParseContext & context);

    /** Initialize from a filename, loading the file and uncompressing if
        necessary. */
    void init(const std::string & filename);
    
    /** Initialize from a memory region. */
    void init(const std::string & filename, const char * start,
              const char * finish, unsigned line = 1, unsigned col = 1);

    void init(const std::string & filename, const char * start,
              size_t length, unsigned line = 1, unsigned col = 1);

    /** Initialize from an istream. */
    void init(const std::string & filename, std::istream & stream,
                                unsigned line = 1, unsigned col = 1,
                                size_t chunk_size = DEFAULT_CHUNK_SIZE);

    ParseContext * context;
    std::unique_ptr<ParseContext> ownedContext;

    template<typename Fn>
    void forEachMember(const Fn & fn)
    {
        int memberNum = 0;

        // This structure takes care of pushing and popping our
        // path entry.  It will make sure the member is always
        // popped no matter what.  Out of line here for clang 3.4.
        struct PathPusher {
        PathPusher(const char * memberName,
                   int memberNum,
                   StreamingJsonParsingContext * context)
        : context(context)
            {
                context->pushPath(memberName, memberNum);
            }

            ~PathPusher()
            {
                context->popPath();
            }
                    
            StreamingJsonParsingContext * const context;
        };

        auto onMember = [&] (const char * memberName, size_t nameLen)
            {
                PathPusher pusher(memberName, memberNum++, this);
                fn();
            };
        
        expectJsonObjectUtf8(onMember);
    }

    virtual void forEachMember(const std::function<void ()> & fn);

    template<typename Fn>
    void forEachElement(const Fn & fn)
    {
        bool first = true;

        auto onElement = [&] (int index, ParseContext &)
            {
                if (first)
                    pushPath(index);
                else replacePath(index);

                fn();

                first = false;
            };
        
        expectJsonArray(*context, onElement);

        if (!first)
            popPath();
    }

    virtual void forEachElement(const std::function<void ()> & fn);

    virtual void skip();

    virtual int expectInt();

    virtual unsigned int expectUnsignedInt();

    virtual long expectLong();

    virtual unsigned long expectUnsignedLong();

    virtual long long expectLongLong();

    virtual unsigned long long expectUnsignedLongLong();

    virtual float expectFloat();

    virtual double expectDouble();

    virtual bool expectBool();

    virtual void expectNull();

    virtual bool matchUnsignedLongLong(unsigned long long & val);

    virtual bool matchLongLong(long long & val);

    virtual bool matchDouble(double & val);

    virtual std::string expectStringAscii();

    virtual ssize_t expectStringAscii(char * value, size_t maxLen);

    virtual Utf8String expectStringUtf8();

    virtual ssize_t expectStringUtf8(char * value, size_t maxLen);

    virtual bool isObject() const;

    virtual bool isString() const;

    virtual bool isArray() const;

    virtual bool isBool() const;

    virtual bool isInt() const;
    
    virtual bool isUnsigned() const;
    
    virtual bool isNumber() const;

    virtual bool isNull() const;

    virtual void exception(const std::string & message) const;

    virtual std::string getContext() const;

    virtual Json::Value expectJson();

    virtual std::string printCurrent();

    void expectJsonObjectUtf8(const std::function<void (const char *, size_t)> & onEntry);

    virtual bool eof() const;
};


/*****************************************************************************/
/* STRUCTURED JSON PARSING CONTEXT                                           */
/*****************************************************************************/

/** This allows an already parsed generic JSON object to be presented to
    a JSON parser to interpret its contents.
*/
struct StructuredJsonParsingContext: public JsonParsingContext {

    StructuredJsonParsingContext(const Json::Value & val);

    const Json::Value * current;
    const Json::Value * top;

    virtual void exception(const std::string & message) const;
    
    virtual std::string getContext() const;

    virtual int expectInt();

    virtual unsigned int expectUnsignedInt();

    virtual long expectLong();

    virtual unsigned long expectUnsignedLong();

    virtual long long expectLongLong();

    virtual unsigned long long expectUnsignedLongLong();

    virtual float expectFloat();

    virtual double expectDouble();

    virtual bool expectBool();

    virtual void expectNull();

    virtual bool matchUnsignedLongLong(unsigned long long & val);

    virtual bool matchLongLong(long long & val);

    virtual bool matchDouble(double & val);

    virtual std::string expectStringAscii();

    virtual ssize_t expectStringAscii(char * value, size_t maxLen);

    virtual Utf8String expectStringUtf8();

    virtual ssize_t expectStringUtf8(char * value, size_t maxLen);

    virtual Json::Value expectJson();

    virtual bool isObject() const;

    virtual bool isString() const;

    virtual bool isArray() const;

    virtual bool isBool() const;

    virtual bool isInt() const;

    virtual bool isUnsigned() const;

    virtual bool isNumber() const;

    virtual bool isNull() const;

    virtual void skip();

    virtual void forEachMember(const std::function<void ()> & fn);

    virtual void forEachElement(const std::function<void ()> & fn);

    virtual std::string printCurrent();

    virtual bool eof() const;
};


/*****************************************************************************/
/* STRING JSON PARSING CONTEXT                                               */
/*****************************************************************************/

struct StringJsonParsingContext
    : public StreamingJsonParsingContext  {

    StringJsonParsingContext(std::string str_,
                             const std::string & filename = "<<internal>>");

    std::string str;
};


} // namespace MLDB
