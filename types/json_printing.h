/* json_printing.h                                                 -*- C++ -*-
   Jeremy Barnes, 26 February 2013
   Copyright (c) 2013 mldb.ai inc.  All rights reserved.

   This file is part of MLDB. Copyright 2015 mldb.ai inc. All rights reserved.

   Context to print out JSON.
*/

#pragma once

#include <string>
#include <ostream>
#include <vector>
#include "mldb/types/string.h"

namespace Json {
struct Value;
} // namespace Json

namespace MLDB {

std::string jsonEscape(const std::string & str);

void jsonEscape(const std::string & str, std::ostream & out);

void jsonEscape(const std::string & str, std::string & out);

bool isJsonValidAscii(char c);

/*****************************************************************************/
/* JSON PRINTING CONTEXT                                                     */
/*****************************************************************************/

struct JsonPrintingContext {

    virtual ~JsonPrintingContext()
    {
    }

    virtual void startObject() = 0;
    virtual void startMember(const Utf8String & memberName) = 0;
    virtual void startMember(const char * memberNameStr, size_t memberNameLen) = 0;
    virtual void endObject() = 0;

    virtual void startArray(int knownSize = -1) = 0;
    virtual void newArrayElement() = 0;
    virtual void endArray() = 0;

    virtual void writeInt(int i) = 0;
    virtual void writeUnsignedInt(unsigned i) = 0;
    virtual void writeLong(long i) = 0;
    virtual void writeUnsignedLong(unsigned long i) = 0;
    virtual void writeLongLong(long long i) = 0;
    virtual void writeUnsignedLongLong(unsigned long long i) = 0;
    virtual void writeFloat(float f) = 0;
    virtual void writeDouble(double d) = 0;
    virtual void writeString(const std::string & s) = 0;
    virtual void writeString(const char * start, size_t len) = 0;
    virtual void writeStringUtf8(const Utf8String & s) = 0;
    virtual void writeStringUtf8(const char * start, size_t len) = 0;
    virtual void writeBool(bool b) = 0;
    virtual void writeNull() = 0;

    virtual void writeJson(const Json::Value & val) = 0;
    virtual void skip() = 0;
};


/*****************************************************************************/
/* STREAM JSON PRINTING CONTEXT                                              */
/*****************************************************************************/

struct StreamJsonPrintingContext
    : public JsonPrintingContext {

    StreamJsonPrintingContext(std::ostream & stream);

    std::ostream & stream;
    bool writeUtf8;          ///< If true, utf8 chars in binary.  False: escaped ASCII

    struct PathEntry {
        PathEntry(bool isObject)
            : isObject(isObject), memberNum(-1)
        {
        }

        bool isObject;
        std::string memberName;
        int memberNum;
    };

    std::vector<PathEntry> path;

    virtual void startObject();

    virtual void startMember(const Utf8String & memberName);
    virtual void startMember(const char * memberNameStr, size_t memberNameLen);

    virtual void endObject();

    virtual void startArray(int knownSize = -1);

    virtual void newArrayElement();

    virtual void endArray();
    
    virtual void skip();

    virtual void writeNull();

    virtual void writeInt(int i);

    virtual void writeUnsignedInt(unsigned int i);

    virtual void writeLong(long int i);

    virtual void writeUnsignedLong(unsigned long int i);

    virtual void writeLongLong(long long int i);

    virtual void writeUnsignedLongLong(unsigned long long int i);

    virtual void writeFloat(float f);

    virtual void writeDouble(double d);

    virtual void writeString(const std::string & s);
    virtual void writeString(const char * start, size_t len);

    virtual void writeStringUtf8(const Utf8String & s);
    virtual void writeStringUtf8(const char * start, size_t len);

    virtual void writeJson(const Json::Value & val);

    virtual void writeBool(bool b);
};


/*****************************************************************************/
/* STRING JSON PRINTING CONTEXT                                              */
/*****************************************************************************/

/** Writes a JSON representation to the given ASCII string. */

struct StringJsonPrintingContext
    : public JsonPrintingContext {

    StringJsonPrintingContext(std::string & str);

    std::string & str;
    bool writeUtf8;          ///< If true, utf8 chars in binary.  False: escaped ASCII

    struct PathEntry {
        PathEntry(bool isObject)
            : isObject(isObject), memberNum(-1)
        {
        }

        bool isObject;
        std::string memberName;
        int memberNum;
    };

    std::vector<PathEntry> path;

    virtual void startObject();

    virtual void startMember(const Utf8String & memberName);
    virtual void startMember(const char * memberNameStr, size_t memberNameLen);

    virtual void endObject();

    virtual void startArray(int knownSize = -1);

    virtual void newArrayElement();

    virtual void endArray();
    
    virtual void skip();

    virtual void writeNull();

    virtual void writeInt(int i);

    virtual void writeUnsignedInt(unsigned int i);

    virtual void writeLong(long int i);

    virtual void writeUnsignedLong(unsigned long int i);

    virtual void writeLongLong(long long int i);

    virtual void writeUnsignedLongLong(unsigned long long int i);

    virtual void writeFloat(float f);

    virtual void writeDouble(double d);

    virtual void writeString(const std::string & s);
    virtual void writeString(const char * start, size_t len);

    virtual void writeStringUtf8(const Utf8String & s);
    virtual void writeStringUtf8(const char * start, size_t len);

    virtual void writeJson(const Json::Value & val);

    virtual void writeBool(bool b);

protected:
    void write(char c);
    void write(char c1, char c2);
    void write(const char * str);
    void write(const char * str, int len);
    void write(const std::string & s);
};

/*****************************************************************************/
/* UTF8 STRING JSON PRINTING CONTEXT                                         */
/*****************************************************************************/

/** Writes a JSON representation to the given Utf8 string.  Note that the
    string CANNOT be modified during writing; direct access to it is
    undefined.

    TODO: we should change this interface (and that of the previous class)
    to have a str() method rather than pass in its result by reference.
*/

struct Utf8StringJsonPrintingContext
    : public StringJsonPrintingContext {

    Utf8StringJsonPrintingContext(Utf8String & str);
    Utf8String & str;
};


/*****************************************************************************/
/* STRUCTURED JSON PRINTING CONTEXT                                          */
/*****************************************************************************/

/** JSON printing context that puts things into a structure. */

struct StructuredJsonPrintingContext
    : public JsonPrintingContext {

    Json::Value & output;
    Json::Value * current;

    StructuredJsonPrintingContext(Json::Value & output);

    std::vector<Json::Value *> path;

    virtual void startObject();

    virtual void startMember(const Utf8String & memberName);
    virtual void startMember(const char * memberNameStr, size_t memberNameLen);

    virtual void endObject();

    virtual void startArray(int knownSize = -1);

    virtual void newArrayElement();

    virtual void endArray();
    
    virtual void skip();

    virtual void writeNull();

    virtual void writeInt(int i);

    virtual void writeUnsignedInt(unsigned int i);

    virtual void writeLong(long int i);

    virtual void writeUnsignedLong(unsigned long int i);

    virtual void writeLongLong(long long int i);

    virtual void writeUnsignedLongLong(unsigned long long int i);

    virtual void writeFloat(float f);

    virtual void writeDouble(double d);

    virtual void writeString(const std::string & s);
    virtual void writeString(const char * start, size_t len);

    virtual void writeStringUtf8(const Utf8String & s);
    virtual void writeStringUtf8(const char * start, size_t len);

    virtual void writeJson(const Json::Value & val);

    virtual void writeBool(bool b);
};

} // namespace MLDB

