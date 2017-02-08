// This file is part of MLDB. Copyright 2015 mldb.ai inc. All rights reserved.

/** basic_value_descriptions.cc

    Jeremy Barnes, 19 August 2015
*/

#include "basic_value_descriptions.h"

namespace MLDB {


struct StringDescription
    : public ValueDescriptionI<std::string, ValueKind::STRING, StringDescription> {

    virtual void parseJsonTyped(std::string * val,
                                JsonParsingContext & context) const
    {
        *val = context.expectStringAscii();
    }

    virtual void printJsonTyped(const std::string * val,
                                JsonPrintingContext & context) const
    {
        context.writeString(*val);
    }

    virtual bool isDefaultTyped(const std::string * val) const
    {
        return val->empty();
    }
};

template class ValueDescriptionI<std::string, ValueKind::STRING, StringDescription>;

struct Utf8StringDescription
    : public ValueDescriptionI<Utf8String, ValueKind::STRING, Utf8StringDescription> {

    virtual void parseJsonTyped(Utf8String * val,
                                JsonParsingContext & context) const
    {
        *val = context.expectStringUtf8();
    }

    virtual void printJsonTyped(const Utf8String * val,
                                JsonPrintingContext & context) const
    {
        context.writeStringUtf8(*val);
    }

    virtual bool isDefaultTyped(const Utf8String * val) const
    {
        return val->empty();
    }
};

template class ValueDescriptionI<Utf8String, ValueKind::STRING, Utf8StringDescription>;

struct Utf32StringDescription
    : public ValueDescriptionI<Utf32String, ValueKind::STRING, Utf32StringDescription> {
    virtual void parseJsonTyped(Utf32String *val,
                                JsonParsingContext & context) const
    {
        auto utf8Str = context.expectStringUtf8();
        *val = Utf32String::fromUtf8(utf8Str);
    }

    virtual void printJsonTyped(const Utf32String *val,
                                JsonPrintingContext & context) const
    {
        std::string utf8Str;
        utf8::utf32to8(val->begin(), val->end(), std::back_inserter(utf8Str));
        context.writeStringUtf8(Utf8String { utf8Str });
    }

    virtual bool isDefaultTyped(const Utf32String *val) const
    {
        return val->empty();
    }
};

template class ValueDescriptionI<Utf32String, ValueKind::STRING, Utf32StringDescription>;

struct SignedIntDescription
    : public ValueDescriptionI<signed int, ValueKind::INTEGER, SignedIntDescription> {

    virtual void parseJsonTyped(signed int * val,
                                JsonParsingContext & context) const
    {
        *val = context.expectInt();
    }
    
    virtual void printJsonTyped(const signed int * val,
                                JsonPrintingContext & context) const
    {
        context.writeInt(*val);
    }
};

template class ValueDescriptionI<signed int, ValueKind::INTEGER, SignedIntDescription>;

struct UnsignedIntDescription
    : public ValueDescriptionI<unsigned int, ValueKind::INTEGER, UnsignedIntDescription> {

    virtual void parseJsonTyped(unsigned int * val,
                                JsonParsingContext & context) const
    {
        *val = context.expectInt();
    }
    
    virtual void printJsonTyped(const unsigned int * val,
                                JsonPrintingContext & context) const
    {
        context.writeInt(*val);
    }
};

template class ValueDescriptionI<unsigned int, ValueKind::INTEGER, UnsignedIntDescription>;

struct SignedLongDescription
    : public ValueDescriptionI<signed long, ValueKind::INTEGER, SignedLongDescription> {

    virtual void parseJsonTyped(signed long * val,
                                JsonParsingContext & context) const
    {
        *val = context.expectLong();
    }
    
    virtual void printJsonTyped(const signed long * val,
                                JsonPrintingContext & context) const
    {
        context.writeLong(*val);
    }
};

template class ValueDescriptionI<signed long, ValueKind::INTEGER, SignedLongDescription>;

struct UnsignedLongDescription
    : public ValueDescriptionI<unsigned long, ValueKind::INTEGER, UnsignedLongDescription> {

    virtual void parseJsonTyped(unsigned long * val,
                                JsonParsingContext & context) const
    {
        *val = context.expectUnsignedLong();
    }
    
    virtual void printJsonTyped(const unsigned long * val,
                                JsonPrintingContext & context) const
    {
        context.writeUnsignedLong(*val);
    }
};

struct SignedLongLongDescription
    : public ValueDescriptionI<signed long long, ValueKind::INTEGER, SignedLongLongDescription> {

    virtual void parseJsonTyped(signed long long * val,
                                JsonParsingContext & context) const
    {
        *val = context.expectLongLong();
    }
    
    virtual void printJsonTyped(const signed long long * val,
                                JsonPrintingContext & context) const
    {
        context.writeLongLong(*val);
    }
};

struct UnsignedLongLongDescription
    : public ValueDescriptionI<unsigned long long, ValueKind::INTEGER, UnsignedLongLongDescription> {

    virtual void parseJsonTyped(unsigned long long * val,
                                JsonParsingContext & context) const
    {
        *val = context.expectUnsignedLongLong();
    }
    
    virtual void printJsonTyped(const unsigned long long * val,
                                JsonPrintingContext & context) const
    {
        context.writeUnsignedLongLong(*val);
    }
};

struct FloatValueDescription
    : public ValueDescriptionI<float, ValueKind::FLOAT, FloatValueDescription> {

    virtual void parseJsonTyped(float * val,
                                JsonParsingContext & context) const
    {
        *val = context.expectFloat();
    }

    virtual void parseJson(void * val,
                           JsonParsingContext & context) const
    {
        *(float *)val = context.expectFloat();
    }

    virtual void printJsonTyped(const float * val,
                                JsonPrintingContext & context) const
    {
        context.writeFloat(*val);
    }
};

struct DoubleValueDescription
    : public ValueDescriptionI<double, ValueKind::FLOAT, DoubleValueDescription> {

    virtual void parseJsonTyped(double * val,
                                JsonParsingContext & context) const
    {
        *val = context.expectDouble();
    }

    virtual void printJsonTyped(const double * val,
                                JsonPrintingContext & context) const
    {
        context.writeDouble(*val);
    }
};

struct JsonValueDescription
    : public ValueDescriptionI<Json::Value, ValueKind::ANY, JsonValueDescription> {

    virtual void parseJsonTyped(Json::Value * val,
                                JsonParsingContext & context) const
    {
        *val = context.expectJson();
    }

    virtual void printJsonTyped(const Json::Value * val,
                                JsonPrintingContext & context) const
    {
        context.writeJson(*val);
    }

    virtual bool isDefaultTyped(const Json::Value * val) const
    {
        return val->isNull();
    }
};

struct BoolDescription
    : public ValueDescriptionI<bool, ValueKind::BOOLEAN, BoolDescription> {

    virtual void parseJsonTyped(bool * val,
                                JsonParsingContext & context) const
    {
        *val = context.expectBool();
    }

    virtual void printJsonTyped(const bool * val,
                                JsonPrintingContext & context) const
    {
        context.writeBool(*val);
    }

    virtual bool isDefaultTyped(const bool * val) const
    {
        return false;
    }
};

DEFINE_VALUE_DESCRIPTION(std::string, StringDescription);
DEFINE_VALUE_DESCRIPTION(Utf8String, Utf8StringDescription);
DEFINE_VALUE_DESCRIPTION(Utf32String, Utf32StringDescription);
DEFINE_VALUE_DESCRIPTION(signed int, SignedIntDescription);
DEFINE_VALUE_DESCRIPTION(unsigned int, UnsignedIntDescription);
DEFINE_VALUE_DESCRIPTION(signed long, SignedLongDescription);
DEFINE_VALUE_DESCRIPTION(unsigned long, UnsignedLongDescription);
DEFINE_VALUE_DESCRIPTION(signed long long, SignedLongLongDescription);
DEFINE_VALUE_DESCRIPTION(unsigned long long, UnsignedLongLongDescription);
DEFINE_VALUE_DESCRIPTION(float, FloatValueDescription);
DEFINE_VALUE_DESCRIPTION(double, DoubleValueDescription);
DEFINE_VALUE_DESCRIPTION(Json::Value, JsonValueDescription);
DEFINE_VALUE_DESCRIPTION(bool, BoolDescription);


} // namespace MLDB

