// This file is part of MLDB. Copyright 2015 mldb.ai inc. All rights reserved.

/** basic_value_descriptions.cc

    Jeremy Barnes, 19 August 2015
*/

#include "basic_value_descriptions.h"

namespace MLDB {

template<typename Type, ValueKind Kind, class Description>
struct StrongComparableValueDescriptionI: public ValueDescriptionI<Type, Kind, Description> {
    static const Type & getValue(const void * val) { return *(const Type *)val; }

    virtual bool hasEqualityComparison() const override { return true; }
    virtual bool compareEquality(const void * val1, const void * val2) const override
    { 
        return getValue(val1) == getValue(val2);
    }
    virtual bool hasLessThanComparison() const override { return true; }
    virtual bool compareLessThan(const void * val1, const void * val2) const
    {
        return getValue(val1) < getValue(val2);
    }
    virtual bool hasStrongOrderingComparison() const
    {
        return true;
    }
    virtual std::strong_ordering compareStrong(const void * val1, const void * val2) const
    {
        return getValue(val1) <=> getValue(val2);
    }

    virtual bool hasWeakOrderingComparison() const { return true; }
    virtual std::weak_ordering compareWeak(const void * val1, const void * val2) const
    {
        return getValue(val1) <=> getValue(val2);
    }

    virtual bool hasPartialOrderingComparison() const { return true; }
    virtual std::partial_ordering comparePartial(const void * val1, const void * val2) const
    {
        return getValue(val1) <=> getValue(val2);
    }
};

template<typename Type, ValueKind Kind, class Description>
struct WeakComparableValueDescriptionI: public ValueDescriptionI<Type, Kind, Description> {
    static const Type & getValue(const void * val) { return *(const Type *)val; }

    virtual bool hasEqualityComparison() const override { return true; }
    virtual bool compareEquality(const void * val1, const void * val2) const override
    { 
        return getValue(val1) == getValue(val2);
    }
    virtual bool hasLessThanComparison() const override { return true; }
    virtual bool compareLessThan(const void * val1, const void * val2) const
    {
        return getValue(val1) < getValue(val2);
    }

    virtual bool hasWeakOrderingComparison() const { return true; }
    virtual std::weak_ordering compareWeak(const void * val1, const void * val2) const
    {
        return getValue(val1) <=> getValue(val2);
    }

    virtual bool hasPartialOrderingComparison() const { return true; }
    virtual std::partial_ordering comparePartial(const void * val1, const void * val2) const
    {
        return getValue(val1) <=> getValue(val2);
    }
};

template<typename Type, ValueKind Kind, class Description>
struct PartialComparableValueDescriptionI: public ValueDescriptionI<Type, Kind, Description> {
    static const Type & getValue(const void * val) { return *(const Type *)val; }

    virtual bool hasEqualityComparison() const override { return true; }
    virtual bool compareEquality(const void * val1, const void * val2) const override
    { 
        return getValue(val1) == getValue(val2);
    }
    virtual bool hasLessThanComparison() const override { return true; }
    virtual bool compareLessThan(const void * val1, const void * val2) const
    {
        return getValue(val1) < getValue(val2);
    }

    virtual bool hasPartialOrderingComparison() const { return true; }
    virtual std::partial_ordering comparePartial(const void * val1, const void * val2) const
    {
        return getValue(val1) <=> getValue(val2);
    }
};

template<typename Type, ValueKind Kind, class Description>
struct OrderedComparableValueDescriptionI: public ValueDescriptionI<Type, Kind, Description> {
    static const Type & getValue(const void * val) { return *(const Type *)val; }

    virtual bool hasEqualityComparison() const override { return true; }
    virtual bool compareEquality(const void * val1, const void * val2) const override
    { 
        return getValue(val1) == getValue(val2);
    }
    virtual bool hasLessThanComparison() const override { return true; }
    virtual bool compareLessThan(const void * val1, const void * val2) const
    {
        return getValue(val1) < getValue(val2);
    }
};

template<typename Type, ValueKind Kind, class Description>
struct EqualityComparableValueDescriptionI: public ValueDescriptionI<Type, Kind, Description> {
    static const Type & getValue(const void * val) { return *(const Type *)val; }

    virtual bool hasEqualityComparison() const override { return true; }
    virtual bool compareEquality(const void * val1, const void * val2) const override
    { 
        return getValue(val1) == getValue(val2);
    }
};

struct StringDescription
    : public OrderedComparableValueDescriptionI<std::string, ValueKind::STRING, StringDescription> {

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
    : public OrderedComparableValueDescriptionI<Utf8String, ValueKind::STRING, Utf8StringDescription> {

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
    : public OrderedComparableValueDescriptionI<Utf32String, ValueKind::STRING, Utf32StringDescription> {
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

struct CharDescription
    : public StrongComparableValueDescriptionI<char, ValueKind::INTEGER, CharDescription> {

    virtual void parseJsonTyped(char * val,
                                JsonParsingContext & context) const
    {
        *val = context.expectInt();
    }
    
    virtual void printJsonTyped(const char * val,
                                JsonPrintingContext & context) const
    {
        context.writeInt(*val);
    }
};

template class ValueDescriptionI<char, ValueKind::INTEGER, CharDescription>;

struct SignedCharDescription
    : public StrongComparableValueDescriptionI<signed char, ValueKind::INTEGER, SignedCharDescription> {

    virtual void parseJsonTyped(signed char * val,
                                JsonParsingContext & context) const
    {
        *val = context.expectInt();
    }
    
    virtual void printJsonTyped(const signed char * val,
                                JsonPrintingContext & context) const
    {
        context.writeInt(*val);
    }
};

template class ValueDescriptionI<signed char, ValueKind::INTEGER, SignedCharDescription>;

struct UnsignedCharDescription
    : public StrongComparableValueDescriptionI<unsigned char, ValueKind::INTEGER, UnsignedCharDescription> {

    virtual void parseJsonTyped(unsigned char * val,
                                JsonParsingContext & context) const
    {
        *val = context.expectInt();
    }
    
    virtual void printJsonTyped(const unsigned char * val,
                                JsonPrintingContext & context) const
    {
        context.writeInt(*val);
    }
};

struct SignedShortIntDescription
    : public StrongComparableValueDescriptionI<signed short int, ValueKind::INTEGER, SignedShortIntDescription> {

    virtual void parseJsonTyped(signed short int * val,
                                JsonParsingContext & context) const
    {
        *val = context.expectInt();
    }
    
    virtual void printJsonTyped(const signed short int * val,
                                JsonPrintingContext & context) const
    {
        context.writeInt(*val);
    }
};

template class ValueDescriptionI<signed short int, ValueKind::INTEGER, SignedShortIntDescription>;

struct UnsignedShortIntDescription
    : public StrongComparableValueDescriptionI<unsigned short int, ValueKind::INTEGER, UnsignedShortIntDescription> {

    virtual void parseJsonTyped(unsigned short int * val,
                                JsonParsingContext & context) const
    {
        *val = context.expectInt();
    }
    
    virtual void printJsonTyped(const unsigned short int * val,
                                JsonPrintingContext & context) const
    {
        context.writeInt(*val);
    }
};

template class ValueDescriptionI<unsigned short int, ValueKind::INTEGER, UnsignedShortIntDescription>;

struct SignedIntDescription
    : public StrongComparableValueDescriptionI<signed int, ValueKind::INTEGER, SignedIntDescription> {

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
    : public StrongComparableValueDescriptionI<unsigned int, ValueKind::INTEGER, UnsignedIntDescription> {

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
    : public StrongComparableValueDescriptionI<signed long, ValueKind::INTEGER, SignedLongDescription> {

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
    : public StrongComparableValueDescriptionI<unsigned long, ValueKind::INTEGER, UnsignedLongDescription> {

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
    : public StrongComparableValueDescriptionI<signed long long, ValueKind::INTEGER, SignedLongLongDescription> {

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
    : public StrongComparableValueDescriptionI<unsigned long long, ValueKind::INTEGER, UnsignedLongLongDescription> {

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

struct HalfValueDescription
    : public PartialComparableValueDescriptionI<half, ValueKind::FLOAT, HalfValueDescription> {

    virtual void parseJsonTyped(half * val,
                                JsonParsingContext & context) const
    {
        *val = context.expectFloat();
    }

    virtual void parseJson(void * val,
                           JsonParsingContext & context) const
    {
        *(half *)val = context.expectFloat();
    }

    virtual void printJsonTyped(const half * val,
                                JsonPrintingContext & context) const
    {
        context.writeFloat(*val);
    }
};

struct FloatValueDescription
    : public PartialComparableValueDescriptionI<float, ValueKind::FLOAT, FloatValueDescription> {

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
    : public PartialComparableValueDescriptionI<double, ValueKind::FLOAT, DoubleValueDescription> {

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
    : public EqualityComparableValueDescriptionI<Json::Value, ValueKind::ANY, JsonValueDescription> {

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
    : public StrongComparableValueDescriptionI<bool, ValueKind::BOOLEAN, BoolDescription> {

    virtual void parseJsonTyped(bool * val,
                                JsonParsingContext & context) const
    {
        *val = context.expectBool();
    }

    virtual void printJsonTyped(const bool * val,
                                JsonPrintingContext & context) const ATTRIBUTE_NO_SANITIZE_UNDEFINED
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
DEFINE_VALUE_DESCRIPTION(char, CharDescription);
DEFINE_VALUE_DESCRIPTION(signed char, SignedCharDescription);
DEFINE_VALUE_DESCRIPTION(unsigned char, UnsignedCharDescription);
DEFINE_VALUE_DESCRIPTION(signed short int, SignedShortIntDescription);
DEFINE_VALUE_DESCRIPTION(unsigned short int, UnsignedShortIntDescription);
DEFINE_VALUE_DESCRIPTION(signed int, SignedIntDescription);
DEFINE_VALUE_DESCRIPTION(unsigned int, UnsignedIntDescription);
DEFINE_VALUE_DESCRIPTION(signed long, SignedLongDescription);
DEFINE_VALUE_DESCRIPTION(unsigned long, UnsignedLongDescription);
DEFINE_VALUE_DESCRIPTION(signed long long, SignedLongLongDescription);
DEFINE_VALUE_DESCRIPTION(unsigned long long, UnsignedLongLongDescription);
DEFINE_VALUE_DESCRIPTION(half, HalfValueDescription);
DEFINE_VALUE_DESCRIPTION(float, FloatValueDescription);
DEFINE_VALUE_DESCRIPTION(double, DoubleValueDescription);
DEFINE_VALUE_DESCRIPTION(Json::Value, JsonValueDescription);
DEFINE_VALUE_DESCRIPTION(bool, BoolDescription);


} // namespace MLDB
