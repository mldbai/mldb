// This file is part of MLDB. Copyright 2015 mldb.ai inc. All rights reserved.

/** basic_value_descriptions.cc

    Jeremy Barnes, 19 August 2015
*/


#include "basic_value_descriptions.h"
#include "comparable_value_descriptions.h"
#include <type_traits>
#include <iostream>

namespace MLDB {

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

template<typename T, typename Desc>
struct IntegralValueDescription: public StrongComparableValueDescriptionI<T, ValueKind::INTEGER, Desc> {

    static constexpr int Width = sizeof(T);
    static constexpr bool Signed = std::is_signed_v<T>;

    virtual void parseJsonTyped(T * val,
                                JsonParsingContext & context) const override
    {
        if constexpr (Signed) {
            if constexpr (Width <= sizeof(int))
                *val = context.expectInt();
            else if constexpr (Width <= sizeof(long))
                *val = context.expectLong();
            else
                *val = context.expectLongLong();
        }
        else {
            if constexpr (Width <= sizeof(int))
                *val = context.expectUnsignedInt();
            else if constexpr (Width <= sizeof(long))
                *val = context.expectUnsignedLong();
            else
                *val = context.expectUnsignedLongLong();
        }
    }
    
    virtual void printJsonTyped(const T * val,
                                JsonPrintingContext & context) const override
    {
        if constexpr (Signed) {
            if constexpr (Width <= sizeof(int))
                context.writeInt(*val);
            else if constexpr (Width <= sizeof(long))
                context.writeLong(*val);
            else
                context.writeLongLong(*val);
        }
        else {
            if constexpr (Width <= sizeof(int))
                context.writeUnsignedInt(*val);
            else if constexpr (Width <= sizeof(long))
                context.writeUnsignedLong(*val);
            else
                context.writeUnsignedLongLong(*val);
        }
    }

    virtual void extractBitField(const void * from, void * to, uint32_t bitOffset, uint32_t bitWidth) const override
    {
        const auto & fromV = *reinterpret_cast<const T *>(from);
        auto & toV = *reinterpret_cast<T *>(to);
        ExcAssertLess(bitWidth, Width * 8);
        ExcAssertLessEqual(bitOffset + bitWidth, Width * 8);
        toV = (fromV >> bitOffset) & ((T(1) << bitWidth)-1);
    }

    virtual void insertBitField(const void * from, void * to, uint32_t bitOffset, uint32_t bitWidth) const override
    {
        const auto & fromV = *reinterpret_cast<const T *>(from);
        auto & toV = *reinterpret_cast<T *>(to);
        ExcAssertLessEqual(bitWidth, Width * 8);
        ExcAssertLessEqual(bitOffset + bitWidth, Width * 8);
        auto fromMask = (T(1) << bitWidth) - 1;
        auto toMask = fromMask << bitOffset;
        ExcAssertEqual((fromV & fromMask), 0);
        toV = (toV & ~toMask) | (fromV << bitWidth);
    }
    
};

#define DEFINE_INTEGRAL_DESCRIPTION(Type, Desc)                                           \
struct Desc##Description : public IntegralValueDescription<Type, Desc##Description> {};   \
template class ValueDescriptionI<Type, ValueKind::INTEGER, Desc##Description>;            \
DEFINE_VALUE_DESCRIPTION(Type, Desc##Description)

DEFINE_INTEGRAL_DESCRIPTION(char, Char);
DEFINE_INTEGRAL_DESCRIPTION(unsigned char, UnsignedChar);
DEFINE_INTEGRAL_DESCRIPTION(signed char, SignedChar);
DEFINE_INTEGRAL_DESCRIPTION(unsigned short, UnsignedShort);
DEFINE_INTEGRAL_DESCRIPTION(signed short, SignedShort);
DEFINE_INTEGRAL_DESCRIPTION(unsigned int, UnsignedInt);
DEFINE_INTEGRAL_DESCRIPTION(signed int, SignedInt);
DEFINE_INTEGRAL_DESCRIPTION(unsigned long, UnsignedLong);
DEFINE_INTEGRAL_DESCRIPTION(signed long, SignedLong);
DEFINE_INTEGRAL_DESCRIPTION(unsigned long long, UnsignedLongLong);
DEFINE_INTEGRAL_DESCRIPTION(signed long long, SignedLongLong);


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
DEFINE_VALUE_DESCRIPTION(half, HalfValueDescription);
DEFINE_VALUE_DESCRIPTION(float, FloatValueDescription);
DEFINE_VALUE_DESCRIPTION(double, DoubleValueDescription);
DEFINE_VALUE_DESCRIPTION(Json::Value, JsonValueDescription);
DEFINE_VALUE_DESCRIPTION(bool, BoolDescription);


} // namespace MLDB

