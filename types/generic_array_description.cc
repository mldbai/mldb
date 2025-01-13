/** generic_array_description.cc                                       -*- C++ -*-
    Jeremy Barnes, 4 January 2015
    This file is part of MLDB. Copyright 2015 mldb.ai inc. All rights reserved.

    Description of generic arrays
*/

#include "generic_array_description.h"

namespace MLDB {

GenericFixedLengthArrayDescription::
GenericFixedLengthArrayDescription(size_t width,
                                size_t align,
                                const std::string & typeName,
                                std::shared_ptr<const ValueDescription> inner,
                                size_t fixedLength)
    : ValueDescription(ValueKind::ARRAY, nullptr, width, align, typeName),
    inner(inner), fixedLength(fixedLength)
{
    ExcAssert(inner);
    ExcAssertEqual(width, inner->width * fixedLength);
}

void
GenericFixedLengthArrayDescription::
parseJson(void * val,
                    JsonParsingContext & context) const
{
    if (!context.isArray())
        context.exception("expected array of " + inner->typeName);
    
    size_t i = 0;
    auto onElement = [&] ()
        {
            if (i >= fixedLength)
                context.exception("too many elements in array");
            inner->parseJson(getElement(val, i++), context);
        };

    context.forEachElement(onElement);

    if (i != fixedLength) {
        context.exception("not enough elements in array; expecting "
                        + std::to_string(fixedLength) + " but got "
                        + std::to_string(i));
    }
}

void
GenericFixedLengthArrayDescription::
printJson(const void * val,
                    JsonPrintingContext & context) const
{
    context.startArray(fixedLength);

    for (size_t i = 0;  i < fixedLength;  ++i) {
        context.newArrayElement();
        inner->printJson(getElement(val, i), context);
    }
    
    context.endArray();
}

bool
GenericFixedLengthArrayDescription::
isDefault(const void * val) const
{
    return false;  // for now
}

LengthModel
GenericFixedLengthArrayDescription::
getArrayLengthModel() const
{
    return LengthModel::FIXED;
}

OwnershipModel
GenericFixedLengthArrayDescription::
getArrayIndirectionModel() const
{
    return OwnershipModel::NONE;
}

size_t
GenericFixedLengthArrayDescription::
getArrayFixedLength() const
{
    return fixedLength;
}

size_t
GenericFixedLengthArrayDescription::
getArrayLength(void * val) const
{
    return fixedLength;
}

void *
GenericFixedLengthArrayDescription::
getArrayElement(void * val, uint32_t element) const
{
    return  getElement(val, element);
}

const void *
GenericFixedLengthArrayDescription::
getArrayElement(const void * val, uint32_t element) const
{
    return  getElement(val, element);
}

const ValueDescription &
GenericFixedLengthArrayDescription::
contained() const
{
    return *this->inner;
}

std::shared_ptr<const ValueDescription>
GenericFixedLengthArrayDescription::
containedPtr() const
{
    return this->inner;
}

void
GenericFixedLengthArrayDescription::
setDefault(void * val) const
{
    for (size_t i = 0;  i < fixedLength;  ++i) {
        inner->setDefault(getElement(val, i));
    }
}

void
GenericFixedLengthArrayDescription::
copyValue(const void * from, void * to) const
{
    for (size_t i = 0;  i < fixedLength;  ++i) {
        inner->copyValue(getElement(from, i), getElement(to, i));
    }
}

void
GenericFixedLengthArrayDescription::
moveValue(void * from, void * to) const
{
    for (size_t i = 0;  i < fixedLength;  ++i) {
        inner->moveValue(getElement(from, i), getElement(to, i));
    }
}

void
GenericFixedLengthArrayDescription::
swapValues(void * from, void * to) const
{
    for (size_t i = 0;  i < fixedLength;  ++i) {
        inner->swapValues(getElement(from, i), getElement(to, i));
    }
}

void
GenericFixedLengthArrayDescription::
initializeDefault(void * mem) const
{
    for (size_t i = 0;  i < fixedLength;  ++i) {
        inner->initializeDefault(getElement(mem, i));
    }
}

void
GenericFixedLengthArrayDescription::
initializeCopy(void * mem, const void * other) const
{
    for (size_t i = 0;  i < fixedLength;  ++i) {
        inner->initializeCopy(getElement(mem, i), getElement(other, i));
    }
}

void
GenericFixedLengthArrayDescription::
initializeMove(void * mem, void * other) const
{
    for (size_t i = 0;  i < fixedLength;  ++i) {
        inner->initializeMove(getElement(mem, i), getElement(other, i));
    }
}

void
GenericFixedLengthArrayDescription::
destruct(void * mem) const
{
    for (size_t i = 0;  i < fixedLength;  ++i) {
        inner->destruct(getElement(mem, i));
    }
}

} // namespace MLDB

