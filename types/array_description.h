/** array_description.h                                        -*- C++ -*-
    Jeremy Barnes, 21 August 2015
    Copyright (c) 2015 Datacratic Inc.  All rights reserved.

    This file is part of MLDB. Copyright 2015 Datacratic. All rights reserved.

    Value description implementations for arrays.
*/

#pragma once

#include <array>
#include "value_description.h"

namespace Datacratic {


/*****************************************************************************/
/* DEFAULT DESCRIPTION FOR ARRAY                                            */
/*****************************************************************************/

template<typename T, size_t N>
struct ArrayDescription
    : public ValueDescriptionI<std::array<T, N>, ValueKind::ARRAY,
                               ArrayDescription<T, N> > {

    ArrayDescription(ValueDescriptionT<T> * inner)
        : inner(inner)
    {
    }

    ArrayDescription(std::shared_ptr<const ValueDescriptionT<T> > inner
                       = getDefaultDescriptionShared((T *)0))
        : inner(inner)
    {
    }

    // Constructor to create a partially-evaluated array description.
    ArrayDescription(ConstructOnly)
    {
    }

    std::shared_ptr<const ValueDescriptionT<T> > inner;

    virtual void parseJsonTyped(std::array<T, N> * val,
                                JsonParsingContext & context) const JML_OVERRIDE
    {
        if (!context.isArray())
            context.exception("expected array of " + inner->typeName);
        
        size_t i = 0;
        auto onElement = [&] ()
            {
                if (i >= N)
                    context.exception("too many elements in array");
                T el;
                inner->parseJsonTyped(&el, context);
                (*val)[i++] = std::move(el);
            };

        context.forEachElement(onElement);

        if (i != N) {
            context.exception("not enough elements in array; expecting "
                              + std::to_string(N) + " but got "
                              + std::to_string(i));
        }
    }

    virtual void printJson(const void * val,
                           JsonPrintingContext & context) const JML_OVERRIDE
    {
        const std::array<T, N> * val2 = reinterpret_cast<const std::array<T, N> *>(val);
        return printJsonTyped(val2, context);
    }

    virtual void printJsonTyped(const std::array<T, N> * val,
                                JsonPrintingContext & context) const JML_OVERRIDE
    {
        context.startArray(N);

        for (size_t i = 0;  i < N;  ++i) {
            context.newArrayElement();
            inner->printJsonTyped(&(*val)[i], context);
        }
        
        context.endArray();
    }

    virtual bool isDefault(const void * val) const JML_OVERRIDE
    {
        const std::array<T, N> * val2 = reinterpret_cast<const std::array<T, N> *>(val);
        return isDefaultTyped(val2);
    }

    virtual bool isDefaultTyped(const std::array<T, N> * val) const JML_OVERRIDE
    {
        return val->empty();
    }

    virtual size_t getArrayLength(void * val) const JML_OVERRIDE
    {
        const std::array<T, N> * val2 = reinterpret_cast<const std::array<T, N> *>(val);
        return val2->size();
    }

    virtual void *
    getArrayElement(void * val, uint32_t element) const JML_OVERRIDE
    {
        std::array<T, N> * val2 = reinterpret_cast<std::array<T, N> *>(val);
        return &val2->at(element);
    }

    virtual const void *
    getArrayElement(const void * val, uint32_t element) const JML_OVERRIDE
    {
        const std::array<T, N> * val2 = reinterpret_cast<const std::array<T, N> *>(val);
        return &val2->at(element);
    }

    virtual const ValueDescription & contained() const JML_OVERRIDE
    {
        return *this->inner;
    }

    virtual void initialize() JML_OVERRIDE
    {
        this->inner = getDefaultDescriptionSharedT<T>();
    }
};


DECLARE_TEMPLATE_VALUE_DESCRIPTION_2(ArrayDescription, std::array, typename, T, size_t, N);

} // namespace Datacratic
