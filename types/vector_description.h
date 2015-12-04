// This file is part of MLDB. Copyright 2015 Datacratic. All rights reserved.

/** vector_description.h                                        -*- C++ -*-
    Jeremy Barnes, 21 August 2015
    Copyright (c) 2015 Datacratic Inc.  All rights reserved.

    Value description implementations for vectors.
*/

#pragma once

#include <vector>
#include "list_description_base.h"
#include "value_description.h"

namespace Datacratic {


/*****************************************************************************/
/* DEFAULT DESCRIPTION FOR VECTOR                                            */
/*****************************************************************************/

template<typename T, typename A = std::allocator<T> >
struct VectorDescription
    : public ValueDescriptionI<std::vector<T, A>, ValueKind::ARRAY, VectorDescription<T, A> >,
      public ListDescriptionBase<T> {

    VectorDescription(ValueDescriptionT<T> * inner)
        : ListDescriptionBase<T>(inner)
    {
    }

    VectorDescription(std::shared_ptr<const ValueDescriptionT<T> > inner
                       = getDefaultDescriptionShared((T *)0))
        : ListDescriptionBase<T>(inner)
    {
    }

    // Constructor to create a partially-evaluated vector description.
    VectorDescription(ConstructOnly)
        : ListDescriptionBase<T>(constructOnly)
    {
    }

    virtual void parseJson(void * val, JsonParsingContext & context) const JML_OVERRIDE
    {
        std::vector<T, A> * val2 = reinterpret_cast<std::vector<T, A> *>(val);
        return parseJsonTyped(val2, context);
    }

    virtual void parseJsonTyped(std::vector<T, A> * val, JsonParsingContext & context) const JML_OVERRIDE
    {
        this->parseJsonTypedList(val, context);
    }

    virtual void printJson(const void * val, JsonPrintingContext & context) const JML_OVERRIDE
    {
        const std::vector<T, A> * val2 = reinterpret_cast<const std::vector<T, A> *>(val);
        return printJsonTyped(val2, context);
    }

    virtual void printJsonTyped(const std::vector<T, A> * val, JsonPrintingContext & context) const JML_OVERRIDE
    {
        this->printJsonTypedList(val, context);
    }

    virtual bool isDefault(const void * val) const JML_OVERRIDE
    {
        const std::vector<T, A> * val2 = reinterpret_cast<const std::vector<T, A> *>(val);
        return isDefaultTyped(val2);
    }

    virtual bool isDefaultTyped(const std::vector<T, A> * val) const JML_OVERRIDE
    {
        return val->empty();
    }

    virtual size_t getArrayLength(void * val) const JML_OVERRIDE
    {
        const std::vector<T, A> * val2 = reinterpret_cast<const std::vector<T, A> *>(val);
        return val2->size();
    }

    virtual void * getArrayElement(void * val, uint32_t element) const JML_OVERRIDE
    {
        std::vector<T, A> * val2 = reinterpret_cast<std::vector<T, A> *>(val);
        return &val2->at(element);
    }

    virtual const void * getArrayElement(const void * val, uint32_t element) const JML_OVERRIDE
    {
        const std::vector<T, A> * val2 = reinterpret_cast<const std::vector<T, A> *>(val);
        return &val2->at(element);
    }

    virtual void setArrayLength(void * val, size_t newLength) const JML_OVERRIDE
    {
        std::vector<T, A> * val2 = reinterpret_cast<std::vector<T, A> *>(val);
        val2->resize(newLength);
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


DECLARE_TEMPLATE_VALUE_DESCRIPTION_2(VectorDescription, std::vector, typename, T, typename, A);

} // namespace Datacratic
