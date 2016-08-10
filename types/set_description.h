// This file is part of MLDB. Copyright 2015 Datacratic. All rights reserved.

/** set_description.h                                        -*- C++ -*-
    Jeremy Barnes, 21 August 2015
    Copyright (c) 2015 Datacratic Inc.  All rights reserved.

    Base class for a std::set
*/

#pragma once

#include "list_description_base.h"

namespace Datacratic {

/*****************************************************************************/
/* SET DESCRIPTION                                                           */
/*****************************************************************************/

template<typename T>
struct SetDescription
    : public ValueDescriptionI<std::set<T>, ValueKind::ARRAY, SetDescription<T> >,
      public ListDescriptionBase<T> {

    SetDescription(ValueDescriptionT<T> * inner)
        : ListDescriptionBase<T>(inner)
    {
    }

    SetDescription(std::shared_ptr<const ValueDescriptionT<T> > inner
                       = getDefaultDescriptionShared((T *)0))
        : ListDescriptionBase<T>(inner)
    {
    }

    // Constructor to create a partially-evaluated vector description.
    SetDescription(ConstructOnly)
        : ListDescriptionBase<T>(constructOnly)
    {
    }

    void parseJson(void * val, JsonParsingContext & context) const override
    {
        std::set<T> * val2 = reinterpret_cast<std::set<T> *>(val);
        return parseJsonTyped(val2, context);
    }

    void parseJsonTyped(std::set<T> * val, JsonParsingContext & context) const override
    {
        this->parseJsonTypedSet(val, context);
    }

    void printJson(const void * val, JsonPrintingContext & context) const override
    {
        const std::set<T> * val2 = reinterpret_cast<const std::set<T> *>(val);
        return printJsonTyped(val2, context);
    }

    void printJsonTyped(const std::set<T> * val, JsonPrintingContext & context) const override
    {
        this->printJsonTypedList(val, context);
    }

    bool isDefault(const void * val) const override
    {
        const std::set<T> * val2 = reinterpret_cast<const std::set<T> *>(val);
        return isDefaultTyped(val2);
    }

    bool isDefaultTyped(const std::set<T> * val) const override
    {
        return val->empty();
    }

    size_t getArrayLength(void * val) const override
    {
        const std::set<T> * val2 = reinterpret_cast<const std::set<T> *>(val);
        return val2->size();
    }

    void * getArrayElement(void * val, uint32_t element) const override
    {
        throw ML::Exception("can't mutate set elements");
    }

    const void * getArrayElement(const void * val, uint32_t element) const override
    {
        const std::set<T> * val2 = reinterpret_cast<const std::set<T> *>(val);
        if (element >= val2->size())
            throw ML::Exception("Invalid set element number");
        auto it = val2->begin();
        for (unsigned i = 0;  i < element;  ++i, ++i) ;
        return &*it;
    }

    void setArrayLength(void * val, size_t newLength) const override
    {
        throw ML::Exception("cannot adjust length of a set");
    }
    
    const ValueDescription & contained() const override
    {
        return *this->inner;
    }

    void initialize() override
    {
        this->inner = getDefaultDescriptionSharedT<T>();
    }
};


DECLARE_TEMPLATE_VALUE_DESCRIPTION_1(SetDescription, std::set, typename, T);

} // namespace Datacratic

