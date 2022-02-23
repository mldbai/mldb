/** span_description.h                                        -*- C++ -*-
    Jeremy Barnes, 21 August 2015
    Copyright (c) 2015 mldb.ai inc.  All rights reserved.

    This file is part of MLDB. Copyright 2015 mldb.ai inc. All rights reserved.

    Value description implementations for spans.
*/

#pragma once

#include <span>
#include "list_description_base.h"
#include "value_description.h"
#include <type_traits>

namespace MLDB {


/*****************************************************************************/
/* DEFAULT DESCRIPTION FOR SPAN                                              */
/*****************************************************************************/

template<typename T, size_t Sz>
struct SpanDescription
    : public ValueDescriptionI<std::span<T, Sz>, ValueKind::ARRAY, SpanDescription<T, Sz> > {

    using InnerT = std::remove_const_t<T>;
    std::shared_ptr<const ValueDescriptionT<InnerT> > inner;

    SpanDescription(ValueDescriptionT<InnerT> * inner)
        : inner(inner)
    {
    }

    SpanDescription(std::shared_ptr<const ValueDescriptionT<InnerT> > inner
                       = getDefaultDescriptionShared((InnerT *)0))
        : inner(std::move(inner))
    {
    }

    // Constructor to create a partially-evaluated span description.
    SpanDescription(ConstructOnly)
    {
    }

    virtual void parseJson(void * val, JsonParsingContext & context) const override
    {
        throw MLDB::Exception("Can't parse Spans");
    }

    virtual void parseJsonTyped(std::span<T, Sz> * val, JsonParsingContext & context) const override
    {
        throw MLDB::Exception("Can't parse Spans");
    }

    virtual void printJson(const void * val, JsonPrintingContext & context) const override
    {
        const std::span<T, Sz> * val2 = reinterpret_cast<const std::span<T, Sz> *>(val);
        return printJsonTyped(val2, context);
    }

    virtual void printJsonTyped(const std::span<T, Sz> * val, JsonPrintingContext & context) const override
    {
        size_t sz = val->size();
        context.startArray(sz);

        auto it = val->begin();
        for (size_t i = 0;  i < sz;  ++i, ++it) {
            ExcAssert(it != val->end());
            context.newArrayElement();
            InnerT v(*it);
            inner->printJsonTyped(&v, context);
        }
        
        context.endArray();
    }

    virtual bool isDefault(const void * val) const override
    {
        const std::span<T, Sz> * val2 = reinterpret_cast<const std::span<T, Sz> *>(val);
        return isDefaultTyped(val2);
    }

    virtual bool isDefaultTyped(const std::span<T, Sz> * val) const override
    {
        return val->empty();
    }

    virtual size_t getArrayLength(void * val) const override
    {
        const std::span<T, Sz> * val2 = reinterpret_cast<const std::span<T, Sz> *>(val);
        return val2->size();
    }

    virtual LengthModel getArrayLengthModel() const override
    {
        return LengthModel::VARIABLE;
    }

    virtual OwnershipModel getArrayIndirectionModel() const override
    {
        return OwnershipModel::UNIQUE;
    }
    
    virtual void * getArrayElement(void * val, uint32_t element) const override
    {
        throw MLDB::Exception("Can't mutate Spans");
    }

    virtual const void * getArrayElement(const void * val, uint32_t element) const override
    {
        const std::span<T, Sz> * val2 = reinterpret_cast<const std::span<T, Sz> *>(val);
        ExcAssertLess(element, val2->size());
        return &val2[element];
    }

    virtual void setArrayLength(void * val, size_t newLength) const override
    {
        throw MLDB::Exception("Can't mutate Spans");
    }
    
    virtual const ValueDescription & contained() const override
    {
        return *this->inner;
    }

    virtual std::shared_ptr<const ValueDescription> containedPtr() const
    {
        return this->inner;
    }

    virtual void initialize() override
    {
        this->inner = getDefaultDescriptionSharedT<InnerT>();
    }
};

#if 0
template<typename T, size_t Sz>
struct SpanDescription<const T, Sz>
    : public ValueDescriptionI<std::span<const T, Sz>, ValueKind::ARRAY, SpanDescription<const T, Sz> > {

    using InnerT = std::remove_const_t<T>;
    std::shared_ptr<const ValueDescriptionT<InnerT> > inner;

    SpanDescription(ValueDescriptionT<InnerT> * inner)
        : inner(inner)
    {
    }

    SpanDescription(std::shared_ptr<const ValueDescriptionT<InnerT> > inner
                       = getDefaultDescriptionShared((InnerT *)0))
        : inner(std::move(inner))
    {
    }

    // Constructor to create a partially-evaluated span description.
    SpanDescription(ConstructOnly)
    {
    }

    virtual void parseJson(void * val, JsonParsingContext & context) const override
    {
        throw MLDB::Exception("Can't parse Spans");
    }

    virtual void parseJsonTyped(std::span<T, Sz> * val, JsonParsingContext & context) const override
    {
        throw MLDB::Exception("Can't parse Spans");
    }

    virtual void printJson(const void * val, JsonPrintingContext & context) const override
    {
        const std::span<T, Sz> * val2 = reinterpret_cast<const std::span<T, Sz> *>(val);
        return printJsonTyped(val2, context);
    }

    virtual void printJsonTyped(const std::span<T, Sz> * val, JsonPrintingContext & context) const override
    {
        size_t sz = val->size();
        context.startArray(sz);

        auto it = val->begin();
        for (size_t i = 0;  i < sz;  ++i, ++it) {
            ExcAssert(it != val->end());
            context.newArrayElement();
            InnerT v(*it);
            inner->printJsonTyped(&v, context);
        }
        
        context.endArray();
    }

    virtual bool isDefault(const void * val) const override
    {
        const std::span<T, Sz> * val2 = reinterpret_cast<const std::span<T, Sz> *>(val);
        return isDefaultTyped(val2);
    }

    virtual bool isDefaultTyped(const std::span<T, Sz> * val) const override
    {
        return val->empty();
    }

    virtual size_t getArrayLength(void * val) const override
    {
        const std::span<T, Sz> * val2 = reinterpret_cast<const std::span<T, Sz> *>(val);
        return val2->size();
    }

    virtual void * getArrayElement(void * val, uint32_t element) const override
    {
        throw MLDB::Exception("Can't mutate Spans");
    }

    virtual const void * getArrayElement(const void * val, uint32_t element) const override
    {
        const std::span<T, Sz> * val2 = reinterpret_cast<const std::span<T, Sz> *>(val);
        return &val2->at(element);
    }

    virtual void setArrayLength(void * val, size_t newLength) const override
    {
        throw MLDB::Exception("Can't mutate Spans");
    }
    
    virtual const ValueDescription & contained() const override
    {
        return *this->inner;
    }

    virtual std::shared_ptr<const ValueDescription> containedPtr() const
    {
        return this->inner;
    }

    virtual void initialize() override
    {
        this->inner = getDefaultDescriptionSharedT<T>();
    }
};
#endif

DECLARE_TEMPLATE_VALUE_DESCRIPTION_2(SpanDescription, std::span, typename, T, size_t, Sz, MLDB::has_default_description<T>::value);

} // namespace MLDB
