/** compact_vector_value_description.h                             -*- C++ -*-
    ???, ??? 2015
    Value descriptions for compact vectors.

    This file is part of MLDB. Copyright 2015 Datacratic. All rights reserved.
*/

#include "value_description.h"
#include "mldb/jml/utils/compact_vector.h"

#pragma once

namespace Datacratic {

template<typename T, size_t I, typename S, bool Sf, typename P, typename A>
struct CompactVectorDescription
    : public ValueDescriptionI<ML::compact_vector<T, I, S, Sf, P, A>, ValueKind::ARRAY,
                               CompactVectorDescription<T, I, S, Sf, P, A> >,
      public ListDescriptionBase<T> {

    CompactVectorDescription(ValueDescriptionT<T> * inner
                             = getDefaultDescription((T *)0))
        : ListDescriptionBase<T>(inner)
    {
    }

    CompactVectorDescription(ConstructOnly)
    {
    }

    virtual void parseJson(void * val, JsonParsingContext & context) const
    {
        ML::compact_vector<T, I, S, Sf, P, A> * val2 = reinterpret_cast<ML::compact_vector<T, I, S, Sf, P, A> *>(val);
        return parseJsonTyped(val2, context);
    }

    virtual void parseJsonTyped(ML::compact_vector<T, I, S, Sf, P, A> * val, JsonParsingContext & context) const
    {
        this->parseJsonTypedList(val, context);
    }

    virtual void printJson(const void * val, JsonPrintingContext & context) const
    {
        const ML::compact_vector<T, I, S, Sf, P, A> * val2 = reinterpret_cast<const ML::compact_vector<T, I, S, Sf, P, A> *>(val);
        return printJsonTyped(val2, context);
    }

    virtual void printJsonTyped(const ML::compact_vector<T, I, S, Sf, P, A> * val, JsonPrintingContext & context) const
    {
        this->printJsonTypedList(val, context);
    }

    virtual bool isDefault(const void * val) const
    {
        const ML::compact_vector<T, I, S, Sf, P, A> * val2 = reinterpret_cast<const ML::compact_vector<T, I, S, Sf, P, A> *>(val);
        return isDefaultTyped(val2);
    }

    virtual bool isDefaultTyped(const ML::compact_vector<T, I, S, Sf, P, A> * val) const
    {
        return val->empty();
    }

    virtual size_t getArrayLength(void * val) const
    {
        const ML::compact_vector<T, I, S, Sf, P, A> * val2 = reinterpret_cast<const ML::compact_vector<T, I, S, Sf, P, A> *>(val);
        return val2->size();
    }

    virtual void * getArrayElement(void * val, uint32_t element) const
    {
        ML::compact_vector<T, I, S, Sf, P, A> * val2 = reinterpret_cast<ML::compact_vector<T, I, S, Sf, P, A> *>(val);
        return &val2->at(element);
    }

    virtual const void * getArrayElement(const void * val, uint32_t element) const
    {
        const ML::compact_vector<T, I, S, Sf, P, A> * val2 = reinterpret_cast<const ML::compact_vector<T, I, S, Sf, P, A> *>(val);
        return &val2->at(element);
    }

    virtual void setArrayLength(void * val, size_t newLength) const
    {
        ML::compact_vector<T, I, S, Sf, P, A> * val2 = reinterpret_cast<ML::compact_vector<T, I, S, Sf, P, A> *>(val);
        val2->resize(newLength);
    }
    
    virtual const ValueDescription & contained() const
    {
        return *this->inner;
    }

    virtual void initialize() JML_OVERRIDE
    {
        this->inner = getDefaultDescriptionSharedT<T>();
    }
};

DECLARE_TEMPLATE_VALUE_DESCRIPTION_6(CompactVectorDescription, ML::compact_vector,
                                     typename, T, size_t, I, typename, S,
                                     bool, Sf, typename, P, typename, A);



} // namespace Datacratic
