/** compact_vector_value_description.h                             -*- C++ -*-
    ???, ??? 2015
    Value descriptions for compact vectors.

    This file is part of MLDB. Copyright 2015 mldb.ai inc. All rights reserved.
*/

#include "value_description.h"
#include "mldb/utils/compact_vector.h"

#pragma once

namespace MLDB {

template<typename T, size_t I, typename S, bool Sf, typename P, typename A>
struct CompactVectorDescription
    : public ValueDescriptionI<compact_vector<T, I, S, Sf, P, A>, ValueKind::ARRAY,
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

    virtual void parseJson(void * val,
                           JsonParsingContext & context) const override
    {
        compact_vector<T, I, S, Sf, P, A> * val2 = reinterpret_cast<compact_vector<T, I, S, Sf, P, A> *>(val);
        return parseJsonTyped(val2, context);
    }

    virtual void parseJsonTyped(compact_vector<T, I, S, Sf, P, A> * val,
                                JsonParsingContext & context) const override
    {
        this->parseJsonTypedList(val, context);
    }

    virtual void printJson(const void * val,
                           JsonPrintingContext & context) const override
    {
        const compact_vector<T, I, S, Sf, P, A> * val2 = reinterpret_cast<const compact_vector<T, I, S, Sf, P, A> *>(val);
        return printJsonTyped(val2, context);
    }

    virtual void printJsonTyped(const compact_vector<T, I, S, Sf, P, A> * val,
                                JsonPrintingContext & context) const override
    {
        this->printJsonTypedList(val, context);
    }

    virtual bool isDefault(const void * val) const override
    {
        const compact_vector<T, I, S, Sf, P, A> * val2 = reinterpret_cast<const compact_vector<T, I, S, Sf, P, A> *>(val);
        return isDefaultTyped(val2);
    }

    virtual bool
    isDefaultTyped(const compact_vector<T, I, S, Sf, P, A> * val) const override
    {
        return val->empty();
    }

    virtual size_t getArrayLength(void * val) const override
    {
        const compact_vector<T, I, S, Sf, P, A> * val2 = reinterpret_cast<const compact_vector<T, I, S, Sf, P, A> *>(val);
        return val2->size();
    }

    virtual void * getArrayElement(void * val, uint32_t element) const override
    {
        compact_vector<T, I, S, Sf, P, A> * val2 = reinterpret_cast<compact_vector<T, I, S, Sf, P, A> *>(val);
        return &val2->at(element);
    }

    virtual const void * getArrayElement(const void * val,
                                         uint32_t element) const override
    {
        const compact_vector<T, I, S, Sf, P, A> * val2 = reinterpret_cast<const compact_vector<T, I, S, Sf, P, A> *>(val);
        return &val2->at(element);
    }

    virtual void setArrayLength(void * val, size_t newLength) const override
    {
        compact_vector<T, I, S, Sf, P, A> * val2 = reinterpret_cast<compact_vector<T, I, S, Sf, P, A> *>(val);
        val2->resize(newLength);
    }
    
    virtual const ValueDescription & contained() const override
    {
        return *this->inner;
    }

    virtual void initialize() override
    {
        this->inner = getDefaultDescriptionSharedT<T>();
    }
};

DECLARE_TEMPLATE_VALUE_DESCRIPTION_6(CompactVectorDescription, compact_vector,
                                     typename, T, size_t, I, typename, S,
                                     bool, Sf, typename, P, typename, A);



} // namespace MLDB
