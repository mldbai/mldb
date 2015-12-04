// This file is part of MLDB. Copyright 2015 Datacratic. All rights reserved.

/** value_descriptions.h                                           -*- C++ -*-
    Jeremy Barnes, 5 January 2015
    Copyright (c) 2015 Datacratic Inc.  All rights reserved.

*/

#pragma once

#include "mldb/types/list_description_base.h"
#include "mldb/jml/stats/distribution.h"

namespace ML {

struct Classifier_Impl;
struct Feature_Space;

} // namespace ML

namespace Datacratic {

template<typename T, typename Underlying>
struct DistributionValueDescription
    : public ValueDescriptionI<ML::distribution<T, Underlying>, ValueKind::ARRAY,
                               DistributionValueDescription<T, Underlying> >,
      public ListDescriptionBase<T> {

    DistributionValueDescription(ValueDescriptionT<T> * inner)
        : ListDescriptionBase<T>(inner)
    {
    }

    DistributionValueDescription(std::shared_ptr<const ValueDescriptionT<T> > inner
                       = getDefaultDescriptionShared((T *)0))
        : ListDescriptionBase<T>(inner)
    {
    }

    DistributionValueDescription(Datacratic::ConstructOnly)
    {
    }

    virtual void parseJson(void * val, JsonParsingContext & context) const
    {
        ML::distribution<T, Underlying> * val2 = reinterpret_cast<ML::distribution<T, Underlying> *>(val);
        return parseJsonTyped(val2, context);
    }

    virtual void parseJsonTyped(ML::distribution<T, Underlying> * val, JsonParsingContext & context) const
    {
        this->parseJsonTypedList(val, context);
    }

    virtual void printJson(const void * val, JsonPrintingContext & context) const
    {
        const ML::distribution<T, Underlying> * val2 = reinterpret_cast<const ML::distribution<T, Underlying> *>(val);
        return printJsonTyped(val2, context);
    }

    virtual void printJsonTyped(const ML::distribution<T, Underlying> * val, JsonPrintingContext & context) const
    {
        this->printJsonTypedList(val, context);
    }

    virtual bool isDefault(const void * val) const
    {
        const ML::distribution<T, Underlying> * val2 = reinterpret_cast<const ML::distribution<T, Underlying> *>(val);
        return isDefaultTyped(val2);
    }

    virtual bool isDefaultTyped(const ML::distribution<T, Underlying> * val) const
    {
        return val->empty();
    }

    virtual size_t getArrayLength(void * val) const
    {
        const ML::distribution<T, Underlying> * val2 = reinterpret_cast<const ML::distribution<T, Underlying> *>(val);
        return val2->size();
    }

    virtual void * getArrayElement(void * val, uint32_t element) const
    {
        ML::distribution<T, Underlying> * val2 = reinterpret_cast<ML::distribution<T, Underlying> *>(val);
        return &val2->at(element);
    }

    virtual const void * getArrayElement(const void * val, uint32_t element) const
    {
        const ML::distribution<T, Underlying> * val2 = reinterpret_cast<const ML::distribution<T, Underlying> *>(val);
        return &val2->at(element);
    }

    virtual void setArrayLength(void * val, size_t newLength) const
    {
        ML::distribution<T, Underlying> * val2 = reinterpret_cast<ML::distribution<T, Underlying> *>(val);
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

DECLARE_TEMPLATE_VALUE_DESCRIPTION_2(DistributionValueDescription, ML::distribution, typename, T, class, Underlying);

Datacratic::ValueDescriptionT<ML::distribution<float> > *
getDefaultDescription(const ML::distribution<float> * = 0);

Datacratic::ValueDescriptionT<ML::distribution<float> > *
getDefaultDescriptionUninitialized(const ML::distribution<float> * = 0);

Datacratic::ValueDescriptionT<ML::distribution<double> > *
getDefaultDescription(const ML::distribution<double> * = 0);

Datacratic::ValueDescriptionT<ML::distribution<double> > *
getDefaultDescriptionUninitialized(const ML::distribution<double> * = 0);

extern template class DistributionValueDescription<float, std::vector<float> >;
extern template class DistributionValueDescription<double, std::vector<double> >;

} // namespace Datacratic
