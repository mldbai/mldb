/** value_descriptions.h                                           -*- C++ -*-
    Jeremy Barnes, 5 January 2015
    Copyright (c) 2015 mldb.ai inc.  All rights reserved.
    This file is part of MLDB. Copyright 2015 mldb.ai inc. All rights reserved.

*/

#pragma once

#include "mldb/types/list_description_base.h"
#include "mldb/jml/stats/distribution.h"

namespace ML {

struct Classifier_Impl;
struct Feature_Space;

} // namespace ML

namespace MLDB {

template<typename T, typename Underlying>
struct DistributionValueDescription
    : public ValueDescriptionI<distribution<T, Underlying>, ValueKind::ARRAY,
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

    DistributionValueDescription(MLDB::ConstructOnly)
    {
    }

    virtual void
    parseJson(void * val, JsonParsingContext & context) const override
    {
        distribution<T, Underlying> * val2 = reinterpret_cast<distribution<T, Underlying> *>(val);
        return parseJsonTyped(val2, context);
    }

    virtual void
    parseJsonTyped(distribution<T, Underlying> * val,
                   JsonParsingContext & context) const override
    {
        this->parseJsonTypedList(val, context);
    }

    virtual void
    printJson(const void * val, JsonPrintingContext & context) const override
    {
        const distribution<T, Underlying> * val2 = reinterpret_cast<const distribution<T, Underlying> *>(val);
        return printJsonTyped(val2, context);
    }

    virtual void
    printJsonTyped(const distribution<T, Underlying> * val,
                   JsonPrintingContext & context) const override
    {
        this->printJsonTypedList(val, context);
    }

    virtual bool isDefault(const void * val) const override
    {
        const distribution<T, Underlying> * val2 = reinterpret_cast<const distribution<T, Underlying> *>(val);
        return isDefaultTyped(val2);
    }

    virtual bool
    isDefaultTyped(const distribution<T, Underlying> * val) const override
    {
        return val->empty();
    }

    virtual size_t getArrayLength(void * val) const override
    {
        const distribution<T, Underlying> * val2 = reinterpret_cast<const distribution<T, Underlying> *>(val);
        return val2->size();
    }

    virtual void * getArrayElement(void * val, uint32_t element) const override
    {
        distribution<T, Underlying> * val2 = reinterpret_cast<distribution<T, Underlying> *>(val);
        return &val2->at(element);
    }

    virtual const
    void * getArrayElement(const void * val, uint32_t element) const override
    {
        const distribution<T, Underlying> * val2 = reinterpret_cast<const distribution<T, Underlying> *>(val);
        return &val2->at(element);
    }

    virtual void setArrayLength(void * val, size_t newLength) const override
    {
        distribution<T, Underlying> * val2 = reinterpret_cast<distribution<T, Underlying> *>(val);
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

DECLARE_TEMPLATE_VALUE_DESCRIPTION_2(DistributionValueDescription, distribution, typename, T, class, Underlying);

MLDB::ValueDescriptionT<distribution<float> > *
getDefaultDescription(const distribution<float> * = 0);

MLDB::ValueDescriptionT<distribution<float> > *
getDefaultDescriptionUninitialized(const distribution<float> * = 0);

MLDB::ValueDescriptionT<distribution<double> > *
getDefaultDescription(const distribution<double> * = 0);

MLDB::ValueDescriptionT<distribution<double> > *
getDefaultDescriptionUninitialized(const distribution<double> * = 0);

extern template class DistributionValueDescription<float, std::vector<float> >;
extern template class DistributionValueDescription<double, std::vector<double> >;

} // namespace MLDB
