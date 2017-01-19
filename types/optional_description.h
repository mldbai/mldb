// This file is part of MLDB. Copyright 2015 mldb.ai inc. All rights reserved.

/** optional.h                                                     -*- C++ -*-
    Jeremy Barnes, 19 August 2015
    Optional type.
*/

#include "optional.h"
#include "value_description.h"
#include <memory>

#pragma once 

namespace MLDB {


template<typename T>
struct OptionalDescription
    : public ValueDescriptionI<Optional<T>, ValueKind::OPTIONAL, OptionalDescription<T> > {

    OptionalDescription(ValueDescriptionT<T> * inner)
        : inner(inner)
    {
    }

    OptionalDescription(std::shared_ptr<const ValueDescriptionT<T> > inner
                       = getDefaultDescriptionShared((T *)0))
        : inner(inner)
    {
    }

    OptionalDescription(ConstructOnly)
    {
    }
   
    std::shared_ptr<const ValueDescriptionT<T> > inner;

    virtual void parseJsonTyped(Optional<T> * val,
                                JsonParsingContext & context) const override
    {
        if (context.isNull()) {
            context.expectNull();
            val->reset();
            return;
        }
        val->reset(new T());
        inner->parseJsonTyped(val->get(), context);
    }

    virtual void printJsonTyped(const Optional<T> * val,
                                JsonPrintingContext & context) const override
    {
        if (!val->get())
            context.skip();
        else inner->printJsonTyped(val->get(), context);
    }

    virtual bool isDefaultTyped(const Optional<T> * val) const override
    {
        return !val->get();
    }

    virtual void * optionalMakeValueTyped(Optional<T> * val) const override
    {
        if (!val->get())
            val->reset(new T());
        return val->get();
    }

    virtual const void *
    optionalGetValueTyped(const Optional<T> * val) const override
    {
        if (!val->get())
            throw MLDB::Exception("no value in optional field");
        return val->get();
    }

    virtual const ValueDescription & contained() const override
    {
        return *inner;
    }

    virtual void initialize() override
    {
        this->inner = getDefaultDescriptionSharedT<T>();
    }
};

DECLARE_TEMPLATE_VALUE_DESCRIPTION_1(OptionalDescription, Optional, typename, T);

} // namespace MLDB
