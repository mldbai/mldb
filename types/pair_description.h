/** pair_description.h                                          -*- C++ -*-
    Jeremy Barnes, 21 August 2015
    Copyright (c) 2015 mldb.ai inc.  All rights reserved.
    This file is part of MLDB. Copyright 2015 mldb.ai inc. All rights reserved.

    Value description for a pointer.
*/

#pragma once

#include "value_description.h"

namespace MLDB {

template<typename T, typename U>
struct PairDescription
    : public ValueDescriptionI<std::pair<T, U>, ValueKind::TUPLE,
                               PairDescription<T, U> > {

    PairDescription(ValueDescriptionT<T> * inner1,
                    ValueDescriptionT<U> * inner2)
        : inner1(inner1), inner2(inner2)
    {
    }

    PairDescription(std::shared_ptr<const ValueDescriptionT<T> > inner1
                       = getDefaultDescriptionShared((T *)0),
                       std::shared_ptr<const ValueDescriptionT<U> > inner2
                       = getDefaultDescriptionShared((U *)0))
        : inner1(inner1), inner2(inner2)
    {
    }

    PairDescription(ConstructOnly)
    {
    }

    std::shared_ptr<const ValueDescriptionT<T> > inner1;
    std::shared_ptr<const ValueDescriptionT<U> > inner2;

    virtual size_t getTupleLength() const override
    {
        return 2;
    }

    virtual std::vector<std::shared_ptr<const ValueDescription> >
    getTupleElementDescriptions() const override
    {
        return { inner1, inner2 };
    }

    virtual const ValueDescription &
    getArrayElementDescription(const void * val, uint32_t element) const override
    {
        if (element == 0)
            return *inner1;
        else if (element == 1)
            return *inner1;
        else throw MLDB::Exception("Invalid element number for pair type '"
                                 + this->typeName + "'");
    }

    virtual void parseJsonTyped(std::pair<T, U> * val,
                                JsonParsingContext & context) const override
    {
        int el = 0;
        auto onElement = [&] ()
            {
                if (el == 0)
                    inner1->parseJsonTyped(&val->first, context);
                else if (el == 1)
                    inner2->parseJsonTyped(&val->second, context);
                else context.exception("expected 2 element array");

                ++el;
            };

        context.forEachElement(onElement);

        if (el != 2)
            context.exception("expected 2 element array");
    }

    virtual void printJsonTyped(const std::pair<T, U> * val,
                                JsonPrintingContext & context) const override
    {
        context.startArray(2);
        context.newArrayElement();
        inner1->printJsonTyped(&val->first, context);
        context.newArrayElement();
        inner2->printJsonTyped(&val->second, context);
        context.endArray();
    }

    virtual bool isDefaultTyped(const std::pair<T, U> * val) const override
    {
        return inner1->isDefaultTyped(&val->first)
            && inner2->isDefaultTyped(&val->second);
    }

    virtual void initialize() override
    {
        this->inner1 = getDefaultDescriptionSharedT<T>();
        this->inner2 = getDefaultDescriptionSharedT<U>();
    }
};

DECLARE_TEMPLATE_VALUE_DESCRIPTION_2(PairDescription, std::pair, typename, T1, typename, T2);

} // namespace MLDB
