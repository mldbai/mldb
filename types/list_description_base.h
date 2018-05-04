/** list_description_base.h                                        -*- C++ -*-
    Jeremy Barnes, 21 August 2015
    Copyright (c) 2015 mldb.ai inc.  All rights reserved.

    This file is part of MLDB. Copyright 2015 mldb.ai inc. All rights reserved.

    Base class for a list (array, list, vector, ...).
*/

#pragma once

#include "value_description.h"

namespace MLDB {

template<typename T>
void clearList(T & list)
{
    list.clear();
}


/*****************************************************************************/
/* LIST DESCRIPTION                                                          */
/*****************************************************************************/

template<typename T>
struct ListDescriptionBase {

    ListDescriptionBase(ValueDescriptionT<T> * inner)
        : inner(inner)
    {
    }

    ListDescriptionBase(std::shared_ptr<const ValueDescriptionT<T> > inner
                        = getDefaultDescriptionShared((T *)0))
        : inner(inner)
    {
    }

    ListDescriptionBase(ConstructOnly)
    {
    }

    std::shared_ptr<const ValueDescriptionT<T> > inner;

    template<typename List>
    void parseJsonTypedList(List * val, JsonParsingContext & context) const
    {
        clearList(*val);

        if (!context.isArray())
            context.exception("expected array of " + inner->typeName);
        
        auto onElement = [&] ()
            {
                T el;
                inner->parseJsonTyped(&el, context);
                val->emplace_back(std::move(el));
            };
        
        context.forEachElement(onElement);
    }

    template<typename List>
    void parseJsonTypedSet(List * val, JsonParsingContext & context) const
    {
        val->clear();

        if (!context.isArray())
            context.exception("expected array of " + inner->typeName);
        
        auto onElement = [&] ()
            {
                T el;
                inner->parseJsonTyped(&el, context);
                val->insert(std::move(el));
            };
        
        context.forEachElement(onElement);
    }

    template<typename List>
    void printJsonTypedList(const List * val, JsonPrintingContext & context) const
    {
        size_t sz = val->size();
        context.startArray(sz);

        auto it = val->begin();
        for (size_t i = 0;  i < sz;  ++i, ++it) {
            ExcAssert(it != val->end());
            context.newArrayElement();
            T v(*it);
            inner->printJsonTyped(&v, context);
        }
        
        context.endArray();
    }
};

} // namespace MLDB
