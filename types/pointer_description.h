// This file is part of MLDB. Copyright 2015 mldb.ai inc. All rights reserved.

/** pointer_description.h                                          -*- C++ -*-
    Jeremy Barnes, 21 August 2015
    Copyright (c) 2015 mldb.ai inc.  All rights reserved.

    Value description for a pointer.
*/

#pragma once

#include <memory>
#include "value_description.h"


namespace MLDB {


template<typename T>
struct PointerDescription
    : public ValueDescriptionI<T*, ValueKind::LINK, PointerDescription<T> > {

    PointerDescription(ValueDescriptionT<T> * inner)
        : inner(inner)
    {
    }

    PointerDescription(std::shared_ptr<const ValueDescriptionT<T> > inner
                       = getDefaultDescriptionShared((T *)0))
        : inner(inner)
    {
    }

    std::shared_ptr<const ValueDescriptionT<T> > inner;

    virtual void parseJsonTyped(T** val, JsonParsingContext & context) const override
    {
        *val = new T();
        inner->parseJsonTyped(*val, context);
    }

    virtual void printJsonTyped(T* const* val, JsonPrintingContext & context) const override
    {
        if (!*val)
            context.skip();
        else inner->printJsonTyped(*val, context);
    }

    virtual bool isDefaultTyped(T* const* val) const override
    {
        return !*val;
    }

    virtual const ValueDescription & contained() const override
    {
        return *this->inner;
    }


    virtual OwnershipModel getOwnershipModel() const override
    {
        return OwnershipModel::NONE;
    }

    static T*& cast(void* obj)
    {
        return *static_cast<T**>(obj);
    }

    virtual void* getLink(void* obj) const override
    {
        return cast(obj);
    }

    virtual void set(
            void* obj, void* value, const ValueDescription* valueDesc) const override
    {
        if (valueDesc->kind != ValueKind::LINK)
            throw MLDB::Exception("assignment of non-link type to link type");

        valueDesc->contained().checkChildOf(&contained());
        cast(obj) = static_cast<T*>(valueDesc->getLink(value));
    }

    virtual void initialize() override
    {
        this->inner = getDefaultDescriptionSharedT<T>();
    }
};

template<typename T>
ValueDescriptionT<T *> * getDefaultDescription(T ** ptr)
{
    return new PointerDescription<T>();
}

template<typename T>
ValueDescriptionT<T *> * getDefaultDescriptionUninitialized(T ** ptr)
{
    return new PointerDescription<T>();
}

template<typename T>
struct ValueDescriptionInit<T *> {
    static MLDB::ValueDescription * create()
    {
        return getDefaultDescriptionUninitialized((T**)0);
    }
};                                                              


template<typename T>
struct UniquePtrDescription
    : public ValueDescriptionI<std::unique_ptr<T>, ValueKind::LINK, UniquePtrDescription<T> > {

    UniquePtrDescription(ValueDescriptionT<T> * inner)
        : inner(inner)
    {
    }

    UniquePtrDescription(std::shared_ptr<const ValueDescriptionT<T> > inner
                       = getDefaultDescriptionShared((T *)0))
        : inner(inner)
    {
    }

    UniquePtrDescription(ConstructOnly)
    {
    }

    std::shared_ptr<const ValueDescriptionT<T> > inner;

    virtual void parseJsonTyped(std::unique_ptr<T> * val,
                                JsonParsingContext & context) const override
    {
        val->reset(new T());
        inner->parseJsonTyped(val->get(), context);
    }

    virtual void printJsonTyped(const std::unique_ptr<T> * val,
                                JsonPrintingContext & context) const override
    {
        if (!val->get())
            context.skip();
        else inner->printJsonTyped(val->get(), context);
    }

    virtual bool isDefaultTyped(const std::unique_ptr<T> * val) const override
    {
        return !val->get();
    }

    virtual const ValueDescription & contained() const override
    {
        return *this->inner;
    }

    virtual OwnershipModel getOwnershipModel() const override
    {
        return OwnershipModel::UNIQUE;
    }

    static std::unique_ptr<T>& cast(void* obj)
    {
        return *static_cast< std::unique_ptr<T>* >(obj);
    }

    virtual void* getLink(void* obj) const override
    {
        return cast(obj).get();
    }

    virtual void set(
            void* obj, void* value, const ValueDescription* valueDesc) const override
    {
        if (valueDesc->kind != ValueKind::LINK)
            throw MLDB::Exception("assignment of non-link type to link type");

        if (valueDesc->getOwnershipModel() != OwnershipModel::NONE)
            throw MLDB::Exception("unsafe link assignement");

        valueDesc->contained().checkChildOf(&contained());
        cast(obj).reset(static_cast<T*>(valueDesc->getLink(value)));
    }

    virtual void initialize() override
    {
        this->inner = getDefaultDescriptionSharedT<T>();
    }
};

DECLARE_TEMPLATE_VALUE_DESCRIPTION_1(UniquePtrDescription, std::unique_ptr, typename, T);


template<typename T>
struct SharedPtrDescription
    : public ValueDescriptionI<std::shared_ptr<T>, ValueKind::LINK, SharedPtrDescription<T> > {

    SharedPtrDescription(std::shared_ptr<const ValueDescriptionT<T> > inner
                       = getDefaultDescriptionShared((T *)0))
        : inner(inner)
    {
    }

    SharedPtrDescription(ValueDescriptionT<T> * inner)
        : inner(inner)
    {
    }

    SharedPtrDescription(ConstructOnly)
    {
    }

    std::shared_ptr<const ValueDescriptionT<T> > inner;

    virtual void parseJsonTyped(std::shared_ptr<T> * val,
                                JsonParsingContext & context) const override
    {
        if (context.isNull()) {
            val->reset();
            context.expectNull();
            return;
        }
        val->reset(new T());
        inner->parseJsonTyped(val->get(), context);
    }

    virtual void printJsonTyped(const std::shared_ptr<T> * val,
                                JsonPrintingContext & context) const override
    {
        if (!val->get())
            context.skip();
        else inner->printJsonTyped(val->get(), context);
    }

    virtual bool isDefaultTyped(const std::shared_ptr<T> * val) const override
    {
        return !val->get();
    }

    virtual const ValueDescription & contained() const override
    {
        return *this->inner;
    }

    virtual OwnershipModel getOwnershipModel() const override
    {
        return OwnershipModel::SHARED;
    }


    static std::shared_ptr<T>& cast(void* obj)
    {
        return *static_cast< std::shared_ptr<T>* >(obj);
    }

    virtual void* getLink(void* obj) const override
    {
        return cast(obj).get();
    }

    virtual void set(
            void* obj, void* value, const ValueDescription* valueDesc) const override
    {
        if (valueDesc->kind != ValueKind::LINK)
            throw MLDB::Exception("assignment of non-link type to link type");

        if (valueDesc->getOwnershipModel() == OwnershipModel::UNIQUE)
            throw MLDB::Exception("unsafe link assignement");

        valueDesc->contained().checkChildOf(&contained());

        // Casting is necessary to make sure the ref count is incremented.
        if (valueDesc->getOwnershipModel() == OwnershipModel::SHARED)
            cast(obj) = cast(value);
        else cast(obj).reset(static_cast<T*>(valueDesc->getLink(value)));
    }

    virtual void initialize() override
    {
        this->inner = getDefaultDescriptionSharedT<T>();
    }
};

DECLARE_TEMPLATE_VALUE_DESCRIPTION_1(SharedPtrDescription, std::shared_ptr, typename, T);

#if 0
template<typename T>
struct PointerValueDescription
    : public ValueDescriptionI<T *, ValueKind::LINK,
                               PointerValueDescription<T> > {

    PointerValueDescription()
    {
    }

    virtual void parseJson(void * val, JsonParsingContext & context) const
    {
        T * * val2 = reinterpret_cast<T * *>(val);
        return parseJsonTyped(val2, context);
    }

    virtual void parseJsonTyped(T * * val, JsonParsingContext & context) const
    {
        throw MLDB::Exception("Can't round-trip a raw pointer through JSON");
    }

    virtual void printJson(const void * val, JsonPrintingContext & context) const
    {
        T * const * val2 = reinterpret_cast<T * const *>(val);
        return printJsonTyped(val2, context);
    }

    virtual void printJsonTyped(T * const * val, JsonPrintingContext & context) const
    {
        if (*val) {
            context.writeString("POINTER");
        }
        else {
            context.writeNull();
        }
    }

    virtual bool isDefault(const void * val) const
    {
        T * const * val2 = reinterpret_cast<T * const *>(val);
        return isDefaultTyped(val2);
    }

    virtual bool isDefaultTyped(T * const * val) const
    {
        return !*val;
    }
};

template<typename T>
MLDB::ValueDescriptionT<T *> *
getDefaultDescription(T * * = 0)
{
    return new MLDB::PointerValueDescription<T>();
}

#endif



} // namespace MLDB
