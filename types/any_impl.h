// This file is part of MLDB. Copyright 2015 mldb.ai inc. All rights reserved.

/** any_impl.h                                                     -*- C++ -*-
    Jeremy Barnes, 22 August 2015
    Copyright (c) 2015 mldb.ai inc.  All rights reserved.

    Implementation of the Any class's template methods.
*/

#pragma once

#include "any.h"
#include "mldb/types/value_description.h"

namespace MLDB {

/*****************************************************************************/
/* ANY                                                                       */
/*****************************************************************************/

template<typename T>
const T &
Any::
as() const
{
    if (!type_)
        throw MLDB::Exception("bad Any cast: null value can't convert to '%s'",
                            MLDB::type_name<T>().c_str());

    // If the same type, conversion is trivial
    if (type_ == &typeid(T))
        return *reinterpret_cast<T *>(obj_.get());

    // Otherwise, go into RTTI and see if we can make it happen
    //const void * res = ML::is_convertible(*type_, typeid(T), obj_.get());
    //if (res)
    //    return *reinterpret_cast<const T *>(res);

    // Otherwise, no conversion is possible
    throw MLDB::Exception("bad Any cast: requested '%s', contained '%s'",
                        MLDB::type_name<T>().c_str(),
                        demangle(*type_).c_str());
}

template<typename T>
T
Any::
convert(const ValueDescription & desc) const
{
    // If the same type, conversion is trivial
    if (type_ == &typeid(T))
        return *reinterpret_cast<T *>(obj_.get());

    // Otherwise, go into RTTI and see if we can make it happen
    //const void * res = ML::is_convertible(*type_, typeid(T), obj_.get());
    //if (res)
    //    return *reinterpret_cast<const T *>(res);

    if (!type_) {
        T result;
        Json::Value v;
        StructuredJsonParsingContext context(v);
        desc.parseJson(&result, context);
        return result;
    }
    else if (type_ == &typeid(Json::Value)) {
        T result;
        StructuredJsonParsingContext context(*reinterpret_cast<const Json::Value *>(obj_.get()));
        desc.parseJson(&result, context);
        return result;
    }

    // Otherwise, no conversion is possible
    throw MLDB::Exception("bad Any conversion: requested '%s', contained '%s'",
                        MLDB::type_name<T>().c_str(),
                        demangle(*type_).c_str());
}

/** Assign a new value of the same type. */
template<typename T>
void
Any::
assign(const T & value)
{
    if (&typeid(T) == type_) {
        // Keep the same description, etc
        obj_.reset(new T(value));
    }
    else {
        *this = Any(value);
    }
}

/** Assign a new value of the same type. */
template<typename T>
void
Any::
assign(T && value)
{
    if (&typeid(T) == type_) {
        // Keep the same description, etc
        obj_.reset(new T(std::move(value)));
    }
    else {
        *this = Any(value);
    }
}

extern template class ValueDescriptionT<Any>;


struct TypedAnyDescription: public ValueDescriptionT<Any> {
    virtual void parseJsonTyped(Any * val,
                                JsonParsingContext & context) const;
    virtual void printJsonTyped(const Any * val,
                                JsonPrintingContext & context) const;
};

/** Alternative value description for Any that only prints the JSON,
    not the type information.  This can't be used to reconstitute.
*/
struct BareAnyDescription: public ValueDescriptionT<Any> {
    virtual void parseJsonTyped(Any * val,
                                JsonParsingContext & context) const;
    virtual void printJsonTyped(const Any * val,
                                JsonPrintingContext & context) const;
    virtual bool isDefaultTyped(const Any * val) const;
};


} // namespace MLDB

