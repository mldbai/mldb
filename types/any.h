/* any.h                                                           -*- C++ -*-

   Jeremy Barnes, July 4 2014
   Copyright (c) 2014 mldb.ai inc.  All rights reserved.

   This file is part of MLDB. Copyright 2015 mldb.ai inc. All rights reserved.
*/

#pragma once

#include "mldb/types/value_description_fwd.h"
#include "mldb/compiler/compiler.h"

namespace MLDB {

/*****************************************************************************/
/* ANY                                                                       */
/*****************************************************************************/

/** Similar to boost::any, but uses a ValueDescription to do its job.  This
    allows, amongst other things, serialization to and from JSON.
*/

struct Any {

    Any() noexcept
        : type_(nullptr), desc_(nullptr)
    {
    }

    template<typename T>
    Any(const T & val,
        const ValueDescriptionT<T> * desc = getDefaultDescriptionShared((T *)0).get(),
        typename std::enable_if<!std::is_same<T, Any>::value>::type * = 0)
    : obj_(new T(val)),
      type_(&typeid(val)),
      desc_(desc)
    {
        //ExcAssertEqual(desc->type, type_);
    }

    /** Construct directly from Json, with a known type */
    Any(const Json::Value & val,
        const ValueDescription * desc);

    /** Construct directly from Json */
    Any(const std::string & jsonValString,
        const ValueDescription * desc);

    Any(const Any & other)
        : obj_(other.obj_),
          type_(other.type_),
          desc_(other.desc_)
    {
    }

    Any(std::nullptr_t)
        : type_(nullptr), desc_(nullptr)
    {
    }

    void swap(Any & other) noexcept
    {
        std::swap(obj_, other.obj_);
        std::swap(type_, other.type_);
        std::swap(desc_, other.desc_);
    }

    /** Decode an object returned from the typed Any serialization. */
    static Any jsonDecodeStrTyped(const std::string & json);

    /** Decode an object returned from the typed Any serialization. */
    static Any jsonDecodeTyped(const Json::Value & json);

    static std::string jsonEncodeStrTyped(const Any & val);

    static Json::Value jsonEncodeTyped(const Any & val);

    template<typename T>
    bool is() const
    {
        return type_ == &typeid(T);
    }

    template<typename T>
    const T & as() const;

    template<typename T>
    T convert(const ValueDescription & desc = *getDefaultDescriptionSharedT<T>()) const;

    bool empty() const
    {
        return !obj_;
    }

    const std::type_info & type() const
    {
        if (type_)
            return *type_;
        return typeid(void);
    }
    
    const ValueDescription & desc() const
    {
        if (!desc_)
            throwNoValueDescription();
        return *desc_;
    }

    /** Assign a new value of the same type. */
    template<typename T>
    void assign(const T & value);

    /** Assign a new value of the same type. */
    template<typename T>
    void assign(T && value);

    /** Get it as JSON */
    Json::Value asJson() const;

    /** Get it as stringified JSON */
    std::string asJsonStr() const;

    void setJson(const Json::Value & val);

    void setJson(Json::Value && val);

    /** Return the value of the given field (for a structure).  Throws if the field
        does not exist.
    */
    Any getField(const std::string & fieldName) const;

private:
    friend class TypedAnyDescription;
    friend class BareAnyDescription;
    std::shared_ptr<void> obj_;
    const std::type_info * type_;
    const ValueDescription * desc_;

    friend bool operator==(const Any & lhs, const Any & rhs);

    /** Conversion function. */
    void convert(void * result, const std::type_info & toType);
    void throwNoValueDescription() const MLDB_NORETURN;
};

PREDECLARE_VALUE_DESCRIPTION(Any);

/** Return a value description for Any that is completely bare (doesn't attempt
    to serialize the type information, just the value information).
*/
std::shared_ptr<ValueDescriptionT<Any> >
getBareAnyDescription();

} // namespace MLDB
