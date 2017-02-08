/* value_description.h                                             -*- C++ -*-
   Jeremy Barnes, 29 March 2013
   Copyright (c) 2013 mldb.ai inc.  All rights reserved.

   This file is part of MLDB. Copyright 2015 mldb.ai inc. All rights reserved.

   Code for description and introspection of values and structures.  Used
   to allow for automated formatters and parsers to be built.
*/

#pragma once

#include "value_description_fwd.h"
#include <string>
#include <memory>
#include <unordered_map>
#include <set>
#include "mldb/arch/exception.h"
#include "mldb/arch/demangle.h"
#include "mldb/base/exc_assert.h"
#include "json_parsing.h"
#include "json_printing.h"
#include "mldb/ext/jsoncpp/value.h"

namespace MLDB {


/*****************************************************************************/
/* VALUE DESCRIPTION                                                         */
/*****************************************************************************/

/** Value Description

    This describes the content of a C++ value and allows it to be
    manipulated programatically.
*/

struct ValueDescription {
    ValueDescription(ValueKind kind,
                     const std::type_info * type,
                     const std::string & typeName = "");

    virtual ~ValueDescription();
    
    ValueKind kind;
    const std::type_info * type;
    std::string typeName;
    std::string documentationUri;

    void setTypeName(const std::string & newName);

    virtual void parseJson(void * val, JsonParsingContext & context) const = 0;
    virtual void printJson(const void * val, JsonPrintingContext & context) const = 0;
    virtual bool isDefault(const void * val) const = 0;
    virtual void setDefault(void * val) const = 0;
    virtual void copyValue(const void * from, void * to) const = 0;
    virtual void moveValue(void * from, void * to) const = 0;
    virtual void swapValues(void * from, void * to) const = 0;
    virtual void * constructDefault() const = 0;
    virtual void destroy(void *) const = 0;

    
    virtual void * optionalMakeValue(void * val) const;
    virtual const void * optionalGetValue(const void * val) const;

    virtual size_t getArrayLength(void * val) const;
    virtual void * getArrayElement(void * val, uint32_t element) const;
    virtual const void * getArrayElement(const void * val, uint32_t element) const;

    /** Return the number of elements in a tuple. */
    virtual size_t getTupleLength() const;

    /** Return the descriptions of all elements of the tuple. */
    virtual std::vector<std::shared_ptr<const ValueDescription> >
    getTupleElementDescriptions() const;

    /** Return the value description for the nth array element.  This is
        necessary for tuple types, which don't have the same type for each
        element.
    */
    virtual const ValueDescription &
    getArrayElementDescription(const void * val, uint32_t element) const;

    virtual void setArrayLength(void * val, size_t newLength) const;
    
    /** Returns the ValueDescription of the key of a map, or throws an
        exception if it's not a map.  For example, a std::map<std::string, int>
        would return the value description for a std::string.
    */
    virtual const ValueDescription & getKeyValueDescription() const;
    virtual const ValueDescription & contained() const;

    virtual OwnershipModel getOwnershipModel() const;

    virtual void* getLink(void* obj) const;

    virtual void set(void* obj, void* value, const ValueDescription* valueDesc) const;


    // Convert from one type to another, making a copy.
    // Default will go through a JSON conversion.
    virtual void convertAndCopy(const void * from,
                                const ValueDescription & fromDesc,
                                void * to) const;

    struct FieldDescription {
        std::string fieldName;
        std::string comment;
        std::shared_ptr<const ValueDescription > description;
        int offset;
        int fieldNum;

        void* getFieldPtr(void* obj) const
        {
            return ((char*) obj) + offset;
        }

        const void * getFieldPtr(const void * obj) const
        {
            return ((const char*) obj) + offset;
        }
    };

    virtual size_t getFieldCount(const void * val) const;

    virtual const FieldDescription *
    hasField(const void * val, const std::string & name) const;

    virtual void forEachField(const void * val,
                              const std::function<void (const FieldDescription &)> & onField) const;

    virtual const FieldDescription & 
    getField(const std::string & field) const;

    virtual const std::vector<std::string> getEnumKeys() const;

    virtual std::vector<std::tuple<int, std::string, std::string> >
    getEnumValues() const;

    // Storage to cache Javascript converters
    mutable JSConverters * jsConverters;
    mutable bool jsConvertersInitialized;

    /** Get the value description for a type name */
    static std::shared_ptr<const ValueDescription>
    get(std::string const & name);

    /** Get the value description for a type */
    static std::shared_ptr<const ValueDescription>
    get(const std::type_info & type);

    /** Get the value description for a type */
    template<typename T>
    static std::shared_ptr<const ValueDescriptionT<T> >
    getType()
    {
        auto base = get(typeid(T));

        if (!base)
            return nullptr;

        auto res
            = std::dynamic_pointer_cast<const ValueDescriptionT<T> >(base);
        if (!res)
            throw MLDB::Exception("logic error in registry: wrong type: "
                                + MLDB::type_name(*base) + " not convertible to "
                                + MLDB::type_name<const ValueDescriptionT<T>>());
        return res;
    }

    /** Get the value description for an object */
    template<typename T>
    static std::shared_ptr<const ValueDescriptionT<T> >
    getType(const T * val)
    {
        // TODO: support polymorphic objects
        return getType<T>();
    }

    virtual bool isSame(const ValueDescription* other) const;

    void checkSame(const ValueDescription* other) const;

    virtual bool isChildOf(const ValueDescription* base) const;

    void checkChildOf(const ValueDescription* base) const;

    /** Initialize a value description that was created with the NoConstruct
        parameter.  This should be idempotent, but does not need to be
        atomic.
    */
    virtual void initialize();

    std::vector< std::shared_ptr<ValueDescription> > parents;
};

/** Register the given value description with the system under the given
    type name.
*/
void registerValueDescription(const std::type_info & type,
                              std::function<ValueDescription * ()>,
                              bool isDefault);

/** Register the value description with a two phase create then
    initialize protocol.  This is needed for recursive structures.
*/
void registerValueDescription(const std::type_info & type,
                              std::function<ValueDescription * ()> createFn,
                              std::function<void (ValueDescription &)> initFn,
                              bool isDefault);

template<typename T>
struct RegisterValueDescription {
    RegisterValueDescription()
    {
        registerValueDescription(typeid(T), [] () { return getDefaultDescription((T*)0); }, true);
    }
};

template<typename T, typename Impl>
struct RegisterValueDescriptionI {
    RegisterValueDescriptionI()
        : done(false)
    {
        registerValueDescription(typeid(T), [] () { return new Impl(); }, true);
    }

    bool done;
};

#define REGISTER_VALUE_DESCRIPTION(type)                                \
    namespace {                                                         \
    static const RegisterValueDescription<type> registerValueDescription##type; \
    }

/*****************************************************************************/
/* VALUE DESCRIPTION TEMPLATE                                                */
/*****************************************************************************/

/** Template class for value description.  This is a type-safe version of a
    value description.
*/
    
template<typename T>
struct ValueDescriptionT : public ValueDescription {

    ValueDescriptionT(ValueKind kind = ValueKind::ATOM)
        : ValueDescription(kind, &typeid(T))
    {
    }

    virtual void parseJson(void * val, JsonParsingContext & context) const override
    {
        T * val2 = reinterpret_cast<T *>(val);
        return parseJsonTyped(val2, context);
    }

    virtual void parseJsonTyped(T * val, JsonParsingContext & context) const
    {
        return parseJson(val, context);
    }

    virtual void printJson(const void * val, JsonPrintingContext & context) const override
    {
        const T * val2 = reinterpret_cast<const T *>(val);
        return printJsonTyped(val2, context);
    }

    virtual void printJsonTyped(const T * val, JsonPrintingContext & context) const
    {
        return printJson(val, context);
    }

    virtual bool isDefault(const void * val) const override
    {
        const T * val2 = reinterpret_cast<const T *>(val);
        return isDefaultTyped(val2);
    }

    virtual bool isDefaultTyped(const T * val) const
    {
        return false;
    }

    virtual void setDefault(void * val) const override
    {
        T * val2 = reinterpret_cast<T *>(val);
        setDefaultTyped(val2);
    }

    virtual void setDefaultTyped(T * val) const
    {
        *val = T();
    }

    virtual void copyValue(const void * from, void * to) const override
    {
        copyValue(to, from, typename std::is_copy_assignable<T>::type());
    }

    virtual void moveValue(void * from, void * to) const override
    {
        moveValue(to, from, typename std::is_move_assignable<T>::type());
    }

    virtual void swapValues(void * from, void * to) const override
    {
        using std::swap;
        auto from2 = reinterpret_cast<T *>(from);
        auto to2 = reinterpret_cast<T *>(to);
        if (from2 == to2)
            return;
        std::swap(*from2, *to2);
    }

    virtual void * constructDefault() const override
    {
        return constructDefault(typename std::is_default_constructible<T>::type());
    }

    virtual void destroy(void * val) const override
    {
        delete (T*)val;
    }

    virtual void set(
            void* obj, void* value, const ValueDescription* valueDesc) const override
    {
        checkSame(valueDesc);
        copyValue(value, obj);
    }

    virtual void * optionalMakeValue(void * val) const override
    {
        T * val2 = reinterpret_cast<T *>(val);
        return optionalMakeValueTyped(val2);
    }

    virtual void * optionalMakeValueTyped(T * val) const
    {
        throw MLDB::Exception("type is not optional");
    }

    virtual const void * optionalGetValue(const void * val) const override
    {
        const T * val2 = reinterpret_cast<const T *>(val);
        return optionalGetValueTyped(val2);
    }

    virtual const void * optionalGetValueTyped(const T * val) const
    {
        throw MLDB::Exception("type is not optional");
    }

private:

    void copyValue(void* obj, const void* value, std::true_type) const
    {
        if (obj == value) return;
        *static_cast<T*>(obj) = *static_cast<const T*>(value);
    }

    void copyValue(void* obj, const void* value, std::false_type) const
    {
        throw MLDB::Exception("type is not copy assignable");
    }


    void moveValue(void* obj, void* value, std::true_type) const
    {
        if (obj == value) return;
        *static_cast<T*>(obj) = std::move(*static_cast<T*>(value));
    }

    void moveValue(void* obj, void* value, std::false_type) const
    {
        throw MLDB::Exception("type is not move assignable");
    }

    // Template parameter so not instantiated for types that are not
    // default constructible
    template<typename X>
    void * constructDefault(X) const
    {
        return new T();
    }

    void * constructDefault(std::false_type) const
    {
        throw MLDB::Exception("type is not default constructible");
    }
};

template<typename T, typename Enable = void>
struct GetDefaultDescriptionMaybe {
    static std::shared_ptr<const ValueDescription> get()
    {
        return nullptr;
    }
};

template<typename T>
struct GetDefaultDescriptionMaybe<T, decltype(getDefaultDescription((T *)0))> {
    static std::shared_ptr<const ValueDescription> get()
    {
        return getDefaultDescriptionShared((T *)0);
    }
};

/** Return the default description for the given type if it exists, or
    otherwise return a null pointer.
*/
    
template<typename T>
inline std::shared_ptr<const ValueDescription>
maybeGetDefaultDescriptionShared(T * = 0)
{
    auto result = GetDefaultDescriptionMaybe<T>::get();
    if (!result) {
        // Look to see if it's registered in the registry so that we can
        // get it
        result = ValueDescription::getType<T>();
    }
    return result;
}

/*****************************************************************************/
/* VALUE DESCRIPTION CONCRETE IMPL                                           */
/*****************************************************************************/

/** Used when there is a concrete description of a value we want to register.

    The main thing that this class does is also registers the value description
    as part of construction.
*/

template<typename T, ValueKind kind, typename Impl>
struct ValueDescriptionI : public ValueDescriptionT<T> {

    static RegisterValueDescriptionI<T, Impl> regme;

    ValueDescriptionI()
        : ValueDescriptionT<T>(kind)
    {
        regme.done = true;
    }
};

template<typename T, ValueKind kind, typename Impl>
RegisterValueDescriptionI<T, Impl>
ValueDescriptionI<T, kind, Impl>::
regme;

inline void * addOffset(void * base, ssize_t offset)
{
    return reinterpret_cast<char *>(base) + offset;
}

inline const void * addOffset(const void * base, ssize_t offset)
{
    return reinterpret_cast<const char *>(base) + offset;
}


/*****************************************************************************/
/* CONVERSION FUNCTIONS                                                      */
/*****************************************************************************/


// jsonDecode implementation for any type which:
// 1) has a default description;
template<typename T>
T jsonDecode(const Json::Value & json, T *)
{
    T result;

    static auto desc = getDefaultDescriptionSharedT<T>();
    StructuredJsonParsingContext context(json);
    desc->parseJson(&result, context);
    return result;
}

// jsonDecode implementation for any type which:
// 1) has a default description;
template<typename T>
T jsonDecodeStr(const std::string & json, T * = 0)
{
    T result;

    static auto desc = getDefaultDescriptionSharedT<T>();
    StreamingJsonParsingContext context(json, json.c_str(), json.c_str() + json.size());
    desc->parseJson(&result, context);
    return result;
}

// jsonDecode implementation for any type which:
// 1) has a default description;
template<typename T>
T jsonDecodeStr(const Utf8String & json, T * = 0)
{
    T result;

    static auto desc = getDefaultDescriptionSharedT<T>();
    StreamingJsonParsingContext context(json.rawString(), json.rawData(), json.rawLength());
    desc->parseJson(&result, context);
    return result;
}

// jsonDecode implementation for any type which:
// 1) has a default description;
// NOTE: this works for UTF-8 or ASCII.
template<typename T>
T jsonDecodeStr(const char * str, size_t len, T * = 0)
{
    T result;

    static auto desc = getDefaultDescriptionSharedT<T>();
    StreamingJsonParsingContext context("<<JSON STR>>", str, len);
    desc->parseJson(&result, context);
    return result;
}

// jsonDecode implementation for any type which:
// 1) has a default description;
template<typename T>
T jsonDecodeStream(std::istream & stream, T * = 0)
{
    T result;

    static auto desc = getDefaultDescriptionSharedT<T>();
    StreamingJsonParsingContext context("<<input stream>>", stream);
    desc->parseJson(&result, context);
    return result;
}

// In-place json decoding
template<typename T, typename V>
void jsonDecode(V && json, T & val)
{
    val = std::move(jsonDecode(json, (T *)0));
}

// In-place json decoding
template<typename T>
void jsonDecodeStr(const std::string & json, T & val)
{
    val = std::move(jsonDecodeStr(json, (T *)0));
}

// In-place json decoding
template<typename T>
void jsonDecodeStream(std::istream & stream, T & val)
{
    val = std::move(jsonDecodeStream(stream, (T *)0));
}

// In-place json decoding
template<typename T>
void jsonDecodeFile(const std::string & filename, T & val)
{
    val = std::move(jsonDecodeFile(filename, (T *)0));
}

// jsonEncode implementation for any type which:
// 1) has a default description;
template<typename T>
Json::Value jsonEncode(const T & obj)
{
    static auto desc = getDefaultDescriptionSharedT<T>();
    Json::Value output;
    StructuredJsonPrintingContext context(output);
    desc->printJson(&obj, context);
    return std::move(context.output);
}

// jsonEncode implementation for any type which:
// 1) has a default description;
template<typename T>
std::string jsonEncodeStr(const T & obj)
{
    static auto desc = getDefaultDescriptionSharedT<T>();
    std::string result;
    result.reserve(116);  /// try to force a 128 byte allocation
    StringJsonPrintingContext context(result);
    desc->printJson(&obj, context);
    return result;
}

// jsonEncode implementation for any type which:
// 1) has a default description;
// This one writes utf-8 characters without escaping them
template<typename T>
Utf8String jsonEncodeUtf8(const T & obj)
{
    static auto desc = getDefaultDescriptionSharedT<T>();
    Utf8String result;
    result.reserve(116); // try for a 128 byte allocation
    Utf8StringJsonPrintingContext context(result);
    context.writeUtf8 = true;
    desc->printJson(&obj, context);
    return result;
}

// jsonEncode implementation for any type which:
// 1) has a default description;
template<typename T>
std::ostream & jsonEncodeToStream(const T & obj,
                                  std::ostream & stream)
{
    static auto desc = getDefaultDescriptionSharedT<T>();
    StreamJsonPrintingContext context(stream);
    desc->printJson(&obj, context);
    return stream;
}

inline Json::Value jsonEncode(const char * str)
{
    return str;
}

// Explicit instantiations of builtin value description classes
extern template struct ValueDescriptionT<std::string>;
extern template struct ValueDescriptionT<Utf8String>;
extern template struct ValueDescriptionT<Utf32String>;
extern template struct ValueDescriptionT<signed int>;
extern template struct ValueDescriptionT<unsigned int>;
extern template struct ValueDescriptionT<signed long>;
extern template struct ValueDescriptionT<unsigned long>;
extern template struct ValueDescriptionT<signed long long>;
extern template struct ValueDescriptionT<unsigned long long>;
extern template struct ValueDescriptionT<float>;
extern template struct ValueDescriptionT<double>;
extern template struct ValueDescriptionT<Json::Value>;
extern template struct ValueDescriptionT<bool>;

template<typename T>
struct ValueDescriptionInit {
    static ValueDescription * create()
    {
        using MLDB::getDefaultDescriptionUninitialized;
        return getDefaultDescriptionUninitialized((T *)0);
    }
};


template<typename T>
std::shared_ptr<const ValueDescriptionT<T> >
getDefaultDescriptionShared(T *)
{
    auto res = ValueDescription::getType<T>();
    if (!res) {
        std::function<ValueDescription * ()> createFn
            = &ValueDescriptionInit<T>::create;

        auto initFn = [] (ValueDescription & desc)
            {
                desc.initialize();
#if 0
                auto * descTyped
                = dynamic_cast<ValueDescriptionT<T>typename GetDefaultDescriptionType<T>::type *>
                    (&desc);
                if (!descTyped)
                    throw MLDB::Exception("Attempt to initialized description for "
                                        + MLDB::type_name<T>() + " of type "
                                        + MLDB::type_name<typename GetDefaultDescriptionType<T>::type >()
                                        + " from value of type "
                                        + MLDB::type_name(desc));
                
                initializeDefaultDescription(*descTyped);
#endif
            };
        
        // For now, register it if it wasn't before.  Eventually this should
        // be done elsewhere.
        registerValueDescription(typeid(T), createFn, initFn, true);

        res = ValueDescription::getType<T>();
    }
    ExcAssert(res);

    auto cast = std::dynamic_pointer_cast<const ValueDescriptionT<T> >(res);

    if (!cast)
        throw MLDB::Exception("logic error in registry: wrong type: "
                            + MLDB::type_name(*res) + " not convertible to "
                            + MLDB::type_name<ValueDescriptionT<T> >());

    return cast;
}

/** Template that returns the type of the default description that should
    be instantiated for the given use case.
*/
template<typename T>
struct GetDefaultDescriptionType {
    typedef typename std::remove_reference<decltype(*getDefaultDescription((T*)0))>::type type;
};


} // namespace MLDB
