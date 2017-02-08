/** map_description.h                                        -*- C++ -*-
    Jeremy Barnes, 21 August 2015
    Copyright (c) 2015 mldb.ai inc.  All rights reserved.

    This file is part of MLDB. Copyright 2015 mldb.ai inc. All rights reserved.

    Value description implementations for maps.
*/

#pragma once

#include "value_description.h"
#include <map>

namespace MLDB {

/*****************************************************************************/
/* DEFAULT DESCRIPTION FOR MAP                                               */
/*****************************************************************************/

inline std::string stringToKey(const std::string & str, std::string *) { return str; }
inline std::string keyToString(const std::string & str) { return str; }

inline Utf8String stringToKey(const std::string & str, Utf8String *) { return Utf8String(str); }
inline std::string keyToString(const Utf8String & str) { return str.rawString(); }

short int stringToKey(const std::string & str, short int *);
unsigned short int stringToKey(const std::string & str, unsigned short int *);
int stringToKey(const std::string & str, int *);
unsigned int stringToKey(const std::string & str, unsigned int *);
long int stringToKey(const std::string & str, long int *);
unsigned long int stringToKey(const std::string & str, unsigned long int *);
long long int stringToKey(const std::string & str, long long int *);
unsigned long long int stringToKey(const std::string & str, unsigned long long int *);

template<typename T>
inline Utf8String keyToString(const T & key)
{
    using std::to_string;
    return to_string(key);
}

template<typename T, typename Enable = void>
struct FreeFunctionKeyCodec {
    static T decode(const std::string & s, T *) { return stringToKey(s, (T *)0); }
    static T decode(const Utf8String & s, T *) { return stringToKey(s, (T *)0); }
    static Utf8String encode(const T & t) { return keyToString(t); }
};

template<typename K, typename T,
         typename KeyCodec = FreeFunctionKeyCodec<K>,
         typename C = std::less<K>,
         typename A = std::allocator<std::pair<const K,T> > >
struct MapValueDescription
    : public ValueDescriptionI<std::map<K, T, C, A>, ValueKind::MAP, MapValueDescription<K, T, KeyCodec, C, A> > {

    MapValueDescription(ConstructOnly)
    {
    }

    MapValueDescription(std::shared_ptr<const ValueDescriptionT<T> > inner
                        = getDefaultDescriptionShared((T *)0),
                        std::shared_ptr<const ValueDescriptionT<K> > key
                        = getDefaultDescriptionShared((K *)0))
        : inner(std::move(inner)), key(std::move(key))
    {
        ExcAssert(this->inner);
        ExcAssert(this->key);
    }

    std::shared_ptr<const ValueDescriptionT<T> > inner;
    std::shared_ptr<const ValueDescriptionT<K> > key;

    typedef ValueDescription::FieldDescription FieldDescription;

    virtual void parseJson(void * val, JsonParsingContext & context) const override
    {
        auto * val2 = reinterpret_cast<std::map<K, T, C, A> *>(val);
        return parseJsonTyped(val2, context);
    }

    virtual void parseJsonTyped(std::map<K, T, C, A> * val, JsonParsingContext & context) const override
    {
        std::map<K, T, C, A> res;

        auto onMember = [&] ()
            {
                K key = KeyCodec::decode(context.fieldName(), (K *)0);
                inner->parseJsonTyped(&res[key], context);
            };

        context.forEachMember(onMember);

        val->swap(res);
    }

    virtual void printJson(const void * val, JsonPrintingContext & context) const override
    {
        auto * val2 = reinterpret_cast<const std::map<K, T, C, A> *>(val);
        return printJsonTyped(val2, context);
    }

    virtual void printJsonTyped(const std::map<K, T, C, A> * val,
                                JsonPrintingContext & context) const override
    {
        context.startObject();
        for (auto & v: *val) {
            context.startMember(KeyCodec::encode(v.first));
            inner->printJsonTyped(&v.second, context);
        }
        context.endObject();
    }

    virtual bool isDefault(const void * val) const override
    {
        auto * val2 = reinterpret_cast<const std::map<K, T, C, A> *>(val);
        return isDefaultTyped(val2);
    }

    virtual bool isDefaultTyped(const std::map<K, T, C, A> * val) const override
    {
        return val->empty();
    }

    virtual size_t getFieldCount(const void * val) const override
    {
        auto * val2 = reinterpret_cast<const std::map<K, T, C, A> *>(val);
        return val2->size();
    }

    virtual const FieldDescription *
    hasField(const void * val, const std::string & name) const override
    {
        throw MLDB::Exception("map hasField: needs work");
        //auto * val2 = reinterpret_cast<const std::map<std::string, T> *>(val);
        //return val2->count(name);
    }

    virtual void forEachField(const void * val,
                              const std::function<void (const FieldDescription &)> & onField) const override
    {
        throw MLDB::Exception("map forEachField: needs work");
    }

    virtual const FieldDescription & 
    getField(const std::string & field) const override
    {
        throw MLDB::Exception("map getField: needs work");
    }

    virtual const ValueDescription & getKeyValueDescription() const override
    {
        ExcAssert(this->key);
        return *this->key;
    }

    virtual const ValueDescription & contained() const override
    {
        ExcAssert(this->inner);
        return *this->inner;
    }

    virtual void initialize() override
    {
        this->key = getDefaultDescriptionSharedT<K>();
        this->inner = getDefaultDescriptionSharedT<T>();
        ExcAssert(this->key);
        ExcAssert(this->inner);
    }
};

template<typename Key, typename Value,
         typename Compare = std::less<Key>,
         typename Alloc = std::allocator<std::pair<const Key,Value> >,
         typename KeyCodec = FreeFunctionKeyCodec<Key> >
struct MapDescription
    : public MapValueDescription<Key, Value, KeyCodec, Compare, Alloc> {
    MapDescription(ValueDescriptionT<Value> * inner)
        : MapValueDescription<Key, Value, KeyCodec, Compare, Alloc>(inner)
    {
    }

    MapDescription(std::shared_ptr<const ValueDescriptionT<Value> > inner
                       = getDefaultDescriptionShared((Value *)0),
                   std::shared_ptr<const ValueDescriptionT<Key> > key
                       = getDefaultDescriptionShared((Key *)0))
        : MapValueDescription<Key, Value, KeyCodec, Compare, Alloc>(std::move(inner), std::move(key))
    {
    }

    MapDescription(ConstructOnly)
        : MapValueDescription<Key, Value, KeyCodec, Compare, Alloc>(constructOnly)
    {
    }
};

DECLARE_TEMPLATE_VALUE_DESCRIPTION_4(MapDescription, std::map, typename, Key, typename, Value, typename, Compare, typename, Alloc);

} // namespace MLDB
