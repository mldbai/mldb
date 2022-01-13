/** bitset.h                                                       -*- C++ -*-
    Jeremy Barnes, 21 September 2017
    Copyright (c) 2017 Element AI Inc.  All rights reserved.
    This file is part of MLDB. Copyright 2015 mldb.ai inc. All rights reserved.

    Enum-backed bitfield helper classes.
*/

#pragma once


#include "mldb/types/value_description.h"
#include "mldb/arch/exception.h"
#include "mldb/arch/bitops.h"
#include "mldb/base/exc_assert.h"
#include <bitset>

namespace MLDB {


/*****************************************************************************/
/* BITSET                                                                    */
/*****************************************************************************/

/** A bitset that is backed by an enumeration which defines what the various
    bits mean.
*/
template<typename BaseEnum>
struct Bitset {
    Bitset(BaseEnum val = BaseEnum(0))
        : val(val)
    {
    }

    Bitset(std::initializer_list<BaseEnum> vals)
        : val(BaseEnum(0))
    {
        for (auto v: vals) {
            set(v);
        }
    }
    
    union {
        BaseEnum val;
        std::bitset<sizeof(BaseEnum) * 8> bits;
    };

    static int bitNum(BaseEnum el)
    {
        return highest_bit((size_t)el, -1);
    }
    
    void set(BaseEnum el)
    {
        ExcAssertGreater((size_t)el, 0);
        bits.set(bitNum(el));
    }

    bool test(BaseEnum el) const
    {
        ExcAssertGreater((size_t)el, 0);
        return bits.test(bitNum(el));
    }
    
    size_t size() const { return bits.count(); }
    bool empty() const { return bits.none(); }

    void clear() { val = (BaseEnum)0; }
};

/*****************************************************************************/
/* BITSET DESCRIPTION                                                        */
/*****************************************************************************/

template<typename T>
struct BitsetDescription
    : public ValueDescriptionI<Bitset<T>, ValueKind::ARRAY, BitsetDescription<T> > {
    std::shared_ptr<const ValueDescriptionT<T> > inner;
    
    BitsetDescription(ValueDescriptionT<T> * inner)
        : inner(inner)
    {
    }

    BitsetDescription(std::shared_ptr<const ValueDescriptionT<T> > inner
                       = getDefaultDescriptionShared((T *)0))
        : inner(inner)
    {
    }

    // Constructor to create a partially-evaluated vector description.
    BitsetDescription(ConstructOnly)
    {
    }

    virtual void parseJson(void * val,
                           JsonParsingContext & context) const override
    {
        Bitset<T> * val2 = reinterpret_cast<Bitset<T> *>(val);
        return parseJsonTyped(val2, context);
    }

    virtual void parseJsonTyped(Bitset<T> * val,
                                JsonParsingContext & context) const override
    {
        if (!context.isArray())
            context.exception("expected array of " + this->inner->typeName);
        
        val->clear();

        auto onElement = [&] ()
            {
                T el;
                this->inner->parseJsonTyped(&el, context);
                val->set(el);
            };
        
        context.forEachElement(onElement);
    }

    virtual void printJson(const void * val,
                           JsonPrintingContext & context) const override
    {
        const Bitset<T> * val2 = reinterpret_cast<const Bitset<T> *>(val);
        return printJsonTyped(val2, context);
    }

    virtual void printJsonTyped(const Bitset<T> * val,
                                JsonPrintingContext & context) const override
    {
        context.startArray(val->size());

        for (unsigned i = 0;  i < val->size();  ++i) {
            if (!val->bits.test(i))
                continue;
            context.newArrayElement();
            T bitval((T)(1 << i));
            this->inner->printJsonTyped(&bitval, context);
        }
        
        context.endArray();
    }

    virtual bool isDefault(const void * val) const override
    {
        const Bitset<T> * val2 = reinterpret_cast<const Bitset<T> *>(val);
        return isDefaultTyped(val2);
    }

    virtual bool isDefaultTyped(const Bitset<T> * val) const override
    {
        return val->empty();
    }

    virtual size_t getArrayLength(void * val) const override
    {
        const Bitset<T> * val2 = reinterpret_cast<const Bitset<T> *>(val);
        return val2->size();
    }

    virtual void * getArrayElement(void * val, uint32_t element) const override
    {
        throw MLDB::Exception("can't mutate set elements");
    }

    virtual const void * getArrayElement(const void * val,
                                         uint32_t element) const override
    {
        const Bitset<T> * val2 = reinterpret_cast<const Bitset<T> *>(val);
        if (element >= val2->size())
            throw MLDB::Exception("Invalid set element number");

        throw MLDB::Exception("done");
    }

    virtual void setArrayLength(void * val, size_t newLength) const override
    {
        throw MLDB::Exception("cannot adjust length of a set");
    }
    
    virtual const ValueDescription & contained() const override
    {
        return *this->inner;
    }

    virtual std::shared_ptr<const ValueDescription> containedPtr() const
    {
        return this->inner;
    }

    virtual void initialize() override
    {
        this->inner = getDefaultDescriptionSharedT<T>();
    }
};

DECLARE_TEMPLATE_VALUE_DESCRIPTION_1(BitsetDescription, Bitset, typename, T, MLDB::has_default_description<T>::value);

} // namespace MLDB
