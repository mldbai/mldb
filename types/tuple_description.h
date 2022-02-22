/** tuple_description.h                                            -*- C++ -*-
    Jeremy Barnes, 24 April 2014
    Copyright (c) 2014 mldb.ai inc.  All rights reserved.

    This file is part of MLDB. Copyright 2015 mldb.ai inc. All rights reserved.

    Value description for a tuple.
*/

#pragma once


#include "mldb/types/value_description.h"
#include <type_traits>

namespace MLDB {

template<typename... T>
struct TupleDescription;

} // namespace MLDB

namespace std {

template<typename... T>
MLDB::TupleDescription<T...> *
getDefaultDescription(tuple<T...> * = 0);

template<typename... T>
MLDB::TupleDescription<T...> *
getDefaultDescriptionUninitialized(tuple<T...> * = 0);

} // namespace std

namespace MLDB {

struct TupleElementDescription {
    int offset;
    std::shared_ptr<const ValueDescription> desc;
};

template<typename Tuple, int n, typename... Types>
struct AddTupleTypes;

template<typename Tuple, int n, typename First, typename... Rest>
struct AddTupleTypes<Tuple, n, First, Rest...> {
    static void go(std::vector<TupleElementDescription> & elements)
    {
        auto desc = getDefaultDescriptionShared((typename std::remove_reference<First>::type *)0);
        static const Tuple tpl;;
        const void * el = &std::get<n>(tpl);
        int offset = (size_t)el - (size_t)(&tpl);

        TupleElementDescription elDesc;
        elDesc.desc = desc;
        elDesc.offset = offset;

        elements.emplace_back(std::move(elDesc));
        AddTupleTypes<Tuple, n + 1, Rest...>::go(elements);
    }
};

template<typename Tuple, int n>
struct AddTupleTypes<Tuple, n> {
    static void go(std::vector<TupleElementDescription> & elements)
    {
    }
};

template<typename... Types>
void addTupleTypes(std::vector<TupleElementDescription> & elements)
{
    elements.clear();
    elements.reserve(sizeof...(Types));
    AddTupleTypes<std::tuple<Types...>, 0, Types...>::go(elements);
    ExcAssertEqual(elements.size(), sizeof...(Types));
}


/*****************************************************************************/
/* TUPLE DESCRIPTION                                                         */
/*****************************************************************************/

template<typename... T>
struct TupleDescription
    : public ValueDescriptionI<std::tuple<T...>, ValueKind::TUPLE,
                               TupleDescription<T...> > {

    TupleDescription()
    //: ValueDescriptionT<std::tuple<T...> >(ValueKind::TUPLE)
    {
        addTupleTypes<T...>(elements);
    }

    TupleDescription(ConstructOnly)
    {
    }

    std::vector<TupleElementDescription> elements;

    virtual void
    parseJsonTyped(std::tuple<T...> * val,
                   JsonParsingContext & context) const override
    {
        if (!context.isArray())
            context.exception("expected array for tuple representation");
        
        int elNum = 0;
        auto onElement = [&] ()
            {
                if (elNum >= elements.size()) {
                    context.exception("Error parsing JSON tuple: too many elements in JSON array");
                }
                elements[elNum].desc
                    ->parseJson(this->getArrayElement(val, elNum), context);
                ++elNum;
            };
        
        context.forEachElement(onElement);        

        if (elNum < sizeof...(T)) {
            context.exception("Error parsing JSON tuple: not enough elements in JSON array");
        }
    }

    virtual void
    printJsonTyped(const std::tuple<T...> * val,
                   JsonPrintingContext & context) const override
    {
        context.startArray(sizeof...(T));

        for (unsigned i = 0;  i < sizeof...(T);  ++i) {
            context.newArrayElement();
            elements[i].desc
                ->printJson(this->getArrayElement(val, i), context);
        }
        
        context.endArray();
    }

    virtual bool isDefaultTyped(const std::tuple<T...> * val) const override
    {
        return false;
    }

    virtual size_t getArrayLength(void * val) const override
    {
        return sizeof...(T);
    }

    virtual size_t getArrayFixedLength() const override
    {
        return sizeof...(T);
    }

    virtual LengthModel getArrayLengthModel() const override
    {
        return LengthModel::FIXED;
    }

    virtual OwnershipModel getArrayIndirectionModel() const override
    {
        return OwnershipModel::NONE;
    }

    virtual void * getArrayElement(void * val, uint32_t element) const override
    {
        return ((char*) val) + elements.at(element).offset;
    }

    virtual const
    void * getArrayElement(const void * val, uint32_t element) const override
    {
        return ((char*) val) + elements.at(element).offset;
        
    }

    virtual size_t getTupleLength() const override
    {
        return sizeof...(T);
    }

    virtual std::vector<std::shared_ptr<const ValueDescription> >
    getTupleElementDescriptions() const override
    {
        std::vector<std::shared_ptr<const ValueDescription> > result;
        for (auto & e: elements)
            result.emplace_back(e.desc);
        return result;
    }

    virtual const ValueDescription &
    getArrayElementDescription(const void * val,
                               uint32_t element) const override
    {
        auto res = elements.at(element).desc.get();
        ExcAssert(res);
        return *res;
    }

    virtual void setArrayLength(void * val, size_t newLength) const override
    {
        throw MLDB::Exception("tuple array lengths can't be set");
    }
    
    virtual const ValueDescription & contained() const override
    {
        throw MLDB::Exception("tuple does not have a consistent contained type");
    }

    virtual void set(void* obj, void* value,
                     const ValueDescription* valueDesc) const override
    {
        throw MLDB::Exception("tuple type set not done");
    }

    virtual void initialize() override
    {
        addTupleTypes<T...>(elements);
    }
};

// std::swap of a tuple with an Any relies on an implicit conversion, which Any doesn't
// like.  This is a hack to avoid instantiating std::swap for tuples, instead we go
// element by element.  Needed for clang 12.
template<size_t First, size_t Last>
struct SwapElements {
    template<typename Tuple>
    static void swap(Tuple & t1, Tuple & t2)
    {
        doSwap(std::get<First>(t1), std::get<First>(t2));
        SwapElements<First + 1, Last>::swap(t1, t2);
    }
};

template<size_t Last>
struct SwapElements<Last, Last> {
    template<typename Tuple>
    static void swap(Tuple & t1, Tuple & t2)
    {
    }
};

template<typename... T>
inline void doSwap(std::tuple<T...> & t1, std::tuple<T...> & t2)
{
    SwapElements<0, sizeof...(T)>::swap(t1, t2);
}

} // namespace MLDB

namespace std {

template<typename... T>
MLDB::TupleDescription<T...> *
getDefaultDescription(std::tuple<T...> *)
{
    return new MLDB::TupleDescription<T...>();
}

template<typename... T>
MLDB::TupleDescription<T...> *
getDefaultDescriptionUninitialized(std::tuple<T...> *)
{
    return new MLDB::TupleDescription<T...>(MLDB::constructOnly);
}

} // namespace std