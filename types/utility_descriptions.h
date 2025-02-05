/** utility_descriptions.h                                         -*- C++ -*-
    Jeremy Barnes, 21 August 2015
    Copyright (c) 2015 mldb.ai inc.  All rights reserved.
    This file is part of MLDB. Copyright 2015 mldb.ai inc. All rights reserved.

    Utility functions for value descriptions.
*/

#pragma once

#include "value_description.h"


namespace MLDB {


/*****************************************************************************/
/* BRIDGED VALUE DESCRIPTION                                                 */
/*****************************************************************************/

struct BridgedValueDescription: public ValueDescription {
    BridgedValueDescription(std::shared_ptr<const ValueDescription> impl);
    std::shared_ptr<const ValueDescription> impl;

    virtual ~BridgedValueDescription();
    
    virtual void parseJson(void * val, JsonParsingContext & context) const override;
    virtual void printJson(const void * val, JsonPrintingContext & context) const override;
    virtual bool isDefault(const void * val) const override;
    virtual void setDefault(void * val) const override;
    virtual void copyValue(const void * from, void * to) const override;
    virtual void moveValue(void * from, void * to) const override;
    virtual void swapValues(void * from, void * to) const override;
    virtual void initializeDefault(void * mem) const override;
    virtual void initializeCopy(void * mem, const void * from) const override;
    virtual void initializeMove(void * mem, void * from) const override;
    virtual void destruct(void * obj) const override;
    virtual bool hasEqualityComparison() const override;
    virtual bool compareEquality(const void * val1, const void * val2) const override;
    virtual bool hasLessThanComparison() const override;
    virtual bool compareLessThan(const void * val1, const void * val2) const override;
    virtual bool hasStrongOrderingComparison() const override;
    virtual std::strong_ordering compareStrong(const void * val1, const void * val2) const override;
    virtual bool hasWeakOrderingComparison() const override;
    virtual std::weak_ordering compareWeak(const void * val1, const void * val2) const override;
    virtual bool hasPartialOrderingComparison() const override;
    virtual std::partial_ordering comparePartial(const void * val1, const void * val2) const override;
    virtual void * optionalMakeValue(void * val) const override;
    virtual const void * optionalGetValue(const void * val) const override;
    virtual size_t getArrayFixedLength() const override;
    virtual size_t getArrayLength(void * val) const override;
    virtual LengthModel getArrayLengthModel() const override;
    virtual OwnershipModel getArrayIndirectionModel() const override;
    virtual void * getArrayElement(void * val, uint32_t element) const override;
    virtual const void * getArrayElement(const void * val, uint32_t element) const override;
    virtual const ValueDescription &
    getArrayElementDescription(const void * val, uint32_t element) const override;
    virtual void setArrayLength(void * val, size_t newLength) const override;
    virtual size_t getTupleLength() const override;
    virtual std::vector<std::shared_ptr<const ValueDescription> >
    getTupleElementDescriptions() const override;
    virtual const ValueDescription & getKeyValueDescription() const override;
    virtual const ValueDescription & contained() const override;
    virtual OwnershipModel getOwnershipModel() const override;
    virtual void* getLink(void* obj) const override;
    virtual const void* getConstLink(const void* obj) const override;
    virtual void set(void* obj, void* value, const ValueDescription* valueDesc) const override;
    virtual void convertAndCopy(const void * from,
                                const ValueDescription & fromDesc,
                                void * to) const override;
    virtual size_t getFieldCount(const void * val) const override;
    virtual const FieldDescription *
    hasField(const void * val, const std::string & name) const override;
    virtual void forEachField(const void * val,
                              const std::function<void (const FieldDescription &)> & onField) const override;
    virtual const FieldDescription & 
    getField(const std::string & field) const override;
    virtual const std::vector<std::string> getEnumKeys() const override;
    virtual std::vector<std::tuple<int, std::string, std::string> >
    getEnumValues() const override;
    virtual bool isSame(const ValueDescription* other) const override;
    virtual bool isChildOf(const ValueDescription* base) const override;
    virtual void initialize() override;
};


/*****************************************************************************/
/* PURE VALUE DESCRIPTION                                                    */
/*****************************************************************************/

template<typename T>
struct PureValueDescription : public ValueDescriptionT<T> {
    PureValueDescription() :
        ValueDescriptionT<T>(ValueKind::ATOM) {
    }

    virtual void parseJson(void * val, JsonParsingContext & context) const override {};
    virtual void printJson(const void * val, JsonPrintingContext & context) const override
    {
        context.writeNull();
    };
    virtual bool isDefault(const void * val) const override { return false; }
    virtual void setDefault(void * val) const override {}
    virtual void copyValue(const void * from, void * to) const override {}
    virtual void moveValue(void * from, void * to) const override {}
    virtual void swapValues(void * from, void * to) const override {}
    virtual void initializeDefault(void * mem) const override {}
    virtual void initializeCopy(void * mem, const void *) const override {}
    virtual void initializeMove(void * mem, void *) const override {}
    virtual void destruct(void *) const override {}

};


/*****************************************************************************/
/* VALUE DESCRIPTION WITH DEFAULT                                            */
/*****************************************************************************/

/** Provides an adaptor that adapts a given value description and adds
    a default value.
*/

template<typename T>
struct ValueDescriptionWithDefault : public BridgedValueDescription {
    ValueDescriptionWithDefault(T defaultValue,
                                std::shared_ptr<const ValueDescriptionT<T> > base)
        : BridgedValueDescription(base),
          defaultValue(defaultValue)
    {
    }
    
    virtual bool isDefault(const void * val_) const override
    {
        const T * val = (const T *)val_;
        return *val == defaultValue;
    }

    virtual void setDefault(void * val_) const override
    {
        T * val = (T *)val_;
        *val = defaultValue;
    }

    virtual void initializeDefault(void * mem) const override
    {
        new (mem) T(defaultValue);
    }

    T defaultValue;
};


/*****************************************************************************/
/* DESCRIPTION FROM BASE                                                     */
/*****************************************************************************/

/** This class is used for when you want to create a value description for
    a class that derives from a base class that provides most or all of its
    functionality (eg, a vector).  It forwards all of the methods to the
    base value description.
*/

template<typename T, typename Base,
         typename BaseDescription
             = typename GetDefaultDescriptionType<Base>::type>
struct DescriptionFromBase
    : public ValueDescriptionT<T> {

    DescriptionFromBase(BaseDescription * inner)
        : inner(inner)
    {
    }

    DescriptionFromBase(std::shared_ptr<const BaseDescription> inner
                      = getDefaultDescriptionShared((Base *)0))
        : inner(inner)
    {
    }

    std::shared_ptr<const BaseDescription> inner;

    constexpr ssize_t offset() const
    {
        return (ssize_t)(static_cast<Base *>((T *)0));
    }

    void * fixPtr(void * ptr) const
    {
        return addOffset(ptr, offset());
    }

    const void * fixPtr(const void * ptr) const
    {
        return addOffset(ptr, offset());
    }

    virtual void parseJson(void * val, JsonParsingContext & context) const override
    {
        inner->parseJson(fixPtr(val), context);
    }

    virtual void parseJsonTyped(T * val, JsonParsingContext & context) const override
    {
        inner->parseJson(fixPtr(val), context);
    }

    virtual void printJson(const void * val, JsonPrintingContext & context) const override
    {
        inner->printJson(fixPtr(val), context);
    }

    virtual void printJsonTyped(const T * val, JsonPrintingContext & context) const override
    {
        inner->printJson(fixPtr(val), context);
    }

    virtual bool isDefault(const void * val) const override
    {
        return inner->isDefault(fixPtr(val));
    }

    virtual bool isDefaultTyped(const T * val) const override
    {
        return inner->isDefault(fixPtr(val));
    }

    virtual size_t getArrayLength(void * val) const override
    {
        return inner->getArrayLength(fixPtr(val));
    }

    virtual size_t getArrayFixedLength() const override
    {
        return inner->getArrayFixedLength();
    }

    virtual LengthModel getArrayLengthModel() const override
    {
        return inner->getArrayLengthModel();
    }

    virtual OwnershipModel getArrayIndirectionModel() const override
    {
        return inner->getArrayIndirectionModel();
    }

    virtual void * getArrayElement(void * val, uint32_t element) const override
    {
        return inner->getArrayElement(fixPtr(val), element);
    }

    virtual const void * getArrayElement(const void * val, uint32_t element) const override
    {
        return inner->getArrayElement(fixPtr(val), element);
    }

    virtual void setArrayLength(void * val, size_t newLength) const override
    {
        inner->setArrayLength(fixPtr(val), newLength);
    }
    
    virtual const ValueDescription & contained() const override
    {
        return inner->contained();
    }
};



} // namespace MLDB
