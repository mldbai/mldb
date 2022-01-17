// This file is part of MLDB. Copyright 2015 mldb.ai inc. All rights reserved.

/** basic_value_descriptions.cc

    Jeremy Barnes, 19 August 2015
*/

#pragma once
#include "value_description.h"

namespace MLDB {

template<typename Type, ValueKind Kind, class Description>
struct StrongComparableValueDescriptionI: public ValueDescriptionI<Type, Kind, Description> {
    static const Type & getValue(const void * val) { return *(const Type *)val; }

    virtual bool hasEqualityComparison() const override { return true; }
    virtual bool compareEquality(const void * val1, const void * val2) const override
    { 
        return getValue(val1) == getValue(val2);
    }
    virtual bool hasLessThanComparison() const override { return true; }
    virtual bool compareLessThan(const void * val1, const void * val2) const
    {
        return getValue(val1) < getValue(val2);
    }
    virtual bool hasStrongOrderingComparison() const
    {
        return true;
    }
    virtual std::strong_ordering compareStrong(const void * val1, const void * val2) const
    {
        return getValue(val1) <=> getValue(val2);
    }

    virtual bool hasWeakOrderingComparison() const { return true; }
    virtual std::weak_ordering compareWeak(const void * val1, const void * val2) const
    {
        return getValue(val1) <=> getValue(val2);
    }

    virtual bool hasPartialOrderingComparison() const { return true; }
    virtual std::partial_ordering comparePartial(const void * val1, const void * val2) const
    {
        return getValue(val1) <=> getValue(val2);
    }
};

template<typename Type, ValueKind Kind, class Description>
struct WeakComparableValueDescriptionI: public ValueDescriptionI<Type, Kind, Description> {
    static const Type & getValue(const void * val) { return *(const Type *)val; }

    virtual bool hasEqualityComparison() const override { return true; }
    virtual bool compareEquality(const void * val1, const void * val2) const override
    { 
        return getValue(val1) == getValue(val2);
    }
    virtual bool hasLessThanComparison() const override { return true; }
    virtual bool compareLessThan(const void * val1, const void * val2) const
    {
        return getValue(val1) < getValue(val2);
    }

    virtual bool hasWeakOrderingComparison() const { return true; }
    virtual std::weak_ordering compareWeak(const void * val1, const void * val2) const
    {
        return getValue(val1) <=> getValue(val2);
    }

    virtual bool hasPartialOrderingComparison() const { return true; }
    virtual std::partial_ordering comparePartial(const void * val1, const void * val2) const
    {
        return getValue(val1) <=> getValue(val2);
    }
};

template<typename Type, ValueKind Kind, class Description>
struct PartialComparableValueDescriptionI: public ValueDescriptionI<Type, Kind, Description> {
    static const Type & getValue(const void * val) { return *(const Type *)val; }

    virtual bool hasEqualityComparison() const override { return true; }
    virtual bool compareEquality(const void * val1, const void * val2) const override
    { 
        return getValue(val1) == getValue(val2);
    }
    virtual bool hasLessThanComparison() const override { return true; }
    virtual bool compareLessThan(const void * val1, const void * val2) const
    {
        return getValue(val1) < getValue(val2);
    }

    virtual bool hasPartialOrderingComparison() const { return true; }
    virtual std::partial_ordering comparePartial(const void * val1, const void * val2) const
    {
        return getValue(val1) <=> getValue(val2);
    }
};

template<typename Type, ValueKind Kind, class Description>
struct OrderedComparableValueDescriptionI: public ValueDescriptionI<Type, Kind, Description> {
    static const Type & getValue(const void * val) { return *(const Type *)val; }

    virtual bool hasEqualityComparison() const override { return true; }
    virtual bool compareEquality(const void * val1, const void * val2) const override
    { 
        return getValue(val1) == getValue(val2);
    }
    virtual bool hasLessThanComparison() const override { return true; }
    virtual bool compareLessThan(const void * val1, const void * val2) const
    {
        return getValue(val1) < getValue(val2);
    }
};

template<typename Type, ValueKind Kind, class Description>
struct EqualityComparableValueDescriptionI: public ValueDescriptionI<Type, Kind, Description> {
    static const Type & getValue(const void * val) { return *(const Type *)val; }

    virtual bool hasEqualityComparison() const override { return true; }
    virtual bool compareEquality(const void * val1, const void * val2) const override
    { 
        return getValue(val1) == getValue(val2);
    }
};

} // namespace MLDB
