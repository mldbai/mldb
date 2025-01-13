/** generic_array_description.h                                       -*- C++ -*-
    Jeremy Barnes, 4 January 2015
    This file is part of MLDB. Copyright 2015 mldb.ai inc. All rights reserved.

    Description of generic arrays
*/

#pragma once

#include "mldb/types/value_description.h"

namespace MLDB {

// Value description for a generic fixed-length array of values.  Does not need to mirror
// a C++ type.  Models std::array<Type, N>.

struct GenericFixedLengthArrayDescription
    : public ValueDescription {

    GenericFixedLengthArrayDescription(size_t width,
                                       size_t align,
                                       const std::string & typeName,
                                       std::shared_ptr<const ValueDescription> inner,
                                       size_t fixedLength);

    std::shared_ptr<const ValueDescription> inner;
    size_t fixedLength = 0;

    inline std::byte * getElement(void * valIn, size_t elNum) const
    {
        std::byte * val = (std::byte *)valIn;
        ExcAssertLess(elNum, fixedLength);
        return val + (inner->width * elNum);
    }

    inline const std::byte * getElement(const void * valIn, size_t elNum) const
    {
        const std::byte * val = (const std::byte *)valIn;
        ExcAssertLess(elNum, fixedLength);
        return val + (inner->width * elNum);    
    }

    virtual void parseJson(void * val,
                           JsonParsingContext & context) const override;
    virtual void printJson(const void * val,
                           JsonPrintingContext & context) const override;
    virtual bool isDefault(const void * val) const override;
    virtual LengthModel getArrayLengthModel() const override;
    virtual OwnershipModel getArrayIndirectionModel() const override;
    virtual size_t getArrayFixedLength() const override;
    virtual size_t getArrayLength(void * val) const override;
    virtual void * getArrayElement(void * val, uint32_t element) const override;
    virtual const void * getArrayElement(const void * val, uint32_t element) const override;
    virtual const ValueDescription & contained() const override;
    virtual std::shared_ptr<const ValueDescription> containedPtr() const override;
    virtual void setDefault(void * val) const override;
    virtual void copyValue(const void * from, void * to) const override;
    virtual void moveValue(void * from, void * to) const override;
    virtual void swapValues(void * from, void * to) const override;
    virtual void initializeDefault(void * mem) const override;
    virtual void initializeCopy(void * mem, const void * other) const override;
    virtual void initializeMove(void * mem, void * other) const override;
    virtual void destruct(void * mem) const override;
};

} // namespace MLDB

