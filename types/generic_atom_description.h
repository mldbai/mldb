/** generic_array_description.h                                       -*- C++ -*-
    Jeremy Barnes, 4 January 2015
    This file is part of MLDB. Copyright 2015 mldb.ai inc. All rights reserved.

    Description of generic arrays
*/

#pragma once

#include "mldb/types/value_description.h"

namespace MLDB {

// Value description for a generic atomic type (whose representation is trivial) with a fixed
// number of bytes.

struct GenericAtomDescription
    : public ValueDescription {

    GenericAtomDescription(size_t width,
                           size_t align,
                           const std::string & typeName);

    virtual void parseJson(void * val,
                           JsonParsingContext & context) const override;
    virtual void printJson(const void * val,
                           JsonPrintingContext & context) const override;
    virtual bool isDefault(const void * val) const override;
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

