/** operation.cc                                                   -*- C++ -*-
    Jeremy Barnes, 4 May 2018
    Copyright (c) 2018 Element AI Inc.  All rights reserved.
    This file is part of MLDB. Copyright 2018 mldb.ai inc. All rights reserved.

    Definition of operations for data processing.
*/

#include "operation.h"

namespace MLDB {


void recordOperation(const Operation & op)
{
    Operation::context().recordOperation(op);
}

void recordVariable(const Variable & var)
{
    Operation::context().recordVariable(var);
}

thread_local std::vector<OperationContext *>
OperationContext::contextStack;

inline OperationContext &
Operation::context()
{
    OperationContext * current = OperationContext::getCurrentContext();
    if (!current) {
        throw MLDB::Exception("Attempt to create operation with no context");
    }
    return *current;
}

std::mutex AbstractDataType::typesMutex;
std::map<Utf8String, std::shared_ptr<const AbstractDataType> >
AbstractDataType::types;

struct InitBlock {
    InitBlock()
    {
        auto adt = std::make_shared<AbstractDataType>("block");
        handle = AbstractDataType::registerType("block", adt);
    }

    std::shared_ptr<void> handle;
} initBlock;

struct InitRow {
    InitRow()
    {
        auto adt = std::make_shared<AbstractDataType>("row");
        handle = AbstractDataType::registerType("row", adt);
    }

    std::shared_ptr<void> handle;
} initRow;

#if 0
struct ImageType: public AbstractDataType {
    virtual void width(Block objectData) const = 0;

    virtual void height(Block objectData) const = 0;
};
#endif

struct InitImage {
    InitImage()
    {
        auto adt = std::make_shared<AbstractDataType>("image");
        adt->addMethodT<Scalar ()>("width");
        adt->addMethodT<Scalar ()>("height");
        handle = AbstractDataType::registerType("image", adt);
    }

    std::shared_ptr<void> handle;
} initImage;

} // namespace MLDB
