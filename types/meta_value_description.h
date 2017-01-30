/** meta_value_description.h                                       -*- C++ -*-
    Jeremy Barnes, 4 January 2015
    This file is part of MLDB. Copyright 2015 mldb.ai inc. All rights reserved.

    Description of value descriptions.
*/

#pragma once

#include "mldb/types/value_description_fwd.h"
#include <vector>
#include "mldb/ext/jsoncpp/value.h"

namespace MLDB {

DECLARE_ENUM_DESCRIPTION(ValueKind);

struct EnumValueRepr {
    int val;
    std::string name;
    std::string comment;
};

DECLARE_STRUCTURE_DESCRIPTION(EnumValueRepr);

struct StructureFieldRepr {
    StructureFieldRepr()
        : description(nullptr)
    {
    }

    std::string fieldName;
    std::string comment;
    const ValueDescription * description;
    Json::Value defaultValue;
    int offset;
};

DECLARE_STRUCTURE_DESCRIPTION(StructureFieldRepr);


struct ValueDescriptionRepr {
    ValueDescriptionRepr()
        : contained(nullptr)
    {
    }

    ValueKind kind;
    std::string cppType;
    std::string typeName;
    std::string documentationUri;

    std::vector<StructureFieldRepr> structureFields;
    std::vector<EnumValueRepr> enumValues;
    std::vector<const ValueDescription *> tupleElements;
    const ValueDescription * contained;
};

DECLARE_STRUCTURE_DESCRIPTION(ValueDescriptionRepr);

std::shared_ptr<ValueDescriptionT<std::shared_ptr<const ValueDescription> > >
getValueDescriptionDescription(bool detailed);

ValueDescriptionT<std::shared_ptr<ValueDescription> > *
getDefaultDescription(std::shared_ptr<ValueDescription> * desc);

ValueDescriptionT<std::shared_ptr<ValueDescription> > *
getDefaultDescriptionUninitalized(std::shared_ptr<ValueDescription> * desc);

ValueDescriptionT<std::shared_ptr<const ValueDescription> > *
getDefaultDescription(std::shared_ptr<const ValueDescription> * desc);

ValueDescriptionT<std::shared_ptr<const ValueDescription> > *
getDefaultDescriptionUninitialized(std::shared_ptr<const ValueDescription> * desc);

ValueDescriptionT<ValueDescription *> *
getDefaultDescription(ValueDescription * * desc);

ValueDescriptionT<ValueDescription *> *
getDefaultDescriptionUninitialized(ValueDescription * * desc);

ValueDescriptionT<ValueDescription const *> *
getDefaultDescription(const ValueDescription * * desc);

ValueDescriptionT<ValueDescription const *> *
getDefaultDescriptionUninitialized(const ValueDescription * * desc);

} // namespace MLDB

