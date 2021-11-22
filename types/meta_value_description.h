/** meta_value_description.h                                       -*- C++ -*-
    Jeremy Barnes, 4 January 2015
    This file is part of MLDB. Copyright 2015 mldb.ai inc. All rights reserved.

    Description of value descriptions.
*/

#pragma once

#include "mldb/types/value_description_fwd.h"
#include "optional.h"
#include <vector>
#include "mldb/ext/jsoncpp/value.h"

namespace MLDB {

// Return a JSON representation of the default value of this particular
// value description.
Json::Value getDefaultValue(const ValueDescription & description);

DECLARE_ENUM_DESCRIPTION(ValueKind);
DECLARE_ENUM_DESCRIPTION(OwnershipModel);
DECLARE_ENUM_DESCRIPTION(LengthModel);

struct EnumValueRepr {
    int val;
    std::string name;
    std::string comment;
};

DECLARE_STRUCTURE_DESCRIPTION(EnumValueRepr);

struct StructureFieldRepr {
    std::string fieldName;
    std::string comment;
    const ValueDescription * description = nullptr;
    Json::Value defaultValue;
    int offset;
};

DECLARE_STRUCTURE_DESCRIPTION(StructureFieldRepr);


struct ValueDescriptionRepr {
    ValueKind kind = ValueKind::ANY;
    std::string cppType;
    std::string typeName;
    std::string documentationUri;

    std::vector<StructureFieldRepr> structureFields;
    std::vector<EnumValueRepr> enumValues;
    std::vector<const ValueDescription *> tupleElements;
    const ValueDescription * contained = nullptr;
    Optional<LengthModel> lengthModel;
    Optional<OwnershipModel> elementModel;
    Optional<size_t> fixedLength;

    size_t width = 0;  // for atomic types
    size_t align = 0;  // for atomic types
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

