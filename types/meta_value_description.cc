// This file is part of MLDB. Copyright 2015 mldb.ai inc. All rights reserved.

/** meta_value_description.cc
    Jeremy Barnes, 4 January 2015
    Copyright (c) 2015 mldb.ai inc.  All rights reserved.

*/

#include "meta_value_description_impl.h"

#include "mldb/types/enum_description.h"
#include "mldb/types/structure_description.h"
#include "mldb/types/vector_description.h"
#include "mldb/types/pointer_description.h"


namespace MLDB {

DEFINE_ENUM_DESCRIPTION(ValueKind);

ValueKindDescription::
ValueKindDescription()
{
    addValue("ATOM",     ValueKind::ATOM,     "Atomic structured type; normally JSON");
    addValue("INTEGER",  ValueKind::INTEGER,  "Integral type");
    addValue("FLOAT",    ValueKind::INTEGER,  "Floating point type");
    addValue("BOOLEAN",  ValueKind::BOOLEAN,  "Boolean type");
    addValue("STRING",   ValueKind::STRING,   "String type");
    addValue("ENUM",     ValueKind::ENUM,     "Enumerated type");
    addValue("OPTIONAL", ValueKind::ENUM,     "Optional type");
    addValue("LINK",     ValueKind::ENUM,     "Link (reference to other object)");
    addValue("ARRAY",    ValueKind::ARRAY,    "Array type");
    addValue("STRUCTURE",ValueKind::STRUCTURE,"Structure type");
    addValue("TUPLE",    ValueKind::TUPLE,    "Tuple type");
    addValue("VARIANT",  ValueKind::VARIANT,  "Variant type");
    addValue("MAP",      ValueKind::MAP,      "Map (associative array) type");
    addValue("ANY",      ValueKind::ANY,      "Can be any type");
}

DEFINE_STRUCTURE_DESCRIPTION(EnumValueRepr);

EnumValueReprDescription::
EnumValueReprDescription()
{
    addField("val", &EnumValueRepr::val,
             "Integral value of enumeration");
    addField("name", &EnumValueRepr::name,
             "Name of enumeration value");
    addField("comment", &EnumValueRepr::comment,
             "Human-readable comment on meaning of value");
}

DEFINE_STRUCTURE_DESCRIPTION(StructureFieldRepr);

StructureFieldReprDescription::
StructureFieldReprDescription()
{
    addField("name", &StructureFieldRepr::fieldName,
             "Name of the field in the structure");
    addField("comment", &StructureFieldRepr::comment,
             "Comment on how to use the field");
    addField("type", &StructureFieldRepr::description,
             "Type of the field");
    addField("offset", &StructureFieldRepr::offset,
             "Offset of field in bytes from structure start");
    addField("default", &StructureFieldRepr::defaultValue,
             "Default value of field");
}

DEFINE_STRUCTURE_DESCRIPTION(ValueDescriptionRepr);

ValueDescriptionReprDescription::
ValueDescriptionReprDescription()
{
    addField("kind", &ValueDescriptionRepr::kind,
             "Broad categorization of type");
    addField("c++TypeName", &ValueDescriptionRepr::cppType,
             "C++ type name");
    addField("typeName", &ValueDescriptionRepr::typeName,
             "Readable human type name");
    addField("documentationUri", &ValueDescriptionRepr::documentationUri,
             "URI at which documentation is kept");
    //addField("parents", &ValueDescriptionRepr::parents,
    //         "Parent type names");
    addField("fields", &ValueDescriptionRepr::structureFields,
             "Fields of structure");
    addField("values", &ValueDescriptionRepr::enumValues,
             "Values of enum");
    addField("contained", &ValueDescriptionRepr::contained,
             "Type that is contained");
    addField("elements", &ValueDescriptionRepr::tupleElements,
             "Elements of tuple");
}

static Json::Value getDefaultValue(const ValueDescription & description)
{
    void * val = description.constructDefault();
    if (!val)
        return Json::Value();
    Json::Value jval;
    StructuredJsonPrintingContext context(jval);
    description.printJson(val, context);
    description.destroy(val);

    return jval;
}

static ValueDescriptionRepr
getRepr(const ValueDescription & desc, bool detailed)
{
    ValueDescriptionRepr result;
    result.kind = desc.kind;
    result.typeName = desc.typeName;
    if (desc.type && demangle(desc.type->name()) != desc.typeName)
        result.cppType = demangle(desc.type->name());
    result.documentationUri = desc.documentationUri;
    //result.parents = desc.parents;
    
    switch (desc.kind) {
    case ValueKind::ATOM:
    case ValueKind::INTEGER:
    case ValueKind::FLOAT:
    case ValueKind::BOOLEAN:
    case ValueKind::STRING:
        break;
    case ValueKind::ENUM:
        if (!detailed)
            break;
        for (auto & v: desc.getEnumValues()) {
            result.enumValues.emplace_back
                (EnumValueRepr{std::get<0>(v), std::get<1>(v), std::get<2>(v)});
        }
        break;
    case ValueKind::OPTIONAL:
        result.contained = &desc.contained();
        break;
    case ValueKind::LINK:
        result.contained = &desc.contained();
        break;
    case ValueKind::TUPLE:
        for (auto & d: desc.getTupleElementDescriptions())
            result.tupleElements.push_back(d.get());
        break;
    case ValueKind::ARRAY:
        result.contained = &desc.contained();
        break;
    case ValueKind::STRUCTURE: {
        if (!detailed)
            break;
        auto onField = [&] (const ValueDescription::FieldDescription & field)
            {
                StructureFieldRepr repr;
                repr.fieldName = field.fieldName;
                repr.comment = field.comment;
                repr.description = field.description.get();
                repr.offset = field.offset;
                repr.defaultValue = getDefaultValue(*field.description);
                result.structureFields.emplace_back(std::move(repr));
            };

        desc.forEachField(nullptr, onField);
        break;
    }
    case ValueKind::VARIANT:
    case ValueKind::MAP:
    case ValueKind::ANY:
        break;
    }

    return result;
}

std::shared_ptr<ValueDescriptionT<std::shared_ptr<const ValueDescription> > >
getValueDescriptionDescription(bool detailed)
{
    return std::make_shared<ValueDescriptionConstPtrDescription>(detailed);
}


ValueDescriptionT<std::shared_ptr<ValueDescription> > *
getDefaultDescription(std::shared_ptr<ValueDescription> * desc)
{
    return new ValueDescriptionPtrDescription();
}

ValueDescriptionT<std::shared_ptr<ValueDescription> > *
getDefaultDescriptionUninitialized(std::shared_ptr<ValueDescription> * desc)
{
    return new ValueDescriptionPtrDescription();
}

ValueDescriptionT<std::shared_ptr<const ValueDescription> > *
getDefaultDescription(std::shared_ptr<const ValueDescription> * desc)
{
    return new ValueDescriptionConstPtrDescription();
}

ValueDescriptionT<std::shared_ptr<const ValueDescription> > *
getDefaultDescriptionUninitialized(std::shared_ptr<const ValueDescription> * desc)
{
    return new ValueDescriptionConstPtrDescription();
}

ValueDescriptionT<ValueDescription *> *
getDefaultDescription(ValueDescription * * desc)
{
    return new ValueDescriptionNakedPtrDescription();
}

ValueDescriptionT<ValueDescription *> *
getDefaultDescriptionUninitialized(ValueDescription * * desc)
{
    return new ValueDescriptionNakedPtrDescription();
}

ValueDescriptionT<ValueDescription const *> *
getDefaultDescription(const ValueDescription * * desc)
{
    return new ValueDescriptionNakedConstPtrDescription();
}

ValueDescriptionT<ValueDescription const *> *
getDefaultDescriptionUninitialized(const ValueDescription * * desc)
{
    return new ValueDescriptionNakedConstPtrDescription();
}

void
ValueDescriptionPtrDescription::
parseJsonTyped(std::shared_ptr<ValueDescription> * val,
                    JsonParsingContext & context) const
{
    throw MLDB::Exception("Can't parse value descriptions");
}

void
ValueDescriptionPtrDescription::
printJsonTyped(const std::shared_ptr<ValueDescription> * val,
                    JsonPrintingContext & context) const
{
    if (!*val) {
        context.writeNull();
        return;
    }
    context.writeJson(jsonEncode(getRepr(**val, detailed)));
    //context.writeString((*val)->typeName);
}

bool
ValueDescriptionPtrDescription::
isDefaultTyped(const std::shared_ptr<ValueDescription> * val) const
{
    return !val->get();
}

void
ValueDescriptionConstPtrDescription::
parseJsonTyped(std::shared_ptr<const ValueDescription> * val,
                    JsonParsingContext & context) const
{
    throw MLDB::Exception("Can't parse value descriptions");
}

void
ValueDescriptionConstPtrDescription::
printJsonTyped(const std::shared_ptr<const ValueDescription> * val,
                    JsonPrintingContext & context) const
{
    if (!*val) {
        context.writeNull();
        return;
    }
    context.writeJson(jsonEncode(getRepr(**val, detailed)));
    //context.writeString((*val)->typeName);
}

bool
ValueDescriptionConstPtrDescription::
isDefaultTyped(const std::shared_ptr<const ValueDescription> * val) const
{
    return !val->get();
}


void
ValueDescriptionNakedPtrDescription::
parseJsonTyped(ValueDescription * * val,
                    JsonParsingContext & context) const
{
    throw MLDB::Exception("Can't parse value descriptions");
}

void
ValueDescriptionNakedPtrDescription::
printJsonTyped(ValueDescription * const * val,
               JsonPrintingContext & context) const
{
    if (!*val) {
        context.writeNull();
        return;
    }
    context.writeJson(jsonEncode(getRepr(**val, detailed)));
    //context.writeString((*val)->typeName);
}

bool
ValueDescriptionNakedPtrDescription::
isDefaultTyped(ValueDescription * const * val) const
{
    return !*val;
}

void
ValueDescriptionNakedConstPtrDescription::
parseJsonTyped(ValueDescription const * * val,
                    JsonParsingContext & context) const
{
    throw MLDB::Exception("Can't parse value descriptions");
}

void
ValueDescriptionNakedConstPtrDescription::
printJsonTyped(ValueDescription const * const * val,
               JsonPrintingContext & context) const
{
    if (!*val) {
        context.writeNull();
        return;
    }
    context.writeJson(jsonEncode(getRepr(**val, detailed)));
    //context.writeString((*val)->typeName);
}

bool
ValueDescriptionNakedConstPtrDescription::
isDefaultTyped(ValueDescription const * const * val) const
{
    return !*val;
}

} // namespace MLDB
