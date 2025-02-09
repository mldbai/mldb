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
#include "mldb/types/optional_description.h"
#include "mldb/types/generic_array_description.h"
#include "mldb/types/generic_enum_description.h"

#include <iostream>

using namespace std;

namespace MLDB {

DEFINE_ENUM_DESCRIPTION_INLINE(ValueKind)
{
    addValue("ATOM",     ValueKind::ATOM,     "Atomic structured type; normally JSON");
    addValue("INTEGER",  ValueKind::INTEGER,  "Integral type");
    addValue("FLOAT",    ValueKind::FLOAT,    "Floating point type");
    addValue("BOOLEAN",  ValueKind::BOOLEAN,  "Boolean type");
    addValue("STRING",   ValueKind::STRING,   "String type");
    addValue("ENUM",     ValueKind::ENUM,     "Enumerated type");
    addValue("OPTIONAL", ValueKind::OPTIONAL, "Optional type");
    addValue("LINK",     ValueKind::LINK,     "Link (reference to other object)");
    addValue("ARRAY",    ValueKind::ARRAY,    "Array type");
    addValue("STRUCTURE",ValueKind::STRUCTURE,"Structure type");
    addValue("TUPLE",    ValueKind::TUPLE,    "Tuple type");
    addValue("VARIANT",  ValueKind::VARIANT,  "Variant type");
    addValue("MAP",      ValueKind::MAP,      "Map (associative array) type");
    addValue("ANY",      ValueKind::ANY,      "Can be any type");
}

DEFINE_ENUM_DESCRIPTION_INLINE(OwnershipModel)
{
    addValue("NONE",     OwnershipModel::NONE,      "Indirect values are not owned");
    addValue("SHARED",   OwnershipModel::SHARED,    "Ownership of indirect values is shared");
    addValue("UNIQUE",   OwnershipModel::UNIQUE,    "Ownership of indirect values is exclusive");
}

DEFINE_ENUM_DESCRIPTION_INLINE(LengthModel)
{
    addValue("FIXED",       LengthModel::FIXED,       "Length is fixed & implicit");
    addValue("VARIABLE",    LengthModel::VARIABLE,    "Length is variable & stored with data");
}

DEFINE_STRUCTURE_DESCRIPTION_INLINE(EnumValueRepr)
{
    addField("val", &EnumValueRepr::val,
             "Integral value of enumeration");
    addField("name", &EnumValueRepr::name,
             "Name of enumeration value");
    addField("comment", &EnumValueRepr::comment,
             "Human-readable comment on meaning of value");
}

DEFINE_STRUCTURE_DESCRIPTION_INLINE(StructureFieldRepr)
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

DEFINE_STRUCTURE_DESCRIPTION_INLINE(ValueDescriptionRepr)
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
    addField("lengthModel", &ValueDescriptionRepr::lengthModel,
             "Array or map length model");
    addField("elementModel", &ValueDescriptionRepr::elementModel,
             "Array element ownership model");
    addField("fixedLength", &ValueDescriptionRepr::fixedLength,
             "Array length when this is fixed and implicit in the definition");
    addField("width", &ValueDescriptionRepr::width,
             "width in bytes of an ATOM type", (size_t)0);
    addField("align", &ValueDescriptionRepr::align,
             "alignment in bytes of an ATOM type", (size_t)0);
}

Json::Value getDefaultValue(const ValueDescription & description)
{
    void * ptr = nullptr;
    if (description.align < 8) {
        ptr = malloc(description.width);
        if (!ptr) {
            MLDB_THROW_BAD_ALLOC("Couldn't allocate memory for default value");
        }
        ExcAssertEqual(((size_t)ptr) % description.align, 0);
    }
    else {
        int res = posix_memalign(&ptr, description.align, description.width);
        if (res != 0) {
            using namespace std;
            cerr << "align: " << description.align << endl;
            cerr << "width: " << description.width << endl;
            cerr << "type: " << description.typeName << endl;
            throw MLDB::Exception(errno, "posix_memalign");
        }
    }
    try {
        description.initializeDefault(ptr);
    } MLDB_CATCH_ALL {
        free(ptr);
        throw;
    }

    auto destruct = [desc=&description] (void * ptr)
    {
        try {
            desc->destruct(ptr);
        } MLDB_CATCH_ALL {
            free(ptr);
            throw;
        }
        free(ptr);
    };

    std::shared_ptr<void> result(ptr, std::move(destruct));

    Json::Value jval;
    StructuredJsonPrintingContext context(jval);
    description.printJson(result.get(), context);

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

    result.width = desc.width;
    result.align = desc.align;

    switch (desc.kind) {
    case ValueKind::INTEGER:
    case ValueKind::FLOAT:
    case ValueKind::BOOLEAN:
        break;
    case ValueKind::ATOM:
        //result.contained = desc.containedPtr().get();
        break;
    case ValueKind::STRING:
        break;
    case ValueKind::ENUM:
        if (!detailed)
            break;
        for (auto & v: desc.getEnumValues()) {
            result.enumValues.emplace_back
                (EnumValueRepr{std::get<0>(v), std::get<1>(v), std::get<2>(v)});
        }
        result.contained = &desc.contained();
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
        result.lengthModel.emplace(desc.getArrayLengthModel());
        result.elementModel.emplace(desc.getArrayIndirectionModel());

        switch (desc.getArrayLengthModel()) {
        case LengthModel::FIXED:
            result.fixedLength.emplace(desc.getArrayFixedLength());
            break;
        case LengthModel::VARIABLE:
            break;
        }
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
    static const auto reprDescription = getDefaultDescriptionSharedT<ValueDescriptionRepr>();
    ValueDescriptionRepr repr;
    reprDescription->parseJsonTyped(&repr, context);

    ValueDescription * valOut = nullptr;

    switch (repr.kind) {
    case ValueKind::INTEGER:
    case ValueKind::FLOAT:
    case ValueKind::BOOLEAN:
    case ValueKind::STRING:
        // These should already be here...
        *val = ValueDescription::get(repr.typeName).get();
        ExcAssert(*val);
        return;

    case ValueKind::ATOM:
        // May need to handle completely generically
        *val = ValueDescription::get(repr.typeName).get();
        if (!(*val)) {
            // We use the contained type instead, if it's there
            // So for example an atom which is a dressed-up integer will fall back to using the
            // underlying integer representation

            ExcAssert(repr.contained);
            std::shared_ptr<const ValueDescription> containedPtr(repr.contained, [] (auto) {});
            auto result = std::make_unique<BridgedValueDescription>(containedPtr);
            valOut = result.release();
            break;
        }
        return;

    case ValueKind::ENUM: {
        auto result = std::make_unique<GenericEnumDescription>(std::shared_ptr<const ValueDescription>(repr.contained, [] (auto) {}), repr.typeName);
        for (auto & val: repr.enumValues) {
            result->addValue(val.name, val.val, val.comment);
        }
        valOut = result.release();
        break;
    }    
    case ValueKind::ARRAY: {
        ExcAssert(repr.lengthModel);
        switch (*repr.lengthModel) {
        case LengthModel::FIXED: {
            ExcAssert(repr.contained);
            std::shared_ptr<const ValueDescription> containedPtr(repr.contained, [] (auto) {});
            auto result = std::make_unique<GenericFixedLengthArrayDescription>(repr.width, repr.align, repr.typeName, containedPtr, *repr.fixedLength);
            valOut = result.release();
            break;
        }
        case LengthModel::VARIABLE:
            throw MLDB::Exception("not clear how to deal with reconstituting value descriptions of variable length arrays");
        }
        break;
    }
    case ValueKind::STRUCTURE: {
        auto result = std::make_unique<GenericStructureDescription>(false /* null accepted */, repr.typeName);
        for (auto & field: repr.structureFields) {
            result->addFieldDesc(field.fieldName, field.offset, field.comment, std::shared_ptr<const ValueDescription>(field.description, [] (auto) {}));
        }
        valOut = result.release();
        break;
    }

    case ValueKind::OPTIONAL:
    case ValueKind::LINK:
    case ValueKind::TUPLE:
    case ValueKind::VARIANT:
    case ValueKind::MAP:
    case ValueKind::ANY:
        throw MLDB::Exception("can't parse value description for this type: " + repr.typeName + " " + jsonEncodeStr(repr));
    }
    ExcAssert(valOut);

    valOut->documentationUri = repr.documentationUri;
    valOut->typeName = repr.typeName;
    valOut->width = repr.width;
    valOut->align = repr.align;

    *val = valOut;
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
