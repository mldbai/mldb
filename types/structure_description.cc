/** structure_description.cc                                -*- C++ -*-
    Jeremy Barnes, 21 August 2015
    Copyright (c) 2015 mldb.ai inc.  All rights reserved.

    This file is part of MLDB. Copyright 2015 mldb.ai inc. All rights reserved.

    Value desriptions for structures.
*/

#include "structure_description.h"
#include <iostream>

using namespace std;

namespace MLDB {


/*****************************************************************************/
/* STRUCTURE DESCRIPTION BASE                                                */
/*****************************************************************************/


StructureDescriptionBase::
StructureDescriptionBase(const std::type_info * type,
                         ValueDescription * owner,
                         const std::string & structName,
                         bool nullAccepted)
    : type(type),
      structName(structName),
      nullAccepted(nullAccepted),
      owner(owner)
{
    ExcAssert(!structName.empty());
}

void
StructureDescriptionBase::
operator = (const StructureDescriptionBase & other)
{
    type = other.type;
    structName = other.structName;
    nullAccepted = other.nullAccepted;

    fieldNames.clear();
    orderedFields.clear();
    fields.clear();
    fieldNames.reserve(other.fields.size());

    // Don't set owner
    for (auto & f: other.orderedFields) {
        const char * s = f->first;
        fieldNames.emplace_back(::strdup(s));
        auto it = fields.insert(make_pair(fieldNames.back().get(), f->second))
            .first;
        orderedFields.push_back(it);
    }
}

void
StructureDescriptionBase::
operator = (StructureDescriptionBase && other)
{
    type = std::move(other.type);
    structName = std::move(other.structName);
    nullAccepted = std::move(other.nullAccepted);
    fields = std::move(other.fields);
    fieldNames = std::move(other.fieldNames);
    orderedFields = std::move(other.orderedFields);
    // don't set owner
}

StructureDescriptionBase::Exception::
Exception(JsonParsingContext & context,
          const std::string & message)
    : MLDB::Exception("at " + context.printPath() + ": " + message)
{
}

StructureDescriptionBase::Exception::
~Exception() throw ()
{
}

void
StructureDescriptionBase::
parseJson(void * output, JsonParsingContext & context) const
{
    try {

        if (!onEntry(output, context)) return;

        if (nullAccepted && context.isNull()) {
            context.expectNull();
            return;
        }
        
        if (!context.isObject()) {
            std::string typeName;
            if (context.isNumber())
                typeName = "number";
            else if (context.isBool())
                typeName = "boolean";
            else if (context.isString())
                typeName = "string";
            else if (context.isNull())
                typeName = "null";
            else if (context.isArray())
                typeName = "array";
            else typeName = "<<unknown type>>";
                    
            std::string msg
                = "expected object of type "
                + structName + ", but instead a "
                + typeName + " was provided";

            if (context.isString())
                msg += ".  Did you accidentally JSON encode your object into a string?";

            context.exception(msg);
        }

        auto onMember = [&] ()
            {
                try {
                    auto n = context.fieldNamePtr();

                    auto it = fields.find(n);
                    if (it == fields.end()) {
                        context.onUnknownField(owner);
                    }
                    else {
                        it->second.description
                        ->parseJson(addOffset(output,
                                              it->second.offset),
                                    context);
                    }
                }
                catch (const Exception & exc) {
                    throw;
                }
                catch (const std::exception & exc) {
                    throw Exception(context, exc.what());
                }
                catch (...) {
                    throw;
                }
            };

        
        context.forEachMember(onMember);

        onExit(output, context);
    }
    catch (const Exception & exc) {
        throw;
    }
    catch (const std::exception & exc) {
        throw Exception(context, exc.what());
    }
    catch (...) {
        throw;
    }
}

void
StructureDescriptionBase::
printJson(const void * input, JsonPrintingContext & context) const
{
    //cerr << "--- Start object " << demangle(this->type->name()) << " at " << input << endl;

    context.startObject();

    for (const auto & it: orderedFields) {
        auto & fd = it->second;
        const void * mbr = addOffset(input, fd.offset);

        //cerr << "field " << demangle(this->type->name()) << "::" << fd.fieldName << " at " << mbr << endl;
        //const unsigned char * mbrc = (const unsigned char *)mbr;
        //cerr << "  first 4 bytes: " << format("%02x %02x %02x %02x", mbrc[0], mbrc[1], mbrc[2], mbrc[3]) << endl;

        // Skip inactive fields
        if (fd.isActive && !fd.isActive(input)) {
            //cerr << "  skipping field " << fd.fieldName << " as isActive is false" << endl;
            continue;
        }

        if (fd.bitField.has_value()) {
            auto & bitField = *fd.bitField;
            char data[fd.description->width];
            memset(data, 0, fd.description->width);            
            //bitField.extract(mbr, data);
            fd.description->extractBitField(mbr, data, bitField.startBit, bitField.bitWidth);
            //cerr << "Extracting bit field " << it->first << ": bytes ";
            //const unsigned char * mbrBytes = (const unsigned char *)mbr;
            //cerr << format("%02x %02x %02x %02x", mbrBytes[0], mbrBytes[1], mbrBytes[2], mbrBytes[3]);
            //cerr << " bits " << bitField.startBit << ":" << bitField.bitWidth << " = " << fd.description->printJsonString(data) << endl;
            context.startMember(it->first);
            fd.description->printJson(data, context);
            //cerr << "  bit field value " << fd.description->printJsonString(data) << endl;
        }
        else {
            if (fd.description->isDefault(mbr)) {
                //cerr << "  skipping due to default value" << endl;
                continue;
            }
            context.startMember(it->first);
            fd.description->printJson(mbr, context);
            //cerr << "  member value " << fd.description->printJsonString(mbr);
        }
    }
        
    context.endObject();
}

// Comparison object to allow const char * objects to be looked up
// in the map and so for comparisons to be done with no memory
// allocations.
bool
StructureDescriptionBase::StrCompare::
operator () (const char * s1, const char * s2) const
{
    char c1 = *s1++, c2 = *s2++;

    if (c1 < c2) return true;
    if (c1 > c2) return false;
    if (c1 == 0) return false;

    c1 = *s1++; c2 = *s2++;
    
    if (c1 < c2) return true;
    if (c1 > c2) return false;
    if (c1 == 0) return false;

    return std::strcmp(s1, s2) < 0;
}

StructureDescriptionBase::FieldDescription &
StructureDescriptionBase::
addFieldDesc(std::string name,
             size_t offset,
             std::string comment,
             std::shared_ptr<const ValueDescription> description)
{
    ExcAssert(description);

    if (fields.count(name.c_str()))
        throw MLDB::Exception("field '" + name + "' added twice");

    fieldNames.emplace_back(::strdup(name.c_str()));
    const char * fieldName = fieldNames.back().get();
    
    auto it = fields.insert
        (Fields::value_type(fieldName, FieldDescription()))
        .first;
    
    FieldDescription & fd = it->second;
    fd.fieldName = fieldName;
    fd.comment = comment;
    fd.description = description;
    fd.offset = offset;
    fd.width = description->width;
    fd.fieldNum = fields.size() - 1;
    orderedFields.push_back(it);
    //using namespace std;
    //cerr << "offset = " << fd.offset << endl;

    fixupAlign(offset + description->width, description->align);

    hasLess = hasLess && description->hasLessThanComparison();
    hasEquality = hasEquality && description->hasLessThanComparison();
    hasPartial = hasPartial && description->hasPartialOrderingComparison();
    hasWeak = hasWeak && description->hasWeakOrderingComparison();
    hasStrong = hasStrong && description->hasStrongOrderingComparison();

    return fd;
}

/// Add a bit field, specified by the containing field and a bit offset
void
StructureDescriptionBase::
addBitFieldDesc(std::string name,
                size_t offset,
                std::string comment,
                std::shared_ptr<const ValueDescription> containingDescription,
                uint32_t bitOffset,
                uint32_t bitWidth)
{
    size_t containingWidth = containingDescription->width * 8;
    ExcAssertLessEqual(bitOffset + bitWidth, containingWidth);
    FieldDescription & fd = addFieldDesc(std::move(name), offset, std::move(comment), containingDescription);

#if 0
    auto extract = [=] (const void * obj, void * val)
    {
        containingDescription->extractBitField(obj, val, bitOffset, bitWidth);
    };

    auto insert = [=] (void * obj, const void * val)
    {
        containingDescription->insertBitField(val, obj, bitOffset, bitWidth);
    };
#endif

    ValueDescription::BitFieldDescription bitFieldDescription = { bitOffset, bitWidth };

    fd.bitField.emplace(std::move(bitFieldDescription));
}                

/// Add a discriminated field, including the function used to know whether it is active
/// or not and a string description of that function.
void
StructureDescriptionBase::
addDiscriminatedFieldDesc(std::string name,
                          size_t offset,
                          std::string comment,
                          std::shared_ptr<const ValueDescription> desc,
                          std::function<bool (const void *)> isActive,
                          std::string isActiveStr)
{
    FieldDescription & fd = addFieldDesc(std::move(name), offset, std::move(comment), std::move(desc));
    fd.isActive = std::move(isActive);
    fd.isActiveStr = std::move(isActiveStr);
}                          

const StructureDescriptionBase::FieldDescription *
StructureDescriptionBase::
hasField(const void * val, const std::string & field) const
{
    auto it = fields.find(field.c_str());
    if (it != fields.end())
        return &it->second;
    return nullptr;
}

const StructureDescriptionBase::FieldDescription *
StructureDescriptionBase::
getFieldDescription(const void * val, const void * field) const
{
    ssize_t offset = (const char *)field - (const char *)val;
    for (auto & f: fields) {
        if (f.second.offset >= offset
            && f.second.offset + f.second.width <= offset)
            return &f.second;
    }
    return nullptr;
}

void
StructureDescriptionBase::
forEachField(const void * val,
             const std::function<void (const FieldDescription &)> & onField) const
{
    for (auto f: orderedFields) {
        onField(f->second);
    }
}

const StructureDescriptionBase::FieldDescription & 
StructureDescriptionBase::
getField(const std::string & field) const
{
    auto it = fields.find(field.c_str());
    if (it != fields.end())
        return it->second;
    throw MLDB::Exception("structure has no field " + field);
}

const StructureDescriptionBase::FieldDescription & 
StructureDescriptionBase::
getFieldByNumber(int fieldNum) const
{
    for (auto & f: fields) {
        if (f.second.fieldNum == fieldNum)
            return f.second;
    }

    throw MLDB::Exception("structure has no field with given number");
}

int
StructureDescriptionBase::
getVersion() const
{
    return this->version;
}

bool
StructureDescriptionBase::
hasEqualityComparison() const
{
    return hasEquality;
}

bool
StructureDescriptionBase::
compareEquality(const void * val1, const void * val2) const
{
    for (auto & [name,f]: fields) {
        if (!f.description->compareEquality(f.getFieldPtr(val1), f.getFieldPtr(val2)))
            return false;
    }
    return true;
}

bool
StructureDescriptionBase::
hasLessThanComparison() const
{
    return hasLess;
}

bool
StructureDescriptionBase::
compareLessThan(const void * val1, const void * val2) const
{
    for (auto it: orderedFields) {
        auto [name,f] = *it;
        //using namespace std;
        //cerr << "field " << name << ": "
        //     << f.description->printJsonString(f.getFieldPtr(val1)) << " vs "
        //     << f.description->printJsonString(f.getFieldPtr(val2)) << ": "
        //     << f.description->compareLessThan(f.getFieldPtr(val1), f.getFieldPtr(val2))
        //     << " vs " << f.description->compareLessThan(f.getFieldPtr(val2), f.getFieldPtr(val1))
        //     << endl;
        if (f.description->compareLessThan(f.getFieldPtr(val1), f.getFieldPtr(val2)))
            return true;
        if (f.description->compareLessThan(f.getFieldPtr(val2), f.getFieldPtr(val1)))
            return false;
    }
    return false;
}

bool
StructureDescriptionBase::
hasStrongOrderingComparison() const
{
    return hasStrong;
}

std::strong_ordering
StructureDescriptionBase::
compareStrong(const void * val1, const void * val2) const
{
    throw MLDB::Exception("not implemented: StructureDescriptionBase::compareStrong");
}

bool
StructureDescriptionBase::
hasWeakOrderingComparison() const
{
    return hasWeak;
}

std::weak_ordering
StructureDescriptionBase::
compareWeak(const void * val1, const void * val2) const
{
    throw MLDB::Exception("not implemented: StructureDescriptionBase::compareWeak");
}

bool
StructureDescriptionBase::
hasPartialOrderingComparison() const
{
    return hasPartial;
}

std::partial_ordering
StructureDescriptionBase::
comparePartial(const void * val1, const void * val2) const
{
    throw MLDB::Exception("not implemented: StructureDescriptionBase::comparePartial");
}



/*****************************************************************************/
/* GENERIC STRUCTURE DESCRIPTION                                             */
/*****************************************************************************/

GenericStructureDescription::
GenericStructureDescription(bool nullAccepted,
                            const std::string & structName)
    : ValueDescription(ValueKind::STRUCTURE, nullptr, 0 /* width */, 0 /* align */, structName),
      StructureDescriptionBase(nullptr, this, structName, nullAccepted)
{
}

void
GenericStructureDescription::
parseJson(void * val, JsonParsingContext & context) const
{
    return StructureDescriptionBase::parseJson(val, context);
}

void
GenericStructureDescription::
printJson(const void * val, JsonPrintingContext & context) const
{
    return StructureDescriptionBase::printJson(val, context);
}

bool
GenericStructureDescription::
isDefault(const void * val) const
{
    return false;
}

void
GenericStructureDescription::
setDefault(void * val) const
{
    for (auto & [name, desc]: this->fields) {
        auto field = desc.getFieldPtr(val);
        desc.description->setDefault(field);
    }
}

void
GenericStructureDescription::
copyValue(const void * from, void * to) const
{
    for (auto & [name, desc]: this->fields) {
        auto fromField = desc.getFieldPtr(from);
        auto toField = desc.getFieldPtr(to);
        desc.description->copyValue(fromField, toField);
    }
}

void
GenericStructureDescription::
moveValue(void * from, void * to) const
{
    for (auto & [name, desc]: this->fields) {
        auto fromField = desc.getFieldPtr(from);
        auto toField = desc.getFieldPtr(to);
        desc.description->moveValue(fromField, toField);
    }
}

void
GenericStructureDescription::
swapValues(void * from, void * to) const
{
    for (auto & [name, desc]: this->fields) {
        auto fromField = desc.getFieldPtr(from);
        auto toField = desc.getFieldPtr(to);
        desc.description->swapValues(fromField, toField);
    }
}

void
GenericStructureDescription::
initializeDefault(void * mem) const
{
    for (auto & [name, desc]: this->fields) {
        desc.description->initializeDefault(desc.getFieldPtr(mem));
    }
}

void
GenericStructureDescription::
initializeCopy(void * mem, const void * val) const
{
    for (auto & [name, desc]: this->fields) {
        auto fromField = desc.getFieldPtr(val);
        auto toField = desc.getFieldPtr(mem);
        desc.description->initializeCopy(toField, fromField);
    }
}

void
GenericStructureDescription::
initializeMove(void * mem, void * val) const
{
    for (auto & [name, desc]: this->fields) {
        auto fromField = desc.getFieldPtr(val);
        auto toField = desc.getFieldPtr(mem);
        desc.description->initializeMove(toField, fromField);
    }
}

void
GenericStructureDescription::
destruct(void * val) const
{
    for (auto & [name, desc]: this->fields) {
        auto field = desc.getFieldPtr(val);
        desc.description->destruct(field);
    }
}

bool
GenericStructureDescription::
onEntry(void * output, JsonParsingContext & context) const
{
    return true;
}

void
GenericStructureDescription::
onExit(void * output, JsonParsingContext & context) const
{
}

void
GenericStructureDescription::
fixupAlign(size_t knownWidth, size_t knownAlign)
{
    if (knownAlign > this->align) {
        this->align = knownAlign;
    }

    if (knownWidth > this->width) {
        this->width = knownWidth;
        while (this->width % this->align != 0)
            ++this->width;
    }
}

} // namespace MLDB
