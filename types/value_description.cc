// This file is part of MLDB. Copyright 2015 mldb.ai inc. All rights reserved.

/* value_description.cc                                            -*- C++ -*-
   Jeremy Barnes, 29 March 2013
   Copyright (c) 2013 mldb.ai inc.  All rights reserved.

   Code for description and introspection of values and structures.  Used
   to allow for automated formatters and parsers to be built.
*/


#include <mutex>
#include "mldb/base/exc_assert.h"
#include "value_description.h"
#include "structure_description.h"
#include <boost/lexical_cast.hpp>


using namespace std;


namespace MLDB {



std::ostream & operator << (std::ostream & stream, ValueKind kind)
{
    switch (kind) {
    case ValueKind::ATOM: return stream << "ATOM";
    case ValueKind::INTEGER: return stream << "INTEGER";
    case ValueKind::FLOAT: return stream << "FLOAT";
    case ValueKind::BOOLEAN: return stream << "BOOLEAN";
    case ValueKind::STRING: return stream << "STRING";
    case ValueKind::ENUM: return stream << "ENUM";
    case ValueKind::OPTIONAL: return stream << "OPTIONAL";
    case ValueKind::ARRAY: return stream << "ARRAY";
    case ValueKind::STRUCTURE: return stream << "STRUCTURE";
    case ValueKind::TUPLE: return stream << "TUPLE";
    case ValueKind::VARIANT: return stream << "VARIANT";
    case ValueKind::MAP: return stream << "MAP";
    case ValueKind::ANY: return stream << "ANY";
    default:
        return stream << "ValueKind(" << std::to_string((int)kind) << ")";
    }
}


/*****************************************************************************/
/* VALUE DESCRIPTION                                                         */
/*****************************************************************************/

ValueDescription::
ValueDescription(ValueKind kind,
                 const std::type_info * type,
                 const std::string & typeName)
    : kind(kind),
      type(type),
      typeName(typeName.empty() ? demangle(type->name()) : typeName),
      jsConverters(nullptr),
      jsConvertersInitialized(false)
{
}

ValueDescription::
~ValueDescription()
{
}
    
void
ValueDescription::
setTypeName(const std::string & newName)
{
    this->typeName = newName;
}

void *
ValueDescription::
optionalMakeValue(void * val) const
{
    throw MLDB::Exception("type is not optional");
}

const void *
ValueDescription::
optionalGetValue(const void * val) const
{
    throw MLDB::Exception("type is not optional");
}

size_t
ValueDescription::
getArrayLength(void * val) const
{
    throw MLDB::Exception("type is not an array");
}

void *
ValueDescription::
getArrayElement(void * val, uint32_t element) const
{
    throw MLDB::Exception("type is not an array");
}

std::vector<std::shared_ptr<const ValueDescription> >
ValueDescription::
getTupleElementDescriptions() const
{
    throw MLDB::Exception("type '" + typeName + "' is not a tuple " + MLDB::type_name(*this));
}

size_t
ValueDescription::
getTupleLength() const
{
    throw MLDB::Exception("type '" + typeName + "' is not a tuple " + MLDB::type_name(*this));
}

const void *
ValueDescription::
getArrayElement(const void * val, uint32_t element) const
{
    throw MLDB::Exception("type is not an array");
}

/** Return the value description for the nth array element.  This is
    necessary for tuple types, which don't have the same type for each
    element.
*/
const ValueDescription &
ValueDescription::
getArrayElementDescription(const void * val, uint32_t element) const
{
    return contained();
}

void
ValueDescription::
setArrayLength(void * val, size_t newLength) const
{
    throw MLDB::Exception("type is not an array");
}

const ValueDescription &
ValueDescription::
getKeyValueDescription() const
{
    throw MLDB::Exception("type '" + typeName + "' has no key");
}

const ValueDescription &
ValueDescription::
contained() const
{
    throw MLDB::Exception("type '" + typeName + "' does not contain another");
}

OwnershipModel
ValueDescription::
getOwnershipModel() const
{
    throw MLDB::Exception("type '" + typeName + "' does not define an ownership type");
}

void*
ValueDescription::
getLink(void* obj) const
{
    throw MLDB::Exception("type '" + typeName + "' is not a link");
}

void
ValueDescription::
set(void* obj, void* value, const ValueDescription* valueDesc) const
{
    throw MLDB::Exception("type '" + typeName + "' can't be written to");
}

size_t
ValueDescription::
getFieldCount(const void * val) const
{
    throw MLDB::Exception("type '" + typeName + "' doesn't support fields");
}

const ValueDescription::FieldDescription *
ValueDescription::
hasField(const void * val, const std::string & name) const
{
    throw MLDB::Exception("type '" + typeName + "' doesn't support fields");
}

void
ValueDescription::
forEachField(const void * val,
             const std::function<void (const FieldDescription &)> & onField) const
{
    throw MLDB::Exception("type '" + typeName + "' doesn't support fields");
}

const ValueDescription::FieldDescription & 
ValueDescription::
getField(const std::string & field) const
{
    throw MLDB::Exception("type '" + typeName + "' doesn't support fields");
}

const std::vector<std::string>
ValueDescription::
getEnumKeys() const
{
    throw MLDB::Exception("type '" + typeName + "' is not an enum");
}

std::vector<std::tuple<int, std::string, std::string> >
ValueDescription::
getEnumValues() const
{
    throw MLDB::Exception("type '" + typeName + "' is not an enum");
}

bool
ValueDescription::
isSame(const ValueDescription* other) const
{
    return type == other->type;
}

void
ValueDescription::
checkSame(const ValueDescription* other) const
{
    ExcCheck(isSame(other),
             "Wrong object type: "
             "expected <" + typeName + "> got <" + other->typeName + ">");
}


bool
ValueDescription::
isChildOf(const ValueDescription* base) const
{
    if (isSame(base)) return true;

    for (const auto& parent : parents)
        if (parent->isChildOf(base))
            return true;

    return false;
}

void
ValueDescription::
checkChildOf(const ValueDescription* base) const
{
    ExcCheck(isChildOf(base),
             "value of type " + typeName +
             " is not convertible to type " + typeName);
}

void
ValueDescription::
initialize()
{
    // Most values aren't recursive, so don't need an initialize() method.
}

namespace {

std::recursive_mutex registryMutex;
std::unordered_map<std::string, std::shared_ptr<const ValueDescription> > & registry()
{
    static std::unordered_map<std::string, std::shared_ptr<const ValueDescription> > result;
    return result;
}

} // file scope

std::shared_ptr<const ValueDescription>
ValueDescription::
get(std::string const & name)
{
    std::unique_lock<std::recursive_mutex> guard(registryMutex);
    auto i = registry().find(name);
    return registry().end() != i ? i->second : nullptr;
}

std::shared_ptr<const ValueDescription>
ValueDescription::
get(const std::type_info & type)
{
    return get(type.name());
}

void registerValueDescription(const std::type_info & type,
                              std::function<ValueDescription * ()> fn,
                              bool isDefault)
{
    registerValueDescription(type, fn, [] (ValueDescription &) {}, isDefault);
}

void
registerValueDescription(const std::type_info & type,
                         std::function<ValueDescription * ()> createFn,
                         std::function<void (ValueDescription &)> initFn,
                         bool isDefault)
{
    std::unique_lock<std::recursive_mutex> guard(registryMutex);

    std::shared_ptr<ValueDescription> desc(createFn());
    ExcAssert(desc);
    registry()[desc->typeName] = desc;
    registry()[type.name()] = desc;
    registry()[demangle(type.name())] = desc;
    initFn(*desc);
#if 0
    cerr << "type " << demangle(type.name())
         << " has description "
         << MLDB::type_name(*desc) << " default " << isDefault << endl;

    if (registry().count(type.name()))
        throw MLDB::Exception("attempt to double register "
                            + demangle(type.name()));
#endif
}

void
ValueDescription::
convertAndCopy(const void * from,
               const ValueDescription & fromDesc,
               void * to) const
{
    Json::Value val;
    StructuredJsonPrintingContext context(val);
    fromDesc.printJson(from, context);

    StructuredJsonParsingContext context2(val);
    parseJson(to, context2);
}

/*****************************************************************************/
/* STRUCTURE DESCRIPTION BASE                                                */
/*****************************************************************************/


StructureDescriptionBase::
StructureDescriptionBase(const std::type_info * type,
                         ValueDescription * owner,
                         const std::string & structName,
                         bool nullAccepted)
    : type(type),
      structName(structName.empty() ? demangle(type->name()) : structName),
      nullAccepted(nullAccepted),
      owner(owner)
{
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
    context.startObject();

    for (const auto & it: orderedFields) {
        auto & fd = it->second;

        auto mbr = addOffset(input, fd.offset);
        if (fd.description->isDefault(mbr))
            continue;
        context.startMember(it->first);
        fd.description->printJson(mbr, context);
    }
        
    context.endObject();
}


/*****************************************************************************/
/* BRIDGED VALUE DESCRIPTION                                                 */
/*****************************************************************************/

BridgedValueDescription::
BridgedValueDescription(std::shared_ptr<const ValueDescription> impl)
    : ValueDescription(impl->kind, impl->type, impl->typeName),
      impl(std::move(impl))
{
    this->documentationUri = this->impl->documentationUri;
}

BridgedValueDescription::
~BridgedValueDescription()
{
}

void
BridgedValueDescription::
parseJson(void * val, JsonParsingContext & context) const
{
    ExcAssert(impl);
    impl->parseJson(val, context);
}

void
BridgedValueDescription::
printJson(const void * val, JsonPrintingContext & context) const
{
    ExcAssert(impl);
    impl->printJson(val, context);
}

bool
BridgedValueDescription::
isDefault(const void * val) const
{
    ExcAssert(impl);
    return impl->isDefault(val);
}

void
BridgedValueDescription::
setDefault(void * val) const
{
    ExcAssert(impl);
    impl->setDefault(val);
}

void
BridgedValueDescription::
copyValue(const void * from, void * to) const
{
    ExcAssert(impl);
    impl->copyValue(from, to);
}

void
BridgedValueDescription::
moveValue(void * from, void * to) const
{
    ExcAssert(impl);
    impl->moveValue(from, to);
}

void
BridgedValueDescription::
swapValues(void * from, void * to) const
{
    ExcAssert(impl);
    swapValues(from, to);
}

void *
BridgedValueDescription::
constructDefault() const
{
    ExcAssert(impl);
    return impl->constructDefault();
}

void
BridgedValueDescription::
destroy(void * val) const
{
    ExcAssert(impl);
    impl->destroy(val);
}

void *
BridgedValueDescription::
optionalMakeValue(void * val) const
{
    ExcAssert(impl);
    return impl->optionalMakeValue(val);
}

const void *
BridgedValueDescription::
optionalGetValue(const void * val) const
{
    ExcAssert(impl);
    return impl->optionalGetValue(val);
}

size_t
BridgedValueDescription::
getArrayLength(void * val) const
{
    ExcAssert(impl);
    return impl->getArrayLength(val);
}

void *
BridgedValueDescription::
getArrayElement(void * val, uint32_t element) const
{
    ExcAssert(impl);
    return impl->getArrayElement(val, element);
}

const void *
BridgedValueDescription::
getArrayElement(const void * val, uint32_t element) const
{
    ExcAssert(impl);
    return impl->getArrayElement(val, element);
}

const ValueDescription &
BridgedValueDescription::
getArrayElementDescription(const void * val, uint32_t element) const
{
    ExcAssert(impl);
    return impl->getArrayElementDescription(val, element);
}

size_t
BridgedValueDescription::
getTupleLength() const
{
    ExcAssert(impl);
    return impl->getTupleLength();
}

std::vector<std::shared_ptr<const ValueDescription> >
BridgedValueDescription::
getTupleElementDescriptions() const
{
    ExcAssert(impl);
    return impl->getTupleElementDescriptions();
}

void
BridgedValueDescription::
setArrayLength(void * val, size_t newLength) const
{
    ExcAssert(impl);
    impl->setArrayLength(val, newLength);
}

const ValueDescription &
BridgedValueDescription::
getKeyValueDescription() const
{
    ExcAssert(impl);
    return impl->getKeyValueDescription();
}

const ValueDescription &
BridgedValueDescription::
contained() const
{
    ExcAssert(impl);
    return impl->contained();
}

OwnershipModel
BridgedValueDescription::
getOwnershipModel() const
{
    ExcAssert(impl);
    return impl->getOwnershipModel();
}

void*
BridgedValueDescription::
getLink(void* obj) const
{
    ExcAssert(impl);
    return impl->getLink(obj);
}

void
BridgedValueDescription::
set(void* obj, void* value, const ValueDescription* valueDesc) const
{
    ExcAssert(impl);
    impl->set(obj, value, valueDesc);
}

void
BridgedValueDescription::
convertAndCopy(const void * from,
               const ValueDescription & fromDesc,
               void * to) const
{
    ExcAssert(impl);
    impl->convertAndCopy(from, fromDesc, to);
}

size_t
BridgedValueDescription::
getFieldCount(const void * val) const
{
    ExcAssert(impl);
    return impl->getFieldCount(val);
}

const ValueDescription::FieldDescription *
BridgedValueDescription::
hasField(const void * val, const std::string & name) const
{
    ExcAssert(impl);
    return impl->hasField(val, name);
}

void
BridgedValueDescription::
forEachField(const void * val,
             const std::function<void (const FieldDescription &)> & onField) const
{
    ExcAssert(impl);
    impl->forEachField(val, onField);
}

const ValueDescription::FieldDescription & 
BridgedValueDescription::
getField(const std::string & field) const
{
    ExcAssert(impl);
    return impl->getField(field);
}

const std::vector<std::string>
BridgedValueDescription::
getEnumKeys() const
{
    ExcAssert(impl);
    return impl->getEnumKeys();
}

std::vector<std::tuple<int, std::string, std::string> >
BridgedValueDescription::
getEnumValues() const
{
    ExcAssert(impl);
    return impl->getEnumValues();
}

bool
BridgedValueDescription::
isSame(const ValueDescription* other) const
{
    ExcAssert(impl);
    return impl->isSame(other);
}

bool
BridgedValueDescription::
isChildOf(const ValueDescription* base) const
{
    ExcAssert(impl);
    return impl->isChildOf(base);
}

void
BridgedValueDescription::
initialize()
{
    // Impl should be initialized already...
}


/*****************************************************************************/
/* UTILITY FUNCTIONS                                                         */
/*****************************************************************************/

short int stringToKey(const std::string & str, short int *)
{
    return boost::lexical_cast<short int>(str);
}

unsigned short int stringToKey(const std::string & str, unsigned short int *)
{
    return boost::lexical_cast<unsigned short int>(str);
}

int stringToKey(const std::string & str, int *)
{
    return boost::lexical_cast<int>(str);
}

unsigned int stringToKey(const std::string & str, unsigned int *)
{
    return boost::lexical_cast<unsigned int>(str);
}

long int stringToKey(const std::string & str, long int *)
{
    return boost::lexical_cast<long int>(str);
}

unsigned long int stringToKey(const std::string & str, unsigned long int *)
{
    return boost::lexical_cast<unsigned long int>(str);
}

long long int stringToKey(const std::string & str, long long int *)
{
    return boost::lexical_cast<long long int>(str);
}

unsigned long long int stringToKey(const std::string & str, unsigned long long int *)
{
    return boost::lexical_cast<unsigned long long int>(str);
}


} // namespace MLDB
