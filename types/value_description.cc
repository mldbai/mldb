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
                 uint32_t width,
                 uint32_t align,
                 const std::string & typeName)
    : kind(kind),
      type(type),
      width(width),
      align(align),
      typeName(typeName.empty() ? (type ? demangle(type->name()) : "") : typeName),
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

Json::Value
ValueDescription::
printJsonStructured(const void * val) const
{
    Json::Value result;
    StructuredJsonPrintingContext context(result);
    printJson(val, context);
    return result;
}

Utf8String
ValueDescription::
printJsonString(const void * val) const
{
    Utf8String result;
    Utf8StringJsonPrintingContext context(result);
    printJson(val, context);
    return result;
}

bool
ValueDescription::
hasEqualityComparison() const
{
    return false;
}

bool
ValueDescription::
compareEquality(const void * val1, const void * val2) const
{
    MLDB_THROW_UNIMPLEMENTED("type " + typeName + " does not support equality comparison");
}

bool
ValueDescription::
hasLessThanComparison() const
{
    return false;
}

bool
ValueDescription::
compareLessThan(const void * val1, const void * val2) const
{
    MLDB_THROW_UNIMPLEMENTED("type " + typeName + " does not support less than comparison");
}

bool
ValueDescription::
hasStrongOrderingComparison() const
{
    return false;
}

std::strong_ordering
ValueDescription::
compareStrong(const void * val1, const void * val2) const
{
    MLDB_THROW_UNIMPLEMENTED("type " + typeName + " does not support strongly ordered comparison");
}

bool
ValueDescription::
hasWeakOrderingComparison() const
{
    return false;
}

std::weak_ordering
ValueDescription::
compareWeak(const void * val1, const void * val2) const
{
    MLDB_THROW_UNIMPLEMENTED("type " + typeName + " does not support weakly ordered comparison");
}

bool
ValueDescription::
hasPartialOrderingComparison() const
{
    return false;
}

std::partial_ordering
ValueDescription::
comparePartial(const void * val1, const void * val2) const
{
    MLDB_THROW_UNIMPLEMENTED("type " + typeName + " does not support partially ordered comparison");
}
void *
ValueDescription::
optionalMakeValue(void * val) const
{
    MLDB_THROW_UNIMPLEMENTED("type is not optional");
}

const void *
ValueDescription::
optionalGetValue(const void * val) const
{
    MLDB_THROW_UNIMPLEMENTED("type is not optional");
}

size_t
ValueDescription::
getArrayLength(void * val) const
{
    MLDB_THROW_UNIMPLEMENTED("type " + typeName + " is not an array: " + type_name(*this));
}

size_t
ValueDescription::
getArrayFixedLength() const
{
    MLDB_THROW_UNIMPLEMENTED("type " + typeName + " is not an array: " + type_name(*this));
}

LengthModel
ValueDescription::
getArrayLengthModel() const
{
    MLDB_THROW_UNIMPLEMENTED("type " + typeName + " is not an array: " + type_name(*this));
}

OwnershipModel
ValueDescription::
getArrayIndirectionModel() const
{
    MLDB_THROW_UNIMPLEMENTED("type " + typeName + " is not an array: " + type_name(*this));
}

void *
ValueDescription::
getArrayElement(void * val, uint32_t element) const
{
    MLDB_THROW_UNIMPLEMENTED("type " + typeName + " is not an array");
}

std::vector<std::shared_ptr<const ValueDescription> >
ValueDescription::
getTupleElementDescriptions() const
{
    MLDB_THROW_UNIMPLEMENTED("type '" + typeName + "' is not a tuple " + MLDB::type_name(*this));
}

size_t
ValueDescription::
getTupleLength() const
{
    MLDB_THROW_UNIMPLEMENTED("type '" + typeName + "' is not a tuple " + MLDB::type_name(*this));
}

const void *
ValueDescription::
getArrayElement(const void * val, uint32_t element) const
{
    MLDB_THROW_UNIMPLEMENTED("type " + typeName + " is not an array");
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
    MLDB_THROW_UNIMPLEMENTED("type " + typeName + " is not an array");
}

const ValueDescription &
ValueDescription::
getKeyValueDescription() const
{
    MLDB_THROW_UNIMPLEMENTED("type '" + typeName + "' has no key");
}

const ValueDescription &
ValueDescription::
contained() const
{
    MLDB_THROW_UNIMPLEMENTED("type '" + typeName + "' does not contain another");
}

std::shared_ptr<const ValueDescription>
ValueDescription::
containedPtr() const
{
    MLDB_THROW_UNIMPLEMENTED("type '" + typeName + "' does not contain another");
}

OwnershipModel
ValueDescription::
getOwnershipModel() const
{
    MLDB_THROW_UNIMPLEMENTED("type '" + typeName + "' does not define an ownership type");
}

void*
ValueDescription::
getLink(void* obj) const
{
    MLDB_THROW_UNIMPLEMENTED("type '" + typeName + "' is not a link");
}

const void*
ValueDescription::
getConstLink(const void* obj) const
{
    MLDB_THROW_UNIMPLEMENTED("type '" + typeName + "' is not a link");
}

void
ValueDescription::
set(void* obj, void* value, const ValueDescription* valueDesc) const
{
    MLDB_THROW_UNIMPLEMENTED("type '" + typeName + "' can't be written to");
}

size_t
ValueDescription::
getFieldCount(const void * val) const
{
    MLDB_THROW_UNIMPLEMENTED("type '" + typeName + "' doesn't support fields");
}

bool
ValueDescription::
hasFixedFieldCount() const
{
    MLDB_THROW_UNIMPLEMENTED("type '" + typeName + "' doesn't support fields");
}

size_t
ValueDescription::
getFixedFieldCount() const
{
    MLDB_THROW_UNIMPLEMENTED("type '" + typeName + "' doesn't support fields");
}

const ValueDescription::FieldDescription *
ValueDescription::
hasField(const void * val, const std::string & name) const
{
    MLDB_THROW_UNIMPLEMENTED("type '" + typeName + "' doesn't support fields");
}

const ValueDescription::FieldDescription *
ValueDescription::
getFieldDescription(const void * val, const void * field) const
{
    MLDB_THROW_UNIMPLEMENTED("type '" + typeName + "' doesn't support fields");
}

void
ValueDescription::
forEachField(const void * val,
             const std::function<void (const FieldDescription &)> & onField) const
{
    MLDB_THROW_UNIMPLEMENTED("type '" + typeName + "' doesn't support fields");
}

const ValueDescription::FieldDescription & 
ValueDescription::
getField(const std::string & field) const
{
    MLDB_THROW_UNIMPLEMENTED("type '" + typeName + "' doesn't support fields");
}

const ValueDescription::FieldDescription & 
ValueDescription::
getFieldByNumber(int fieldNum) const
{
    MLDB_THROW_UNIMPLEMENTED("type '" + typeName + "' doesn't support fields");
}

const std::vector<std::string>
ValueDescription::
getEnumKeys() const
{
    MLDB_THROW_UNIMPLEMENTED("type '" + typeName + "' is not an enum");
}

std::vector<std::tuple<int, std::string, std::string> >
ValueDescription::
getEnumValues() const
{
    MLDB_THROW_UNIMPLEMENTED("type '" + typeName + "' is not an enum");
}

int
ValueDescription::
getVersion() const
{
    return -1;
}

bool
ValueDescription::
isSame(const ValueDescription* other) const
{
    return type && other->type && *type == *other->type;
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

void ValueDescriptionInitBase::initialize(ValueDescription & desc)
{
    desc.initialize();
}

void registerValueDescription(const std::type_info & type,
                              std::function<ValueDescription * ()> fn,
                              bool isDefault)
{
    registerValueDescription(type, fn, ValueDescriptionInitBase::initialize, isDefault);
}

void registerValueDescriptionFunctions(const std::type_info & type,
                              ValueDescription * (*create) (),
                              bool isDefault)
{
    registerValueDescription(type, create, ValueDescriptionInitBase::initialize, isDefault);
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
registerValueDescriptionFunctions(const std::type_info & type,
                         ValueDescription * (*create) (),
                         void (*initialize) (ValueDescription &),
                         bool isDefault)
{
    std::unique_lock<std::recursive_mutex> guard(registryMutex);

    std::shared_ptr<ValueDescription> desc(create());
    ExcAssert(desc);
    registry()[desc->typeName] = desc;
    registry()[type.name()] = desc;
    registry()[demangle(type.name())] = desc;
    initialize(*desc);
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
/* BRIDGED VALUE DESCRIPTION                                                 */
/*****************************************************************************/

BridgedValueDescription::
BridgedValueDescription(std::shared_ptr<const ValueDescription> impl)
    : ValueDescription(impl->kind, impl->type, impl->width, impl->align, impl->typeName),
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

void
BridgedValueDescription::
initializeDefault(void * mem) const
{
    ExcAssert(impl);
    impl->initializeDefault(mem);
}

void
BridgedValueDescription::
initializeCopy(void * mem, const void * from) const
{
    ExcAssert(impl);
    return impl->initializeCopy(mem, from);
}

void
BridgedValueDescription::
initializeMove(void * mem, void * from) const
{
    ExcAssert(impl);
    return impl->initializeMove(mem, from);
}

void
BridgedValueDescription::
destruct(void * val) const
{
    ExcAssert(impl);
    impl->destruct(val);
}

void *
BridgedValueDescription::
optionalMakeValue(void * val) const
{
    ExcAssert(impl);
    return impl->optionalMakeValue(val);
}

bool
BridgedValueDescription::
hasEqualityComparison() const
{
    ExcAssert(impl);
    return impl->hasEqualityComparison();
}

bool
BridgedValueDescription::
compareEquality(const void * val1, const void * val2) const
{
    ExcAssert(impl);
    return impl->compareEquality(val1, val2);
}

bool
BridgedValueDescription::
hasLessThanComparison() const
{
    ExcAssert(impl);
    return impl->hasLessThanComparison();
}

bool
BridgedValueDescription::
compareLessThan(const void * val1, const void * val2) const
{
    ExcAssert(impl);
    return impl->compareLessThan(val1, val2);
}

bool
BridgedValueDescription::
hasStrongOrderingComparison() const
{
    ExcAssert(impl);
    return impl->hasStrongOrderingComparison();
}

std::strong_ordering
BridgedValueDescription::
compareStrong(const void * val1, const void * val2) const
{
    ExcAssert(impl);
    return impl->compareStrong(val1, val2);
}

bool
BridgedValueDescription::
hasWeakOrderingComparison() const
{
    ExcAssert(impl);
    return impl->hasWeakOrderingComparison();
}

std::weak_ordering
BridgedValueDescription::
compareWeak(const void * val1, const void * val2) const
{
    ExcAssert(impl);
    return impl->compareWeak(val1, val2);
}

bool
BridgedValueDescription::
hasPartialOrderingComparison() const
{
    ExcAssert(impl);
    return impl->hasPartialOrderingComparison();
}

std::partial_ordering
BridgedValueDescription::
comparePartial(const void * val1, const void * val2) const
{
    ExcAssert(impl);
    return impl->comparePartial(val1, val2);
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
getArrayFixedLength() const
{
    ExcAssert(impl);
    return impl->getArrayFixedLength();
}

size_t
BridgedValueDescription::
getArrayLength(void * val) const
{
    ExcAssert(impl);
    return impl->getArrayLength(val);
}

LengthModel
BridgedValueDescription::
getArrayLengthModel() const
{
    ExcAssert(impl);
    return impl->getArrayLengthModel();
}

OwnershipModel
BridgedValueDescription::
getArrayIndirectionModel() const
{
    ExcAssert(impl);
    return impl->getArrayIndirectionModel();
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

const void*
BridgedValueDescription::
getConstLink(const void* obj) const
{
    ExcAssert(impl);
    return impl->getConstLink(obj);
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

template<typename Int, typename Res>
static Int doParseInt(const std::string & str,
                      Res (&fn) (const std::string &, std::size_t *, int))
{
    size_t n;
    Res res = fn(str, &n, 10 /* base */);
    if (n != str.size()) {
        throw std::invalid_argument("string does not contain an integer");
    }
    Int res2 = res;
    if ((Res)res2 != res) {
        throw std::out_of_range("integer is out of range");
    }
    return res2;
}

short int stringToKey(const std::string & str, short int *)
{
    return doParseInt<short int>(str, std::stoi);
}

unsigned short int stringToKey(const std::string & str, unsigned short int *)
{
    return doParseInt<unsigned short int>(str, std::stoul);
}

int stringToKey(const std::string & str, int *)
{
    return doParseInt<int>(str, std::stoi);
}

unsigned int stringToKey(const std::string & str, unsigned int *)
{
    return doParseInt<unsigned int>(str, std::stoul);
}

long int stringToKey(const std::string & str, long int *)
{
    return doParseInt<long int>(str, std::stol);
}

unsigned long int stringToKey(const std::string & str, unsigned long int *)
{
    return doParseInt<unsigned long int>(str, std::stoul);
}

long long int stringToKey(const std::string & str, long long int *)
{
    return doParseInt<long long int>(str, std::stoll);
}

unsigned long long int stringToKey(const std::string & str, unsigned long long int *)
{
    return doParseInt<unsigned long long int>(str, std::stoull);
}


} // namespace MLDB
