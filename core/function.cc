/* function.cc
   Jeremy Barnes, 21 January 2014
   Copyright (c) 2014 Datacratic Inc.  All rights reserved.

   This file is part of MLDB. Copyright 2015 Datacratic. All rights reserved.

   Function support.
*/

#include "mldb/core/function.h"
#include "mldb/types/basic_value_descriptions.h"
#include "mldb/types/meta_value_description.h"
#include "mldb/sql/sql_expression.h"
#include "mldb/sql/sql_expression_operations.h"
#include "mldb/types/map_description.h"
#include "mldb/types/any_impl.h"
#include "mldb/rest/rest_request_router.h"


using namespace std;


namespace Datacratic {

#if 0
using MLDB::Coord;


/*****************************************************************************/
/* VALUE MAP KEY                                                             */
/*****************************************************************************/

ValueMapKey::
ValueMapKey()
    : utf8Start(nullptr), utf8Len(0)
{
}

ValueMapKey::
ValueMapKey(Utf8String strIn)
    : str(std::move(strIn)), utf8Start(str.rawData()),
      utf8Len(str.rawLength())
{
}

ValueMapKey::
ValueMapKey(const Coord & id)
    : str(id.toUtf8String()), utf8Start(str.rawData()),
      utf8Len(str.rawLength())
{
}

ValueMapKey::
ValueMapKey(const char * utf8Start, size_t utf8Len)
    : utf8Start(utf8Start), utf8Len(utf8Len)
{
}

ValueMapKey::
ValueMapKey(const char * utf8Start)
    : utf8Start(utf8Start), utf8Len(strlen(utf8Start))
{
}

ValueMapKey::
ValueMapKey(std::string strIn)
    : str(std::move(strIn)),
      utf8Start(str.rawData()),
      utf8Len(str.rawLength())
{
}

ValueMapKey
ValueMapKey::
ref(const Utf8String & name)
{
    return ValueMapKey(name.rawData(), name.rawLength());
}

ValueMapKey
ValueMapKey::
ref(const std::string & name)
{
    return ValueMapKey(name.data(), name.length());
}

ValueMapKey
ValueMapKey::
ref(const Coord & name)
{
    // TODO: really a reference...
    return ValueMapKey(name);
}

ValueMapKey
ValueMapKey::
ref(const char * name)
{
    return ValueMapKey(name);
}

Coord
ValueMapKey::
toCoord() const
{
    return Coord(utf8Start, utf8Len);
}

Utf8String
ValueMapKey::
toUtf8String() const
{
    if (!str.empty())
        return str;
    return rawString();  // TODO: we may be able to avoid a check here
}

std::string
ValueMapKey::
rawString() const
{
    if (!str.empty())
        return str.rawString();
    return string(utf8Start, utf8Start + utf8Len);
}
    
bool
ValueMapKey::
startsWith(const Utf8String & str) const
{
    if (utf8Len < str.rawLength())
        return false;
    return strncmp(utf8Start, str.rawData(), str.rawLength()) == 0;
}

bool
ValueMapKey::
operator < (const ValueMapKey & other) const
{
    size_t toCmp = std::min(utf8Len, other.utf8Len);
    int res = strncmp(utf8Start, other.utf8Start, toCmp);
    bool result = (res < 0 || (res == 0 && utf8Len < other.utf8Len));
    if (false && result != (toUtf8String() < other.toUtf8String())) {
        cerr << "comparing " << toUtf8String() << " with " << other.toUtf8String()
             << endl;
        cerr << "toCmp = " << toCmp << endl;
        cerr << "res = " << res << endl;
        abort();
    }
    return result;
}

bool
ValueMapKey::
operator == (const ValueMapKey & other) const
{
    if (utf8Len != other.utf8Len)
        return false;
    return strncmp(utf8Start, other.utf8Start, utf8Len) == 0;
}

ValueMapKey stringToKey(const std::string & str, ValueMapKey *)
{
    return ValueMapKey(str);
}

std::string keyToString(const ValueMapKey & key)
{
    return key.rawString();
}

//std::string keyToString(ValueMapKey && key);

struct ValueMapKeyDescription 
    : public ValueDescriptionI<Datacratic::ValueMapKey, ValueKind::ATOM, ValueMapKeyDescription> {

    virtual void parseJsonTyped(Datacratic::ValueMapKey * val,
                                JsonParsingContext & context) const
    {
        *val = context.expectStringUtf8();
    }

    virtual void printJsonTyped(const Datacratic::ValueMapKey * val,
                                JsonPrintingContext & context) const
    {
        context.writeStringUtf8(val->toUtf8String());
    }

    virtual bool isDefaultTyped(const Datacratic::ValueMapKey * val) const
    {
        return val->empty();
    }
};

DEFINE_VALUE_DESCRIPTION(ValueMapKey, ValueMapKeyDescription);

#endif

namespace MLDB {

#if 0
/*****************************************************************************/
/* FUNCTION STEP INFO                                                        */
/*****************************************************************************/

DEFINE_STRUCTURE_DESCRIPTION(FunctionValueInfo);

FunctionValueInfoDescription::
FunctionValueInfoDescription()
{
    addField("valueInfo", &FunctionValueInfo::valueInfo,
             "Information about the type and domain of the value");
}

DEFINE_STRUCTURE_DESCRIPTION(FunctionValues);

FunctionValuesDescription::
FunctionValuesDescription()
{
    addField("values", &FunctionValues::values,
             "Values that step touches");
}
#endif

DEFINE_STRUCTURE_DESCRIPTION(FunctionInfo);

FunctionInfoDescription::
FunctionInfoDescription()
{
    addField("input", &FunctionInfo::input,
             "Input values that the function reads from");
    addField("output", &FunctionInfo::output,
             "Output values that the function writes to");
}

DEFINE_STRUCTURE_DESCRIPTION_NAMED(FunctionPolyConfigDescription, PolyConfigT<Function>);

FunctionPolyConfigDescription::
FunctionPolyConfigDescription()
{
    addField("id", &PolyConfig::id,
             "ID of the function.  This indicates what name the function will "
             "be called when accessed via the REST API.  If emtpy, a name "
             "will be automatically created.");
    addField("type", &PolyConfig::type,
             "Type of the function.  This indicates what implementation type "
             "to use for the function.");
    addFieldDesc("params", &PolyConfig::params,
                 "Function configuration parameters.  This is always an object, and "
                 "the type of object accepted depends upon the 'type' field.",
                 getBareAnyDescription());
    addField("persistent", &PolyConfig::persistent,
             "If true, then this function will have its configuration stored "
             "and will be reloaded on startup", false);

    setTypeName("FunctionConfig");
    documentationUri = "/doc/builtin/functions/FunctionConfig.md";
}

#if 0

/*****************************************************************************/
/* FUNCTION OUTPUT                                                           */
/*****************************************************************************/

DEFINE_STRUCTURE_DESCRIPTION(FunctionOutput);

FunctionOutputDescription::
FunctionOutputDescription()
{
    addField("values", &FunctionOutput::values,
             "Returned values");
}

FunctionOutput::
FunctionOutput(ExpressionValue val)
{
    auto onColumn = [&] (Coord & columnName,
                         ExpressionValue & val)
        {
            this->set(std::move(columnName), std::move(val));
            return true;
        };

    val.forEachColumnDestructive(onColumn);
}

FunctionOutput & 
FunctionOutput::
operator = (ExpressionValue val)
{
    FunctionOutput newMe(std::move(val));
    values.swap(newMe.values);
    return *this;
}

void
FunctionOutput::
set(ValueMapKey key, ExpressionValue value)
{
    auto it = values.find(key);
    if (it != values.end()) {
        throw HttpReturnException(400, "Attempt to change the value '"
                                  + key.toUtf8String() + "' from current value of "
                                  + jsonEncodeStr(it->second)
                                  + " to " + jsonEncodeStr(value)
                                  + ".  Values may only be written once.");
    }
    values[key] = std::move(value);
}

void
FunctionOutput::
update(const FunctionOutput & output)
{
    for (auto & u: output.values) {
        if (values.count(u.first))
            throw HttpReturnException(400, "Attempt to double write to value '"
                                      + u.first.toUtf8String() + "'");
        values[u.first] = u.second;
    }
}

void
FunctionOutput::
update(FunctionOutput && output)
{
    if (values.empty()) {
        values = std::move(output.values);
        return;
    }

    for (auto & u: output.values) {
        if (values.count(u.first))
            throw HttpReturnException(400, "Attempt to double write to value '"
                                      + u.first.toUtf8String() + "'");
        values[u.first] = std::move(u.second);
    }
}


/*****************************************************************************/
/* FUNCTION CONTEXT                                                          */
/*****************************************************************************/

const ExpressionValue FunctionContext::NONE;

const ExpressionValue *
FunctionContext::
findValue(const ColumnName & name) const
{
    ExcAssert(!name.empty());

    auto key = ValueMapKey::ref(name[0]);

    auto it = values.find(key);
    if (it == values.end()) {
        return nullptr;
    }
    else if (name.size() == 1) {
        return &it->second;
    }
    else {
        return it->second.findNestedColumn(name.removePrefix(name[0])); 
    }             
}

const ExpressionValue *
FunctionContext::
findValue(const Coord & name) const
{
    ExcAssert(!name.empty());

    auto key = ValueMapKey::ref(name);

    auto it = values.find(key);
    if (it == values.end()) {
        return nullptr;
    }
    return &it->second;
}

const ExpressionValue *
FunctionContext::
findValue(const Utf8String & name) const
{
    return findValue(Coord(name));
}

const ExpressionValue *
FunctionContext::
findValue(const std::string & name) const
{
    return findValue(Coord(name));
}

const ExpressionValue *
FunctionContext::
findValue(const char * name) const
{
    return findValue(Coord(name, strlen(name)));
}

void
FunctionContext::
initializeFrom(const FunctionContext & other)
{
    for (auto & p: values) {
        auto it = other.values.find(p.first);
        if (it != other.values.end() && !it->second.empty())
            p.second = it->second;
    }
}

ExpressionValue
FunctionContext::
get(const Utf8String & name) const
{
    auto key = ValueMapKey::ref(name);
    auto it = values.find(key);
    if (it == values.end()) {
        throw HttpReturnException(400, "couldn't find named value '" + name + "'",
                                  "context", *this);
    }
    if (it->second.empty()) {
        return ExpressionValue();
    }
    return it->second;
}

ExpressionValue
FunctionContext::
get(const Coord & name) const
{
    auto key = ValueMapKey::ref(name);
    auto it = values.find(key);
    if (it == values.end()) {
        throw HttpReturnException(400, "couldn't find named value '"
                                  + name.toUtf8String() + "'",
                                  "context", *this);
    }
    if (it->second.empty()) {
        return ExpressionValue();
    }
    return it->second;
}

ExpressionValue
FunctionContext::
getValueOrNull(const Utf8String & name) const
{
    auto key = ValueMapKey::ref(name);
    return getValueOrNull(key);
}

ExpressionValue
FunctionContext::
getValueOrNull(const Coord & name) const
{
    auto key = ValueMapKey::ref(name);
    return getValueOrNull(key);
}

ExpressionValue
FunctionContext::
getValueOrNull(const char * name) const
{
    auto key = ValueMapKey::ref(name);
    return getValueOrNull(key);
}

ExpressionValue
FunctionContext::
getValueOrNull(const ValueMapKey & key) const
{
    auto it = values.find(key);
    if (it == values.end()) {
        return ExpressionValue();
    }
    if (it->second.empty()) {
        return ExpressionValue();
    }
    return it->second;
}

CellValue
FunctionContext::
getTyped(const Utf8String & name, CellValue *) const
{
    return get(name).getAtom();
}

ExpressionValue
FunctionContext::
getTyped(const Utf8String & name, ExpressionValue *) const
{
    return get(name);
}

RowValue
FunctionContext::
getTyped(const Utf8String & name, RowValue *) const
{
    // Find everything prefixed with this name
    auto it = values.lower_bound(name);
    if (it == values.end())
        return RowValue();

    RowValue result;

    if (it->first == name) {
        // The value was represented directly
        it->second.appendToRow(ColumnName(), result);
        return result;
    }

    while (it != values.end() && it->first.startsWith(name + ".")) {
        it->second.appendToRow(ColumnName(""), result);
        ++it;
    }

    return result;
}

void
FunctionContext::
update(const FunctionOutput & output)
{
    for (auto & u: output.values) {
        values[u.first] = u.second;
    }
}

void
FunctionContext::
update(FunctionOutput && output)
{
    if (values.empty()) {
        values = std::move(output.values);
        return;
    }

    for (auto & u: output.values) {
        values[u.first] = std::move(u.second);
    }
}

DEFINE_STRUCTURE_DESCRIPTION(FunctionContext);

FunctionContextDescription::
FunctionContextDescription()
{
    addField("arguments", &FunctionContext::values,
             "Current arguments");
}


/*****************************************************************************/
/* FUNCTION VALUE INFO                                                       */
/*****************************************************************************/

void
FunctionValueInfo::
merge(const FunctionValueInfo & other)
{
    valueInfo = ExpressionValueInfo::getCovering(valueInfo, other.valueInfo);
}


/*****************************************************************************/
/* FUNCTION VALUES                                                           */
/*****************************************************************************/

FunctionValues::
FunctionValues(const ExpressionValueInfo & rowInfo)
{
    for (auto & col: rowInfo.getKnownColumns()) {
        addValue(col.columnName.toUtf8String(), col.valueInfo);
    }
}

void
FunctionValues::
addValue(const Utf8String & name,
         std::shared_ptr<ExpressionValueInfo> valueInfo)
{
    if (!this->values.insert({name, valueInfo}).second)
        throw HttpReturnException(400, "Attempt to add value '" + name + "' twice");
}

void
FunctionValues::
addEmbeddingValue(const std::string & name,
                  ssize_t numDimensions)
{
    addValue(Utf8String(name), std::make_shared<EmbeddingValueInfo>(numDimensions));
}

void
FunctionValues::
addRowValue(const std::string & name)
{
    addValue(Utf8String(name),
           std::make_shared<RowValueInfo>(vector<KnownColumn>(),
                                          SCHEMA_OPEN));
}

void
FunctionValues::
addRowValue(const std::string & name,
          const std::vector<KnownColumn> & knownColumns,
          SchemaCompleteness completeness)
{
    addValue(Utf8String(name),
           std::make_shared<RowValueInfo>(knownColumns, completeness));
}
   
void
FunctionValues::
addAtomValue(const std::string & name)
{
    addValue(Utf8String(name), std::make_shared<AtomValueInfo>());
}

void
FunctionValues::
addStringValue(const std::string & name)
{
    addValue(Utf8String(name), std::make_shared<Utf8StringValueInfo>());
}

void
FunctionValues::
addTimestampValue(const std::string & name)
{
    addValue(Utf8String(name), std::make_shared<TimestampValueInfo>());
}

void
FunctionValues::
addBlobValue(const std::string & name)
{
    addValue(Utf8String(name), std::make_shared<BlobValueInfo>());
}

void
FunctionValues::
addNumericValue(const std::string & name)
{
    addValue(Utf8String(name), std::make_shared<NumericValueInfo>());
}

const FunctionValueInfo &
FunctionValues::
getValueInfo(const Utf8String & name) const
{
    auto key = ValueMapKey::ref(name);
    auto it = values.find(key);
    if (it == values.end())
        throw HttpReturnException(400, "value '" + name + "' not found");
    return it->second;
}

void
FunctionValues::
checkValueCompatibleAsInputTo(const Utf8String & otherName,
                            const FunctionValueInfo & otherValueInfo) const
{
    auto it = values.find(otherName);
    if (it == values.end())
        throw HttpReturnException
            (400, "value '" + otherName
             + "' is required as input to a following function, but was not "
             + "available in the passed input.");

    // Check it's actually convertible to the other value type
    if (!it->second.valueInfo->isConvertibleTo(*otherValueInfo.valueInfo)) {
        throw HttpReturnException
            (400, "value '" + otherName
             + "' cannot be converted from its given type to the required "
             + "type.");
    }
}

void
FunctionValues::
checkCompatibleAsInputTo(const FunctionValues & expectedInput) const
{
    for (auto & p: expectedInput.values) {
        checkValueCompatibleAsInputTo(p.first.toUtf8String(), p.second);
    }

    // TODO: check for extra unknown values
}

void
FunctionValues::
merge(const FunctionValues & other)
{
    for (auto & p: other.values) {
        FunctionValueInfo & info = values[p.first];
        if (!info.valueInfo)
            info = p.second;
        else {
            info.merge(p.second);
        }
    }
}

std::shared_ptr<RowValueInfo>
FunctionValues::
toRowInfo() const
{
    vector<KnownColumn> knownColumns;

    for (auto & p: values) {
        knownColumns.emplace_back(p.first.toCoord(),
                                  p.second.valueInfo,
                                  COLUMN_IS_DENSE);
    }
    
    return std::make_shared<RowValueInfo>(knownColumns, SCHEMA_CLOSED);
}

#endif

/*****************************************************************************/
/* FUNCTION APPLIER                                                          */
/*****************************************************************************/

FunctionOutput
FunctionApplier::
apply(const FunctionContext & input) const
{ 
    ExcAssert(function);
    return function->apply(*this, input);
}


/*****************************************************************************/
/* FUNCTION                                                                  */
/*****************************************************************************/

Function::
Function(MldbServer * server)
    : server(server)
{
}

Function::
~Function()
{
}

Any
Function::
getStatus() const
{
    return Any();
}

Any
Function::
getDetails() const
{
    return Any();
}


std::unique_ptr<FunctionApplier>
Function::
bind(SqlBindingScope & outerContext,
     const FunctionValues & input) const
{
    std::unique_ptr<FunctionApplier> result(new FunctionApplier());
    result->function = this;
    result->info = getFunctionInfo();

    // TO RESOLVE BEFORE MERGE
    throw HttpReturnException(600, "Function::bind(): check compatibility");
#if 0
    // Check that all values on the passed input are compatible with the required
    // inputs.
    for (auto & p: result->info.input.values) {
        input.checkValueCompatibleAsInputTo(p.first.toUtf8String(), p.second);
    }
#endif

    return result;
}

FunctionInfo
Function::
getFunctionInfo() const
{
    throw HttpReturnException(400, "Function " + ML::type_name(*this)
                        + " needs to override getFunctionInfo()");
}

RestRequestMatchResult
Function::
handleRequest(RestConnection & connection,
              const RestRequest & request,
              RestRequestParsingContext & context) const
{
    Json::Value error;
    error["error"] = "Function of type '" + ML::type_name(*this)
        + "' does not respond to custom route '" + context.remaining + "'";
    error["details"]["verb"] = request.verb;
    error["details"]["resource"] = request.resource;
    connection.sendErrorResponse(400, error);
    return RestRequestRouter::MR_ERROR;
}

} // namespace MLDB
} // namespace Datacratic
