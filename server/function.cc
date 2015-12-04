// This file is part of MLDB. Copyright 2015 Datacratic. All rights reserved.

/* function.cc
   Jeremy Barnes, 21 January 2014
   Copyright (c) 2014 Datacratic Inc.  All rights reserved.

   Function support.
*/

#include "mldb/server/function.h"
#include "mldb/server/function_collection.h"
#include "mldb/server/mldb_server.h"
#include "mldb/types/basic_value_descriptions.h"
#include "mldb/types/meta_value_description.h"
#include "mldb/sql/sql_expression.h"
#include "mldb/sql/sql_expression_operations.h"
#include "mldb/server/function_contexts.h"
#include "mldb/types/map_description.h"
#include "mldb/types/any_impl.h"

using namespace std;


namespace Datacratic {


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
ValueMapKey(const Id & id)
    : str(id.toUtf8String()), utf8Start(str.rawData()),
      utf8Len(str.rawLength())
{
}

ValueMapKey::
ValueMapKey(const char * utf8Start, size_t utf8Len)
    : utf8Start(utf8Start), utf8Len(utf8Len)
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
ref(const Id & name)
{
    if (name.type == Id::SHORTSTR || name.type == Id::STR)
        return ValueMapKey(name.stringData(), name.toStringLength());
    else return ValueMapKey(name.toUtf8String());
}

Id
ValueMapKey::
toId() const
{
    return Id(utf8Start, utf8Len);
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


namespace MLDB {


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
    auto onColumn = [&] (Id & columnName,
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
findValue(const Utf8String & name) const
{
    auto key = ValueMapKey::ref(name);

    auto it = values.find(key);
    if (it == values.end())
    {
        auto jt = name.find('.');
        if (jt != name.end())
        {
            //"x.y form"
            Utf8String head(name.begin(), jt);
            ++jt;
            auto kt = values.find(head);

            if (kt != values.end())
            {
                Utf8String tail(jt, name.end());

                return kt->second.findNestedField(tail); 
            }             

        }

        return nullptr;
    }
       
    return &it->second;
}

const ExpressionValue *
FunctionContext::
findValue(const std::string & name) const
{
    return findValue(Utf8String(name));
}

const ExpressionValue *
FunctionContext::
findValue(const ColumnName & name) const
{
    return findValue(name.toUtf8String());
}

const ExpressionValue *
FunctionContext::
findValue(const char * name) const
{
    return findValue(Utf8String(name));
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
        // Look for a path, ie x.y
        auto it2 = name.find('.');
        if (it2 != name.end()) {
            // Split it.  Look for that field
            Utf8String key(name.begin(), it2);
            it = values.find(key);
            if (it != values.end()) {
                Utf8String val(boost::next(it2), name.end());
                return it->second.getField(val);
            }
        }
        
        throw HttpReturnException(400, "couldn't find named value '" + name + "'",
                                  "context", *this);
    }
    if (it->second.empty()) {
        return ExpressionValue();
        throw HttpReturnException(400, "Value '" + name
                                  + "' was read but has not been written",
                                  "value", name);
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
getValueOrNull(const ColumnName & name) const
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
        Utf8String name = key.toUtf8String();
        // Look for a path, ie x.y
        auto it2 = name.find('.');
        if (it2 != name.end()) {
            // Split it.  Look for that field
            Utf8String key(name.begin(), it2);
            it = values.find(key);
            if (it != values.end()) {
                Utf8String val(boost::next(it2), name.end());
                return it->second.getField(val);
            }
        }
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
        
        it->second.appendToRow(ColumnName(""), result);
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
/* FUNCTION VALUE INFO                                                         */
/*****************************************************************************/

void
FunctionValueInfo::
merge(const FunctionValueInfo & other)
{
    valueInfo = ExpressionValueInfo::getCovering(valueInfo, other.valueInfo);
}


/*****************************************************************************/
/* FUNCTION VALUES                                                             */
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
        knownColumns.emplace_back(p.first.toId(), p.second.valueInfo,
                                  COLUMN_IS_DENSE);
    }

    return std::make_shared<RowValueInfo>(knownColumns, SCHEMA_CLOSED);
}


/*****************************************************************************/
/* FUNCTION INFO                                                             */
/*****************************************************************************/

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

    // Check that all values on the passed input are compatible with the required
    // inputs.
    for (auto & p: result->info.input.values) {
        input.checkValueCompatibleAsInputTo(p.first.toUtf8String(), p.second);
    }

    return result;
}

FunctionInfo
Function::
getFunctionInfo() const
{
    throw HttpReturnException(400, "Function " + ML::type_name(*this)
                        + " needs to override getFunctionInfo()");
}


/*****************************************************************************/
/* SERIAL FUNCTION STEP CONFIG                                                  */
/*****************************************************************************/

SerialFunctionStepConfig::
SerialFunctionStepConfig()
    : with(SelectExpression::parse("*")),
      extract(SelectExpression::parse("*"))
{
}

DEFINE_STRUCTURE_DESCRIPTION(SerialFunctionStepConfig);

SerialFunctionStepConfigDescription::
SerialFunctionStepConfigDescription()
{
    addParent<PolyConfigT<Function> >();

    addField("with", &SerialFunctionStepConfig::with,
             "SQL select expression that provides the input values"
             "to the function.  This can be used to "
             "connect values up to output values of previous functions that don't "
             "necessarily have the same names, or to calculate values in "
             "place.  The default will extract all values from all previous "
             "functions and input values and make them available to values with the "
             "same name.",
             SelectExpression::parse("*"));
    addField("extract", &SerialFunctionStepConfig::extract,
             "SQL select expression that extracts values from the output values "
             "of the function and adds them to the final output of the function.  "
             "This allows for the output of values to be renamed, modified or "
             "ignored.  The default will extract all output values of the function "
             "and add them with the same name to the Serial function's result.",
             SelectExpression::parse("*"));
}


/*****************************************************************************/
/* SERIAL FUNCTION CONFIG                                                       */
/*****************************************************************************/

DEFINE_STRUCTURE_DESCRIPTION(SerialFunctionConfig);

SerialFunctionConfigDescription::
SerialFunctionConfigDescription()
{
    addField("steps", &SerialFunctionConfig::steps,
             "A list of functions that will be executed in the given order in "
             "order to produce the output of the function.");
}


/*****************************************************************************/
/* FUNCTION STEP APPLIER                                                        */
/*****************************************************************************/

struct FunctionStepApplier::Itl {
    Itl(SqlBindingScope & outerContext,
        const FunctionValues & input,
        const Function & function,
        const SelectExpression & with,
        const SelectExpression & extract)
        : withBindingContext(outerContext, input),
          boundWith(with.bind(withBindingContext)),
          applier(function.bind(withBindingContext, *boundWith.info)),
          extractBindingContext(outerContext, applier->info.output),
          boundExtract(extract.bind(extractBindingContext)),
          outputValues(*boundExtract.info)
    {
    }

    FunctionExpressionContext withBindingContext;
    BoundSqlExpression boundWith;
    std::unique_ptr<FunctionApplier> applier;
    FunctionExpressionContext extractBindingContext;
    BoundSqlExpression boundExtract;
    FunctionValues outputValues;
};

FunctionStepApplier::
FunctionStepApplier(SqlBindingScope & outerContext,
                    const FunctionValues & input,
                    const Function & function,
                    const SelectExpression & with,
                    const SelectExpression & extract)
    : function(&function),
      itl(new Itl(outerContext, input, function, with, extract))
{
}

FunctionStepApplier::
~FunctionStepApplier()
{
}

FunctionOutput
FunctionStepApplier::
apply(const FunctionContext & input) const
{
    //cerr << "input is " << jsonEncode(input) << endl;
            
    // Run the WITH expression to get our values from the input
    auto withRow = itl->withBindingContext.getRowContext(input);
    FunctionOutput withOutput;
    withOutput = itl->boundWith(withRow);

    FunctionContext selectContext;
    selectContext.update(std::move(withOutput));

    FunctionOutput stepOutput = itl->applier->apply(selectContext);

    //cerr << "stepOutput = " << jsonEncode(stepOutput) << endl;

    FunctionContext extractContext;
    extractContext.update(std::move(stepOutput));

    // Now the extract
    auto extractRow = itl->extractBindingContext.getRowContext(extractContext);
    return itl->boundExtract(extractRow);
}

const FunctionValues &
FunctionStepApplier::
getOutput() const
{
    return itl->outputValues;
}


/*****************************************************************************/
/* SERIAL FUNCTION                                                           */
/*****************************************************************************/

SerialFunction::
SerialFunction(MldbServer * owner,
            PolyConfig config,
            const std::function<bool (const Json::Value &)> & onProgress)
    : Function(owner)
{
    functionConfig = config.params.convert<SerialFunctionConfig>();

    for (auto & s: functionConfig.steps) {
        steps.emplace_back(obtainFunction(server, s, onProgress), s.with, s.extract);
    }
}

SerialFunction::
SerialFunction(MldbServer * owner, std::vector<Step> steps)
    : Function(owner), steps(std::move(steps))
{
}

SerialFunction::
SerialFunction(MldbServer * owner)
    : Function(owner)
{
}

void
SerialFunction::
init(std::vector<Step> steps)
{
    ExcAssert(this->steps.empty());
    this->steps = std::move(steps);
}

/** This infers what the input to a WITH clause needs to look like in order
    to generate the given input for a function.  Each variable that is mentioned
    in the requiredWithOutput is traced back to the input requirements to
    generate that variable, and these are extracted.
*/
static FunctionValues
inferInputFromOutput(const FunctionValues & requiredWithOutput,
                     const SelectExpression & with)
{
    FunctionValues inputInfo;

    for (auto & p: requiredWithOutput.values) {
        Utf8String name = p.first.toUtf8String();

        // Check the with expression to find how to generate it
        bool foundName = false;

        // Now scan clauses
        for (auto & clause: with.clauses) {
            //cerr << "trying clause " << clause->surface << endl;
            
            const WildcardExpression * wildcard
                = dynamic_cast<const WildcardExpression*>(clause.get());

            if (wildcard) {
                // TODO: excluding...

                // See if this variable matches
                if (name.startsWith(wildcard->asPrefix)) {
                    //cerr << "Matches wildcard" << endl;

                    Utf8String inputName = name;
                    inputName.replace(0, wildcard->asPrefix.length(),
                                         wildcard->prefix);
                            
                    if (foundName)
                        throw HttpReturnException
                            (400, "Multiple ways to produce value '" + name + "' for function");
                    foundName = true;

                    auto it = inputInfo.values.find(inputName);
                    if (it != inputInfo.values.end()) {
                        continue;
                    }

                    auto & valueInfo = inputInfo.values[inputName];
                    valueInfo = p.second;
                    continue;
                }
            }

            const ComputedVariable * computed
                = dynamic_cast<const ComputedVariable *>(clause.get());

            if (computed) {
                if (computed->alias == name) {

                    foundName = true;

                    // Record all of the inputs to the expression
                    auto vars = computed->expression->variableNames();

                    for (auto & v: vars) {
                        
                        // If we ask for a variable with a scope, we
                        // are asking for something that's bound
                        // externally, and so we skip it.

                        if (!v.first.scope.empty())
                            continue;

                        auto & valueInfo = inputInfo.values[v.first.name];

                        // TODO: infer types
                        
                        if (!valueInfo.valueInfo)
                            valueInfo.valueInfo.reset(new AnyValueInfo());
                    }
                }
            }
        }
        
        if (!foundName)
            throw HttpReturnException(400, "Couldn't find how to obtain input value '"
                                      + name + "' from extract clause");
    }

    return inputInfo;
}

FunctionInfo
SerialFunction::Step::
getStepInfo() const
{
    FunctionInfo result;

    // 1.  Ask the function what it needs for input and output

    auto functionInfo = function->getFunctionInfo();

    cerr << "functionInfo = " << jsonEncode(functionInfo);

    // 2.  The output of the function is what the extract clause
    //     will output.

    ExtractContext extractContext(function->server, functionInfo.output);
    
    // Bind the expression so that we know everything that is read
    // from.
    BoundSqlExpression boundExtract = extract.bind(extractContext);

    cerr << "boundExtract.info = " << jsonEncode(boundExtract.info) << endl;

    result.output = *boundExtract.info;

    // 3.  The input to the function is what the with context requires
    result.input = inferInputFromOutput(functionInfo.input, with);

    cerr << "returning result " << jsonEncode(result) << endl;
    
    return result;
}

struct SerialFunctionApplier: public FunctionApplier {

    SerialFunctionApplier(SqlBindingScope & outerContext,
                          const SerialFunction * function,
                          const FunctionValues & inputInfo)
    {
        this->function = function;
        this->info.input = inputInfo;

        FunctionValues currentInput = inputInfo;

        // Create all of the steps
        for (const SerialFunction::Step & step: function->steps) {
            this->steps.emplace_back(outerContext, currentInput, *step.function, step.with, step.extract);
            
            // The output of the sub-function is copied to the output of the function
            this->info.output.merge(this->steps.back().getOutput());

            // The output of the sub-function is available as input to the next functions
            currentInput.merge(this->steps.back().getOutput());
        }
    }

    std::vector<FunctionStepApplier> steps;
};

std::unique_ptr<FunctionApplier>
SerialFunction::
bind(SqlBindingScope & outerContext,
     const FunctionValues & input) const
{
    std::unique_ptr<SerialFunctionApplier> result
        (new SerialFunctionApplier(outerContext, this, input));
    return std::move(result);
}

FunctionOutput
SerialFunction::
apply(const FunctionApplier & applier,
      const FunctionContext & context) const
{
    auto & serialApplier = static_cast<const SerialFunctionApplier &>(applier);

    ExcAssertEqual(serialApplier.steps.size(), this->steps.size());
    
    FunctionContext input = context;
    FunctionOutput output;

    for (unsigned i = 0;  i < serialApplier.steps.size();  ++i) {
        auto & step = serialApplier.steps[i];
        try {
            FunctionOutput stepOutput = step.apply(input);
            if (i != serialApplier.steps.size() - 1)
                input.update(stepOutput);
            output.update(std::move(stepOutput));
        } catch (const std::exception & exc) {
            rethrowHttpException(-1, "Error applying step " + to_string(i) + " of type "
                                 + ML::type_name(*step.function) + " to function: "
                                 + exc.what(),
                                 "inputContext", context);
        }
    }

    return output;
}

FunctionInfo
SerialFunction::
getFunctionInfo() const
{
    // Go through one by one

    // The input is the union of the input of each of the function steps
    // The output is the union of the output of each of the function steps

    // Values that aren't satisfied internally and so need to be passed from
    // the output in.
    FunctionValues externalInput;

    // Values that are accumulated from the output of previous functions
    FunctionValues internalInput;

    // Values that are written as part of the output
    FunctionValues output;

    for (auto & s: steps) {
        FunctionInfo stepInfo = s.getStepInfo();
        
        cerr << "step has info " << jsonEncode(stepInfo);

        // Each input value is satisfied from the accumulated output, or if
        // not available there, from the required output.

        for (auto & p: stepInfo.input.values) {
            if (internalInput.values.count(p.first)) {
                // TODO: check compatibility
            }
            else {
                // Satisfy externally
                if (externalInput.values.count(p.first)) {
                    // TODO: check compatibility
                }
                else {
                    externalInput.addValue(p.first.toUtf8String(), p.second.valueInfo);
                }
            }
        }
        
        internalInput.merge(stepInfo.output);
        output.merge(stepInfo.output);
    }
    
    return { externalInput, output };
}

Any
SerialFunction::
getStatus() const
{
    vector<Any> result;
    for (auto & s: steps) {
        result.push_back(s.function->getStatus());
    }
    return result;
}


/*****************************************************************************/
/* FUNCTIONS                                                                 */
/*****************************************************************************/

namespace {

RegisterFunctionType<SerialFunction, SerialFunctionConfig>
regSerialFunction(builtinPackage(),
                  "serial",
                  "Function type that runs several functions in order",
                  "functions/Serial.md.html");

} // file scope

} // namespace MLDB
} // namespace Datacratic
