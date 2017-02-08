/* function.cc
   Jeremy Barnes, 21 January 2014
   Copyright (c) 2014 mldb.ai inc.  All rights reserved.

   This file is part of MLDB. Copyright 2015 mldb.ai inc. All rights reserved.

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




namespace MLDB {

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
    addField("deterministic", &PolyConfig::deterministic,
             "If false, then the result of the function will be re-computed for each row of the query,"
             " even if the arguments are not row-dependent. If true, then it is assumed for optimization purposes "
             " that calling the function with the same input will always return the same value for a single SQL query"
             , false);

    setTypeName("FunctionConfig");
    documentationUri = "/doc/builtin/functions/FunctionConfig.md";
}


/*****************************************************************************/
/* FUNCTION INFO                                                             */
/*****************************************************************************/

void
FunctionInfo::
checkInputCompatibility(const ExpressionValueInfo & input) const
{
    // For now, say yes...
}

void
FunctionInfo::
checkInputCompatibility(const std::vector<std::shared_ptr<ExpressionValueInfo> > & input) const
{
    if (input.size() != this->input.size()) {
        throw HttpReturnException
            (400, "Wrong number of arguments (" + to_string(input.size())
             + ") passed to user function expecting " + to_string(this->input.size()));
    }
    // For now, say yes...
}


/*****************************************************************************/
/* FUNCTION APPLIER                                                          */
/*****************************************************************************/

ExpressionValue
FunctionApplier::
apply(const ExpressionValue & input) const
{ 
    ExcAssert(function);
    return function->apply(*this, input);
}


/*****************************************************************************/
/* FUNCTION                                                                  */
/*****************************************************************************/

Function::
Function(MldbServer * server, const PolyConfig& config)
    : server(server)
{
    config_ = make_shared<PolyConfig>(config);
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
     const std::vector<std::shared_ptr<ExpressionValueInfo> > & input) const
{
    std::unique_ptr<FunctionApplier> result(new FunctionApplier());
    result->function = this;
    result->info = getFunctionInfo();
    result->info.checkInputCompatibility(input);
    if (config_)
        result->info.deterministic = config_->deterministic;
    else
        result->info.deterministic = false;
    return result;
}

FunctionInfo
Function::
getFunctionInfo() const
{
    throw HttpReturnException(400, "Function " + MLDB::type_name(*this)
                        + " needs to override getFunctionInfo()");
}

RestRequestMatchResult
Function::
handleRequest(RestConnection & connection,
              const RestRequest & request,
              RestRequestParsingContext & context) const
{
    Json::Value error;
    error["error"] = "Function of type '" + MLDB::type_name(*this)
        + "' does not respond to custom route '" + context.remaining + "'";
    error["details"]["verb"] = request.verb;
    error["details"]["resource"] = request.resource;
    connection.sendErrorResponse(400, error);
    return RestRequestRouter::MR_ERROR;
}

} // namespace MLDB

