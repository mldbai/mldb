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
     const std::shared_ptr<RowValueInfo> & input) const
{
    std::unique_ptr<FunctionApplier> result(new FunctionApplier());
    result->function = this;
    result->info = getFunctionInfo();
    result->info.checkInputCompatibility(*input);
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
