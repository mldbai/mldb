/** value_function.cc                                             -*- C++ -*-
    Jeremy Barnes, 14 April 2016
    This file is part of MLDB. Copyright 2016 mldb.ai inc. All rights reserved.

*/

#include "value_function.h"
#include "mldb/types/value_description.h"
#include "mldb/types/meta_value_description.h"
#include <unordered_map>



namespace MLDB {

// expression_value_description.cc
std::tuple<std::shared_ptr<ExpressionValueInfo>,
           ValueFunction::FromInput,
           ValueFunction::ToOutput>
toValueInfo(std::shared_ptr<const ValueDescription> desc);

/*****************************************************************************/
/* VALUE FUNCTION                                                            */
/*****************************************************************************/

ValueFunction::
ValueFunction(MldbServer * server,
              const PolyConfig& config,
              std::shared_ptr<const ValueDescription> inputDescription,
              std::shared_ptr<const ValueDescription> outputDescription)
    : Function(server, config),
      inputDescription(std::move(inputDescription)),
      outputDescription(std::move(outputDescription))
{
    std::tie(inputInfo, fromInput, std::ignore)
        = toValueInfo(this->inputDescription);
    std::tie(outputInfo, std::ignore, toOutput)
        = toValueInfo(this->outputDescription);
}
    
Any
ValueFunction::
getStatus() const
{
    return Any();
}

FunctionInfo
ValueFunction::
getFunctionInfo() const
{
    FunctionInfo result;
    result.input.emplace_back(this->inputInfo);
    result.output = this->outputInfo;
    return result;
}

} // namespace MLDB

