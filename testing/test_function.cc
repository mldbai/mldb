// This file is part of MLDB. Copyright 2015 Datacratic. All rights reserved.


#include "test_function.h"
#include "mldb/server/mldb_server.h"
#include "mldb/server/function_collection.h"
#include "mldb/types/basic_value_descriptions.h"
#include "mldb/ml/value_descriptions.h"

using namespace std;

namespace Datacratic {
namespace MLDB {

DEFINE_STRUCTURE_DESCRIPTION(TestFunctionConfig);


std::atomic<int> TestFunction::cnt(0);

TestFunctionConfigDescription::
TestFunctionConfigDescription()
{
}


TestFunction::
TestFunction(MldbServer * owner,
          PolyConfig config,
          const std::function<bool (const Json::Value &)> & onProgress)
    : Function(owner)
{
    ++TestFunction::cnt;
}

TestFunction::
~TestFunction()
{
    --TestFunction::cnt;
}

Any
TestFunction::
getStatus() const
{
    return Any();
}

FunctionOutput
TestFunction::
apply(const FunctionApplier & applier,
      const FunctionContext & context) const
{
    FunctionOutput result;
    result.set("cnt", ExpressionValue((int)cnt, Date::now()));
    return result;
}

FunctionInfo
TestFunction::
getFunctionInfo() const
{
    FunctionInfo result;
    result.output.addNumericValue("cnt");
    return result;
}


namespace {

Package testPackage("test");

RegisterFunctionType<TestFunction, TestFunctionConfig>
regTestFunction(testPackage,
                "testfunction",
                "Test function",
                "functions/ClassifierTest.md");

} // file scope

} // namespace MLDB
} // namespace Datacratic
