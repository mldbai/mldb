// This file is part of MLDB. Copyright 2015 mldb.ai inc. All rights reserved.


#include "test_function.h"
#include "mldb/server/mldb_server.h"
#include "mldb/server/function_collection.h"
#include "mldb/types/basic_value_descriptions.h"
#include "mldb/ml/value_descriptions.h"

using namespace std;


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
    : Function(owner, config)
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

ExpressionValue
TestFunction::
apply(const FunctionApplier & applier,
      const ExpressionValue & context) const
{
    StructValue result;
    result.emplace_back("cnt", ExpressionValue((int)cnt, Date::now()));
    return std::move(result);
}

FunctionInfo
TestFunction::
getFunctionInfo() const
{
    vector<KnownColumn> cols;
    cols.emplace_back(PathElement("cnt"), std::make_shared<NumericValueInfo>(),
                      COLUMN_IS_DENSE);

    FunctionInfo result;
    result.output = std::make_shared<RowValueInfo>(cols, SCHEMA_CLOSED);
    cols.clear();
    result.input.emplace_back(new RowValueInfo(cols, SCHEMA_CLOSED));
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

