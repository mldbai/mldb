/**                                                                 -*- C++ -*-
 * mock_procedure.cc
 * Mich, 2016-10-18
 * This file is part of MLDB. Copyright 2016 mldb.ai inc. All rights reserved.
 **/

#include "mock_procedure.h"

#include <thread>
#include <chrono>
#include "mldb/utils/progress.h"
#include "mldb/rest/cancellation_exception.h"
#include "mldb/types/any_impl.h"
#include "mldb/types/basic_value_descriptions.h"

using namespace std;

namespace MLDB {

MockProcedureConfig::
MockProcedureConfig() : durationMs(5000), refreshRateMs(50)
{
}

DEFINE_STRUCTURE_DESCRIPTION(MockProcedureConfig);

MockProcedureConfigDescription::
MockProcedureConfigDescription()
{
    addField("durationMs", &MockProcedureConfig::durationMs,
             "Total duration in ms the procedure will fake work.", 5000);
    addField("refreshRateMs", &MockProcedureConfig::refreshRateMs,
             "The frequency at which the procedure will update its fake "
             "progress", 50);
    addParent<ProcedureConfig>();

    onPostValidate = [&] (MockProcedureConfig * cfg,
                          JsonParsingContext & context)
    {
        if (cfg->durationMs < 1) {
            throw MLDB::Exception("durationMs must be >= 1");
        }
        if (cfg->refreshRateMs < 1) {
            throw MLDB::Exception("refreshRateMs must be >= 1");
        }
    };
}

MockProcedure::
MockProcedure(MldbServer * owner,
              PolyConfig polyConfig,
              const std::function<bool (const Json::Value &)> & onProgress)
    : Procedure(owner)
{
    config = polyConfig.params.convert<MockProcedureConfig>();
}

RunOutput
MockProcedure::
run(const ProcedureRunConfig & run,
    const std::function<bool (const Json::Value &)> & onProgress) const
{
    constexpr int NUM_STEPS = 2;

    auto runProcConf = applyRunConfOverProcConf(config, run);
    Progress mockProgress;

    std::shared_ptr<Step> iterationStep = mockProgress.steps({
        make_pair("step 1", "counter"),
        make_pair("step 2", "counter")
    });

    auto doIt = [&]() {
        int value = 0;
        int durationMs = runProcConf.durationMs / NUM_STEPS;
        while (durationMs > 0) {
            iterationStep->value = value++;
            bool keepGoing = onProgress(jsonEncode(mockProgress));
            if (!keepGoing) {
                throw MLDB::CancellationException("Procedure mock cancelled");
            }
            this_thread::sleep_for(chrono::milliseconds(
                std::min(durationMs, runProcConf.refreshRateMs)));
            durationMs -= runProcConf.refreshRateMs;
        }
        return value;
    };

    for (int i = 0; i < NUM_STEPS; ++i) {
        int lastValue = doIt();
        if (lastValue == -1) {
            break;
        }
        if (i < NUM_STEPS - 1) {
            iterationStep = iterationStep->nextStep(lastValue + 1);
        }
    }

    return RunOutput{};
}

Any
MockProcedure::
getStatus() const
{
    return Any();
}

static RegisterProcedureType<MockProcedure, MockProcedureConfig>
regMockProcedure(
    builtinPackage(),
    "Mock procedure that fakes progressing on something for a configurable "
    "amount of time. Useful to test progression and cancellation.",
    "procedures/MockProcedure.md.html",
    nullptr /* static route */,
    { MldbEntity::INTERNAL_ENTITY });


} // namespace MLDB
