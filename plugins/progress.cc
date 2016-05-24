/**
 * progress.cc
 * Guy Dumais, 2016-04-28
 * Mich, 2016-05-24
 * This file is part of MLDB. Copyright 2016 Datacratic. All rights reserved.
 **/
#include "progress.h"

#include "mldb/types/basic_value_descriptions.h"

namespace Datacratic {
namespace MLDB {

using namespace std;

shared_ptr<Step>
Step::
nextStep() {
    ended = Date::now();
    percent = 1;
    auto nextStep = _nextStep.lock();
    if (nextStep) {
        nextStep->started = Date::now();
    }
    return nextStep;
}

DEFINE_STRUCTURE_DESCRIPTION(Step);
StepDescription::
StepDescription()
{
    addField("name", &Step::name, "");
    addField("started", &Step::started, "");
    addField("ended", &Step::ended, "");
    addField("percent", &Step::percent, "");
}

std::shared_ptr<Step>
Progress::
steps(std::vector<std::string> names) {
    std::shared_ptr<Step> previousStep;
    for (const auto & name : names) {
        std::shared_ptr<Step> step = std::make_shared<Step>(name);
        if (previousStep)
            previousStep->_nextStep = step;
        _steps.push_back(step);
        previousStep = step;
    }
    auto firstStep = _steps.begin();
    if (firstStep != _steps.end())
        (*firstStep)->started = Date::now();
    return *firstStep;
}

DEFINE_STRUCTURE_DESCRIPTION(Progress);
ProgressDescription::
ProgressDescription()
{
    addField("steps", &Progress::_steps, "");
}

DEFINE_STRUCTURE_DESCRIPTION(IterationProgress);
IterationProgressDescription::
IterationProgressDescription()
{
    addField("percent", &IterationProgress::percent, "");
}

} // namespace MLDB
} // namespace Datacratic
