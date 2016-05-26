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
nextStep(float endValue) {
    ended = Date::now();
    value = endValue;
    auto nextStep = _nextStep.lock();
    if (!nextStep) {
        throw ML::Exception("No next step!");
    }
    nextStep->started = Date::now();
    return nextStep;
}

DEFINE_STRUCTURE_DESCRIPTION(Step);
StepDescription::
StepDescription()
{
    addField("name", &Step::name, "");
    addField("started", &Step::started, "");
    addField("ended", &Step::ended, "");
    addField("value", &Step::value, "");
    addField("type", &Step::type, "");
}

shared_ptr<Step>
Progress::
steps(vector<pair<string, string>> nameAndTypes) {
    std::shared_ptr<Step> previousStep;
    for (const auto & nameAndType : nameAndTypes) {
        auto step = std::make_shared<Step>(nameAndType.first,
                                           nameAndType.second);
        if (previousStep) {
            previousStep->_nextStep = step;
        }
        _steps.push_back(step);
        previousStep = step;
    }
    auto firstStep = _steps.begin();
    if (firstStep != _steps.end()) {
        (*firstStep)->started = Date::now();
    }
    return *firstStep;
}

DEFINE_STRUCTURE_DESCRIPTION(Progress);
ProgressDescription::
ProgressDescription()
{
    addField("steps", &Progress::_steps, "");
}

} // namespace MLDB
} // namespace Datacratic
