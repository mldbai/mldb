/**
 * progress.cc
 * Guy Dumais, 2016-04-28
 * Mich, 2016-05-24
 * This file is part of MLDB. Copyright 2016 mldb.ai inc. All rights reserved.
 **/
#include "progress.h"

#include "mldb/types/basic_value_descriptions.h"
#include "mldb/types/optional_description.h"


namespace MLDB {

using namespace std;

shared_ptr<Step>
Step::
nextStep(float endValue) {
    ended = Date::now();
    value = endValue;
    auto nextStep = _nextStep.lock();
    if (!nextStep) {
        throw MLDB::Exception("No next step!");
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

DEFINE_STRUCTURE_DESCRIPTION(ProgressState);
ProgressStateDescription::
ProgressStateDescription()
{
    addField("count", &ProgressState::count, 
             "the number of items processed so far");
    addField("total", &ProgressState::total, 
             "if available, the total number of items to processed");
}

ProgressState::
ProgressState() 
    : count(0) 
{}

ProgressState::
ProgressState(uint64_t total_) 
    : count(0)
{
    total.emplace(total_);
}

ProgressState &
ProgressState::
operator = (uint64_t count_) {
    count = count_;
    return *this;
}

ConvertProgressToJson::
ConvertProgressToJson(const std::function<bool(const Json::Value &)> & onJsonProgress)
    : onJsonProgress(onJsonProgress)
{
}

bool
ConvertProgressToJson::
operator () (const ProgressState & progress)
{
    Json::Value value;
    ExcAssert(*progress.total);
    value["percent"] = (float)  progress.count / *progress.total;
    return onJsonProgress(value);
}

} // namespace MLDB

