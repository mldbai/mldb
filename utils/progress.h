/**
 * progress.h
 * Guy Dumais, 2016-04-28
 * Mich, 2016-05-24
 * This file is part of MLDB. Copyright 2016 mldb.ai inc. All rights reserved.
 **/
#pragma once

#include <string>
#include <memory>
#include <vector>
#include <functional>

#include "mldb/types/date.h"
#include "mldb/types/optional.h"
#include "mldb/types/value_description_fwd.h"


namespace MLDB {

// process rate for common operations (e.g iterating a dataset)
static constexpr unsigned short PROGRESS_RATE = 50;
// progress rate for operations are very fast (e.g. importing a CSV)
static constexpr unsigned short PROGRESS_RATE_LOW = 10000;

struct Step {
    Step(const std::string & name, const std::string & type)
        : name(name), value(0), type(type) {}
    Step() {}

    // signals that the step is completed and that the next one can start
    std::shared_ptr<Step> nextStep(float endValue);

    std::string name;
    Date started;
    Date ended;
    float value;
    std::string type;
    std::weak_ptr<Step> _nextStep;
};
DECLARE_STRUCTURE_DESCRIPTION(Step);

struct Progress {
    Progress() {}
    std::shared_ptr<Step>
        steps(std::vector<std::pair<std::string, std::string>> nameAndTypes);
    std::vector<std::shared_ptr<Step> > _steps;
};
DECLARE_STRUCTURE_DESCRIPTION(Progress);

struct ProgressState {
    ProgressState();
    ProgressState(uint64_t total);

    ProgressState & operator = (uint64_t count);

    uint64_t count;
    Optional<uint64_t> total;
    
};

DECLARE_STRUCTURE_DESCRIPTION(ProgressState);

typedef std::function<bool(const ProgressState &)> ProgressFunc;

/* This is a temporary conversion helper to avoid 
   changing all the procedure run signature.
   TODO - MLDB-2110 - fix all the progress signature */
struct ConvertProgressToJson {
    ConvertProgressToJson(const std::function<bool(const Json::Value &)> & onJsonProgress);
    const std::function<bool(const Json::Value &)> & onJsonProgress;
    bool operator () (const ProgressState & progress);
}
                           ;

} // namespace MLDB

