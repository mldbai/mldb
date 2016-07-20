/**
 * progress.h
 * Guy Dumais, 2016-04-28
 * Mich, 2016-05-24
 * This file is part of MLDB. Copyright 2016 Datacratic. All rights reserved.
 **/
#pragma once

#include <string>
#include <memory>
#include <vector>

#include "mldb/types/date.h"
#include "mldb/types/value_description_fwd.h"

namespace Datacratic {
namespace MLDB {

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

struct IterationProgress {
    IterationProgress() : percent(0) {}
    float percent;
};
DECLARE_STRUCTURE_DESCRIPTION(IterationProgress);

} // namespace MLDB
} // namespace Datacratic
