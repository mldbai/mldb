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
#include "mldb/types/value_description.h"

namespace Datacratic {
namespace MLDB {

struct Step {
    Step(const std::string & name)
        : name(name), percent(0) {}
    Step() {}

    // signals that the step is completed and that the next one can start
    std::shared_ptr<Step> nextStep();

    std::string name;
    Date started;
    Date ended;
    float percent;
    std::weak_ptr<Step> _nextStep;
};
DECLARE_STRUCTURE_DESCRIPTION(Step);

struct Progress {
    Progress() {}
    std::shared_ptr<Step> steps(std::vector<std::string> names);
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
