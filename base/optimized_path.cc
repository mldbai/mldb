/** optimized_path.h                                               -*- C++ -*-
    Jeremy Barnes, 1 July 2016
    This file is part of MLDB. Copyright 2016 mldb.ai inc. All rights reserved.

*/

#include "optimized_path.h"
#include "mldb/arch/exception.h"
#include "mldb/base/exc_assert.h"
#include <mutex>
#include <map>
#include <regex>
#include "mldb/jml/utils/environment.h"
#include "mldb/base/optimized_path.h"


namespace MLDB {


/*****************************************************************************/
/* OPTIMIZED PATH                                                            */
/*****************************************************************************/

struct OptimizedPath::Itl {
    Itl(std::string name)
        : name(std::move(name)),
          randSeed(1), ineligible(0),
          eligibleAndTaken(0),
          eligibleAndNotTaken(0)
    {
    }

    std::string name;
    int level;
    unsigned randSeed;
    std::atomic<uint64_t> ineligible, eligibleAndTaken, eligibleAndNotTaken;
};

/// Allow the default level to be controlled by the environment
static EnvOption<std::string>
DEFAULT_PATH_OPTIMIZATION_LEVEL("MLDB_DEFAULT_PATH_OPTIMIZATION_LEVEL", "ALWAYS");

namespace {
int getDefaultLevel()
{
    std::string level;
    for (auto & c:  std::string(DEFAULT_PATH_OPTIMIZATION_LEVEL))
        level.push_back(tolower(c));

    if (level == "always")
        return OptimizedPath::ALWAYS;
    else if (level == "sometimes")
        return OptimizedPath::SOMETIMES;
    else if (level == "never")
        return OptimizedPath::NEVER;
    else {
        throw MLDB::Exception("Couldn't parse default path optimization level '"
                            + std::string(DEFAULT_PATH_OPTIMIZATION_LEVEL)
                            + "'.  Options "
                            "are 'always', 'sometimes', 'never'.");
    }
}

std::mutex pathRegistryMutex;
std::map<std::string, OptimizedPath *> pathRegistry;
int defaultLevel = -1;
} // file scope

void
OptimizedPath::
setDefault(int frequency)
{
    if (frequency == DEFAULT)
        frequency = getDefaultLevel();
    std::unique_lock<std::mutex> guard(pathRegistryMutex);
    if (defaultLevel == frequency)
        return;  // no change
    defaultLevel = frequency;

    // Update the short-circult flag for all those set to the default
    for (auto & p: pathRegistry) {
        if (p.second->itl->level == DEFAULT) {
            p.second->alwaysTake.store(defaultLevel == ALWAYS,
                                       std::memory_order_relaxed);
        }
    }
}

void
OptimizedPath::
setOptimization(OptimizedPath * path, int frequency)
{
    ExcAssert(path);

    path->itl->level = frequency;
    bool alwaysTake
        = frequency == ALWAYS
        || (frequency == DEFAULT && defaultLevel == ALWAYS);
    if (path->alwaysTake.load(std::memory_order_relaxed)
        != alwaysTake)
        path->alwaysTake.store(alwaysTake, std::memory_order_seq_cst);
}

void
OptimizedPath::
setOptimization(const std::string & pathName, int frequency)
{
    std::unique_lock<std::mutex> guard(pathRegistryMutex);
    auto it = pathRegistry.find(pathName);
    if (it == pathRegistry.end())
        throw MLDB::Exception("Couldn't find path name '"
                            + pathName + "' setting optimization level");
    setOptimization(it->second, frequency);
}

size_t
OptimizedPath::
setOptimizationRegex(const std::string & pathNameRegex,
                     int frequency)
{
    std::regex regex(pathNameRegex);
    std::unique_lock<std::mutex> guard(pathRegistryMutex);
    size_t result = 0;
    for (auto & p: pathRegistry) {
        if (std::regex_match(p.first, regex)) {
            ++result;
            setOptimization(p.second, frequency);
        }
    }
    return result;
}

OptimizedPath::
OptimizedPath(const std::string & name)
    : alwaysTake(true),
      itl(new (storage) Itl(name), [] (Itl *) {})
{
    std::unique_lock<std::mutex> guard(pathRegistryMutex);
    // Initialize here to avoid problems if this library is initialized after
    // others.
    if (defaultLevel == -1)
        defaultLevel = getDefaultLevel();
    setOptimization(this, defaultLevel);
    if (!pathRegistry.insert({name, this}).second) {
        throw MLDB::Exception("Optimized path '" + name + "' was registered twice");
    }
}

OptimizedPath::
~OptimizedPath()
{
    std::unique_lock<std::mutex> guard(pathRegistryMutex);
    pathRegistry.erase(itl->name);
}

bool
OptimizedPath::
maybeTake() const
{
    auto level = itl->level;
    if (level == DEFAULT)
        level = defaultLevel;

    bool result;
    if (level == NEVER) {
        result = false;
    }
    else if (level == ALWAYS) {
        result = true;
    }
    else {
        result = rand_r(&itl->randSeed) < level;
    }
    (result ? itl->eligibleAndTaken: itl->eligibleAndNotTaken)
        .fetch_add(1, std::memory_order_relaxed);
    return result;
}

bool
OptimizedPath::
maybeTake(bool canTake) const
{
    if (!canTake) {
        itl->ineligible.fetch_add(1, std::memory_order_relaxed);
        return false;
    }
    return maybeTake();
}

} // namespace MLDB
