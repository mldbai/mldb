/** optimized_path.h                                               -*- C++ -*-
    Jeremy Barnes, 1 July 2016
    This file is part of MLDB. Copyright 2016 mldb.ai inc. All rights reserved.

*/

#pragma once

#include "mldb/compiler/compiler.h"
#include <memory>
#include <string>
#include <vector>
#include <atomic>
#include <limits>

namespace MLDB {


/*****************************************************************************/
/* OPTIMIZED PATH                                                            */
/*****************************************************************************/

/** Support for testing equivalence of an optimized and a non-optimized path
    during testing.
    
    This class allows an optimized path to be registered, and for whether
    or not that path is taken to be controlled by testing code to allow for
    unit testing of the equivalence of an optimized and a non-optimized
    codepath.
*/

struct OptimizedPath {
    /** Register an optimized path.  The name must be unique*/
    OptimizedPath(const std::string & name);
    ~OptimizedPath();

    MLDB_ALWAYS_INLINE bool take(bool canTake) const
    {
        // Fast path, which will always be used in production.  Definitely
        // take the optimized path if it's optimized.
        if (MLDB_LIKELY(alwaysTake.load(std::memory_order_relaxed)))
            return canTake;

        // Otherwise, lookup internally whether we should take it.  Slow
        // path
        return maybeTake(canTake);
    }

    /** Should the path be taken? */
    MLDB_ALWAYS_INLINE bool take() const
    {
        // Fast path, which will always be used in production.  Definitely
        // take the optimized path.
        if (MLDB_LIKELY(alwaysTake.load(std::memory_order_relaxed)))
            return true;

        // Otherwise, lookup internally whether we should take it.  Slow
        // path
        return maybeTake();
    }

    MLDB_ALWAYS_INLINE bool operator () () const
    {
        return take();
    }
    
    MLDB_ALWAYS_INLINE bool operator () (bool canTake) const
    {
        return take(canTake);
    }
    
    /// Argument to set() that makes the path be always taken
    static constexpr int ALWAYS = std::numeric_limits<int>::max();

    /// Argument to set() that makes the path be never taken
    static constexpr int NEVER  = 0;

    /// Argument to set() that makes the path be taken 50% of the time
    static constexpr int SOMETIMES = ALWAYS / 2;

    /// Argument to set() that will put the level back to its default
    static constexpr int DEFAULT = -1;

    /** Set the default level of optimization.  Any optimized path which
        hasn't been explicitly set will be set to this level.
        Can be changed throughout the program's execution.
    */
    static void setDefault(int frequency);

    /** Control an optimized path with the given path name.  Callable
        from multiple threads.
    */
    static void setOptimization(const std::string & pathName, int frequency);

    /** Control an optimized path with the given path name regex.  Callable
        from multiple threads.

        Returns the number of paths that were matched by the regex.
    */
    static size_t setOptimizationRegex(const std::string & pathNameRegex,
                                       int frequency);

    /** List the known optimization levels. */
    static std::vector<std::string> known();

private:
    bool maybeTake() const;

    /** Non-optimized slow path to figure out if we need to take a path or
        not.
    */
    bool maybeTake(bool canTake) const;

    /** Perform the actual work to set the optimization level. */
    static void setOptimization(OptimizedPath * path, int frequency);

    std::atomic<bool> alwaysTake;
    struct Itl;
    std::shared_ptr<Itl> itl;
    char storage[128];  ///< Storage for future expansion with ABI compatibility

    OptimizedPath(const OptimizedPath & other) = delete;
    OptimizedPath(OptimizedPath && other) = delete;
    OptimizedPath & operator = (const OptimizedPath &) = delete;
    OptimizedPath & operator = (OptimizedPath &&) = delete;
};

} // namespace MLDB


