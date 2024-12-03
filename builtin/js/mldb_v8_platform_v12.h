/** mldb_v8_platform_v12.h
    Jeremy Barnes, 12 June 2015
    Copyright (c) 2015 mldb.ai inc.  All rights reserved.
    This file is part of MLDB. Copyright 2015 mldb.ai inc. All rights reserved.

    MLDB Platform object for V8 major version 12.
*/

#pragma once

#include "v8-version.h"
#include "mldb_v8_platform_common.h"

namespace MLDB {

#if V8_MAJOR_VERSION == 12

/*****************************************************************************/
/* TASK RUNNER                                                               */
/*****************************************************************************/

struct V8MldbTaskRunner12: public V8MldbTaskRunnerCommon {

    V8MldbTaskRunner12(V8MldbPlatformCommon * platform)
        : V8MldbTaskRunnerCommon(platform)
    {
    }

    /**
     * Returns true if idle tasks are enabled for this TaskRunner.
     */
    virtual bool IdleTasksEnabled() override
    {
        return false;
    }

    /**
     * Returns true if non-nestable tasks are enabled for this TaskRunner.
     */
    virtual bool NonNestableTasksEnabled() const override { return true; }

    /**
     * Returns true if non-nestable delayed tasks are enabled for this TaskRunner.
     */
    virtual bool NonNestableDelayedTasksEnabled() const override { return false; }
};

/*****************************************************************************/
/* TASK RUNNER                                                               */
/*****************************************************************************/

struct V8MldbPlatform12: public V8MldbPlatformCommon {
    V8MldbPlatform12(MldbEngine * engine)
        : V8MldbPlatformCommon(engine, new V8MldbTaskRunner12(this))
    {
    }

    virtual ~V8MldbPlatform12()
    {
    }

    virtual V8MldbTaskRunnerCommon * createRunner() override
    {
        return new V8MldbTaskRunner12(this);
    }

    /**
     * Creates and returns a JobHandle associated with a Job.
     */
    virtual std::unique_ptr<v8::JobHandle> CreateJobImpl(
        v8::TaskPriority priority, std::unique_ptr<v8::JobTask> job_task,
        const v8::SourceLocation& location) override
    {
        return v8::platform::NewDefaultJobHandle(
            this, priority, std::move(job_task), NumberOfWorkerThreads());
    }

    /**
     * Schedules a task with |priority| to be invoked on a worker thread.
     */
    virtual void PostTaskOnWorkerThreadImpl(v8::TaskPriority priority,
                                            std::unique_ptr<v8::Task> task,
                                            const v8::SourceLocation& location) override
    {
        std::shared_ptr<v8::Task> taskPtr(task.release());
        auto lambda = [taskPtr] ()
            {
                taskPtr->Run();
            };
        ThreadPool::instance().add(std::move(lambda));
    }

    /**
     * Schedules a task with |priority| to be invoked on a worker thread after
     * |delay_in_seconds| expires.
     */
    virtual void PostDelayedTaskOnWorkerThreadImpl(
        v8::TaskPriority priority, std::unique_ptr<v8::Task> task,
        double delay_in_seconds, const v8::SourceLocation& location) override
    {
        throw MLDB::Exception("PostDelayedTaskOnWorkerThreadImpl");
    }
};

using V8MldbPlatform = V8MldbPlatform12;

#endif /* V8_MAJOR_VERSION == 12 */


} // namespace MLDB