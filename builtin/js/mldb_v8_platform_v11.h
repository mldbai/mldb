/** mldb_v8_platform_v11.h
    Jeremy Barnes, 12 June 2015
    Copyright (c) 2015 mldb.ai inc.  All rights reserved.
    This file is part of MLDB. Copyright 2015 mldb.ai inc. All rights reserved.

    MLDB Platform object for V8 major version 11.
*/

#pragma once

#include "v8-version.h"
#include "mldb_v8_platform_common.h"

namespace MLDB {

#if V8_MAJOR_VERSION == 11

/*****************************************************************************/
/* TASK RUNNER                                                               */
/*****************************************************************************/

struct V8MldbTaskRunner11: public V8MldbTaskRunnerCommon {

    V8MldbTaskRunner11(V8MldbPlatformCommon * platform)
        : V8MldbTaskRunnerCommon(platform)
    {
    }

    /**
     * Schedules a task to be invoked by this TaskRunner. The TaskRunner
     * implementation takes ownership of |task|.
     */
    virtual void PostTask(std::unique_ptr<v8::Task> task) override
    {
        PostDelayedTask(std::move(task), 0.0);
    }

    /**
     * Schedules a task to be invoked by this TaskRunner. The TaskRunner
     * implementation takes ownership of |task|. The |task| cannot be nested
     * within other task executions.
     *
     * Tasks which shouldn't be interleaved with JS execution must be posted with
     * |PostNonNestableTask| or |PostNonNestableDelayedTask|. This is because the
     * embedder may process tasks in a callback which is called during JS
     * execution.
     *
     * In particular, tasks which execute JS must be non-nestable, since JS
     * execution is not allowed to nest.
     *
     * Requires that |TaskRunner::NonNestableTasksEnabled()| is true.
     */
    virtual void PostNonNestableTask(std::unique_ptr<v8::Task> task) override
    {
        PostDelayedTask(std::move(task), 0.0);
    }

    /**
     * Schedules a task to be invoked by this TaskRunner. The task is scheduled
     * after the given number of seconds |delay_in_seconds|. The TaskRunner
     * implementation takes ownership of |task|.
     */
    virtual void PostDelayedTask(std::unique_ptr<v8::Task> task,
                                    double delay_in_seconds) override
    {
        auto deadline = std::chrono::steady_clock::now()
            + std::chrono::nanoseconds((long long)delay_in_seconds * 1000000000);
        std::unique_lock<std::mutex> guard(mutex);
        queue.emplace(deadline, std::move(task));
        if (queue.empty() || deadline < std::get<0>(queue.top())) {
            foregroundLoopCondition.notify_one();
        }
    }

    /**
     * Schedules a task to be invoked by this TaskRunner. The task is scheduled
     * after the given number of seconds |delay_in_seconds|. The TaskRunner
     * implementation takes ownership of |task|. The |task| cannot be nested
     * within other task executions.
     *
     * Tasks which shouldn't be interleaved with JS execution must be posted with
     * |PostNonNestableTask| or |PostNonNestableDelayedTask|. This is because the
     * embedder may process tasks in a callback which is called during JS
     * execution.
     *
     * In particular, tasks which execute JS must be non-nestable, since JS
     * execution is not allowed to nest.
     *
     * Requires that |TaskRunner::NonNestableDelayedTasksEnabled()| is true.
     */
    virtual void PostNonNestableDelayedTask(std::unique_ptr<v8::Task> task,
                                            double delay_in_seconds) override
    {
        PostDelayedTask(std::move(task), delay_in_seconds);
    }

    /**
     * Schedules an idle task to be invoked by this TaskRunner. The task is
     * scheduled when the embedder is idle. Requires that
     * |TaskRunner::IdleTasksEnabled()| is true. Idle tasks may be reordered
     * relative to other task types and may be starved for an arbitrarily long
     * time if no idle time is available. The TaskRunner implementation takes
     * ownership of |task|.
     */
    virtual void PostIdleTask(std::unique_ptr<v8::IdleTask> task) override
    {
        throw MLDB::Exception("PostIdleTask");
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

struct V8MldbPlatform11: public V8MldbPlatformCommon {
    V8MldbPlatform11(MldbEngine * engine)
        : V8MldbPlatformCommon(engine, new V8MldbTaskRunner11(this))
    {
    }

    virtual ~V8MldbPlatform11()
    {
    }

    virtual V8MldbTaskRunnerCommon * createRunner() override
    {
        return new V8MldbTaskRunner11(this);
    }

    /**
     * Schedules a task to be invoked on a worker thread.
     */
    virtual void CallOnWorkerThread(std::unique_ptr<v8::Task> task) override
    {
        std::shared_ptr<v8::Task> taskPtr(task.release());
        auto lambda = [taskPtr] ()
            {
                taskPtr->Run();
            };
        ThreadPool::instance().add(std::move(lambda));
    }

    /**
     * Schedules a task to be invoked on a worker thread after |delay_in_seconds|
     * expires.
     */
    virtual void CallDelayedOnWorkerThread(std::unique_ptr<v8::Task> task,
                                            double delay_in_seconds) override
    {
        // TODO: don't create so many threads...
        std::shared_ptr<v8::Task> sharedTask(task.release());
        auto runTask = [sharedTask, delay_in_seconds]
        {
            std::this_thread::sleep_for(std::chrono::milliseconds(uint64_t(delay_in_seconds * 1000)));
            sharedTask->Run();
        };

        std::thread(std::move(runTask)).detach();
    }


    /**
     * Schedules a task that blocks the main thread to be invoked with
     * high-priority on a worker thread.
     */
    virtual void CallBlockingTaskOnWorkerThread(std::unique_ptr<v8::Task> task) override
    {
        std::shared_ptr<v8::Task> taskPtr(task.release());
        auto lambda = [taskPtr] ()
            {
                taskPtr->Run();
            };
        std::thread(std::move(lambda)).detach();
    }

    /**
     * Schedules a task to be invoked with low-priority on a worker thread.
     */
    virtual void CallLowPriorityTaskOnWorkerThread(std::unique_ptr<v8::Task> task) override
    {
        // Embedders may optionally override this to process these tasks in a low
        // priority pool.
        std::shared_ptr<v8::Task> taskPtr(task.release());
        auto lambda = [taskPtr] ()
            {
                taskPtr->Run();
            };
        ThreadPool::instance().add(std::move(lambda));
    }

    virtual std::unique_ptr<v8::JobHandle> CreateJob(
        v8::TaskPriority priority, std::unique_ptr<v8::JobTask> job_task) override
    {
        return v8::platform::NewDefaultJobHandle(
            this, priority, std::move(job_task), NumberOfWorkerThreads());
    }
};

using V8MldbPlatform = V8MldbPlatform11;

#endif /* V8_MAJOR_VERSION == 11 */

} // namespace MLDB