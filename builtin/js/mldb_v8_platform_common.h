/** mldb_v8_platform_common.h
    Jeremy Barnes, 12 June 2015
    Copyright (c) 2015 mldb.ai inc.  All rights reserved.
    This file is part of MLDB. Copyright 2015 mldb.ai inc. All rights reserved.

    MLDB Platform object for V8 major version 11.
*/

#pragma once

#include "v8.h"
#include <thread>
#include <chrono>
#include <atomic>
#include <mutex>
#include <condition_variable>
#include <shared_mutex>
#include <queue>
#include <map>
#include "mldb/core/mldb_engine.h"
#include "mldb/base/thread_pool.h"
#include "mldb/base/exc_assert.h"
#include "libplatform/libplatform.h"

namespace MLDB {

/*****************************************************************************/
/* MLDB V8 TRACING CONTROLLER                                                */
/*****************************************************************************/

/**
 * V8 Tracing controller.
 *
 * Can be implemented by an embedder to record trace events from V8.
 */
class MldbV8TracingController: public v8::TracingController {
 public:
  virtual ~MldbV8TracingController() = default;

  /**
   * Called by TRACE_EVENT* macros, don't call this directly.
   * The name parameter is a category group for example:
   * TRACE_EVENT0("v8,parse", "V8.Parse")
   * The pointer returned points to a value with zero or more of the bits
   * defined in CategoryGroupEnabledFlags.
   **/
  virtual const uint8_t* GetCategoryGroupEnabled(const char* name) override {
    static uint8_t no = 0;
    return &no;
  }

  /**
   * Adds a trace event to the platform tracing system. These function calls are
   * usually the result of a TRACE_* macro from trace_event_common.h when
   * tracing and the category of the particular trace are enabled. It is not
   * advisable to call these functions on their own; they are really only meant
   * to be used by the trace macros. The returned handle can be used by
   * UpdateTraceEventDuration to update the duration of COMPLETE events.
   */
  virtual uint64_t AddTraceEvent(
      char phase, const uint8_t* category_enabled_flag, const char* name,
      const char* scope, uint64_t id, uint64_t bind_id, int32_t num_args,
      const char** arg_names, const uint8_t* arg_types,
      const uint64_t* arg_values,
      std::unique_ptr<v8::ConvertableToTraceFormat>* arg_convertables,
      unsigned int flags) override {
    return 0;
  }
  virtual uint64_t AddTraceEventWithTimestamp(
      char phase, const uint8_t* category_enabled_flag, const char* name,
      const char* scope, uint64_t id, uint64_t bind_id, int32_t num_args,
      const char** arg_names, const uint8_t* arg_types,
      const uint64_t* arg_values,
      std::unique_ptr<v8::ConvertableToTraceFormat>* arg_convertables,
      unsigned int flags, int64_t timestamp) override {
    return 0;
  }

  /**
   * Sets the duration field of a COMPLETE trace event. It must be called with
   * the handle returned from AddTraceEvent().
   **/
  virtual void UpdateTraceEventDuration(const uint8_t* category_enabled_flag,
                                        const char* name, uint64_t handle) override {}

  /** Adds tracing state change observer. */
  virtual void AddTraceStateObserver(TraceStateObserver*) override {}

  /** Removes tracing state change observer. */
  virtual void RemoveTraceStateObserver(TraceStateObserver*) override {}
};

/*****************************************************************************/
/* MLDB V8 TASK RUNNER COMMON                                                */
/*****************************************************************************/

struct V8MldbPlatformCommon;

struct V8MldbTaskRunnerCommon: public v8::TaskRunner {
    V8MldbTaskRunnerCommon(V8MldbPlatformCommon * platform)
        : platform(platform), 
            start(std::chrono::steady_clock::now()),
            shutdown(false),
            foregroundMessageLoop(std::bind(&V8MldbTaskRunnerCommon::runForegroundLoop,
                                            this))
    {
    }

    ~V8MldbTaskRunnerCommon()
    {
        shutdown = true;
        foregroundLoopCondition.notify_all();
        std::unique_lock<std::mutex> guard(mutex);
        foregroundMessageLoop.join();
    }

    V8MldbPlatformCommon * platform;
    std::chrono::time_point<std::chrono::steady_clock> start;
    std::mutex mutex;
    std::atomic<bool> shutdown;
    std::condition_variable foregroundLoopCondition;

    typedef std::chrono::time_point<std::chrono::steady_clock> TimePoint;
    typedef std::tuple<TimePoint, std::shared_ptr<v8::Task>> DelayedEntry;
    std::priority_queue<DelayedEntry, std::vector<DelayedEntry>,
                        std::greater<DelayedEntry> >
        queue;

    std::thread foregroundMessageLoop;

    void runForegroundLoop()
    {
        std::unique_lock<std::mutex> guard(mutex);

        while (!shutdown.load()) {
            if (queue.empty()) {
                foregroundLoopCondition.wait(guard);
            }
            else {
                TimePoint nextWakeup = std::get<0>(queue.top());
                foregroundLoopCondition.wait_until(guard, nextWakeup);
            }

            if (shutdown.load())
                return;

            while (!queue.empty()
                && (std::chrono::steady_clock::now()
                    < std::get<0>(queue.top()))) {
                auto entry = queue.top();
                queue.pop();
                guard.unlock();
                try {
                    std::get<1>(entry)->Run();
                } catch (...) {
                    guard.lock();
                    throw;
                }
                guard.lock();
            }
        };
    }

#if 0
    /**
     * Implementation of above methods with an additional `location` argument.
     */
    virtual void PostTaskImpl(std::unique_ptr<v8::Task> task,
                                const v8::SourceLocation& location) override
    {
        PostDelayedTaskImpl(std::move(task), 0.0, location);
    }

    virtual void PostNonNestableTaskImpl(std::unique_ptr<v8::Task> task,
                                        const v8::SourceLocation& location) override
    {
        PostDelayedTaskImpl(std::move(task), 0.0, location);
    }
    virtual void PostDelayedTaskImpl(std::unique_ptr<v8::Task> task,
                                    double delay_in_seconds,
                                    const v8::SourceLocation& location) override
    {
        auto deadline = std::chrono::steady_clock::now()
            + std::chrono::nanoseconds((long long)delay_in_seconds * 1000000000);
        std::unique_lock<std::mutex> guard(mutex);
        queue.emplace(deadline, std::move(task));
        if (queue.empty() || deadline < std::get<0>(queue.top())) {
            foregroundLoopCondition.notify_one();
        }
    }
    virtual void PostNonNestableDelayedTaskImpl(std::unique_ptr<v8::Task> task,
                                                double delay_in_seconds,
                                                const v8::SourceLocation& location) override
    {
        PostDelayedTaskImpl(std::move(task), delay_in_seconds, location);
    }
    virtual void PostIdleTaskImpl(std::unique_ptr<v8::IdleTask> task,
                                    const v8::SourceLocation& location) override
    {
        throw MLDB::Exception("PostIdleTask");
    }

#endif /* V8_HAS_OLD_TASK_RUNNER */
    virtual bool IdleTasksEnabled() override
    {
        return false;
    }

    /**
     * Returns true if non-nestable tasks are enabled for this TaskRunner.
     */
    virtual bool NonNestableTasksEnabled() const override { return true; }

};


/*****************************************************************************/
/* MLDB V8 PLATFORM COMMON                                                   */
/*****************************************************************************/

/**
 * V8 Platform abstraction layer.
 *
 * The embedder has to provide an implementation of this interface before
 * initializing the rest of V8.
 */
struct V8MldbPlatformCommon: public v8::Platform {

    std::shared_ptr<V8MldbTaskRunnerCommon> runner;

    // Protects isolateRunners
    std::shared_mutex isolateRunnersMutex;

    // Per-isolate runner (for older v8)
    // TODO: stop leaking them...
    std::map<const v8::Isolate *, std::shared_ptr<V8MldbTaskRunnerCommon> > isolateRunners;

    std::shared_ptr<V8MldbTaskRunnerCommon> getIsolateRunner(const v8::Isolate * isolate)
    {
        {
            std::shared_lock lock(isolateRunnersMutex);
            auto it = isolateRunners.find(isolate);
            if (it != isolateRunners.end())
            return it->second;
        }

        std::shared_ptr<V8MldbTaskRunnerCommon> runner(this->createRunner());

        std::unique_lock lock(isolateRunnersMutex);
        auto it = isolateRunners.emplace(isolate, std::move(runner)).first;
        return it->second;
    }

    void removeIsolate(const v8::Isolate * isolate)
    {
        std::unique_lock lock(isolateRunnersMutex);
        isolateRunners.erase(isolate);
    }
    
protected:
    V8MldbPlatformCommon(MldbEngine * engine, V8MldbTaskRunnerCommon * runner_impl)
        : runner(runner_impl)
    {
    }

    virtual V8MldbTaskRunnerCommon * createRunner() = 0;

public:
    virtual ~V8MldbPlatformCommon()
    {
    }

    /**
     * Allows the embedder to manage memory page allocations.
     */
    virtual v8::PageAllocator* GetPageAllocator() override {
        // TODO(bbudge) Make this abstract after all embedders implement this.
        return nullptr;
    }

    /**
     * Enables the embedder to respond in cases where V8 can't allocate large
     * blocks of memory. V8 retries the failed allocation once after calling this
     * method. On success, execution continues; otherwise V8 exits with a fatal
     * error.
     * Embedder overrides of this function must NOT call back into V8.
     */
    virtual void OnCriticalMemoryPressure() override {
        // TODO(bbudge) Remove this when embedders override the following method.
        // See crbug.com/634547.
    }

#if 0
    /**
     * Enables the embedder to respond in cases where V8 can't allocate large
     * memory regions. The |length| parameter is the amount of memory needed.
     * Returns true if memory is now available. Returns false if no memory could
     * be made available. V8 will retry allocations until this method returns
     * false.
     *
     * Embedder overrides of this function must NOT call back into V8.
     */
    virtual bool OnCriticalMemoryPressure(size_t length) override { return false; }
#endif

    /**
     * Gets the number of worker threads used by
     * Call(BlockingTask)OnWorkerThread(). This can be used to estimate the number
     * of tasks a work package should be split into. A return value of 0 means
     * that there are no worker threads available. Note that a value of 0 won't
     * prohibit V8 from posting tasks using |CallOnWorkerThread|.
     */
    virtual int NumberOfWorkerThreads() override
    {
        return ThreadPool::instance().numThreads();
    }

    /**
     * Returns a TaskRunner which can be used to post a task on the foreground.
     * The TaskRunner's NonNestableTasksEnabled() must be true. This function
     * should only be called from a foreground thread.
     */
    virtual std::shared_ptr<v8::TaskRunner> GetForegroundTaskRunner(
        v8::Isolate* isolate) override
    {
        return getIsolateRunner(isolate);
    }

    // Older interface
    /**
     * Schedules a task to be invoked on a foreground thread wrt a specific
     * |isolate|. Tasks posted for the same isolate should be execute in order of
     * scheduling. The definition of "foreground" is opaque to V8.
     */
    virtual void CallOnForegroundThread(v8::Isolate* isolate, v8::Task* task)
    {
        std::unique_ptr<v8::Task> pTask(task);
	    return getIsolateRunner(isolate)->PostTask(std::move(pTask));
    }

    /**
     * Schedules a task to be invoked on a foreground thread wrt a specific
     * |isolate| after the given number of seconds |delay_in_seconds|.
     * Tasks posted for the same isolate should be execute in order of
     * scheduling. The definition of "foreground" is opaque to V8.
     */
    virtual void CallDelayedOnForegroundThread(v8::Isolate* isolate, v8::Task* task, double delay)
    {
        std::unique_ptr<v8::Task> pTask(task);
        return getIsolateRunner(isolate)->PostDelayedTask(std::move(pTask), delay);
    }

    /**
     * Returns true if idle tasks are enabled for the given |isolate|.
     */
    virtual bool IdleTasksEnabled(v8::Isolate* isolate) override { return false; }

#if (V8_PLATFORM_HAS_JOB_INTERFACE)
    /**
     * Posts |job_task| to run in parallel. Returns a JobHandle associated with
     * the Job, which can be joined or canceled.
     * This avoids degenerate cases:
     * - Calling CallOnWorkerThread() for each work item, causing significant
     *   overhead.
     * - Fixed number of CallOnWorkerThread() calls that split the work and might
     *   run for a long time. This is problematic when many components post
     *   "num cores" tasks and all expect to use all the cores. In these cases,
     *   the scheduler lacks context to be fair to multiple same-priority requests
     *   and/or ability to request lower priority work to yield when high priority
     *   work comes in.
     * A canonical implementation of |job_task| looks like:
     * class MyJobTask : public JobTask {
     *  public:
     *   MyJobTask(...) : worker_queue_(...) {}
     *   // JobTask:
     *   void Run(JobDelegate* delegate) override {
     *     while (!delegate->ShouldYield()) {
     *       // Smallest unit of work.
     *       auto work_item = worker_queue_.TakeWorkItem(); // Thread safe.
     *       if (!work_item) return;
     *       ProcessWork(work_item);
     *     }
     *   }
     *
     *   size_t GetMaxConcurrency() const override {
     *     return worker_queue_.GetSize(); // Thread safe.
     *   }
     * };
     * auto handle = PostJob(TaskPriority::kUserVisible,
     *                       std::make_unique<MyJobTask>(...));
     * handle->Join();
     *
     * PostJob() and methods of the returned JobHandle/JobDelegate, must never be
     * called while holding a lock that could be acquired by JobTask::Run or
     * JobTask::GetMaxConcurrency -- that could result in a deadlock. This is
     * because [1] JobTask::GetMaxConcurrency may be invoked while holding
     * internal lock (A), hence JobTask::GetMaxConcurrency can only use a lock (B)
     * if that lock is *never* held while calling back into JobHandle from any
     * thread (A=>B/B=>A deadlock) and [2] JobTask::Run or
     * JobTask::GetMaxConcurrency may be invoked synchronously from JobHandle
     * (B=>JobHandle::foo=>B deadlock).
     *
     * A sufficient PostJob() implementation that uses the default Job provided in
     * libplatform looks like:
     *  std::unique_ptr<JobHandle> PostJob(
     *      TaskPriority priority, std::unique_ptr<JobTask> job_task) override {
     *    return v8::platform::NewDefaultJobHandle(
     *        this, priority, std::move(job_task), NumberOfWorkerThreads());
     * }
     */
    virtual std::unique_ptr<JobHandle> PostJob (
        TaskPriority priority, std::unique_ptr<JobTask> job_task) override
    {
        return v8::platform::NewDefaultJobHandle(
            this, priority, std::move(job_task), NumberOfWorkerThreads());
    }

#endif // V8_PLATFORM_HAS_JOB_INTERFACE

    /**
     * Monotonically increasing time in seconds from an arbitrary fixed point in
     * the past. This function is expected to return at least
     * millisecond-precision values. For this reason,
     * it is recommended that the fixed point be no further in the past than
     * the epoch.
     **/
    virtual double MonotonicallyIncreasingTime() override
    {
        std::chrono::duration<double> diff
            = std::chrono::steady_clock::now() - runner->start;
        return diff.count();
    }

    /**
     * Current wall-clock time in milliseconds since epoch.
     * This function is expected to return at least millisecond-precision values.
     */
    virtual double CurrentClockTimeMillis() override { return SystemClockTimeMillis(); }

    /**
     * Returns a function pointer that print a stack trace of the current stack
     * on invocation. Disables printing of the stack trace if nullptr.
     */
    virtual StackTracePrinter GetStackTracePrinter() override { return nullptr; }

    /**
     * Returns an instance of a v8::TracingController. This must be non-nullptr.
     */
    virtual v8::TracingController* GetTracingController() override
    {
        return new MldbV8TracingController();
    };

    /**
     * Tells the embedder to generate and upload a crashdump during an unexpected
     * but non-critical scenario.
     */
    virtual void DumpWithoutCrashing() override {}
};


} // namespace MLDB

