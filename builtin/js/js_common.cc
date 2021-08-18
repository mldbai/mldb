/** js_common.cc
    Jeremy Barnes, 12 June 2015
    Copyright (c) 2015 mldb.ai inc.  All rights reserved.
    This file is part of MLDB. Copyright 2015 mldb.ai inc. All rights reserved.

*/

#include "js_common.h"
#include "js_utils.h"
#include "js_plugin.h"
#include "dataset_js.h"
#include "procedure_js.h"
#include "sensor_js.h"
#include "procedure_js.h"
#include "function_js.h"
#include "mldb/vfs/fs_utils.h"
#include "mldb/vfs/filter_streams.h"
#include "mldb/sql/cell_value.h"
#include "mldb/sql/expression_value.h"
#include "mldb/types/basic_value_descriptions.h"
#include "mldb_js.h"
#include "libplatform/libplatform.h"
#include "mldb/base/thread_pool.h"
#include <boost/algorithm/string.hpp>
#include <regex>
#include "mldb/compiler/filesystem.h"
#include "mldb/arch/wait_on_address.h"
#include <thread>
#include <queue>
#include <condition_variable>
#include <shared_mutex>
#include "v8-version.h"
#include <errno.h>


using namespace std;
using namespace v8;

#define V8_PLATFORM_HAS_NON_NESTABLE_TASK_INTERFACE (V8_MAJOR_VERSION > 8)
#define V8_PLATFORM_HAS_JOB_INTERFACE (V8_MAJOR_VERSION > 8)

#if V8_PLATFORM_HAS_NON_NESTABLE_TASK_INTERFACE
#  define NON_NESTABLE_OVERRIDE override
#else
#  define NON_NESTABLE_OVERRIDE
#endif

namespace MLDB {

Logging::Category mldbJsCategory("javascript");

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
      std::unique_ptr<ConvertableToTraceFormat>* arg_convertables,
      unsigned int flags) override {
    return 0;
  }
  virtual uint64_t AddTraceEventWithTimestamp(
      char phase, const uint8_t* category_enabled_flag, const char* name,
      const char* scope, uint64_t id, uint64_t bind_id, int32_t num_args,
      const char** arg_names, const uint8_t* arg_types,
      const uint64_t* arg_values,
      std::unique_ptr<ConvertableToTraceFormat>* arg_convertables,
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

/**
 * V8 Platform abstraction layer.
 *
 * The embedder has to provide an implementation of this interface before
 * initializing the rest of V8.
 */
struct V8MldbPlatform: public v8::Platform {
    struct MldbTaskRunner: public v8::TaskRunner {
        MldbTaskRunner(V8MldbPlatform * platform)
            : platform(platform), 
              start(std::chrono::steady_clock::now()),
              shutdown(false),
              foregroundMessageLoop(std::bind(&MldbTaskRunner::runForegroundLoop,
                                              this))
        {
        }

        ~MldbTaskRunner()
        {
            shutdown = true;
            foregroundLoopCondition.notify_all();
            std::unique_lock<std::mutex> guard(mutex);
            foregroundMessageLoop.join();
        }

        V8MldbPlatform * platform;
        std::chrono::time_point<std::chrono::steady_clock> start;
        std::mutex mutex;
        std::atomic<bool> shutdown;
        std::condition_variable foregroundLoopCondition;

        typedef std::chrono::time_point<std::chrono::steady_clock> TimePoint;
        typedef std::tuple<TimePoint, std::shared_ptr<Task>> DelayedEntry;
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

        /**
         * Schedules a task to be invoked by this TaskRunner. The TaskRunner
         * implementation takes ownership of |task|.
         */
        virtual void PostTask(std::unique_ptr<Task> task) override
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
        virtual void PostNonNestableTask(std::unique_ptr<Task> task) NON_NESTABLE_OVERRIDE
        {
            throw MLDB::Exception("PostNonNestableTask");
        }

        /**
         * Schedules a task to be invoked by this TaskRunner. The task is scheduled
         * after the given number of seconds |delay_in_seconds|. The TaskRunner
         * implementation takes ownership of |task|.
         */
        virtual void PostDelayedTask(std::unique_ptr<Task> task,
                                     double delay_in_seconds) NON_NESTABLE_OVERRIDE
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
        virtual void PostNonNestableDelayedTask(std::unique_ptr<Task> task,
                                                double delay_in_seconds) NON_NESTABLE_OVERRIDE
        {
            throw MLDB::Exception("PostNonNestableDelayedTask");
        }

        /**
         * Schedules an idle task to be invoked by this TaskRunner. The task is
         * scheduled when the embedder is idle. Requires that
         * |TaskRunner::IdleTasksEnabled()| is true. Idle tasks may be reordered
         * relative to other task types and may be starved for an arbitrarily long
         * time if no idle time is available. The TaskRunner implementation takes
         * ownership of |task|.
         */
        virtual void PostIdleTask(std::unique_ptr<IdleTask> task) override
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
        virtual bool NonNestableTasksEnabled() const { return true; }

        /**
         * Returns true if non-nestable delayed tasks are enabled for this TaskRunner.
         */
        virtual bool NonNestableDelayedTasksEnabled() const { return false; }
    };

    std::shared_ptr<MldbTaskRunner> runner;

    // Protects isolateRunners
    std::shared_mutex isolateRunnersMutex;

    // Per-isolate runner (for older v8)
    // TODO: stop leaking them...
    std::map<const Isolate *, std::shared_ptr<MldbTaskRunner> > isolateRunners;

    std::shared_ptr<MldbTaskRunner> getIsolateRunner(const Isolate * isolate)
    {
	{
	    std::shared_lock lock(isolateRunnersMutex);
	    auto it = isolateRunners.find(isolate);
	    if (it != isolateRunners.end())
		return it->second;
	}

	auto runner = std::make_shared<MldbTaskRunner>(this);

	std::unique_lock lock(isolateRunnersMutex);
	auto it = isolateRunners.emplace(isolate, std::move(runner)).first;
	return it->second;
    }

    void removeIsolate(const Isolate * isolate)
    {
	std::unique_lock lock(isolateRunnersMutex);
	isolateRunners.erase(isolate);
    }
    
    V8MldbPlatform(MldbEngine * engine)
        : runner(new MldbTaskRunner(this))
    {
    }

    ~V8MldbPlatform()
    {
    }

    /**
     * Allows the embedder to manage memory page allocations.
     */
    virtual PageAllocator* GetPageAllocator() override {
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
        Isolate* isolate) override
    {
        return getIsolateRunner(isolate);
    }

    /**
     * Schedules a task to be invoked on a worker thread.
     */
    virtual void CallOnWorkerThread(std::unique_ptr<Task> task) override
    {
        std::shared_ptr<Task> taskPtr(task.release());
        auto lambda = [taskPtr] ()
            {
                taskPtr->Run();
            };
        ThreadPool::instance().add(std::move(lambda));
    }

    /**
     * Schedules a task that blocks the main thread to be invoked with
     * high-priority on a worker thread.
     */
    virtual void CallBlockingTaskOnWorkerThread(std::unique_ptr<Task> task) override {
        std::shared_ptr<Task> taskPtr(task.release());
        auto lambda = [taskPtr] ()
            {
                taskPtr->Run();
            };
        std::thread(std::move(lambda)).detach();
    }

    /**
     * Schedules a task to be invoked with low-priority on a worker thread.
     */
    virtual void CallLowPriorityTaskOnWorkerThread(std::unique_ptr<Task> task) NON_NESTABLE_OVERRIDE {
        // Embedders may optionally override this to process these tasks in a low
        // priority pool.
        std::shared_ptr<Task> taskPtr(task.release());
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
    virtual void CallDelayedOnWorkerThread(std::unique_ptr<Task> task,
                                            double delay_in_seconds) NON_NESTABLE_OVERRIDE
    {
        // TODO: don't create so many threads...
        std::shared_ptr<Task> sharedTask(task.release());
        auto runTask = [sharedTask, delay_in_seconds]
        {
            std::this_thread::sleep_for(std::chrono::milliseconds(uint64_t(delay_in_seconds * 1000)));
            sharedTask->Run();
        };

        std::thread(std::move(runTask)).detach();
    }

    // Older interface
    /**
     * Schedules a task to be invoked on a foreground thread wrt a specific
     * |isolate|. Tasks posted for the same isolate should be execute in order of
     * scheduling. The definition of "foreground" is opaque to V8.
     */
    virtual void CallOnForegroundThread(v8::Isolate* isolate, v8::Task* task)
    {
	std::unique_ptr<Task> pTask(task);
	return getIsolateRunner(isolate)->PostTask(std::move(pTask));
    }

    /**
     * Schedules a task to be invoked on a foreground thread wrt a specific
     * |isolate| after the given number of seconds |delay_in_seconds|.
     * Tasks posted for the same isolate should be execute in order of
     * scheduling. The definition of "foreground" is opaque to V8.
     */
    virtual void CallDelayedOnForegroundThread(Isolate* isolate, Task* task, double delay)
    {
	std::unique_ptr<Task> pTask(task);
	return getIsolateRunner(isolate)->PostDelayedTask(std::move(pTask), delay);
    }

    /**
     * Returns true if idle tasks are enabled for the given |isolate|.
     */
    virtual bool IdleTasksEnabled(Isolate* isolate) override { return false; }

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
#endif // V8_PLATFORM_HAS_TASK_INTERFACE
  
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
    virtual TracingController* GetTracingController() override
    {
        return new MldbV8TracingController();
    };

    /**
     * Tells the embedder to generate and upload a crashdump during an unexpected
     * but non-critical scenario.
     */
    virtual void DumpWithoutCrashing() NON_NESTABLE_OVERRIDE {}
};

void
JsIsolate::
init(bool forThisThreadOnly)
{
    Isolate::CreateParams create_params;
    create_params.array_buffer_allocator =
        v8::ArrayBuffer::Allocator::NewDefaultAllocator();

    this->isolate = v8::Isolate::New(create_params);


    this->isolate->Enter();

    this->isolate->SetCaptureStackTraceForUncaughtExceptions(true, 128 /* frame limit */,
                                                            v8::StackTrace::kDetailed);
    this->isolate->Exit();

    if (forThisThreadOnly) {
        // Lock this isolate to our thread for ever
        locker.reset(new v8::Locker(this->isolate));

        // We have a stack of currently entered isolates, and what we
        // want to put this one on the bottom of the stack
        //
        // This allows, when v8 has exited the last one, for our thread
        // specific isolate to stay entered, so that there is no cost to
        // enter it for a function that's called on a worker thread.
        //
        // current isolate
        // next oldest
        // next oldest
        // oldest
        // <ours should go here>
        // ---------
        // 
        // To do so, we need to pop them all off momentarily, push ours,
        // and push all the old ones back again.

        // Pop them all off momentarily
        std::vector<v8::Isolate *> oldIsolates;
        oldIsolates.reserve(128);
        while (v8::Isolate::GetCurrent()) {
            oldIsolates.push_back(v8::Isolate::GetCurrent());
            v8::Isolate::GetCurrent()->Exit();
        }

        // Push ours on
        this->isolate->Enter();

        // Push the others back on top
        while (!oldIsolates.empty()) {
            oldIsolates.back()->Enter();
            oldIsolates.pop_back();
        }
    }
}

V8Init::
V8Init(MldbEngine * engine)
{
    static std::atomic<bool> alreadyDone(false);
    if (alreadyDone)
        return;

    static std::mutex mutex;
    std::unique_lock<std::mutex> guard(mutex);

    if (alreadyDone)
        return;

    const char * v8_argv[] = {
        "--lazy", "false"
        ,"--always_opt", "true"
    };
    int v8_argc = sizeof(v8_argv) / sizeof(const char *);

    v8::V8::SetFlagsFromCommandLine(&v8_argc, (char **)&v8_argv, false);

#if 0 // startup data should be embedded now..
    // TODO: linuxisms...
    char exePath[PATH_MAX];
    ssize_t pathLen = readlink("/proc/self/exe", exePath, PATH_MAX);
    if (pathLen == -1)
        throw AnnotatedException
            (400, "Couldn't find path to the MLDB executable; "
             "is the /proc filesystem mounted?  ("
             + string(strerror(errno)) + ")");
    std::filesystem::path path(exePath, exePath + pathLen);
    auto libPath = path.parent_path().parent_path() / "lib" / "libv8.so";

    v8::V8::InitializeExternalStartupData(libPath.c_str());
#endif

    v8::V8::InitializePlatform(new V8MldbPlatform(engine));
    v8::V8::Initialize();

    alreadyDone = true;
}

CellValue from_js(const JS::JSValue & value, CellValue *)
{
    v8::Isolate* isolate = v8::Isolate::GetCurrent();
    v8::HandleScope scope(isolate);
    auto context = isolate->GetCurrentContext();

    if (value->IsNull() || value->IsUndefined())
        return CellValue();
    else if (value->IsNumber())
        return CellValue(JS::check(value->NumberValue(context)));
    else if (value->IsDate())
        return CellValue(Date::fromSecondsSinceEpoch(JS::check(value->NumberValue(context)) / 1000.0));
    else if (value->IsObject()) {
        // Look if it's already a CellValue
        JsPluginContext * cxt = JsContextScope::current();
        if (cxt->CellValue.Get(isolate)->HasInstance(value)) {
            return CellValueJS::getShared(value);
        }
        else {
            // Try to go through JSON
            Json::Value val = JS::fromJS(value);
            return jsonDecode<CellValue>(val);
        }
    }
    else return CellValue(utf8str(value));
}

void to_js(JS::JSValue & value, const CellValue & val)
{
    v8::Isolate* isolate = v8::Isolate::GetCurrent();

    if (val.empty())
        value = v8::Null(isolate);
    else if (val.isExactDouble())
        to_js(value, val.toDouble());
    else if (val.isUtf8String())
        to_js(value, val.toUtf8String());
    else if (val.isTimestamp()) {
        to_js(value, val.toTimestamp());
    }
    else if (val.isString()) {
        to_js(value, val.toString());
    }
    else {
        // Get our context so we can return a proper object
        JsPluginContext * cxt = JsContextScope::current();
        value = CellValueJS::create(val, cxt);
    }
}

PathElement from_js(const JS::JSValue & value, PathElement *)
{
    if (value->IsNull() || value->IsUndefined())
        return PathElement();
    return JS::from_js(value, (Utf8String *)0);
}

PathElement from_js_ref(const JS::JSValue & value, PathElement *)
{
    if (value->IsNull() || value->IsUndefined())
        return PathElement();
    return JS::from_js(value, (Utf8String *)0);
}

void to_js(JS::JSValue & value, const PathElement & val)
{
    v8::Isolate* isolate = v8::Isolate::GetCurrent();
    if (val.null())
        value = v8::Null(isolate);
    return to_js(value, val.toUtf8String());
}

Path from_js(const JS::JSValue & value, Path *)
{
    if (value->IsNull() || value->IsUndefined())
        return Path();
    if (value->IsArray()) {
        auto vals = JS::from_js(value, (std::vector<PathElement> *)0);
        return Path(vals.data(), vals.size());
    }
    return jsonDecode<Path>(JS::from_js(value, (Json::Value *)0));
}

Path from_js_ref(const JS::JSValue & value, Path *)
{
    return from_js(value, (Path *)0);
}

void to_js(JS::JSValue & value, const Path & val)
{
    v8::Isolate* isolate = v8::Isolate::GetCurrent();
    if (val.empty())
        value = v8::Null(isolate);
    return to_js(value, vector<PathElement>(val.begin(), val.end()));
}

void to_js(JS::JSValue & value, const ExpressionValue & val)
{
    if (val.isAtom()) {
        to_js(value, val.getAtom());
    }
    else if (val.isEmbedding()) {
        // TODO: numberarray
        to_js(value, val.extractJson());
    }
    else {
        to_js(value, val.extractJson());
    }
}

ExpressionValue from_js(const JS::JSValue & value, ExpressionValue *)
{
    // NOTE: we currently pretend that CellValue and ExpressionValue
    // are the same thing; they are not.  We will eventually need to
    // allow proper JS access to full-blown ExpressionValue objects,
    // backed with a JS object.

    CellValue val = from_js(value, (CellValue *)0);
    return ExpressionValue(val, Date::notADate());
}

ScriptStackFrame
parseV8StackFrame(const std::string & v8StackFrameMessage)
{
    ScriptStackFrame result;

    static std::regex format1("[ ]*at (.*) \\((.*):([0-9]+):([0-9]+)\\)");
    static std::regex format2("[ ]*at (.*):([0-9]+):([0-9]+)");

    std::smatch what;
    if (std::regex_match(v8StackFrameMessage, what, format1)) {
        ExcAssertEqual(what.size(), 5);
        result.functionName = what[1];
        result.scriptUri = what[2];
        result.lineNumber = std::stoi(what[3]);
        result.columnStart = std::stoi(what[4]);
    }
    else if (std::regex_match(v8StackFrameMessage, what, format2)) {
        ExcAssertEqual(what.size(), 4);
        result.scriptUri = what[1];
        result.lineNumber = std::stoi(what[2]);
        result.columnStart = std::stoi(what[3]);
    }
    else {
        result.functionName = v8StackFrameMessage;
    }

    return result;
}



/** Convert an exception to its representation. */
ScriptException convertException(const v8::TryCatch & trycatch,
                                 const Utf8String & contextStr)
{
    using namespace v8;

    auto isolate = v8::Isolate::GetCurrent();
    auto context = isolate->GetCurrentContext();

    if (!trycatch.HasCaught())
        throw MLDB::Exception("function didn't return but no result");
    
    Handle<Value> exception = trycatch.Exception();
    String::Utf8Value exception_str(isolate, exception);
    
    ScriptException result;
    result.context.push_back(contextStr);
    string where = "(unknown error location)";

    Handle<Message> message = trycatch.Message();

    Json::Value jsonException = JS::fromJS(exception);

    result.extra = jsonException;

    if (!message.IsEmpty()) {
        Utf8String msgStr = JS::utf8str(message->Get());
        Utf8String sourceLine = JS::utf8str(message->GetSourceLine(context));
        Utf8String scriptUri = JS::utf8str(message->GetScriptResourceName());
        
        //cerr << "msgStr = " << msgStr << endl;
        //cerr << "sourceLine = " << sourceLine << endl;
        
        int line = JS::check(message->GetLineNumber(context));
        int column = message->GetStartColumn();
        int endColumn = message->GetEndColumn();
        
        // Note: in the case of backslashed lines, the columns may go past
        // the length of the text in sourceLine (MLDB-980)
        if (column <= sourceLine.length())
            sourceLine.replace(column, 0, "[[[[");
        if (endColumn + 4 <= sourceLine.length())
            sourceLine.replace(endColumn + 4, 0, "]]]]");

        where = MLDB::format("file '%s', line %d, column %d, source '%s': %s",
                           scriptUri.rawData(),
                           line, column,
                           sourceLine.rawData(),
                           msgStr.rawData());

        result.message = msgStr;
        result.where = where;
        result.scriptUri = JS::utf8str(message->GetScriptResourceName());
        result.lineNumber = line;
        result.columnStart = column;
        result.columnEnd = endColumn;
        result.lineContents = sourceLine;

        auto stack = message->GetStackTrace();

        if (!stack.IsEmpty()) {

            for (unsigned i = 0;  i < stack->GetFrameCount();  ++i) {
                auto frame = stack->GetFrame(isolate, i);
                ScriptStackFrame frameRep;
                frameRep.scriptUri = JS::utf8str(frame->GetScriptNameOrSourceURL());
                frameRep.functionName = JS::utf8str(frame->GetFunctionName());
                frameRep.lineNumber = frame->GetLineNumber();
                frameRep.columnStart = frame->GetColumn();

                Json::Value extra;
                extra["isEval"] = frame->IsEval();
                extra["isConstructor"] = frame->IsConstructor();
                
                frameRep.extra = std::move(extra);

                result.stack.emplace_back(std::move(frameRep));
            }
        }
        else {
            auto stack2 = trycatch.StackTrace(context);

            if (!stack2.IsEmpty()) {
                string traceMessage = JS::cstr(stack2);

                vector<string> traceLines;
                boost::split(traceLines, traceMessage,
                             boost::is_any_of("\n"));

                for (unsigned i = 1;  i < traceLines.size();  ++i) {
                    ScriptStackFrame frame = parseV8StackFrame(traceLines[i]);
                    result.stack.emplace_back(std::move(frame));
                }
            }
        }
        
    }

    return result;
}


/*****************************************************************************/
/* JS OBJECT BASE                                                            */
/*****************************************************************************/

JsObjectBase::
~JsObjectBase()
{
    if (js_object_.IsEmpty()) return;
    //if (!js_object_.IsNearDeath()) {
    //    ::fprintf(stderr, "JS object is not near death");
    //    std::terminate();
    //}
    //v8::Isolate* isolate = v8::Isolate::GetCurrent();
    //js_object_->SetInternalField(0, v8::Undefined(isolate));
    //js_object_->SetInternalField(1, v8::Undefined(isolate));
    js_object_.Reset();
}

JsPluginContext *
JsObjectBase::
getContext(const v8::Handle<v8::Object> & val)
{
    return reinterpret_cast<JsPluginContext *>
        (v8::Handle<v8::External>::Cast
         (val->GetInternalField(1))->Value());
}

void
JsObjectBase::
wrap(v8::Handle<v8::Object> handle, JsPluginContext * context)
{
    v8::Isolate* isolate = v8::Isolate::GetCurrent();

    ExcAssert(js_object_.IsEmpty());

    if (handle->InternalFieldCount() == 0) {
        throw MLDB::Exception("InternalFieldCount is zero; are you forgetting "
                            "to use 'new'?");
    }

    ExcAssertEqual(handle->InternalFieldCount(), 2);

    handle->SetInternalField(0, v8::External::New(isolate, this));
    handle->SetInternalField(1, v8::External::New(isolate, context));

    js_object_.Reset(isolate, handle);
    registerForGarbageCollection();
}

/** Set this object up to be garbage collected once there are no more
    references to it in the javascript. */
void
JsObjectBase::
registerForGarbageCollection()
{
    js_object_.SetWeak(this, garbageCollectionCallback,
                       v8::WeakCallbackType::kParameter);
}
    
void
JsObjectBase::
NoConstructor(const v8::FunctionCallbackInfo<v8::Value> & args)
{
    args.GetReturnValue().Set(args.This());
}


v8::Handle<v8::FunctionTemplate>
JsObjectBase::
CreateFunctionTemplate(const char * name,
                       v8::FunctionCallback constructor)
{
    using namespace v8;
        
    v8::Isolate* isolate = v8::Isolate::GetCurrent();

    v8::Handle<v8::FunctionTemplate> t
        = FunctionTemplate::New(isolate, constructor);

    t->InstanceTemplate()->SetInternalFieldCount(2);
    t->SetClassName(JS::createString(isolate, name));

    return t;
}

    
void
JsObjectBase::
garbageCollectionCallback(const v8::WeakCallbackInfo<JsObjectBase> & info)
{
    JsObjectBase * obj = info.GetParameter();
    delete obj;
}



/*****************************************************************************/
/* JS PLUGIN CONTEXT                                                         */
/*****************************************************************************/

JsPluginContext::
JsPluginContext(const Utf8String & pluginName,
                MldbEngine * engine,
                std::shared_ptr<LoadedPluginResource> pluginResource)
    : categoryName(pluginName.rawString() + " plugin"),
      loaderName(pluginName.rawString() + " loader"),
      category(categoryName.c_str()),
      loader(loaderName.c_str()),
      engine(engine),
      pluginResource(pluginResource)
{
    using namespace v8;

    static V8Init v8Init(engine);

    this->isolate.init(false /* for this thread only */);
    auto isolate = this->isolate.isolate;

    v8::Locker locker(isolate);
    v8::Isolate::Scope isolate_scope(isolate);

    HandleScope handle_scope(isolate);

    // Create a new context.
    this->context.Reset(isolate, Context::New(isolate));
    
    // Enter the created context
    Context::Scope context_scope(this->context.Get(isolate));
    
    auto context = this->context.Get(isolate);

    // This is how we set it
    // https://code.google.com/p/v8/issues/detail?id=54
    v8::Local<v8::Object> globalPrototype
        = context->Global()->GetPrototype().As<v8::Object>();
    
    auto plugin = JS::toLocalChecked(JsPluginJS::registerMe()->NewInstance(context));
    plugin->SetInternalField(0, v8::External::New(isolate, this));
    plugin->SetInternalField(1, v8::External::New(isolate, this));
    if (pluginResource)
        JS::check(plugin->Set(context, JS::createString(isolate, "args"), JS::toJS(jsonEncode(pluginResource->args))));
    JS::check(globalPrototype->Set(context, JS::createString(isolate, "plugin"), plugin));

    auto mldb = JS::toLocalChecked(MldbJS::registerMe()->NewInstance(context));
    this->mldb.Reset(isolate, mldb);
    mldb->SetInternalField(0, v8::External::New(isolate, this->engine));
    mldb->SetInternalField(1, v8::External::New(isolate, this));
    JS::check(globalPrototype->Set(context, JS::createString(isolate, "mldb"), mldb));

    Stream.Reset(isolate, StreamJS::registerMe());
    Dataset.Reset(isolate, DatasetJS::registerMe());
    Function.Reset(isolate, FunctionJS::registerMe());
    Sensor.Reset(isolate, SensorJS::registerMe());
    Procedure.Reset(isolate, ProcedureJS::registerMe());
    CellValue.Reset(isolate, CellValueJS::registerMe());
    RandomNumberGenerator.Reset(isolate,
                                RandomNumberGeneratorJS::registerMe());

    auto requireTemplate
        = FunctionTemplate::New(isolate,
                                &JsPluginContext::require,
                                v8::External::New(isolate, this));
    JS::check(globalPrototype
                ->Set(context, JS::createString(isolate, "require"),
                      JS::toLocalChecked(requireTemplate->GetFunction(context))));
}

JsPluginContext::
~JsPluginContext()
{
}

std::tuple<Utf8String,
           Utf8String,
           FsObjectInfo>
JsPluginContext::
findModuleSource(const Utf8String & moduleName)
{
    std::vector<Utf8String> searchPath;

    if (moduleName.rawString().find("mldb/") == 0) {
        // It's an internal MLDB plugin; we look only for code that is
        // handled with MLDB
        searchPath = { "file://build/x86_84/lib/mldb/js", "file://mldb/builtin/js" };
    }
    else {
        throw AnnotatedException(400, "Only require modules under mldb are "
                                 "supported",
                                 "required", moduleName);
    }

    for (auto & path: searchPath) {
        Utf8String filename = path + "/" + moduleName + ".js";
        if (tryGetUriObjectInfo(filename.rawString())) {
            MLDB_TRACE_EXCEPTIONS(false);
            try {
                filter_istream stream(filename.rawString());
                return { filename, stream.readAll(), stream.info() };
            } MLDB_CATCH_ALL {
                continue;
            }
        }
    }

    throw AnnotatedException(400, "Unable to find javascript module " + moduleName,
                             "moduleName", moduleName,
                             "searchPath", searchPath);
}

void
JsPluginContext::
require(const v8::FunctionCallbackInfo<v8::Value> & args)
{
    v8::Isolate* isolate = args.GetIsolate();
    v8::EscapableHandleScope scope(isolate);
    try {
        auto context
            = reinterpret_cast<JsPluginContext *>
            (v8::Handle<v8::External>::Cast(args.Data())->Value());
        
        Utf8String moduleName = JS::getArg<Utf8String>(args, 0, "moduleName");

        auto exports = context->getModule(moduleName);

        args.GetReturnValue().Set(scope.Escape(exports));
    } HANDLE_JS_EXCEPTIONS(args);
}

v8::Handle<v8::Object>
JsPluginContext::
getModule(const Utf8String & moduleName)
{
    auto isolate = this->isolate.isolate;
    auto context = isolate->GetCurrentContext();
    //cerr << "require " << moduleName << endl;

    if (moduleName == "mldb") {
        // MLDB object is special; we get it from the context
        v8::Local<v8::Object> mldb = this->mldb.Get(isolate);
        return mldb;
    }

    using namespace v8;
        
    // Load, compile and run the module
    Utf8String jsFunctionFilename;
    Utf8String jsFunctionSource;
    FsObjectInfo jsFunctionInfo;

    std::tie(jsFunctionFilename, jsFunctionSource, jsFunctionInfo)
        = findModuleSource(moduleName);
        
    // Create a string containing the JavaScript source code.
    Handle<String> source = JS::createString(isolate, jsFunctionSource);

    // Compile the source code.
    TryCatch trycatch(isolate);
    trycatch.SetVerbose(true);

    auto origin = JS::createScriptOrigin(isolate, jsFunctionFilename);
    auto script = Script::Compile(context, source, &origin);
    
    if (script.IsEmpty()) {  
        auto rep = convertException(trycatch, "Compiling plugin script");
        throw AnnotatedException(400, "Exception compiling plugin script", rep);
    }

    auto globals = context->Global();
    auto module = v8::Object::New(isolate);

    JS::check(globals->Set(context, JS::createString(isolate, "module"), module));
        
    // Run the script to get the result.
    auto result = JS::toLocalChecked(script)->Run(context);

    if (result.IsEmpty()) {  
        auto rep = convertException(trycatch, "Running plugin script");
        MLDB_TRACE_EXCEPTIONS(false);
        throw AnnotatedException(400, "Exception running plugin script", rep);
    }

    // We ignore the result after checking that there is no exception

    JS::check(globals->Delete(context, JS::createString(isolate, "module")));

    auto exports = JS::toLocalChecked(module->Get(context, JS::createString(isolate, "exports")));

    return JS::toObject(exports);
}

std::vector<Utf8String>
JsPluginContext::
knownModules()
{
    // TODO: scan search path
    return { "mldb", "mldb/unittest" };
}


/*****************************************************************************/
/* JS CONTEXT SCOPE                                                          */
/*****************************************************************************/

JsContextScope::
JsContextScope(JsPluginContext * context)
    : context(context)
{
    enter(context);
}

JsContextScope::
JsContextScope(const v8::Handle<v8::Object> & val)
    : context(JsObjectBase::getContext(val))
{
    enter(context);
}

JsContextScope::
~JsContextScope()
{
    exit(context);
}

static __thread std::vector<JsPluginContext *> * jsContextStack = nullptr;

JsPluginContext *
JsContextScope::
current()
{
    if (!jsContextStack || jsContextStack->empty())
        throw MLDB::Exception("attempt to retrieve JS context stack with nothing on it");
    return jsContextStack->back();
}

void
JsContextScope::
enter(JsPluginContext * context)
{
    if (!jsContextStack)
        jsContextStack = new std::vector<JsPluginContext *>();
    jsContextStack->push_back(context);
}

void
JsContextScope::
exit(JsPluginContext * context)
{
    if (current() != context)
        throw MLDB::Exception("JS context stack consistency error");
    jsContextStack->pop_back();
}

namespace JS {


Json::Value
fromJsForRestParams(const JSValue & val)
{
    auto isolate = v8::Isolate::GetCurrent();
    auto context = isolate->GetCurrentContext();
    //cerr << "converting " << cstr(val) << " to rest params" << endl;

    if (val->IsDate()) {
        //cerr << "date" << endl;
        double ms = JS::check(val->NumberValue(context));
        MLDB::CellValue cv(Date::fromSecondsSinceEpoch(ms / 1000.0));
        return jsonEncode(cv);
    }
    else if (val->IsArray()) {
        auto arrayPtr = v8::Array::Cast(*val);

        Json::Value result;

        for (int i=0; i<arrayPtr->Length(); ++i) {
            auto val = arrayPtr->Get(context, i);
            result[i] = fromJsForRestParams(val);
        }
        
        return result;
    }
    else if (val->IsObject()) {

        auto objPtr = v8::Object::Cast(*val);

        Json::Value result;

        v8::Local<v8::Array> properties = JS::toLocalChecked(objPtr->GetOwnPropertyNames(context));
        
        for (int i=0; i<properties->Length(); ++i) {
            v8::Local<v8::Value> key = JS::toLocalChecked(properties->Get(context, i));
            v8::Local<v8::Value> val = JS::toLocalChecked(objPtr->Get(context, key));
            result[utf8str(key)] = fromJsForRestParams(val);
        }
        
        return result;
    }
    else {
        Json::Value jval = JS::fromJS(val);
        //cerr << "got val " << jval << endl;
        return jval;
    }
}

RestParams
from_js(const JSValue & val, const RestParams *)
{
    auto isolate = v8::Isolate::GetCurrent();
    auto context = isolate->GetCurrentContext();

    RestParams result;

    if (val->IsArray()) {

        auto arrPtr = v8::Array::Cast(*val);

        for (int i=0; i<arrPtr->Length(); ++i) {
            auto arrPtr2 = v8::Array::Cast(*JS::toLocalChecked(arrPtr->Get(context, i)));
            if(arrPtr2->Length() != 2) {
                throw MLDB::Exception("invalid length for pair extraction");
            }
            
            Json::Value param = fromJsForRestParams(arrPtr2->Get(context, 1));

            if (param.isString())
                result.emplace_back(utf8str(arrPtr2->Get(context, 0)),
                                    param.asString());
            else
                result.emplace_back(utf8str(arrPtr2->Get(context, 0)),
                                    param.toStringNoNewLine());
        }

        return result;
    }
    else if (val->IsObject()) {

        //cerr << "rest params from object " << cstr(val) << endl;

        auto objPtr = v8::Object::Cast(*val);

        v8::Local<v8::Array> properties = JS::toLocalChecked(objPtr->GetOwnPropertyNames(context));
        
        for(int i=0; i<properties->Length(); ++i) {
            v8::Local<v8::Value> key = JS::toLocalChecked(properties->Get(context, i));
            v8::Local<v8::Value> val = JS::toLocalChecked(objPtr->Get(context, key));
            Json::Value param = fromJsForRestParams(val);

            if (param.isString())
                result.emplace_back(utf8str(key),
                                    param.asString());
            else
                result.emplace_back(utf8str(key),
                                    param.toStringNoNewLine());
        }
        
        //cerr << "got " << jsonEncode(result) << endl;

        return result;
    }
    else throw MLDB::Exception("couldn't convert JS value '%s' to REST parameters",
                             cstr(val).c_str());
}

} // namespace JS

} // namespace MLDB
