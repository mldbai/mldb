/** processing_state.cc
    Jeremy Barnes, 29 November 2013
    Copyright (c) 2013 mldb.ai inc.  All rights reserved.
    This file is part of MLDB. Copyright 2015 mldb.ai inc. All rights reserved.

*/

#include "processing_state.h"
#include <atomic>
#include <exception>
#include <mutex>
#include <chrono>
#include <thread>
#include <cstring>
#include <compare>
#include <queue>
#include <functional>
#include "mldb/base/thread_pool.h"
#include "mldb/base/exc_assert.h"
#include "mldb/base/exc_check.h"
#include "mldb/arch/spinlock.h"
#include "mldb/ext/concurrentqueue/blockingconcurrentqueue.h"

namespace MLDB {

using moodycamel::BlockingConcurrentQueue;

/*****************************************************************************/
/* PROCESSING STATE INTERNAL                                                 */
/*****************************************************************************/

struct ProcessingState::Itl {
    Itl(int maxParallelism)
        : tp_(maxParallelism)
    {
    }

#if 0
    // Running a thread ties up a core for just this job, which is not good in
    // the context of larger amounts of work.
    void runThread()
    {
        while (!stopped()) {
            WorkType work;

            // TODO: we shouldn't need to give a certain amount of time
            if (!work_.wait_dequeue_timed(work, 1000)) {
                continue;
            }

            if (work == WorkType::STOP)
                break;
            
            Job job;
            {
                std::unique_lock<Spinlock> guard(queue_mutex_);
                if (queue_.empty())
                    continue;
                job = queue_.top();
            }

            try {
                job.fn();
            } MLDB_CATCH_ALL {
                takeException(job.info);
                break;
            }
        }
    }
#endif

    std::atomic<int> stop_ = 0;
    std::atomic<int> has_exception_ = 0;
    std::exception_ptr exc_ptr_;
    std::string exc_info_;

    struct Job {
        int priority;
        std::string info;
        std::function<void ()> fn;
        auto operator <=> (const Job & other) const { return priority <=> other.priority; }
    };

    int max_parallelism_ = -1;

    Spinlock queue_mutex_;
    std::priority_queue<Job> queue_;

    enum class WorkType {
        STOP,  // The queue should stop running
        JOB    // There is a queued job
    };

    // All work is put here so that the threads know when to wake up and look for some.
    BlockingConcurrentQueue<WorkType> work_;
    ThreadWorkGroup tp_;
};

/*****************************************************************************/
/* PROCESSING STATE                                                          */
/*****************************************************************************/


ProcessingState(int maxParallelism = -1)
    : max_parallelism_(maxParallelism == -1 ? numCpus() : maxParallelism),
      itl(new Itl(max_parallelism_))
{
    // Start the threads to do the work (they will sit there waiting for it)
    for (int i = 0;  i < max_parallelism_;  ++i) {
        tp_.add(std::bind(&ProcessingState::runThread, this));
    }
}

ProcessingState::~ProcessingState()
{
}

// Must be called from a catch statement. This takes ownership of the currently active
// exception and stops the processing.
void ProcessingState::takeException(std::string info)
{
    using namespace std;
    cerr << "taking exception in " << info << endl;
    ExcAssert(std::current_exception());
    if (has_exception_.fetch_add(1) == 0) {
        exc_ptr_ = std::current_exception();
        exc_info_ = std::move(info);
    }
    stop();
}

bool ProcessingState::hasException() const
{
    return has_exception_;
}

void ProcessingState::stop()
{
    stop_ = true;
    for (int i = 0;  i <= max_parallelism_;  ++i) {
        work_.enqueue(WorkType::STOP);
    }
}

void ProcessingState::submit(int priority, std::string info, std::function<void ()> fn)
{
    ExcCheckGreaterEqual(priority, 0, "invalid priority for submitted function");
    {
        std::unique_lock<Spinlock> guard(queue_mutex_);
        queue_.emplace(priority, std::move(info), std::move(fn));
        work_.enqueue(WorkType::JOB);
    }
}

void ProcessingState::waitForAll()
{
    tp_.waitForAll();
}

void ProcessingState::rethrowIfException()
{
    if (exc_ptr_)
        std::rethrow_exception(exc_ptr_);
}

} // namespace MLDB
