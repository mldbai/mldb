/** compute_context.cc
    Jeremy Barnes, 29 November 2013
    Copyright (c) 2013 mldb.ai inc.  All rights reserved.
    This file is part of MLDB. Copyright 2015 mldb.ai inc. All rights reserved.

*/

#include "compute_context.h"
#include <atomic>
#include <exception>
#include <mutex>
#include <chrono>
#include <thread>
#include <cstring>
#include <compare>
#include <queue>
#include <functional>
#include <iostream>

#include "mldb/base/thread_pool.h"
#include "mldb/base/exc_assert.h"
#include "mldb/base/exc_check.h"
#include "mldb/arch/spinlock.h"
#include "mldb/ext/concurrentqueue/blockingconcurrentqueue.h"

using namespace std;

namespace MLDB {

using moodycamel::BlockingConcurrentQueue;

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



/*****************************************************************************/
/* PROCESSING STATE INTERNAL                                                 */
/*****************************************************************************/

struct ComputeContext::Itl {
    Itl(int maxParallelism)
        : max_parallelism_(maxParallelism), tp_(ThreadPool::instance(), maxParallelism, false /* handle exceptions */)
    {
    }

    Itl(Itl & parent)
        : max_parallelism_(parent.max_parallelism_), tp_(parent.tp_)
    {
    }

    std::atomic<State> state_ = ComputeContext::RUNNING;
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

    void stop_work()
    {
        for (int i = 0;  i <= max_parallelism_;  ++i) {
            work_.enqueue(Itl::WorkType::STOP);
        }
    }

    void submit(int priority, std::string info, std::function<void ()> fn)
    {
        ExcCheckGreaterEqual(priority, 0, "invalid priority for submitted function");
        {
            std::unique_lock<Spinlock> guard(queue_mutex_);
            queue_.emplace(priority, std::move(info), std::move(fn));
            work_.enqueue(WorkType::JOB);
        }
    }

    void work()
    {
        tp_.work();
    }

    void work_until_finished()
    {
        tp_.waitForAll();
    }

    void rethrow_if_exception()
    {
        if (exc_ptr_)
            std::rethrow_exception(exc_ptr_);
    }

    // All work is put here so that the threads know when to wake up and look for some.
    BlockingConcurrentQueue<WorkType> work_;
    ThreadPool tp_;
};

/*****************************************************************************/
/* PROCESSING STATE                                                          */
/*****************************************************************************/


ComputeContext::ComputeContext(int maxParallelism)
    : itl_(new Itl(maxParallelism)), state_(itl_->state_)
{
}

ComputeContext::ComputeContext(ComputeContext & parent, int maxParallelism)
    : itl_(new Itl(*parent.itl_)), state_(itl_->state_)
{
}

ComputeContext::~ComputeContext()
{
}

// Must be called from a catch statement. This takes ownership of the currently active
// exception and stops the processing, unless it has already been stopped for another
// reason (exception, explicit stop).
void ComputeContext::take_exception(std::string info)
{
    using namespace std;
    cerr << "taking exception in " << info << endl;
    ExcAssert(std::current_exception());
    auto current_state = state_.load(std::memory_order_relaxed);
    while (current_state == RUNNING) {
        if (state_.compare_exchange_weak(current_state, STOPPED_EXCEPTION)) {
            itl_->exc_ptr_ = std::current_exception();
            itl_->exc_info_ = std::move(info);
        }
    }

    stop();
}

bool ComputeContext::has_exception() const
{
    return state_.load(std::memory_order_relaxed) == STOPPED_EXCEPTION;
}

void ComputeContext::stop()
{
    auto current_state = state_.load(std::memory_order_relaxed);
    while (current_state == RUNNING) {
        if (state_.compare_exchange_weak(current_state, STOPPED_EXCEPTION))
            break;
    }

    itl_->stop_work();
}

void ComputeContext::submit(int priority, std::string info, std::function<void ()> fn)
{
    itl_->submit(priority, info, std::move(fn));
}

void ComputeContext::work()
{
    itl_->work();
}

void ComputeContext::work_until_finished()
{
    itl_->work_until_finished();
}

void ComputeContext::rethrow_if_exception()
{
    itl_->rethrow_if_exception();
}

bool ComputeContext::single_threaded() const
{
    return itl_->max_parallelism_ == 1;
}

} // namespace MLDB
