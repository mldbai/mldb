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
#include "mldb/base/scope.h"

#include "mldb/arch/spinlock.h"

using namespace std;

namespace MLDB {


/*****************************************************************************/
/* PROCESSING STATE INTERNAL                                                 */
/*****************************************************************************/

struct ComputeContext::Itl {
    Itl(int maxParallelism)
        : tp_(ThreadPool::instance(), maxParallelism, false /* handle exceptions */)
    {
    }

    Itl(Itl & parent, int maxParallelism)
        : tp_(parent.tp_, maxParallelism)
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

    Spinlock queue_mutex_;
    std::priority_queue<Job> queue_;
    int jobs_running_ = 0;

    ThreadPool tp_;

    bool single_threaded() const
    {
        return tp_.numThreads() == 0; 
    }

    size_t thread_count() const
    {
        return tp_.numThreads();
    }

    void stop(State reason)
    {
        ExcCheck(reason != RUNNING, "invalid reason for stopping");
        auto current_state = state_.load(std::memory_order_relaxed);
        while (current_state == RUNNING) {
            if (state_.compare_exchange_weak(current_state, reason))
                break;
        }

        stop_work();
    }

    void take_exception(std::string info)
    {
        ExcAssert(std::current_exception());
        auto current_state = state_.load(std::memory_order_relaxed);
        while (current_state == RUNNING) {
            if (state_.compare_exchange_weak(current_state, STOPPED_EXCEPTION)) {
                exc_ptr_ = std::current_exception();
                exc_info_ = std::move(info);
            }
        }

        stop(STOPPED_EXCEPTION);
    }

    void stop_work()
    {
        return;
        ExcCheck(state_.load(std::memory_order_seq_cst) != ComputeContext::RUNNING, "invalid state for stopping work");

        for (;;) {
            std::unique_lock guard{queue_mutex_};
            if (jobs_running_ == 0)
                break;
            
            guard.unlock();
            tp_.work();
            std::this_thread::yield();
        }
    }

    bool run_one()
    {
        bool result = false;
        //cerr << "running one in state " << state_ << endl;

        while (state_.load(std::memory_order_relaxed) == ComputeContext::RUNNING) {
            Job job;
            {
                std::unique_lock<Spinlock> guard(queue_mutex_);
                if (queue_.empty())
                    return result;
                job = queue_.top();
                queue_.pop();
                result = true;
            }

            try {
                Scope_Exit(std::unique_lock guard{this->queue_mutex_}; --this->jobs_running_);
                job.fn();
            } MLDB_CATCH_ALL {
                this->take_exception(job.info);
                return result;
            }
        }
        return result;
    }

    void submit(int priority, std::string info, std::function<void ()> fn)
    {
        auto runOne = [this] ()
        {
            this->run_one();
        };

        ExcCheckGreaterEqual(priority, 0, "invalid priority for submitted function");
        {
            std::unique_lock<Spinlock> guard(queue_mutex_);
            queue_.emplace(priority, std::move(info), std::move(fn));
            ++jobs_running_;
        }

        if (single_threaded()) {
            runOne();
            return;
        }

        try {
            tp_.add(runOne);
        } MLDB_CATCH_ALL {
            std::unique_lock<Spinlock> guard(queue_mutex_);
            --jobs_running_;
            throw;
        }
    }

    void work()
    {
        run_one();
        // Contribute to thread pool in case there is something there that is blocking the
        // work here
        tp_.work();
    }

    void work_until_finished()
    {
        // Note: race condition here... each job can submit more jobs, which means we can only
        // finish this loop when both a) there is nothing more to run in the thread pool and
        // b) there is nothing more to submit in the queue. However, these two are not
        // synchronized behind the same lock, so we can't check them atomically.
        //cerr << "working until finished" << endl;

        for (;;) {
            {
                std::unique_lock guard{queue_mutex_};
                if (jobs_running_ == 0 && queue_.empty()) {
                    break;
                }
            }

            if (!run_one()) {
                tp_.waitForAll();
            }
            tp_.work();
            std::this_thread::yield();
        }

        tp_.waitForAll();
        {
            std::unique_lock guard{queue_mutex_};
            ExcAssertEqual(jobs_running_, 0);
        }
    }

    void rethrow_if_exception()
    {
        if (exc_ptr_)
            std::rethrow_exception(exc_ptr_);
    }
};

/*****************************************************************************/
/* PROCESSING STATE                                                          */
/*****************************************************************************/


ComputeContext::ComputeContext(int maxParallelism)
    : itl_(new Itl(maxParallelism)), state_(itl_->state_)
{
}

ComputeContext::ComputeContext(ComputeContext & parent, int maxParallelism)
    : itl_(new Itl(*parent.itl_, maxParallelism)), state_(itl_->state_)
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
    itl_->take_exception(info);
}

bool ComputeContext::has_exception() const
{
    return state_.load(std::memory_order_relaxed) == STOPPED_EXCEPTION;
}

void ComputeContext::stop(State reason)
{
    itl_->stop(reason);
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
    return itl_->single_threaded();
}

size_t ComputeContext::thread_count() const
{
    return itl_->thread_count();
}

} // namespace MLDB
