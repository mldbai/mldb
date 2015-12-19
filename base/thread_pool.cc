/** thread_pool.cc
    Jeremy Barnes, 18 December 2015
    Copyright (c) 2015 Datacratic Inc.  All rights reserved.

    This file is part of MLDB. Copyright 2015 Datacratic. All rights reserved.

    Implementation of the tread pool.
*/

#include "thread_pool.h"
#include "thread_pool_impl.h"
#include "mldb/arch/thread_specific.h"
#include "mldb/arch/futex.h"
#include "mldb/arch/rcu_protected.h"
#include "arch/cpu_info.h"
#include <atomic>
#include <vector>
#include <thread>
#include <iostream>


using namespace std;


namespace Datacratic {

int numCpus()
{
    return ML::num_cpus();
}

/*****************************************************************************/
/* THREAD POOL                                                               */
/*****************************************************************************/

/** This is the internal implementation of the ThreadPool class.

    There are two kinds of threads: pool threads and external threads.
    
    Pool threads are those which are created as part of the thread pool, and
    whose sole function is to perform work.  These will continualy scan for
    work, and once there is no work, go to sleep until there is more.  There
    is a fixed, known number of pool threads.

    External threads may submit work, and may even contribute to getting work
    done while they are waiting for it to be finished.  Frequently we will
    end up with an external thread producing lots of work for the worker
    threads, but not being able to do much itself.  So the ability to
    handle lots of work being submitted by a given thread but not much being
    done by it is important.
*/

struct ThreadPool::Itl {

    struct ThreadEntry {
        ThreadEntry(Itl * owner = nullptr, int threadNum = -1)
            : owner(owner), threadNum(threadNum),
              queue(new ThreadQueue<ThreadJob>())
        {
        }

        ~ThreadEntry()
        {
            // If we were never associated, we have nothing to do
            if (!owner)
                return;

            // Thread is being shutdown.  But what to do with
            // its work?  If it's a shutdown due to an exception,
            // OK.  But otherwise it should have waited for it
            // to be done.
            owner->unpublishThread(this);
        }

        Itl * owner;
        int threadNum;
        std::shared_ptr<ThreadQueue<ThreadJob> > queue;

        /// List of queues on other threads
        std::vector<std::shared_ptr<ThreadQueue<ThreadJob> > > queues;
    };

    /// This allows us to have one threadEntry per thread
    ML::ThreadSpecificInstanceInfo<ThreadEntry, void> threadEntries;

    /// Our internal worker threads
    std::vector<std::thread> workers;

    struct Jobs {
        std::atomic<int> submitted;
        std::atomic<int> finished;
    };

    union {
        Jobs jobs;
        std::atomic<uint64_t> startedFinished;
    };

    std::atomic<uint64_t> jobsStolen, jobsWithFullQueue, jobsRunLocally;

    // Note: unsigned so we wrap around properly
    unsigned jobsRunning() const
    {
        uint64_t val = startedFinished;

        union {
            Jobs jobs;
            uint64_t startedFinished;
        };

        startedFinished = val;
        
        return jobs.submitted - jobs.finished;
    }

    int jobsSubmitted() const
    {
        return jobs.submitted;
    }

    int jobsFinished() const
    {
        return jobs.finished;
    }

    /// Non-zero when we're shutting down.
    std::atomic<int> shutdown;
    
    /// Number of sleeping threads
    std::atomic<int> threadsSleeping;

    Itl(int numThreads)
        : jobsStolen(0),
          jobsWithFullQueue(0),
          jobsRunLocally(0),
          shutdown(0),
          threadsSleeping(0),
          knownThreads(gcLock)
    {
        jobs.submitted = 0;
        jobs.finished = 0;

        for (unsigned i = 0;  i < numThreads;  ++i) {
            workers.emplace_back([this, i] () { this->runWorker(i); });
        }

        while (threadsSleeping < numThreads)
            ;

        getEntry();

        std::vector<std::shared_ptr<ThreadQueue<ThreadJob> > > threadQueues;

        for (auto & t: *knownThreads()) {
            threadQueues.emplace_back(t->queue);
        }

        for (auto & t: *knownThreads()) {
            t->queues = threadQueues;
        }
    }

    ~Itl()
    {
        this->shutdown = 1;
        
        knownThreads.replace(new std::vector<ThreadEntry *>(), false /* defer */);
        
        this->shutdown = 2;

        ML::futex_wake(threadsSleeping, -1 /* all threads */);
        
        for (auto & w: workers)
            w.join();
    }

    ThreadEntry & getEntry()
    {
        ThreadEntry * threadEntry = threadEntries.get();
        ExcAssert(threadEntry);

        // If it's not initialized yet, this is the first time we've
        // seen this thread.  So we initialize the thread's entry.
        if (!threadEntry->owner) {
            threadEntry->owner = this;
            publishThread(threadEntry);
        }

        return *threadEntry;
    }

    void add(ThreadJob job)
    {
        //ThreadEntry & entry = getEntry();

        jobs.submitted += 1;

        if (getEntry().queue->push(job)) {
            if (threadsSleeping)
                ML::futex_wake(threadsSleeping, 1);
        }
        else {
            // The queue was full.  Do the work here, hopefully someone
            // will steal some work in the meantime.
            ++jobsWithFullQueue;
            runJob(job);
        }
    }

    /** Runs as much work as possible in this thread's queue.  Returns
        true if some work was obtained.
    */
    bool runMine()
    {
        ThreadEntry & entry = getEntry();

        bool result = false;

        // First, do all of our work
        ThreadJob job;
        while (entry.queue->pop(job)) {
            result = true;
            ++jobsRunLocally;
            runJob(job);
        }
        
        return result;
    }

    /** Steal one bit of work from another thread and run it.  Returns
        true if some work was obtained.
    */
    bool stealWork()
    {
#if 1
        RcuLocked<std::vector<ThreadEntry *> > thr;

        {
            //std::unique_lock<std::mutex> guard(knownThreadsLock);
            thr = knownThreads();
        }

        bool result = false;
        
        for (ThreadEntry * thread: *thr) {
            if (shutdown)
                return false;

            ExcAssert(thread);
            ThreadJob job;
            while (thread->queue->steal(job)) {
                // TODO: if there is a local job enqeued, bail out
                ++jobsStolen;
                runJob(job);
            }
        }
        
        return result;
#else
        ThreadEntry & entry = getEntry();

        bool result = false;

        //cerr << "size is " << entry.queues.size() << endl;

        for (auto & q: entry.queues) {
            if (q == entry.queue)
                continue;

            ThreadJob job;
            while (q->steal(job)) {
                // TODO: if there is a local job enqeued, bail out
                ++jobsStolen;
                runJob(job);
            }
        }

        return result;
#endif
    }

    void runJob(const ThreadJob & job)
    {
        job();
        jobs.finished += 1;
    }

    /** Wait for all work in all threads to be done, and return when it
        is.
    */
    void waitForAll()
    {
        while (!shutdown && jobsRunning() > 0) {
            if (!runMine())
                stealWork();
        }
    }

    void runWorker(int workerNum)
    {
        //ThreadEntry & entry = getEntry();

        int itersWithNoWork = 0;

        while (!shutdown) {
            if (!runMine()) {
                if (!stealWork()) {
                    // Nothing to do, for now.  Wait for something to
                    // wake us up.
                    ++itersWithNoWork;
                    if (itersWithNoWork == 10) {
                        ++threadsSleeping;
                        ML::futex_wait(threadsSleeping, threadsSleeping, 0.005);
                        --threadsSleeping;
                        itersWithNoWork = 0;
                    }
                    else {
                        std::this_thread::sleep_for(std::chrono::microseconds(100));
                    }
                }
            }
        }
    }

    void publishThread(ThreadEntry * thread)
    {
        ExcAssert(thread);

        if (shutdown)
            return;

        std::unique_lock<std::mutex> guard(knownThreadsLock);

        for (;;) {
            auto oldThreads = knownThreads();

            if (shutdown)
                return;

            std::unique_ptr<std::vector<ThreadEntry *> > newThreads
                (new std::vector<ThreadEntry *>(*oldThreads));
            for (ThreadEntry * t: *newThreads) {
                if (!t) {
                    //for (ThreadEntry * t: *newThreads) {
                    //    cerr << "got thread " << t << endl;
                    //}
                    cerr << "old threads had size of " << oldThreads->size() << endl;
                    cerr << "known threads has size of " << knownThreads()->size() << endl;
                    cerr << "total of " << newThreads->size() << " threads" << endl;
                    cerr << "old threads is at " << oldThreads.ptr << endl;
                    cerr << "known threads is at " << knownThreads().ptr << endl;
                }
                ExcAssert(t);
            }
            newThreads->push_back(thread);
            if (knownThreads.cmp_xchg(oldThreads, newThreads,
                                      true /* defer */))
                return;
        }
    }

    void unpublishThread(ThreadEntry * thread)
    {
        if (shutdown)
            return;

        std::unique_lock<std::mutex> guard(knownThreadsLock);

        for (;;) {
            auto oldThreads = knownThreads();

            if (shutdown)
                return;

            std::unique_ptr<std::vector<ThreadEntry *> > newThreads
                (new std::vector<ThreadEntry *>(*oldThreads));
            for (ThreadEntry * t: *newThreads)
                ExcAssert(t);
            auto it = std::find(newThreads->begin(), newThreads->end(),
                                thread);
            if (it == newThreads->end()) {
                throw std::logic_error("didn't find thread to unpublish");
            }
            newThreads->erase(it);
            if (knownThreads.cmp_xchg(oldThreads, newThreads,
                                      true /* defer */))
                return;
        }
    }

    /// List of all the queues that we know about
    //std::vector<std::shared_ptr<ThreadQueue<ThreadJob> > > queues;

    std::mutex knownThreadsLock;
    GcLock gcLock;
    RcuProtected<std::vector<ThreadEntry *> > knownThreads;
};

ThreadPool::
ThreadPool(int numThreads)
    : itl(new Itl(numThreads))
{
}

ThreadPool::
~ThreadPool()
{
}

void
ThreadPool::
add(ThreadJob job)
{
    itl->add(std::move(job));
}

void
ThreadPool::
waitForAll() const
{
    itl->waitForAll();
}

uint64_t
ThreadPool::
jobsRunning() const
{
    return itl->jobsRunning();
}

uint64_t
ThreadPool::
jobsSubmitted() const
{
    return itl->jobsSubmitted();
}

uint64_t
ThreadPool::
jobsFinished() const
{
    return itl->jobsFinished();
}

uint64_t
ThreadPool::
jobsStolen() const
{
    return itl->jobsStolen;
}

uint64_t
ThreadPool::
jobsWithFullQueue() const
{
    return itl->jobsWithFullQueue;
}

uint64_t
ThreadPool::
jobsRunLocally() const
{
    return itl->jobsRunLocally;
}

} // namespace Datacratic
