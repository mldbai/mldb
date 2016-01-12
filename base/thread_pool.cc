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

    typedef std::vector<std::shared_ptr<ThreadQueue<ThreadJob> > > Queues;

    /// List of all the queues that we know about
    std::atomic<Queues *> queues;

    Itl(int numThreads)
        : jobsStolen(0),
          jobsWithFullQueue(0),
          jobsRunLocally(0),
          shutdown(0),
          threadsSleeping(0),
          queues(new Queues())
    {
        jobs.submitted = 0;
        jobs.finished = 0;

        for (unsigned i = 0;  i < numThreads;  ++i) {
            workers.emplace_back([this, i] () { this->runWorker(i); });
        }

        while (threadsSleeping < numThreads)
            ;

        getEntry();
    }

    ~Itl()
    {
        this->shutdown = 1;
        
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
        bool result = false;
        
        for (const std::shared_ptr<ThreadQueue<ThreadJob> > & q: *queues.load()) {
            if (shutdown)
                return false;
            
            ThreadJob job;
            while (q->steal(job)) {
                ++jobsStolen;
                runJob(job);
                //runMine();
            }
        }
        
        return result;
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
                        ML::futex_wait(threadsSleeping, threadsSleeping, 0.1);
                        --threadsSleeping;
                        itersWithNoWork = 0;
                    }
                    else {
                        //std::this_thread::yield();
                        std::this_thread::sleep_for(std::chrono::microseconds(100));
                    }
                }
            }
        }
    }

    void publishThread(ThreadEntry * thread)
    {
        ExcAssert(thread);
        
        //cerr << "publishing thread" << endl;

        while (!shutdown) {
            Queues * oldQueues = queues.load();
            
            std::unique_ptr<Queues> newQueues(new Queues(*oldQueues));
            newQueues->emplace_back(thread->queue);

            if (queues.compare_exchange_strong(oldQueues, newQueues.get())) {
                newQueues.release();
                // TODO: delete oldQueues :)
                return;
            }
        }
    }

    void unpublishThread(ThreadEntry * thread)
    {
        ExcAssert(thread);

        while (!shutdown) {

            Queues * oldQueues = queues.load();
            
            std::unique_ptr<Queues> newQueues(new Queues(*oldQueues));
            auto it = std::find(newQueues->begin(), newQueues->end(), thread->queue);
            ExcAssert(it != newQueues->end());
            newQueues->erase(it);

            if (queues.compare_exchange_strong(oldQueues, newQueues.get())) {
                newQueues.release();
                // TODO: delete oldQueues :)
                return;
            }
        }
    }
};

ThreadPool::
ThreadPool(int numThreads)
    : itl(new Itl(numThreads))
{
}

ThreadPool::
~ThreadPool()
{
    itl.reset();
    cerr << "done destructor" << endl;
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
