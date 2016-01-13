/** thread_pool.cc
    Jeremy Barnes, 18 December 2015
    Copyright (c) 2015 Datacratic Inc.  All rights reserved.

    This file is part of MLDB. Copyright 2015 Datacratic. All rights reserved.

    Implementation of the lockless, work stealing thread pool.
*/

#include "thread_pool.h"
#include "thread_pool_impl.h"
#include "mldb/arch/thread_specific.h"
#include "arch/cpu_info.h"
#include <atomic>
#include <condition_variable>
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
    
    /// A thread's local copy of the list of queues that may have work in
    /// them, including an epoch number.
    struct Queues: public std::vector<std::shared_ptr<ThreadQueue<ThreadJob> > > {
        Queues(uint64_t epoch)
            : epoch(epoch)
        {
        }

        /// Epoch number for this set of queues.  By comparing with the
        /// thread pool's epoch number, we can see if the list is out of
        /// date or not.
        uint64_t epoch;
    };

    struct ThreadEntry {
        ThreadEntry(Itl * owner = nullptr, int workerNum = -1)
            : owner(owner), workerNum(workerNum),
              queue(new ThreadQueue<ThreadJob>()),
              queues(new Queues(0))
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

        /// The ThreadPool we're owned by
        Itl * owner;

        /// Our thread number; used to choose a starting point in the
        /// list of available queues and avoid races.  This is only
        /// set for worker threads; for others it will be -1 as they
        /// don't normally scavenge for work to do.
        int workerNum;

        /// Our reference to our work queue.  It's a shared pointer
        /// because others may continue to reference it even after
        /// our thread has been destroyed, and allowing this avoids
        /// a lot of synchronization and locking.
        std::shared_ptr<ThreadQueue<ThreadJob> > queue;

        /// The list of queues that we know about over all threads.
        /// This is a cached copy that we occasionally check to see
        /// if it needs to be updated.
        std::shared_ptr<const Queues> queues;
    };

    /// This allows us to have one threadEntry per thread
    ML::ThreadSpecificInstanceInfo<ThreadEntry, void> threadEntries;

    /// Our internal worker threads
    std::vector<std::thread> workers;

    /// Job statistics.  This is designed to allow for a single atomic
    /// access to the full 64 bits to allow determiniation if all
    /// jobs have been terminated at a given point in time.
    struct Jobs {
        std::atomic<int> submitted;
        std::atomic<int> finished;
    };

    union {
        Jobs jobs;
        std::atomic<uint64_t> startedFinished;
    };

    /// Statistics counters for debugging and information
    std::atomic<uint64_t> jobsStolen, jobsWithFullQueue, jobsRunLocally;

    /// Non-zero when we're shutting down.
    std::atomic<int> shutdown;
    
    /// Number of sleeping threads.  This is used to
    /// help triggering wakeups if there are no sleeping threads
    /// to be woken.
    std::atomic<int> threadsSleeping;
    
    /// Mutex for the wakeup condition variable
    std::mutex wakeupMutex;

    /// Wakeup condition variable
    std::condition_variable wakeupCv;

    /// Epoch number for thread creation.  We can use this to
    /// tell if a set of queues is current, by checking for
    /// matching epoch numbers.
    std::atomic<uint64_t> threadCreationEpoch;

    /// List of all the queues that could contain work to steal.
    /// Protected by queuesMutex, since in C++11 shared_ptrs
    /// can't be atomically modified.
    std::shared_ptr<const Queues> queues;

    /// Mutex to protect modification to the queue state
    std::mutex queuesMutex;

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

    Itl(int numThreads)
        : jobsStolen(0),
          jobsWithFullQueue(0),
          jobsRunLocally(0),
          shutdown(0),
          threadsSleeping(0),
          threadCreationEpoch(0),
          queues(new Queues(threadCreationEpoch))
    {
        jobs.submitted = 0;
        jobs.finished = 0;

        for (unsigned i = 0;  i < numThreads;  ++i) {
            workers.emplace_back([this, i] () { this->runWorker(i); });
        }

        getEntry();
    }

    ~Itl()
    {
        {
            std::unique_lock<std::mutex> guard(queuesMutex);
            this->shutdown = 1;
        }
        
        {
            wakeupCv.notify_all();
        }
        
        for (auto & w: workers)
            w.join();
    }

    ThreadEntry & getEntry(int workerNum = -1)
    {
        ThreadEntry * threadEntry = threadEntries.get();
        ExcAssert(threadEntry);

        // If it's not initialized yet, this is the first time we've
        // seen this thread.  So we initialize the thread's entry and
        // publish its queue to the list of queues.
        if (!threadEntry->owner) {
            threadEntry->owner = this;
            threadEntry->workerNum = workerNum;
            publishThread(threadEntry);
        }

        return *threadEntry;
    }

    void add(ThreadJob job)
    {
        //ThreadEntry & entry = getEntry();

        jobs.submitted += 1;

        if (getEntry().queue->push(job)) {
            // There is a possible race condition here: if we add this
            // job just before the first thread goes to sleep,
            // then it will miss the wakeup.  We deal with this by having
            // the threads never sleep for too long, so that if we do
            // miss a wakeup it won't be the end of the world.
            if (threadsSleeping) {
                wakeupCv.notify_one();
            }
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
    bool runMine(ThreadEntry & entry)
    {
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
    bool stealWork(ThreadEntry & entry)
    {
        bool foundWork = false;
        
        // Check if we have the latest list of queues, by looking at
        // the epoch number.
        if (threadCreationEpoch.load() != entry.queues->epoch) {
            // A thread has been created or destroyed, and so our list
            // of queues is out of date.  Refresh them if we can obtain
            // the mutex.  If we can't refresh it's not a big deal, since
            // at worst we have references to queues that are no longer
            // replenished or we're missing some work.  When we run out of
            // work to do, we'll try again.
            std::unique_lock<std::mutex> guard(queuesMutex, std::try_to_lock);

            if (guard) {
                // We successfully locked the mutex.  Now we can read queues
                // and take a reference to it.
                entry.queues = queues;
                ExcAssertEqual(entry.queues->epoch, threadCreationEpoch);
            }
        }

        for (unsigned i = 0;  i < entry.queues->size() && !shutdown;  ++i) {
            // Try to avoid all threads starting looking for work at the
            // same place.
            int n = (entry.workerNum + i) % entry.queues->size();

            const std::shared_ptr<ThreadQueue<ThreadJob> > & q
                = entry.queues->at(n);

            if (q == entry.queue)
                continue;  // our own thread

            ThreadJob job;
            while (q->steal(job)) {
                ++jobsStolen;
                runJob(job);
                runMine(entry);
            }
        }
        
        return foundWork;
    }

    /** Run a job we successfully dequeued from a queue somewhere. */
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
        ThreadEntry & entry = getEntry();

        while (!shutdown && jobsRunning() > 0) {
            if (!runMine(entry))
                stealWork(entry);
        }
    }

    /** Run a worker thread. */
    void runWorker(int workerNum)
    {
        ThreadEntry & entry = getEntry(workerNum);

        int itersWithNoWork = 0;

        while (!shutdown) {
            if (!runMine(entry)) {
                if (!stealWork(entry)) {
                    // Nothing to do, for now.  Wait for something to
                    // wake us up.  We try 10 times, and if there is
                    // nothing to do then we go to sleep and wait for
                    // some more work to come.
                    ++itersWithNoWork;
                    if (itersWithNoWork == 10) {
                        ++threadsSleeping;
                        std::unique_lock<std::mutex> guard(wakeupMutex);

                        // We can't sleep forever, since we allow for
                        // wakeups to be missed for efficiency reasons,
                        // and so we need to poll every now and again.
                        wakeupCv.wait_for(guard, std::chrono::milliseconds(1));

                        --threadsSleeping;
                        itersWithNoWork = 0;
                    }
                    else {
                        // We didn't find any work, but it's not yet time
                        // to give up on it.  We wait a small amount of
                        // time and try again.
                        //std::this_thread::yield();
                        std::this_thread::sleep_for(std::chrono::microseconds(100));
                    }
                }
            }
        }
    }

    void publishThread(ThreadEntry * thread)
    {
        if (shutdown)
            return;
        ExcAssert(thread);
        std::unique_lock<std::mutex> guard(queuesMutex);
        if (shutdown)
            return;
        
        std::shared_ptr<Queues> newQueues(new Queues(*queues));

        do {
            newQueues->epoch = threadCreationEpoch.fetch_add(1) + 1;
        } while (newQueues->epoch == 0);

        newQueues->emplace_back(thread->queue);
        queues = newQueues;
    }

    void unpublishThread(ThreadEntry * thread)
    {
        if (shutdown)
            return;
        ExcAssert(thread);
        std::unique_lock<std::mutex> guard(queuesMutex);
        if (shutdown)
            return;
        
        std::shared_ptr<Queues> newQueues(new Queues(*queues));

        do {
            newQueues->epoch = threadCreationEpoch.fetch_add(1) + 1;
        } while (newQueues->epoch == 0);

        // Note: std::find triggers a compiler bug in GCC 4.8, so
        // we unroll it explicitly here.
        bool foundThreadToUnpublish = false;
        for (auto it = newQueues->begin(), end = newQueues->end();
             !foundThreadToUnpublish && it != end;  ++it) {
            if (*it == thread->queue) {
                newQueues->erase(it);
                foundThreadToUnpublish = true;
            }
        }
        ExcAssert(foundThreadToUnpublish);
        
        queues = newQueues;
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
