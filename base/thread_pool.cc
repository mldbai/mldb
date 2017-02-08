/** thread_pool.cc
    Jeremy Barnes, 18 December 2015
    Copyright (c) 2015 mldb.ai inc.  All rights reserved.

    This file is part of MLDB. Copyright 2015 mldb.ai inc. All rights reserved.

    Implementation of the lockless, work stealing thread pool.
*/

#include "thread_pool.h"
#include "thread_pool_impl.h"
#include "mldb/arch/thread_specific.h"
#include "mldb/arch/demangle.h"
#include "mldb/jml/utils/environment.h"
#include <atomic>
#include <condition_variable>
#include <vector>
#include <thread>
#include <iostream>


using namespace std;


namespace MLDB {

static EnvOption<int, true /* trace */>
NUM_CPUS("NUM_CPUS", std::thread::hardware_concurrency());

int numCpus()
{
    return NUM_CPUS;
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

struct ThreadPool::Itl: public std::enable_shared_from_this<ThreadPool::Itl> {
    
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
              queues(new Queues(0)),
              lastFound(-1)
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

        /// The last queue number we found work in
        int lastFound;
    };

    /// This allows us to have one threadEntry per thread
    ThreadSpecificInstanceInfo<ThreadEntry, void> threadEntries;

    /// Our internal worker threads
    std::vector<std::thread> workers;

    /// Job statistics.  This is designed to allow for a single atomic
    /// access to the full 64 bits to allow determiniation if all
    /// jobs have been terminated at a given point in time.
    std::atomic<uint32_t> submitted;
    std::atomic<uint32_t> finished;

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

    /// If we're in a hierarchy, this is our parent and we don't have
    /// any queues ourselves.
    ThreadPool::Itl * parent;

    /// The number of jobs currently enqueued or running on the
    /// parent.
    std::atomic<size_t> parentJobs;

    /// The maximum number of parallel jobs in the parent
    size_t maxParentJobs;

    /** Return the number of jobs running.  If there are more than
        2^31 jobs running, this may give the wrong answer.
    */
    unsigned jobsRunning() const
    {
        uint32_t s = submitted.load();
        uint32_t f = finished.load();

        return s - f;
    }

    /** Return the number of jobs submitted.  Wraps around at INT_MAX. */
    int jobsSubmitted() const
    {
        return submitted;
    }

    /** Return the number of jobs finished.  Wraps around at INT_MAX. */
    int jobsFinished() const
    {
        return finished;
    }

    Itl(int numThreads)
        : jobsStolen(0),
          jobsWithFullQueue(0),
          jobsRunLocally(0),
          shutdown(0),
          threadsSleeping(0),
          threadCreationEpoch(0),
          queues(new Queues(threadCreationEpoch)),
          parent(nullptr),
          parentJobs(0),
          maxParentJobs(0)
    {
        submitted = 0;
        finished = 0;

        for (unsigned i = 0;  i < numThreads;  ++i) {
            workers.emplace_back([this, i] () { this->runWorker(i); });
        }

        getEntry();
    }

    Itl(Itl & parent, size_t maxParentJobs)
        : jobsStolen(0),
          jobsWithFullQueue(0),
          jobsRunLocally(0),
          shutdown(0),
          threadsSleeping(0),
          threadCreationEpoch(0),
          queues(new Queues(threadCreationEpoch)),
          parent(&parent),
          parentJobs(0),
          maxParentJobs(maxParentJobs)
    {
        submitted = 0;
        finished = 0;
        getEntry();
    }

    ~Itl()
    {
        {
            std::unique_lock<std::mutex> guard(queuesMutex);
            this->shutdown = 1;
        }
        wakeupCv.notify_all();

        // Any parent jobs will see that we're shutting down or
        // will lose their reference and not run.

        // Remove any worker tasks
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

    void runParentWorker()
    {
        while (!shutdown && (this->work())) ;
        --this->parentJobs;
    }

    /** Add a new job to be run.  This is lock-free except for the very
        first call from a given thread to a given thread pool, in which
        case there are locks taken for some of the bookkeeping.

        If this thread's queue is full, then it will run the job
        immediately to make forward progress and give time to the rest of
        the system to clear out some work from the queue.
    */
    void add(ThreadJob job)
    {
        submitted += 1;

        std::unique_ptr<ThreadJob> overflow
            (getEntry().queue->push(new ThreadJob(std::move(job))));

        if (!overflow) {
            if (parent) {
                // If there aren't enough jobs alredy, we submit a new
                // one.
                size_t numWereActive = parentJobs.fetch_add(1);
                if (numWereActive >= maxParentJobs) {
                    --parentJobs;
                }
                else {
                    // Get a weak pointer to ourself so that we can know
                    // if we're still alive or not.
                    auto weakThis = std::weak_ptr<Itl>(this->shared_from_this());

                    auto parentJob = [weakThis] ()
                        {
                            // GCC 4.8 uses a try/catch to implement lock()
                            // we avoid logging an exception message here
                            // by trying first, and then disabling exceptions.
                            if (weakThis.expired())
                                return;
                            MLDB_TRACE_EXCEPTIONS(false);
                            auto strongThis = weakThis.lock();
                            if (strongThis)
                                strongThis->runParentWorker();
                        };

                    if (!weakThis.expired())
                        parent->add(parentJob);
                }
            }
            else {
                // There is a possible race condition here: if we add this
                // job just before the first thread goes to sleep,
                // then it will miss the wakeup.  We deal with this by having
                // the threads never sleep for too long, so that if we do
                // miss a wakeup it won't be the end of the world.
                if (threadsSleeping) {
                    wakeupCv.notify_one();
                }
            }
        }
        else {
            // The queue was full.  Do the work here, hopefully someone
            // will steal some work in the meantime.
            ++jobsWithFullQueue;
            runJob(*overflow);
        }
    }

    /** Runs as much work as possible in this thread's queue.  Returns
        true if some work was obtained.
    */
    bool runMine(ThreadEntry & entry)
    {
        bool result = false;

        // First, do all of our work
        ThreadJob * job;
        while ((job = entry.queue->pop())) {
            result = true;
            ++jobsRunLocally;
            runJob(*job);
            delete job;
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

        auto stealFrom = [&] (int n)
            {
                const std::shared_ptr<ThreadQueue<ThreadJob> > & q
                    = entry.queues->at(n);
                
                ThreadJob * job;
                while ((job = q->steal())) {
                    entry.lastFound = n;

                    ++jobsStolen;

                    runJob(*job);
                    foundWork = true;
                    delete job;
                    runMine(entry);
                }
            };

        size_t nq = entry.queues->size();

        if (entry.lastFound > 0 && entry.lastFound < nq) {
            stealFrom(entry.lastFound);
        }

        for (unsigned i = 0;  i < nq && !shutdown;  ++i) {
            // Try to avoid all threads starting looking for work at the
            // same place.
            int n = entry.lastFound + i;
            while (n < 0)
                n += nq;
            while (n >= nq)
                n -= nq;
            stealFrom(n);
        }
        
        return foundWork;
    }

    /** Run a job we successfully dequeued from a queue somewhere. */
    void runJob(const ThreadJob & job)
    {
        try {
            job();
            finished += 1;
        } catch (const std::exception & exc) {
            finished += 1;
            cerr << "ERROR: job submitted to ThreadPool of type "
                 << demangle(job.target_type())
                 << " threw exception: " << exc.what() << endl;
            cerr << "A Job in a ThreadPool which throws an exception "
                 << "causes the program to crash, which is happening now"
                 << endl;
            abort();
        }
        MLDB_CATCH_ALL {
            finished += 1;
            cerr << "ERROR: job submitted to ThreadPool of type "
                 << demangle(job.target_type())
                 << " threw exception " << getExceptionString() << endl;
            cerr << "A Job in a ThreadPool which throws an exception "
                 << "causes the program to crash, which is happening now"
                 << endl;
            abort();
        }
    }

    /** Wait for all work in all threads to be done, and return when it
        is.
    */
    void waitForAll()
    {
        ThreadEntry & entry = getEntry();

        while (!shutdown && jobsRunning() > 0) {
            //cerr << "jobsRunning() = " << jobsRunning() << endl;
            if (!runMine(entry))
                stealWork(entry);
            //std::this_thread::sleep_for(std::chrono::milliseconds(10));
        }
    }

    /** Perform some work, if possible.  Returns true if work was done,
        or false if none was available.
    */
    bool work()
    {
        ThreadEntry & entry = getEntry();

        bool result;
        if (!(result = runMine(entry)))
            result = stealWork(entry);
        return result;
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

                    // Look for when we're idle, and if we are just sleep
                    // MLDB-1538
                    uint32_t s = submitted.load(std::memory_order_relaxed);
                    uint32_t f = finished.load(std::memory_order_relaxed);

                    if (s == f) {
                        // We're idle.  No need to look for a job; we almost
                        // certainly won't find one.
                        ++threadsSleeping;
                        std::unique_lock<std::mutex> guard(wakeupMutex);

                        // We can't sleep forever, since we allow for
                        // wakeups to be missed for efficiency reasons,
                        // and so we need to poll every now and again.
                        wakeupCv.wait_for(guard, std::chrono::milliseconds(250));

                        --threadsSleeping;
                        itersWithNoWork = 0;
                    }

                    if (itersWithNoWork == 10) {
                        ++threadsSleeping;
                        std::unique_lock<std::mutex> guard(wakeupMutex);

                        // We can't sleep forever, since we allow for
                        // wakeups to be missed for efficiency reasons,
                        // and so we need to poll every now and again.
                        wakeupCv.wait_for(guard, std::chrono::milliseconds(10));

                        --threadsSleeping;
                        itersWithNoWork = 0;
                    }
                    else {
                        // We didn't find any work, but it's not yet time
                        // to give up on it.  We wait a small amount of
                        // time and try again.
                        std::this_thread::yield();
                        std::this_thread::sleep_for(std::chrono::milliseconds(1));
                    }
                } else {
                    itersWithNoWork = 0;
                }
            }
            else {
                itersWithNoWork = 0;
            }
        }
    }

    /** A new thread has made itself known to this thread pool.  Publish
        its queue in the list of known queues so that other threads may
        steal work from it.
    */
    void publishThread(ThreadEntry * thread)
    {
        if (shutdown)
            return;
        ExcAssert(thread);
        std::unique_lock<std::mutex> guard(queuesMutex);
        if (shutdown)
            return;
        
        std::shared_ptr<Queues> newQueues(new Queues(*queues));

        // Don't allow epoch zero to be used on a wraparound, as its
        // reserved for the empty set in the constructor.
        do {
            newQueues->epoch = threadCreationEpoch.fetch_add(1) + 1;
        } while (newQueues->epoch == 0);

        newQueues->emplace_back(thread->queue);
        queues = std::move(newQueues);
    }

    /** A thread has exited and so its queue is no longer available for
        work stealing.  Publish the reduced list of queues.
    */
    void unpublishThread(ThreadEntry * thread)
    {
        if (shutdown)
            return;

        // Finish all the jobs first, otherwise they will simply
        // disappear.
        while (runMine(*thread)) ;

        ExcAssert(thread);
        std::unique_lock<std::mutex> guard(queuesMutex);
        if (shutdown)
            return;
        
        std::shared_ptr<Queues> newQueues(new Queues(*queues));

        // Don't allow epoch zero to be used on a wraparound, as its
        // reserved for the empty set in the constructor.
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
        
        queues = std::move(newQueues);
    }

    void dump()
    {
        cerr << "ThreadPool at " << this << endl;
        cerr << workers.size() << " workers" << endl;
        cerr << "submitted " << submitted << " finished " << finished
             << endl;
        cerr << "stolen " << jobsStolen << " full " << jobsWithFullQueue
             << " local " << jobsRunLocally << endl;
        cerr << "shutdown " << shutdown << endl;
        cerr << "sleeping " << threadsSleeping << endl;
        cerr << "epoch " << threadCreationEpoch << endl;
        cerr << queues->size() << " queues" << endl;
        for (auto & q: *queues) {
            cerr << "  queue with " << q->num_queued_ << " bottom " << q->bottom_
                 << " top " << q->top_ << endl;
        }
    }
};

ThreadPool::
ThreadPool(int numThreads)
    : itl(std::make_shared<Itl>(numThreads))
{
}

ThreadPool::
ThreadPool(ThreadPool & parent, int numThreads)
    : itl(std::make_shared<Itl>(*parent.itl, numThreads))
{
}

ThreadPool::
~ThreadPool()
{
    itl.reset();
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

void
ThreadPool::
work() const
{
    itl->work();
}

size_t
ThreadPool::
numThreads() const
{
    return itl->workers.size();
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

ThreadPool &
ThreadPool::
instance()
{
    static ThreadPool result(numCpus());
    return result;
}

} // namespace MLDB
