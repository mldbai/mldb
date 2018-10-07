/** thread_queue_test.cc
    Jeremy Barnes, 18 December 2015
    Copyright (c) 2015 mldb.ai inc.  All rights reserved.

    This file is part of MLDB. Copyright 2015 mldb.ai inc. All rights reserved.

    Test of the thread pool.
*/

#define BOOST_TEST_MAIN
#define BOOST_TEST_DYN_LINK

#include "mldb/base/thread_pool.h"
#include "mldb/base/thread_pool_impl.h"
#include "mldb/arch/timers.h"
#include "mldb/base/exc_assert.h"
#include "mldb/base/parallel.h"

#include <boost/test/unit_test.hpp>
#include <atomic>
#include <thread>
#include <cassert>
#include <iostream>

using namespace std;
using namespace MLDB;

// For the purposes of the tests, we make integers pass
// for pointers to avoid having to actually run jobs.
// The value zero is reserved for "no value was available".
struct Int64ThreadQueue {
    ThreadQueue<void> q;

    int64_t Push(int64_t n) {
        ExcAssert(n != 0);
        return reinterpret_cast<int64_t>
            (q.push(reinterpret_cast<void *>(n)));
    };

    int64_t Steal() { return reinterpret_cast<int64_t>(q.steal()); };

    int64_t Pop(int* path = 0) {
        return reinterpret_cast<int64_t>(q.pop(path));
    };

    size_t num_queued() { return q.num_queued_; }
};

// Check basic functionality and invariants in one thread
void RunBasicTest(Int64ThreadQueue* q) {
    // Pop fails with no elements
    BOOST_CHECK_EQUAL(q->num_queued(), 0);
    BOOST_CHECK_EQUAL(q->Pop(), 0);

    // Pop works for one element
    BOOST_CHECK_EQUAL(q->num_queued(), 0);
    q->Push(1);
    BOOST_CHECK_EQUAL(q->num_queued(), 1);
    BOOST_CHECK_EQUAL(q->Pop(), 1);
    BOOST_CHECK_EQUAL(q->Pop(), 0);
    BOOST_CHECK_EQUAL(q->Steal(), 0);

    // Steal works for one element
    BOOST_CHECK_EQUAL(q->num_queued(), 0);
    q->Push(1);
    BOOST_CHECK_EQUAL(q->num_queued(), 1);
    BOOST_CHECK_EQUAL(q->Steal(), 1);
    BOOST_CHECK_EQUAL(q->Pop(), 0);
    BOOST_CHECK_EQUAL(q->Steal(), 0);

    // Steal removes earliest element
    BOOST_CHECK_EQUAL(q->num_queued(), 0);
    q->Push(1);
    q->Push(2);
    BOOST_CHECK_EQUAL(q->Steal(), 1);
    BOOST_CHECK_EQUAL(q->Pop(), 2);
    BOOST_CHECK_EQUAL(q->Pop(), 0);
    BOOST_CHECK_EQUAL(q->Steal(), 0);

    // Pop removes latest element
    BOOST_CHECK_EQUAL(q->num_queued(), 0);
    q->Push(1);
    q->Push(2);
    BOOST_CHECK_EQUAL(q->Pop(), 2);
    BOOST_CHECK_EQUAL(q->Steal(), 1);
    BOOST_CHECK_EQUAL(q->Pop(), 0);
    BOOST_CHECK_EQUAL(q->Steal(), 0);
};

// Check basic functionality and invariants in one thread
BOOST_AUTO_TEST_CASE(Basics) {
    Int64ThreadQueue q;
    RunBasicTest(&q);
}

// Check basic functionality and invariants in one thread with wraparound
BOOST_AUTO_TEST_CASE(BasicsWithWraparoundINT_MAX) {
    Int64ThreadQueue q;
    q.q.top_ = q.q.bottom_ = INT_MAX;
    RunBasicTest(&q);
}

// Check basic functionality and invariants in one thread with wraparound
BOOST_AUTO_TEST_CASE(BasicsWithWraparoundINT_MAXMinusOne) {
    Int64ThreadQueue q;
    q.q.top_ = q.q.bottom_ = INT_MAX - 1;
    RunBasicTest(&q);
}

// Check basic functionality and invariants in one thread with wraparound
BOOST_AUTO_TEST_CASE(BasicsWithWraparoundINT_MIN) {
    Int64ThreadQueue q;
    q.q.top_ = q.q.bottom_ = INT_MIN;
    RunBasicTest(&q);
}

// Check basic functionality and invariants in one thread with wraparound
BOOST_AUTO_TEST_CASE(BasicsWithWraparoundINT_MINPlusOne) {
    Int64ThreadQueue q;
    q.q.top_ = q.q.bottom_ = INT_MIN + 1;
    RunBasicTest(&q);
}

// Test driver for one element races.  This is testing the low-level
// consistency of the lockless deque used for job queuing.
void TestRaceForOneElement(int num_stealing_threads, bool pop_element_in_race,
                           const std::string & test) {
    // In this test, we push one single element and set up a race
    // to pop or steal it.
    // We test the invariants that:
    // - exactly one of the threads wins the race
    // - the data structure is consistent afterwards
    // - the element returned is the one pushed

    constexpr int kNumTrials = 5000;

    Int64ThreadQueue q;

    std::atomic<int> current_epoch(0);

    struct StealThread {
        std::atomic<int64_t> stolen_element;
        std::atomic<int> acknowledged_epoch;
        Int64ThreadQueue* q;
        std::atomic<int>* current_epoch;
        std::unique_ptr<std::thread> thread;
        char padding[128];  // avoid false sharing

        StealThread()
            : acknowledged_epoch(-1), q(nullptr), current_epoch(nullptr) {}

        // Required so we can put it in a vector
        StealThread(StealThread&& other)
            : q(other.q),
              current_epoch(other.current_epoch),
              thread(std::move(other.thread)) {}

        ~StealThread()
        {
            if (thread)
                thread->join();
        }

        void Start(Int64ThreadQueue* q, std::atomic<int>* current_epoch) {
            this->q = q;
            this->current_epoch = current_epoch;

            thread.reset(new std::thread([&]() { this->Run(); }));
        }

        void Run() {
            int known_epoch = 0;

            while (current_epoch->load() != -1) {
                // Busy wait until we're in a new epoch.  This is basically
                // a barrier operation.
                while (known_epoch == current_epoch->load())
                    ;

                // We're in another epoch
                known_epoch = current_epoch->load();

                // Try to steal one element, and report back the result
                stolen_element = q->Steal();

                // Acknowledge we're done with this epoch
                acknowledged_epoch = known_epoch;
            }
        }

        void AwaitAcknowledgement() {
            while (acknowledged_epoch != current_epoch->load())
                sched_yield();
        }
    };

    // If there are more stealing threads than real cores, then the
    // context switching overhead is vastly higher than the amount
    // of work done, and the test runs for a long time.
    // We cap it at the number of cpus / 2 (guess at number of real cores)
    // minus one (for the pushing thread).  That way there is always a
    // real core for each part of the work to run on.
    int max_stealing_threads = numCpus() / 2 - 1;
    if (max_stealing_threads < 2)
        max_stealing_threads = 2;

    if (num_stealing_threads > max_stealing_threads) {
        cerr << "warning: running test " << test << " with "
             << max_stealing_threads << " stealing threads instead of "
             << num_stealing_threads << " due to lack of physical cores"
             << endl;
        num_stealing_threads = max_stealing_threads;
    }
    
    // Steal threads are run outside of the trial loop to avoid
    // starting threads on every new trial.  They are more like coroutines
    // under this model, synchronized via the epoch counter.
    std::vector<StealThread> steal_threads(num_stealing_threads);
    for (auto& t : steal_threads) {
        t.Start(&q, &current_epoch);
    }

    for (int i = 0; i < kNumTrials; ++i) {
        // Push an element onto the queue
        q.Push(i + 1);

        // Tell the steal threads that we're in a new epoch,
        // so they can try to steal it
        ++current_epoch;

        // Try to pop it ourselves, if we're doing a test where we participate
        // in the race.  The path variable can be used to diagnose test failures;
        // the path of the run before or during the failure is likely the place
        // that caused the error.
        int path = 0;
        int64_t popped_element = 0;
        if (pop_element_in_race) {
            popped_element = q.Pop(&path);
        }

        // Wait for the steal threads to acknowledge they've finished the epoch
        int n MLDB_UNUSED = 0;
        for (auto& t : steal_threads) {
#if 0
            if (current_epoch != t.acknowledged_epoch) {
                cerr << "waiting for thread " << (n++) << " to acknowledge in "
                     << current_epoch << " currently in " << t.acknowledged_epoch
                     << " of " << kNumTrials << " in " << test << endl;
            }
#endif
            t.AwaitAcknowledgement();
        }

        if (false) {
            cerr << "element: stolen "
                 << (steal_threads.empty()
                     ? 0
                     : steal_threads[0].stolen_element.load()) << " popped "
                 << popped_element << " nqueued " << q.num_queued() << " path "
                 << path;
        }

        // Now check the elements.  We should have exactly one winner,
        // which has popped the correct element.

        bool found_winner = false;

        if (popped_element != 0) {
            BOOST_CHECK_EQUAL(popped_element, i + 1);
            found_winner = true;
        }

        for (auto& t : steal_threads) {
            if (t.stolen_element == 0) {
                continue;
            }
            if (found_winner) {
                BOOST_CHECK_EQUAL(false && "More than one winner", true);
            }
            BOOST_CHECK_EQUAL(t.stolen_element, i + 1);
            found_winner = true;
        }

        BOOST_CHECK_EQUAL(found_winner, true);

        BOOST_CHECK_EQUAL(q.num_queued(), 0);
    }

    current_epoch = -1;
}

// Make sure that elements can be pushed then popped
BOOST_AUTO_TEST_CASE(PopOneElement) {
    TestRaceForOneElement(0 /* steal thread */, true /* pop elements */,
                          "PoOneElement");
}

// Make sure that elements can be pushed then stolen
BOOST_AUTO_TEST_CASE(StealOneElement) {
    TestRaceForOneElement(1 /* steal thread */, false /* pop elements */,
                          "StealOneElement");
}

// Make sure that in a race between one popping thread and one stealing
// thread, exactly one of them wins.
BOOST_AUTO_TEST_CASE(PopAndOneStealThreadRaceForLastElement) {
    TestRaceForOneElement(1 /* steal thread */, true /* pop elements */,
                          "PopAndOneStealThreadRaceForLastElement");
}

// Make sure that in a race between two stealing threads, exactly one
// wins.  Two threads gives the highest likelyhood of catching a situation
// where none of them win.
BOOST_AUTO_TEST_CASE(TwoStealThreadsRaceForLastElement) {
    TestRaceForOneElement(2 /* steal threads */, false /* pop elements */,
                          "TwoStealThreadsRaceForLastElement");
}

// Make sure that multiple stealing threads competing against each other work.
BOOST_AUTO_TEST_CASE(ManyStealThreadsRaceForLastElement) {
    TestRaceForOneElement(8 /* steal threads */, false /* pop elements */,
                          "ManyStealThreadsRaceForLastElement");
}

// Many stealing threads competing with a pop thread
BOOST_AUTO_TEST_CASE(PopAndManyStealThreadsRaceForLastElement) {
    TestRaceForOneElement(8 /* steal threads */, true /* pop elements */,
                          "PopAndManyStealThreadsRaceForLastElement");
}

// A more involved test, that includes testing the queue when it's filled
// up.  We make sure that we can steal and pop all elements simultaneously
// over multiple threads.  Parameter tells us where we initialize top and
// bottom pointers so that we can test wraparound.
void TestPushPopSteal(std::uint_fast32_t init_top_and_bottom = 0) {
    cerr << "testing with top and bottom " << init_top_and_bottom << endl;

    // One thread; push and pop with simultaneous stealing; ensure balanced
    constexpr int kNumIters = 20;
    constexpr int kNumStealThreads = 8;
    for (int i = 0; i < kNumIters; ++i) {
        int num_to_push_pop = 100000;
        // LOG(INFO) << "test iteration " << i;

        Int64ThreadQueue q;
        q.q.top_ = q.q.bottom_ = init_top_and_bottom;

        std::vector<std::thread> threads;
        std::atomic<int> num_to_finish(num_to_push_pop);

        std::vector<int> item_is_done(num_to_push_pop, 0);

        auto MarkItemAsDone = [&](int64_t item) {
            if (item) {
                item -= 1;  // remove offset added on push
                if (item_is_done.at(item) != 0) {
                    cerr << "item " << item << " was "
                         << item_is_done.at(item) << endl;
                }
                item_is_done.at(item) += 1;
                if (item_is_done.at(item) != 1) {
                    cerr << "item " << item << " is "
                         << item_is_done.at(item) << endl;
                }
                --num_to_finish;
            };
        };

        auto run_steal_thread = [&]() {
            while (num_to_finish > 0) {
                MarkItemAsDone(q.Steal());
            }
        };

        for (int j = 0; j < kNumStealThreads; ++j) {
            threads.emplace_back(run_steal_thread);
        }

        for (int j = 0; j < num_to_push_pop; /* no inc */) {
            int64_t overflow = q.Push(j + 1);

            // Attempt a pop on queue overflow or on every 8th push
            if (j % 8 == 0 || overflow) {
                int64_t item = q.Pop();
                if (item) {
                    MarkItemAsDone(item);
                }
            }
            if (!overflow) j += 1;
        }

        while (num_to_finish > 0) {
            int64_t item = q.Pop();
            if (!item) {
                break;
            }
            MarkItemAsDone(item);
        }

        for (auto & t: threads)
            t.join();
        threads.clear();

        BOOST_CHECK_EQUAL(num_to_finish, 0);

        for (int count : item_is_done) {
            BOOST_CHECK_EQUAL(count, 1);
        }
    }
}

BOOST_AUTO_TEST_CASE(PushPopSteal) {
    TestPushPopSteal(0 /* top and bottom of empty queue */);
}

BOOST_AUTO_TEST_CASE(PushPopStealWithWraparound) {
    TestPushPopSteal(std::numeric_limits<uint_fast32_t>::max() - 10 /* top and bottom of empty queue */);
}

