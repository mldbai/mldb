// This file is part of MLDB. Copyright 2015 mldb.ai inc. All rights reserved.

/* message_channel_test.cc                                         -*- C++ -*-
   Jeremy Barnes, 24 September 2012
   Copyright (c) 2012 mldb.ai inc.  All rights reserved.

   Test for message channel ()
*/

#define BOOST_TEST_MAIN
#define BOOST_TEST_DYN_LINK

#include <boost/test/unit_test.hpp>

#include "mldb/io/message_loop.h"
#include "mldb/io/typed_message_channel.h"
#include <sys/socket.h>
#include "mldb/jml/utils/guard.h"
#include "mldb/arch/exception_handler.h"
#include "mldb/utils/testing/watchdog.h"
#include "mldb/jml/utils/testing/fd_exhauster.h"
#include "mldb/jml/utils/vector_utils.h"
#include <chrono>
#include <thread>
#include <boost/thread/thread.hpp>
#include "mldb/utils/testing/watchdog.h"


using namespace std;
using namespace ML;
using namespace MLDB;



BOOST_AUTO_TEST_CASE( test_message_channel )
{
    TypedMessageSink<std::string> sink(1000);
    
    std::atomic<int> numSent(0), numReceived(0);
    
    sink.onEvent = [&] (const std::string & str)
        {
            numReceived += 1;
        };

    volatile bool finished = false;

    auto pushThread = [&] ()
        {
            for (unsigned i = 0;  i < 1000;  ++i) {
                sink.push("hello");
                numSent += 1;
            }
        };

    auto processThread = [&] ()
        {
            while (!finished) {
                sink.processOne();
            }
        };

    int numPushThreads = 2;
    int numProcessThreads = 1;

    for (unsigned i = 0;  i < 100;  ++i) {
        // Test for PLAT-106; the expected behaviour is no deadlock.
        ML::Watchdog watchdog(2.0);

        finished = false;

        boost::thread_group pushThreads;
        for (unsigned i = 0;  i < numPushThreads;  ++i)
            pushThreads.create_thread(pushThread);

        boost::thread_group processThreads;
        for (unsigned i = 0;  i < numProcessThreads;  ++i)
            processThreads.create_thread(processThread);
    
        pushThreads.join_all();

        cerr << "finished push threads" << endl;
    
        finished = true;

        processThreads.join_all();
    }
}

namespace MLDB {

BOOST_AUTO_TEST_CASE( test_typed_message_queue )
{
    {
        size_t numNotifications(0);
        auto onNotify = [&]() {
            numNotifications++;
            return true;
        };
        TypedMessageQueue<string> queue(onNotify, 5);

        /* testing constructor */
        BOOST_CHECK_EQUAL(queue.maxMessages_, 5);
        BOOST_CHECK_EQUAL(queue.pending_, false);
        BOOST_CHECK_EQUAL(queue.queue_.size(), 0);

        /* push */
        queue.push_back("first message");
        BOOST_CHECK_EQUAL(queue.pending_, true);
        BOOST_CHECK_EQUAL(queue.queue_.size(), 1);
        BOOST_CHECK_EQUAL(queue.queue_.front(), "first message");
        BOOST_CHECK_EQUAL(numNotifications, 0);

        /* process one */
        queue.processOne();
        /* only "pop_front" affects "pending_" */
        BOOST_CHECK_EQUAL(queue.pending_, true);
        BOOST_CHECK_EQUAL(queue.queue_.size(), 1);
        BOOST_CHECK_EQUAL(numNotifications, 1);

        queue.queue_.pop();
        queue.processOne();
        /* only "pop_front" affects "pending_" */
        BOOST_CHECK_EQUAL(queue.pending_, true);
        BOOST_CHECK_EQUAL(queue.queue_.size(), 0);
        BOOST_CHECK_EQUAL(numNotifications, 2);

        /* pop front 1: a single element */
        queue.queue_.emplace("first message");
        auto msgs = queue.pop_front(1);
        BOOST_CHECK_EQUAL(msgs.size(), 1);
        BOOST_CHECK_EQUAL(msgs[0], "first message");
        BOOST_CHECK_EQUAL(queue.queue_.size(), 0);

        /* pop front 2: too many elements requested */
        queue.queue_.emplace("blabla 1");
        queue.queue_.emplace("blabla 2");
        msgs = queue.pop_front(10);
        BOOST_CHECK_EQUAL(msgs.size(), 2);
        BOOST_CHECK_EQUAL(queue.queue_.size(), 0);

        /* pop front 3: all elements requested */
        queue.queue_.emplace("blabla 1");
        queue.queue_.emplace("blabla 2");
        msgs = queue.pop_front(0);
        BOOST_CHECK_EQUAL(msgs.size(), 2);
        BOOST_CHECK_EQUAL(queue.queue_.size(), 0);
    }

    /* multiple producers and a MessageLoop */
    {
        const int numThreads(20);
        const size_t numMessages(100000);

        cerr << "tests with a message loop\n";

        ML::Watchdog watchdog(120);

        MessageLoop loop;
        loop.start();

        size_t numNotifications(0);
        size_t numPopped(0);

        shared_ptr<TypedMessageQueue<string> > queue;
        auto onNotify = [&]() {
            numNotifications++;
            auto msgs = queue->pop_front(0);
            numPopped += msgs.size();
            if (msgs.size() > 0) {
                cerr << ("received " + to_string(numPopped) + " msgs;"
                         " last = " + msgs.back() + "\n");
            }
            return true;
        };
        queue.reset(new TypedMessageQueue<string>(onNotify, 1000));
        loop.addSource("queue", queue);

        size_t sliceSize = numMessages/numThreads;
        auto threadFn = [&] (int threadNum) {
            size_t base = threadNum * sliceSize;
            float sleepTime = 0.1 * threadNum;
            for (size_t i = 0; i < sliceSize; i++) {
                while (!queue->push_back("This is message "
                                         + to_string(base + i))) {
                    std::this_thread::sleep_for(std::chrono::milliseconds(
                        int(sleepTime * 1000)));
                }
            }
        };

        vector<thread> workers;
        for (int i = 0; i < numThreads; i++) {
            workers.emplace_back(threadFn, i);
        }
        for (thread & worker: workers) {
            worker.join();
        }
        cerr << "done pushing " + to_string(numMessages) + " msgs\n";

        while (numPopped < numMessages) {
            std::this_thread::sleep_for(std::chrono::milliseconds(200));
        };
        
        cerr << ("numNotifications: " + to_string(numNotifications)
                 + "; numPopped: "  + to_string(numPopped)
                 + "\n");
    }
}

} // namespace MLDB
