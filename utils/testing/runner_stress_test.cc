// This file is part of MLDB. Copyright 2015 mldb.ai inc. All rights reserved.

#define BOOST_TEST_MAIN
#define BOOST_TEST_DYN_LINK

#include <sys/types.h>
#include <sys/wait.h>

#include <mutex>
#include <thread>
#include <chrono>

#include <boost/test/unit_test.hpp>

#include "mldb/arch/exception.h"
#include "mldb/arch/futex.h"
#include "mldb/base/exc_assert.h"
#include "mldb/jml/utils/string_functions.h"
#include "mldb/utils/testing/watchdog.h"
#include "mldb/io/message_loop.h"
#include "mldb/utils/runner.h"
#include "mldb/utils/sink.h"
#include "mldb/types/date.h"

#include <iostream>

#include "signals.h"

using namespace std;
using namespace MLDB;

// #define BOOST_CHECK_EQUAL(x,y)  { ExcCheckEqual((x), (y), ""); }

struct _Init {
    _Init() {
        signal(SIGPIPE, SIG_IGN);
    }
} myInit;

struct HelperCommands : vector<string>
{
    HelperCommands()
        : vector<string>(),
          active_(0)
    {}

    void reset() { active_ = 0; }

    string nextCommand()
    {
        if (active_ < size()) {
            int active = active_;
            active_++;
            return at(active);
        }
        else {
            return "";
        }
    }

    void sendOutput(bool isStdOut, const string & data)
    {
        char cmdBuffer[1024];
        int len = data.size();
        int totalLen = len + 3 + sizeof(int);
        sprintf(cmdBuffer, (isStdOut ? "out" : "err"));
        memcpy(cmdBuffer + 3, &len, sizeof(int));
        memcpy(cmdBuffer + 3 + sizeof(int), data.c_str(), len);
        push_back(string(cmdBuffer, totalLen));
    }

    void sendExit(int code)
    {
        char cmdBuffer[1024];
        int totalLen = 3 + sizeof(int);
        sprintf(cmdBuffer, "xit");
        memcpy(cmdBuffer + 3, &code, sizeof(int));
        push_back(string(cmdBuffer, totalLen));
    };

    void sendAbort()
    {
        push_back("abt");
    }

    int active_;
};

#if 1
/* stress test that runs 20 threads in parallel, where each thread:
- invoke "execute", with 10000 messages to stderr and stdout (each)
  received from the stdin sink
- compare those messages with a fixture
and
- the parent thread that outputs messages on stderr and on stdout until all
  threads are done
- wait for the termination of all threads
- ensures that all child process have properly exited
*/
BOOST_AUTO_TEST_CASE( test_stress_runner )
{
    ML::Watchdog wd(120);
    vector<thread> threads;
    int nThreads(20);
    vector<pid_t> childPids(nThreads);
    int msgsToSend(10000);
    atomic<int> nRunning(0);

    std::atomic<int> activeThreads(nThreads);

    auto onTerminate = [&] (const RunResult &) {
    };

    auto runThread = [&] (int threadNum) {
        /* preparation */
        HelperCommands commands;
        string receivedStdOut, expectedStdOut;
        string receivedStdErr, expectedStdErr;
        size_t stdInBytes(0);

        receivedStdOut.reserve(msgsToSend * 80);
        expectedStdOut.reserve(msgsToSend * 80);
        receivedStdErr.reserve(msgsToSend * 80);
        expectedStdErr.reserve(msgsToSend * 80);

        expectedStdOut = "helper: ready\n";
        for (int i = 0; i < msgsToSend; i++) {
            string stdOutData = (to_string(threadNum)
                                 + ":" + to_string(i)
                                 + ": this is a message to stdout\n\t"
                                 + "and a tabbed line");
            commands.sendOutput(true, stdOutData);
            expectedStdOut += stdOutData + "\n";
            string stdErrData = (to_string(threadNum)
                                 + ":" + to_string(i)
                                 + ": this is a message to stderr\n\t"
                                 + "and a tabbed line");
            commands.sendOutput(false, stdErrData);
            expectedStdErr += stdErrData + "\n";
        }
        commands.sendExit(0);

        expectedStdOut += "helper: exit with code 0\n";

        /* execution */
        MessageLoop loop;
        Runner runner;

        loop.addSource("runner", runner);
        loop.start();

        auto onStdOut = [&] (string && message) {
            receivedStdOut += message;
        };
        auto stdOutSink = make_shared<CallbackInputSink>(onStdOut);
        auto onStdErr = [&] (string && message) {
            receivedStdErr += message;
        };
        auto stdErrSink = make_shared<CallbackInputSink>(onStdErr);

        auto & stdInSink = runner.getStdInSink();
        runner.run({"build/x86_64/bin/runner_test_helper"},
                   onTerminate, stdOutSink, stdErrSink);

        for (const string & command: commands) {
            while (!stdInSink.write(string(command))) {
                std::this_thread::sleep_for(std::chrono::milliseconds(10));
            }
            stdInBytes += command.size();
        }

        runner.waitStart();
        pid_t pid = runner.childPid();
        nRunning++;
        // cerr << "running with pid: " + to_string(pid) + "\n";

        childPids[threadNum] = pid;

        // cerr << "sleeping\n";
        std::this_thread::sleep_for(std::chrono::seconds(1));

        /* before closing stdinsink, wait until all bytes are correctly
           sent */
        {
            AsyncFdOutputSink & sinkPtr = (AsyncFdOutputSink &) stdInSink;
            while (sinkPtr.bytesSent() != stdInBytes) {
                std::this_thread::sleep_for(std::chrono::milliseconds(200));
            }
            if (sinkPtr.queueEnabled()) {
                stdInSink.requestClose();
            }
        }

        // cerr << "waiting termination...\n";
        runner.waitTermination();
        // cerr << "terminated\n";

        loop.shutdown();

        BOOST_CHECK_EQUAL(receivedStdOut, expectedStdOut);
        BOOST_CHECK_EQUAL(receivedStdErr, expectedStdErr);

        --activeThreads;
        // cerr << "activeThreads now: " + to_string(activeThreads) + "\n";
        if (activeThreads == 0) {
            ML::futex_wake(activeThreads);
        }
        cerr << "thread shutting down\n";
    };

    /* initialize childPids with a non-random bad value, so that we can know
     * later whether the pids were correctly initialized from the workers */
    for (int i = 0; i < nThreads; i++) {
        childPids[i] = 0xdeadface;
    }

    std::atomic_thread_fence(std::memory_order_seq_cst);

    for (int i = 0; i < nThreads; i++) {
        threads.emplace_back(runThread, i);
    }

    std::atomic_thread_fence(std::memory_order_seq_cst);

    /* attempting to interfere with stdout/stderr as long as all thread have
     * not redirected their output channels yet (are not running) */
    while (nRunning < nThreads) {
        cout << "performing interference on stdout\n";
        cerr << "performing interference on stderr\n";
        // int n = activeThreads;
        // ML::futex_wait(activeThreads, n);
    }

    for (thread & current: threads) {
        current.join();
    }
 
    /* ensure children have all exited... */
    BOOST_CHECK_EQUAL(childPids.size(), threads.size());
    for (const int & pid: childPids) {
        if (pid != 0xdeadface) {
            /* the child may already be done when childPid was invoked */
            if (pid > 0) {
                waitpid(pid, NULL, WNOHANG);
                int errno_ = errno;
                BOOST_CHECK_EQUAL(errno_, ECHILD);
            }
        }
        else {
            throw MLDB::Exception("pid was never set");
        }
    }
}

#endif
