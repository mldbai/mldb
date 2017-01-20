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
#include "mldb/arch/threads.h"
#include "mldb/base/exc_assert.h"
#include "mldb/vfs/filter_streams.h"
#include "mldb/jml/utils/guard.h"
#include "mldb/jml/utils/string_functions.h"
#include "mldb/jml/utils/vector_utils.h"
#include "mldb/ext/jsoncpp/value.h"
#include "mldb/io/message_loop.h"
#include "mldb/utils/runner.h"
#include "mldb/utils/sink.h"
#include "mldb/types/date.h"
#include "mldb/types/libc_value_descriptions.h"
#include "mldb/ext/jsoncpp/json.h"

#include <iostream>

#include "runner_test.h"

#include "signals.h"

using namespace std;
using namespace MLDB;

// #define BOOST_CHECK_EQUAL(x,y)  { ExcCheckEqual((x), (y), ""); }

struct _Init {
    _Init() {
        signal(SIGPIPE, SIG_IGN);
    }
} myInit;

#if 1

void dumpSigStatus()
{
    filter_istream stream("/proc/self/status");
    string line;
    while (stream) {
        getline(stream, line);
        if (line.empty())
            return;
        if (line.find("Sig") == 0 || line.find("Shd") == 0)
            cerr << line << endl;
    }
}


vector<string> pendingSignals()
{
    sigset_t sigs;
    int res = sigpending(&sigs);
    if (res == -1)
        throw MLDB::Exception(errno, "sigpending");
    
    vector<string> result;
    for (unsigned i = 1;  i < 32;  ++i)
        if (sigismember(&sigs, i))
            result.push_back(strsignal(i));
    return result;
}

BOOST_AUTO_TEST_CASE( test_runner_no_sigchld )
{
    BlockedSignals blockedSigs2(SIGCHLD);

    BOOST_CHECK_EQUAL(pendingSignals(), vector<string>());

    MessageLoop loop;
    Runner runner;
    std::mutex runResultLock;
    RunResult runResult;
    bool isTerminated = false;

    vector<string> command = {
        "shasdasdsadas", "-c", "echo hello"
    };
    
    auto onTerminate = [&] (const RunResult & result)
        {
            cerr << "command has terminated" << endl;
            std::unique_lock<std::mutex> guard(runResultLock);
            runResult = result;
            isTerminated = true;
        };
    
    auto onData = [&] (const std::string && data)
        {
            cerr << data;
        };
    
    auto onClose = [] (){};
    
    auto stdErrSink = make_shared<CallbackInputSink>(onData, onClose);
    loop.addSource("runner", runner);
    loop.start();

    runner.run(command, onTerminate, nullptr, stdErrSink);

    cerr << "waiting for start" << endl;
    BOOST_REQUIRE_EQUAL(runner.waitStart(1.0), false);
    cerr << "done waiting for start" << endl;

    runner.waitTermination();

    BOOST_CHECK(isTerminated);
    BOOST_CHECK_EQUAL(runResult.state, RunResult::LAUNCH_ERROR);
}
#endif

#if 1
/* ensures that the basic callback system works */
BOOST_AUTO_TEST_CASE( test_runner_callbacks )
{
    BlockedSignals blockedSigs2(SIGCHLD);

    MessageLoop loop;

    RunnerTestHelperCommands commands;
    commands.sendOutput(true, "hello stdout");
    commands.sendOutput(true, "hello stdout2");
    commands.sendOutput(false, "hello stderr");
    commands.sendExit(0);

    string receivedStdOut, expectedStdOut;
    string receivedStdErr, expectedStdErr;

    expectedStdOut = ("helper: ready\nhello stdout\nhello stdout2\n"
                      "helper: exit with code 0\n");
    expectedStdErr = "hello stderr\n";

    int done = false;
    auto onTerminate = [&] (const RunResult & result) {
        done = true;
        ML::futex_wake(done);
    };

    auto onStdOut = [&] (string && message) {
        // cerr << "received message on stdout: /" + message + "/" << endl;
        receivedStdOut += message;
    };
    auto stdOutSink = make_shared<CallbackInputSink>(onStdOut);

    auto onStdErr = [&] (string && message) {
        // cerr << "received message on stderr: /" + message + "/" << endl;
        receivedStdErr += message;
    };
    auto stdErrSink = make_shared<CallbackInputSink>(onStdErr);

    Runner runner;
    loop.addSource("runner", runner);
    loop.start();

    auto & stdInSink = runner.getStdInSink();
    runner.run({"build/x86_64/bin/runner_test_helper"},
               onTerminate, stdOutSink, stdErrSink);
    for (const string & command: commands) {
        while (!stdInSink.write(string(command))) {
            std::this_thread::sleep_for(std::chrono::milliseconds(100));
        }
    }
    stdInSink.requestClose();

    while (!done) {
        ML::futex_wait(done, false);
    }

    BOOST_CHECK_EQUAL(ML::hexify_string(receivedStdOut),
                      ML::hexify_string(expectedStdOut));
    BOOST_CHECK_EQUAL(ML::hexify_string(receivedStdErr),
                      ML::hexify_string(expectedStdErr));

    loop.shutdown();
}
#endif

#if 1
/* ensures that the returned status is properly set after termination */
BOOST_AUTO_TEST_CASE( test_runner_normal_exit )
{
    BlockedSignals blockedSigs(SIGCHLD);

    auto nullSink = make_shared<NullInputSink>();

    /* normal termination, with code */
    {
        MessageLoop loop;

        RunnerTestHelperCommands commands;
        commands.sendExit(123);

        RunResult result;
        auto onTerminate = [&] (const RunResult & newResult) {
            result = newResult;
        };
        Runner runner;
        loop.addSource("runner", runner);
        loop.start();

        auto & stdInSink = runner.getStdInSink();
        runner.run({"build/x86_64/bin/runner_test_helper"},
                   onTerminate, nullSink, nullSink);
        for (const string & command: commands) {
            stdInSink.write(string(command));
        }
        stdInSink.requestClose();
        runner.waitRunning();
        runner.waitTermination();

        BOOST_CHECK_EQUAL(result.state, RunResult::RETURNED);
        BOOST_CHECK_EQUAL(result.returnCode, 123);

        loop.shutdown();
    }

    /* aborted termination, with signum */
    {
        MessageLoop loop;

        RunnerTestHelperCommands commands;
        commands.sendAbort();

        RunResult result;
        auto onTerminate = [&] (const RunResult & newResult) {
            result = newResult;
        };
        Runner runner;
        loop.addSource("runner", runner);
        loop.start();

        auto & stdInSink = runner.getStdInSink();
        runner.run({"build/x86_64/bin/runner_test_helper"},
                   onTerminate, nullSink, nullSink);
        for (const string & command: commands) {
            stdInSink.write(string(command));
        }
        stdInSink.requestClose();
        runner.waitRunning();
        runner.waitTermination();

        BOOST_CHECK_EQUAL(result.state, RunResult::SIGNALED);
        BOOST_CHECK_EQUAL(result.signum, SIGABRT);

        loop.shutdown();
    }
}
#endif

#if 1
/* test the behaviour of the Runner class when attempting to launch a missing
 * executable, mostly mimicking bash */
BOOST_AUTO_TEST_CASE( test_runner_missing_exe )
{
    BlockedSignals blockedSigs(SIGCHLD);

    MessageLoop loop;

    loop.start();

    RunResult result;
    auto onTerminate = [&] (const RunResult & newResult) {
        cerr << "called onTerminate" << endl;
        result = newResult;
    };

    /* running a program that does not exist */
    {
        Runner runner;
        loop.addSource("runner1", runner);

        cerr << "running 1" << endl;
        runner.run({"/this/command/is/missing"}, onTerminate);
        cerr << "running 1b" << endl;
        runner.waitRunning();
        runner.waitTermination();

        BOOST_CHECK_EQUAL(result.state, RunResult::LAUNCH_ERROR);
        BOOST_CHECK_EQUAL(result.returnCode, -1);
        BOOST_CHECK_EQUAL(result.launchErrno, ENOENT);
        
        loop.removeSource(&runner);
        runner.waitConnectionState(AsyncEventSource::DISCONNECTED);
    }

    /* running a non-executable but existing file */
    {
        Runner runner;
        loop.addSource("runner2", runner);

        runner.run({"/dev/null"}, onTerminate);
        runner.waitRunning();
        runner.waitTermination();

        BOOST_CHECK_EQUAL(result.state, RunResult::LAUNCH_ERROR);
        BOOST_CHECK_EQUAL(result.launchErrno, EACCES);

        loop.removeSource(&runner);
        runner.waitConnectionState(AsyncEventSource::DISCONNECTED);
    }

    /* running a non-executable but existing non-file */
    {
        Runner runner;
        loop.addSource("runner2", runner);

        runner.run({"/dev"}, onTerminate);
        runner.waitRunning();
        runner.waitTermination();

        BOOST_CHECK_EQUAL(result.state, RunResult::LAUNCH_ERROR);
        BOOST_CHECK_EQUAL(result.launchErrno, EACCES);

        loop.removeSource(&runner);
        runner.waitConnectionState(AsyncEventSource::DISCONNECTED);
    }

    loop.shutdown();
}
#endif

#if 1
/* test the "execute" function */
BOOST_AUTO_TEST_CASE( test_runner_execute )
{
    cerr << "execute test" << endl;

    string received;
    auto onStdOut = [&] (string && message) {
        received = move(message);
    };
    auto stdOutSink = make_shared<CallbackInputSink>(onStdOut, nullptr);

    auto result = execute({"/bin/cat", "-"},
                          stdOutSink, nullptr, "hello callbacks");
    BOOST_CHECK_EQUAL(received, "hello callbacks");
    BOOST_CHECK_EQUAL(result.state, RunResult::RETURNED);
    BOOST_CHECK_EQUAL(result.returnCode, 0);

    /* If stdin is not closed, then "cat" will wait an block indefinitely.
       This test ensures this does not happen and thus that the closing of stdin
       works via the "closeStdin" parameter. */
    result = execute({"/bin/cat"},
                      stdOutSink, nullptr, "", true);
    BOOST_CHECK_EQUAL(result.state, RunResult::RETURNED);
    BOOST_CHECK_NE(result.returnCode, 0);
}
#endif

#if 1
/* perform multiple runs with the same Runner and ensures task-specific
 * components are properly segregated */
BOOST_AUTO_TEST_CASE( test_runner_cleanup )
{
    MessageLoop loop;

    Runner runner;
    loop.addSource("runner", runner);
    loop.start();

    auto nullSink = make_shared<NullInputSink>();

    auto onTerminate = [&] (const RunResult & runResult) {
    };

    auto performLoop = [&] (const string & loopData) {
        RunnerTestHelperCommands commands;
        commands.sendOutput(true, loopData);
        commands.sendExit(0);

        string expectedStdOut("helper: ready\n" + loopData
                              + "\nhelper: exit with code 0\n");
        string receivedStdOut;
        auto onStdOut = [&] (string && message) {
            // cerr << "received message on stdout: /" + message + "/" << endl;
            receivedStdOut += message;
        };
        auto stdOutSink = make_shared<CallbackInputSink>(onStdOut);

        auto & stdInSink = runner.getStdInSink();
        runner.run({"build/x86_64/bin/runner_test_helper"},
                   onTerminate, stdOutSink, nullSink);
        for (const string & command: commands) {
            stdInSink.write(string(command));
        }
        stdInSink.requestClose();
        runner.waitRunning();
        runner.waitTermination();

        BOOST_CHECK_EQUAL(ML::hexify_string(receivedStdOut),
                          ML::hexify_string(expectedStdOut));
    };

    for (int i = 0; i < 5; i++) {
        performLoop(to_string(i));
    }

    loop.shutdown();
}
#endif

#if 1
/* Ensures that the output is received as soon as it is emitted, and not by
 * chunks. This is done by expecting different types of strings: a simple one
 * with a few chars, another one with two carriage returns and a third one
 * with 1024 chars. The test works by ensuring that all strings are received
 * one by one, with a relatively precise and constant delay of 1 second
 * between them. */
static void
test_runner_no_output_delay_helper(bool stdout)
{
    double delays[3];
    int sizes[3];
    int pos(stdout ? -1 : 0);
    shared_ptr<CallbackInputSink> stdOutSink(nullptr);
    shared_ptr<CallbackInputSink> stdErrSink(nullptr);

    Date start = Date::now();
    Date last = start;

    auto onCapture = [&] (string && message) {
        Date now = Date::now();
        if (pos > -1 && pos < 3) {
            /* skip "helper: ready" message */
            delays[pos] = now.secondsSinceEpoch() - last.secondsSinceEpoch();
            sizes[pos] = message.size();
        }
        pos++;
        last = now;
    };
    if (stdout) {
        stdOutSink.reset(new CallbackInputSink(onCapture));
    }
    else {
        stdErrSink.reset(new CallbackInputSink(onCapture));
    }

    RunnerTestHelperCommands commands;
    commands.sendSleep(10);
    commands.sendOutput(stdout, "first");
    commands.sendSleep(10);
    commands.sendOutput(stdout, "second\nsecond");
    commands.sendSleep(10);

    string third;
    for (int i = 0; i < 128; i++) {
        third += "abcdefgh";
    }
    commands.sendOutput(stdout, third);
    commands.sendSleep(10);
    commands.sendExit(0);

    MessageLoop loop;
    Runner runner;
    loop.addSource("runner", runner);
    loop.start();

    auto onTerminate = [&] (const RunResult & result) {
    };

    auto & stdInSink = runner.getStdInSink();
    runner.run({"/usr/bin/stdbuf", "-o0",
                "build/x86_64/bin/runner_test_helper"},
               onTerminate, stdOutSink, stdErrSink);
    for (const string & command: commands) {
        while (!stdInSink.write(string(command))) {
            std::this_thread::sleep_for(std::chrono::milliseconds(100));
        }
    }
    stdInSink.requestClose();
    runner.waitRunning();
    runner.waitTermination();

    BOOST_CHECK_EQUAL(sizes[0], 6);
    BOOST_CHECK(delays[0] >= 0.9);
    BOOST_CHECK_EQUAL(sizes[1], 14);
    BOOST_CHECK(delays[1] >= 0.9);
    BOOST_CHECK_EQUAL(sizes[2], 1025);
    BOOST_CHECK(delays[2] >= 0.9);

    for (int i = 0; i < 3; i++) {
        ::fprintf(stderr, "%d: size: %d; delay: %f\n", i, sizes[i], delays[i]);
    }
}

BOOST_AUTO_TEST_CASE( test_runner_no_output_delay_stdout )
{
    test_runner_no_output_delay_helper(true);
}

BOOST_AUTO_TEST_CASE( test_runner_no_output_delay_stderr )
{
    test_runner_no_output_delay_helper(false);
}
#endif

#if 1
/* invoke "execute" multiple time with the same MessageLoop as parameter */
BOOST_AUTO_TEST_CASE( test_runner_multi_execute_single_loop )
{
    MessageLoop loop;

    loop.start();

    auto result
           = execute(loop, {"/bin/echo", "Test 1"});
    BOOST_CHECK_EQUAL(result.state, RunResult::RETURNED);
    BOOST_CHECK_EQUAL(result.returnCode, 0);

    result = execute(loop, {"/bin/echo", "Test 2"});
    BOOST_CHECK_EQUAL(result.state, RunResult::RETURNED);
    BOOST_CHECK_EQUAL(result.returnCode, 0);

    result = execute(loop, {"/bin/echo", "Test 3"});
    BOOST_CHECK_EQUAL(result.state, RunResult::RETURNED);
    BOOST_CHECK_EQUAL(result.returnCode, 0);
}
#endif

#if 1
BOOST_AUTO_TEST_CASE( test_runner_fast_execution_multiple_threads )
{
    volatile bool shutdown = false;
    
    std::atomic<int> doneIterations(0);

    auto doThread = [&] (int threadNum)
        {
            while (!shutdown) {
                auto result = execute({ "/bin/true" },
                                      std::make_shared<OStreamInputSink>(&std::cout),
                                      std::make_shared<OStreamInputSink>(&std::cerr));

                ExcAssertEqual(result.returnCode, 0);
                cerr << threadNum;

                ++doneIterations;
            }
        };

    std::vector<std::unique_ptr<std::thread> > threads;

    for (unsigned i = 0;  i < 8;  ++i)
        threads.emplace_back(new std::thread(std::bind(doThread, i)));

    std::this_thread::sleep_for(std::chrono::seconds(2));

    shutdown = true;

    for (auto & t: threads)
        t->join();
    
    cerr << "did " << doneIterations << " runner iterations" << endl;
}
#endif

#if 1
BOOST_AUTO_TEST_CASE( test_timeval_value_description )
{
    /* printing */
    {
        timeval tv;
        tv.tv_sec = 1;
        tv.tv_usec = 2; /* 1.000002 secs */

        Json::Value expected
            = Json::parse("{\"tv_sec\": 1, \"tv_usec\": 2}");
        Json::Value result = jsonEncode<timeval>(tv);
        BOOST_CHECK_EQUAL(result, expected);
    }

    /* parsing */
    {
        string input = "{\"tv_sec\": 12, \"tv_usec\": 3456}";
        struct timeval tv = jsonDecodeStr<timeval>(input);
        BOOST_CHECK_EQUAL(tv.tv_sec, 12);
        BOOST_CHECK_EQUAL(tv.tv_usec, 3456);
    }
}
#endif

#if 1
/* This test ensures that running a program from a thread does not cause the
 * program to be killed when the thread exits, due to prctl PR_SET_PDEATHSIG
 * (http://man7.org/linux/man-pages/man2/prctl.2.html) being active when
 * pthread_exit is called. */
BOOST_AUTO_TEST_CASE( test_set_prctl_from_thread )
{
    MessageLoop loop;
    loop.start();

    auto runner = make_shared<Runner>();
    loop.addSource("runner", runner);
    runner->waitConnectionState(AsyncEventSource::CONNECTED);

    std::mutex lock;

    RunResult runResult;
    auto onTerminate = [&] (const RunResult & result) {
        cerr << to_string(gettid()) + ": process terminated\n";
        runResult = result;
        lock.unlock();
    };

    auto threadProc = [=] () {
        runner->run({"/bin/sleep", "3"}, onTerminate);
        cerr << to_string(gettid()) + ": process launched\n";
    };

    lock.lock();

    cerr << to_string(gettid()) + ": launching thread\n";
    thread launcherThread(threadProc);
    launcherThread.join();
    cerr << to_string(gettid()) + ": thread joined\n";

    lock.lock();

    cerr << to_string(gettid()) + ": runner done\n";
    loop.shutdown();
    lock.unlock();

    BOOST_CHECK_EQUAL(runResult.state, RunResult::RETURNED);
    BOOST_CHECK_EQUAL(runResult.returnCode, 0);
    BOOST_CHECK_EQUAL(runResult.signum, -1);
}
#endif

#if 1
/* This test ensures that onTerminate is called with the appropriate RunResult
 * when the runWrapper process fails. */
BOOST_AUTO_TEST_CASE( test_unexisting_runner_helper )
{
    BlockedSignals blockedSigs2(SIGCHLD);
    ML::Call_Guard guard([&] { Runner::runnerHelper.clear(); });
    Runner::runnerHelper = "/this/executable/does/not/exist";

    auto runResult = execute({"/bin/sleep", "1"});

    BOOST_CHECK_EQUAL(runResult.state, RunResult::LAUNCH_ERROR);
    BOOST_CHECK_EQUAL(runResult.returnCode, -1);
    BOOST_CHECK_EQUAL(runResult.signum, -1);
    BOOST_CHECK_EQUAL(runResult.processStatus(), 127); /* "command not found" */
}
#endif

#if 1
/* This test ensures that onTerminate is called with the appropriate RunResult
 * when the runWrapper process fails and that the handling of file descriptors
 * properly separates the channels between the previous and following
 * processes. */
BOOST_AUTO_TEST_CASE( test_runner_reuse )
{
    MessageLoop loop;
    loop.start();

    auto runner = make_shared<Runner>();
    loop.addSource("runner", runner);
    runner->waitConnectionState(AsyncEventSource::CONNECTED);

    int terminateCount(0);
    std::mutex lock;

    vector<string> stdouts;
    string currentStdout;
    auto onStdOut = [&] (string && message) {
        // cerr << "received message on stdout: /" + message + "/" << endl;
        currentStdout += message;
    };
    auto stdOutSink = make_shared<CallbackInputSink>(onStdOut);

    RunResult runResult;
    Runner::OnTerminate onTerminate;
    onTerminate = [&] (const RunResult & result) {
        stdouts.push_back(currentStdout);
        currentStdout.clear();
        terminateCount++;
        cerr << "terminateCount: " + to_string(terminateCount) + "\n";
        if (terminateCount < 2) {
            cerr << "launching subsequent process...\n";
            auto & stdInSink = runner->getStdInSink();
            stdInSink.write("second");
            stdInSink.requestClose();
            runner->run({"/bin/cat", "-"}, onTerminate, stdOutSink);
            cerr << "subsequent process started\n";
        }
        else {
            lock.unlock();
        }
    };

    lock.lock();
    auto & stdInSink = runner->getStdInSink();
    stdInSink.write("first");
    stdInSink.requestClose();
    runner->run({"/bin/cat", "-"}, onTerminate, stdOutSink);

    lock.lock();
    loop.shutdown();
    lock.unlock();

    BOOST_CHECK_EQUAL(stdouts[0], "first");
    BOOST_CHECK_EQUAL(stdouts[1], "second");
}
#endif

#if 1
/* This test ensures that waitRunning properly follows the sequence of launches
 * whenever an exception occurs in the launch phase. */
BOOST_AUTO_TEST_CASE( test_runner_waitRunning_exceptions )
{
    const unsigned int maxRuns(3);
    BlockedSignals blockedSigs(SIGCHLD);
    MessageLoop loop;
    loop.start();

    auto runner = make_shared<Runner>();
    loop.addSource("runner", runner);
    runner->waitConnectionState(AsyncEventSource::CONNECTED);

    vector<string> command{"/bin/sleep", "2"};

    std::atomic<int> terminateCount(0);
    auto doTerminate = [&] (int runCount) {
        cerr << ("runCount: " + to_string(runCount)
                 + "; terminateCount: " + to_string(terminateCount)
                 + "\n");
        terminateCount++;
        ML::futex_wake(terminateCount);
    };
    for (int i = 0; i < maxRuns; i++) {
        auto onTerminate = [=] (const RunResult & result) {
            doTerminate(i);
        };

        runner->run(command, onTerminate);
        runner->waitRunning();
    }

    while (terminateCount < maxRuns) {
        int current(terminateCount);
        ML::futex_wait(terminateCount, current);
    }

    loop.shutdown();
}
#endif
