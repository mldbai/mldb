// This file is part of MLDB. Copyright 2015 mldb.ai inc. All rights reserved.

#include "mldb/builtin/python/python_interpreter.h"
#include "mldb/builtin/python/capture_stream.h"
#include "mldb/utils/testing/watchdog.h"
#include "mldb/arch/format.h"

#define BOOST_TEST_MAIN
#define BOOST_TEST_DYN_LINK

#include "mldb/utils/split.h"
#include <boost/test/unit_test.hpp>

#include <thread>
#include <chrono>
#include <atomic>

using namespace std;

using namespace MLDB;

BOOST_AUTO_TEST_CASE( test_python_interpreter )
{
    PythonInterpreter interpreter;
}

BOOST_AUTO_TEST_CASE( test_capture_streams )
{
    PythonInterpreter interpreter;
    
    auto threadScope = interpreter.mainThread().enter();

    auto main_module = boost::python::import("__main__");
    auto main_namespace = main_module.attr("__dict__");

    std::vector<std::string> written;

    auto onWrite = [&] (const EnterThreadToken & token,
                        std::string str)
        {
            cerr << "got string " << str << " of length "
                 << str.length() << endl;
            written.emplace_back(std::move(str));
        };
    
    auto captureToken = setStdStream(*threadScope, onWrite, "stdout");
    
    PythonThread::exec(*threadScope,
                       "print('hello')",
                       MLDB::format("%s:%d", __FILE__, __LINE__),
                       main_namespace);

    BOOST_CHECK_EQUAL(written.at(0), "hello");
    BOOST_CHECK_EQUAL(written.at(1), "\n");
    
    // Uninstall and verify it works
    captureToken.reset();

    PythonThread::exec(*threadScope,
                       "print('world')",
                       MLDB::format("%s:%d", __FILE__, __LINE__),
                       main_namespace);
    
    BOOST_CHECK_EQUAL(written.size(), 2);
}

BOOST_AUTO_TEST_CASE( test_python_interpreter_multithreaded )
{
    // Make sure that multithreaded initialization of interpreters works
    {
        PythonInterpreter interpreter;
    }

    std::atomic<bool> finished(false);
    std::atomic<int64_t> iterations(0);

    auto runTest = [&] ()
        {
            while (!finished) {
                {
                    PythonInterpreter interpreter;
                }
                ++iterations;
            }
        };

    constexpr size_t numThreads = 4;

    Watchdog watchdog(10.0);  // allow 10 seconds

    std::vector<std::thread> threads;
    for (size_t i = 0;  i < numThreads;  ++i) {
        threads.emplace_back(runTest);
    }

    std::this_thread::sleep_for(std::chrono::seconds(1));

    finished = true;

    for (auto & t: threads)
        t.join();

    cerr << "initialized " << iterations << " interpreters in one second"
         << endl;
}

BOOST_AUTO_TEST_CASE(test_exec)
{
    PythonInterpreter interpreter;
    auto threadScope = interpreter.mainThread().enter();

    auto main_module = boost::python::import("__main__");
    auto main_namespace = main_module.attr("__dict__");

    PythonThread::exec(*threadScope,
                       "world = 'hello'",
                       MLDB::format("%s:%d", __FILE__, __LINE__),
                       main_namespace);
    
    boost::python::object world = main_namespace["world"];
    std::string val = boost::python::extract<std::string>(world);

    BOOST_CHECK_EQUAL(val, "hello");
}

BOOST_AUTO_TEST_CASE(test_multithreaded_exec)
{
    // Test running multiple threads in different interpreters
    std::atomic<bool> finished(false);

    constexpr size_t numThreads = 4;

    Watchdog watchdog(10.0);  // allow 10 seconds

    auto testThread = [&] (int threadNum)
        {
            PythonInterpreter interpreter;

            auto threadScope = interpreter.mainThread().enter();

            auto main_module = boost::python::import("__main__");
            auto main_namespace = main_module.attr("__dict__");

            size_t iter;
            for (iter = 0;  !finished;  ++iter) {
                {
                    auto release = releaseGil();
                    std::this_thread::sleep_for(std::chrono::milliseconds(5));
                }

                PythonThread::exec(*threadScope,
                                   MLDB::format("world%d = 'hello%d'",
                                                iter, threadNum),
                                   MLDB::format("%s:%d", __FILE__, __LINE__),
                                   main_namespace);

                {
                    auto release = releaseGil();
                    std::this_thread::sleep_for(std::chrono::milliseconds(5));
                }

                boost::python::object world = main_namespace[MLDB::format("world%d", iter)];
                std::string val = boost::python::extract<std::string>(world);
                
                BOOST_CHECK_EQUAL(val, "hello" + to_string(threadNum));
            }
            
            cerr << "thread " << threadNum << " ran " << iter << " iterations"
                 << endl;
        };

    std::vector<std::thread> threads;
    for (size_t i = 0;  i < numThreads;  ++i) {
        threads.emplace_back(testThread, i);
    }

    std::this_thread::sleep_for(std::chrono::seconds(1));

    finished = true;

    for (auto & t: threads)
        t.join();
}

BOOST_AUTO_TEST_CASE(test_multithreaded_exec_single_interpreter)
{
    // Test running multiple threads in different interpreters
    std::atomic<bool> finished(false);

    constexpr size_t numThreads = 4;

    Watchdog watchdog(10.0);  // allow 10 seconds

    PythonInterpreter interpreter;
    
    auto testThread = [&] (int threadNum)
        {
            auto thread = interpreter.newThread();
            auto threadScope = thread.enter();

            auto main_module = boost::python::import("__main__");
            auto main_namespace = main_module.attr("__dict__");

            size_t iter;
            for (iter = 0;  !finished;  ++iter) {
                {
                    auto release = releaseGil();
                    std::this_thread::sleep_for(std::chrono::milliseconds(5));
                }

                PythonThread::exec(*threadScope,
                                   MLDB::format("world%d = 'hello%d'",
                                                threadNum, iter),
                                   MLDB::format("%s:%d", __FILE__, __LINE__),
                                   main_namespace);

                {
                    auto release = releaseGil();
                    std::this_thread::sleep_for(std::chrono::milliseconds(5));
                }

                boost::python::object world = main_namespace[MLDB::format("world%d", threadNum)];
                std::string val = boost::python::extract<std::string>(world);
                
                BOOST_CHECK_EQUAL(val, "hello" + to_string(iter));
            }
            
            cerr << "thread " << threadNum << " ran " << iter << " iterations"
                 << endl;
        };

    std::vector<std::thread> threads;
    for (size_t i = 0;  i < numThreads;  ++i) {
        threads.emplace_back(testThread, i);
    }

    std::this_thread::sleep_for(std::chrono::seconds(1));

    finished = true;

    for (auto & t: threads)
        t.join();
}
