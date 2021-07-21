// This file is part of MLDB. Copyright 2015 mldb.ai inc. All rights reserved.

#define BOOST_TEST_MAIN
#define BOOST_TEST_DYN_LINK

#include <iostream>

#include <boost/test/unit_test.hpp>

#include <chrono>
#include <thread>
#include "mldb/utils/testing/watchdog.h"

#include "mldb/io/typed_message_channel.h"
#include "mldb/io/message_loop.h"

using namespace std;
using namespace MLDB;


/* This test ensures that adding sources works correctly when needsPoll is
 * set. Otherwise, the watchdog will be triggered. */
BOOST_AUTO_TEST_CASE( test_addSource_with_needsPoll )
{
    MLDB::Watchdog wd(5);
    MessageLoop loop;
    loop.needsPoll = true;

    TypedMessageSink<string> aSource(123);
    loop.addSource("source", aSource);
    loop.start();
    aSource.waitConnectionState(AsyncEventSource::CONNECTED);

    loop.removeSource(&aSource);
    aSource.waitConnectionState(AsyncEventSource::DISCONNECTED);
}

/* This test ensures that adding sources works correctly independently of
 * whether the loop has been started or not, even with a ridiculous amount of
 * sources. */
BOOST_AUTO_TEST_CASE( test_addSource_after_before_start )
{
    MLDB::Watchdog wd(30);
    const int numSources(100);

    typedef shared_ptr<TypedMessageSink<string> > TestSource;

    /* before "start" */
    {
        MessageLoop loop;
        vector<TestSource> sources;
        for (int i = 0; i < numSources; i++) {
            sources.emplace_back(new TypedMessageSink<string>(5));
        }

        cerr << "before adding" << endl;
        for (auto & source: sources) {
            cerr << ".";
            loop.addSource("source", source);
        }

        cerr << "done adding" << endl;
        loop.start();

        cerr << "added before start\n";
        for (auto & source: sources) {
            cerr << ".";
            source->waitConnectionState(AsyncEventSource::CONNECTED);
        }
        cerr << endl;

        cerr << "asleep" << endl;
        std::this_thread::sleep_for(std::chrono::seconds(1));
        cerr << "cleanup" << endl;

        /* cleanup */
        for (auto & source: sources) {
            cerr << "x";
            loop.removeSource(source.get());
        }
        for (auto & source: sources) {
            cerr << "X";
            source->waitConnectionState(AsyncEventSource::DISCONNECTED);
        }

        cerr << "*************** DONE TEST1 *********************" << endl;
    }

    /* after "start" */
    {
        MessageLoop loop;
        vector<TestSource> sources;
        for (int i = 0; i < numSources; i++) {
            sources.emplace_back(new TypedMessageSink<string>(5));
        }

        loop.start();

        cerr << "added after start\n";
        for (auto & source: sources) {
            cerr << ".";
            loop.addSource("source", source);
        }

        cerr << "waiting for connected" << endl;
        for (auto & source: sources) {
            cerr << "o";
            source->waitConnectionState(AsyncEventSource::CONNECTED);
        }

        cerr << "asleep" << endl;
        std::this_thread::sleep_for(std::chrono::seconds(1));
        cerr << "awake" << endl;
        
        /* cleanup */
        for (auto & source: sources) {
            cerr << "x";
            loop.removeSource(source.get());
        }
        for (auto & source: sources) {
            cerr << "X";
            source->waitConnectionState(AsyncEventSource::DISCONNECTED);
        }
    }
}
