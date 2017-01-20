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
    ML::Watchdog wd(5);
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
    ML::Watchdog wd(30);
    const int numSources(1000);

    typedef shared_ptr<TypedMessageSink<string> > TestSource;

    /* before "start" */
    {
        MessageLoop loop;
        vector<TestSource> sources;
        for (int i = 0; i < numSources; i++) {
            sources.emplace_back(new TypedMessageSink<string>(5));
        }

        for (auto & source: sources) {
            loop.addSource("source", source);
        }

        loop.start();

        cerr << "added before start\n";
        for (auto & source: sources) {
            source->waitConnectionState(AsyncEventSource::CONNECTED);
        }

        std::this_thread::sleep_for(std::chrono::seconds(1));

        /* cleanup */
        for (auto & source: sources) {
            loop.removeSource(source.get());
        }
        for (auto & source: sources) {
            source->waitConnectionState(AsyncEventSource::DISCONNECTED);
        }
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
            loop.addSource("source", source);
        }

        for (auto & source: sources) {
            source->waitConnectionState(AsyncEventSource::CONNECTED);
        }

        std::this_thread::sleep_for(std::chrono::seconds(1));

        /* cleanup */
        for (auto & source: sources) {
            loop.removeSource(source.get());
        }
        for (auto & source: sources) {
            source->waitConnectionState(AsyncEventSource::DISCONNECTED);
        }
    }
}
