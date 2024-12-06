// This file is part of MLDB. Copyright 2015 mldb.ai inc. All rights reserved.

/** thread_specitic_test.cc                                 -*- C++ -*-
    Rémi Attab, 30 Jul 2013
    Copyright (c) 2013 mldb.ai inc.  All rights reserved.

    Tests for the instanced TLS class.

*/

#define BOOST_TEST_MAIN
#define BOOST_TEST_DYN_LINK

#include "mldb/arch/thread_specific.h"

#include <boost/test/unit_test.hpp>
#include <atomic>
#include <thread>
#include <cassert>
#include <array>

using namespace std;
using namespace MLDB;

struct Data
{
    Data() : init(true)
    {
        init = true;
        constructed++;
    }

    ~Data()
    {
        check();
        init = false;
        destructed++;
    }

    void check() { assert(init); }

    bool init;

    static std::atomic<size_t> constructed;
    static std::atomic<size_t> destructed;
    static void validate()
    {
        assert(constructed == destructed);
        constructed = destructed = 0;
    }
};

std::atomic<size_t> Data::constructed = 0;
std::atomic<size_t> Data::destructed = 0;

typedef ThreadSpecificInstanceInfo<Data> Tls;


BOOST_AUTO_TEST_CASE(sanityTest)
{
    {
        ThreadSpecificInstanceInfo<Data> data;
        data.get();
    }

    BOOST_CHECK_EQUAL(Data::constructed, Data::destructed);
}

BOOST_AUTO_TEST_CASE( test_single_thread )
{
    {
        Tls t1;
        t1.get()->check();
    }
    Data::validate();

    {
        Tls t1;
        {
            Tls t2;
            t2.get()->check();
        }
        {
            Tls t2;
            Tls t3;
            t2.get()->check();
            t3.get()->check();
        }
    }
    Data::validate();

    {
        unique_ptr<Tls> t1(new Tls());
        unique_ptr<Tls> t2(new Tls());
        t1.reset();
        t2.reset();
    }
    Data::validate();
}


struct TlsThread
{
    TlsThread(Tls& tls) : tls(tls), done(false)
    {
        th = thread([=,this] { this->run(); });
    }

    ~TlsThread()
    {
        done = true;
        th.join();
    }

    void run()
    {
        tls.get()->check();
        while(!done);
    }

    Tls& tls;
    atomic<bool> done;
    thread th;
};


BOOST_AUTO_TEST_CASE( test_multi_threads_simple )
{
    Tls tls;

    {
        TlsThread t1(tls);
    }
    Data::validate();

    {
        TlsThread t1(tls);
        TlsThread t2(tls);
    }
    Data::validate();

    {
        TlsThread t1(tls);
        {
            TlsThread t2(tls);
        }
        {
            TlsThread t2(tls);
            TlsThread t3(tls);
        }
    }
    Data::validate();

    {
        unique_ptr<TlsThread> t1(new TlsThread(tls));
        unique_ptr<TlsThread> t2(new TlsThread(tls));
        t1.reset();
        t2.reset();
    }
}

BOOST_AUTO_TEST_CASE( test_multi_instance )
{
    enum {
        Instances = 64,
        Threads = 3
    };

    array<Tls*, Instances> instances;

    for (auto& instance : instances) instance = new Tls();

    auto runThread = [&] {
        for (auto& instance : instances)
            instance->get()->check();
    };
    for (size_t i = 0; i < Threads; ++i)
        thread(runThread).join();

    for (Tls* instance : instances) delete instance;

    Data::validate();
}

BOOST_AUTO_TEST_CASE(test_multi_get)
{
    ThreadSpecificInstanceInfo<std::shared_ptr<int>> info;

    bool hadInfo = false;
    std::shared_ptr<int> & i1 = *info.get(&hadInfo);
    BOOST_CHECK_EQUAL(hadInfo, false);
    BOOST_CHECK(!i1);
    i1.reset(new int(0));
    std::shared_ptr<int> & i2 = *info.get(&hadInfo);
    BOOST_CHECK_EQUAL(hadInfo, true);
    BOOST_CHECK_EQUAL(i1, i2);
    *i2 = 1;
    BOOST_CHECK_EQUAL(*i1, *i2);
}

BOOST_AUTO_TEST_CASE(stress_test_destroy_object_and_thread)
{
    struct S {
        ThreadSpecificInstanceInfo<Data> info;
    };

    std::atomic<bool> finished(false);

    // These threads instantiate and destroy instances of info
    std::vector<std::thread> instanceThreads;

    auto doInstanceThread = [&] ()
    {
        while (!finished) {
            S s;
            auto d = s.info.get();
            d->check();
        }
    };

    for (size_t i = 0;  i < 4;  ++i) {
        instanceThreads.emplace_back(doInstanceThread);
    }

    // Now we're creating and destroying instances, create and destroy threads
    auto doEphemeralThread = [&] ()
    {
        S s;
        auto d = s.info.get();
        d->check();
    };

    for (size_t i = 0;  i < 2000;  ++i) {
        if ((i+1) % 1000 == 0) {
            cerr << "done " << i+1 << endl;
        }
        std::thread t(doEphemeralThread);
        t.join();
    }

    finished = true;
    for (auto & t: instanceThreads)
        t.join();

    Data::validate();
}

#if 0
BOOST_AUTO_TEST_CASE(stress_test_destroy_object_and_thread_shared)
{
    struct S {
        ThreadSpecificInstanceInfo<Data> info;
    };

    std::atomic<bool> finished(false);

    // These threads instantiate and destroy instances of info
    std::vector<std::thread> instanceThreads;

    std::shared_ptr<S> s(new S());

    auto doInstanceThread = [&] ()
    {
        while (!finished) {
            auto d = s->info.get();
            d->check();
        }
    };

    for (size_t i = 0;  i < 8;  ++i) {
        instanceThreads.emplace_back(doInstanceThread);
    }

    finished = true;
    for (auto & t: instanceThreads)
        t.join();

    Data::validate();
}
#endif
