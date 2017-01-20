// This file is part of MLDB. Copyright 2015 mldb.ai inc. All rights reserved.

/* watch_test.cc
   Jeremy Barnes, 31 March 2014
   Copyright (c) 2014 mldb.ai inc.  All rights reserved.

   Test of watches.
*/

#include "mldb/watch/watch.h"
#include "mldb/watch/watch_impl.h"
#include "mldb/utils/testing/watchdog.h"

#define BOOST_TEST_MAIN
#define BOOST_TEST_DYN_LINK

#include <boost/test/unit_test.hpp>

using namespace std;
using namespace MLDB;

BOOST_AUTO_TEST_CASE( test_move )
{
    WatchesT<std::string> watches;
    std::vector<Watch> w;
    w.emplace_back(watches.add());
    BOOST_CHECK_EQUAL(watches.size(), 1);
    
    watches.trigger("hello");
    BOOST_CHECK_EQUAL(w.back().pop<std::string>(), "hello");
}

BOOST_AUTO_TEST_CASE( test_wait_after_trigger )
{
    WatchesT<std::string> watches;

    auto w = watches.add();

    watches.trigger("hello");

    BOOST_CHECK_EQUAL(w.pop(), "hello");
}

BOOST_AUTO_TEST_CASE( test_timeout_wait_no_event )
{
    WatchesT<std::string> watches;

    auto w = watches.add();

    Date before = Date::now();
    {
        MLDB_TRACE_EXCEPTIONS(false);
        BOOST_CHECK_THROW(w.wait(0.1), std::exception);
    }
    Date after = Date::now();

    BOOST_CHECK_GE(after.secondsSince(before), 0.1);
    BOOST_CHECK_LE(after.secondsSince(before), 1.0);
}

BOOST_AUTO_TEST_CASE( test_watch_attached_detached )
{
    WatchT<std::string> w;

    {
        WatchesT<std::string> watches;
        w = watches.add();
        BOOST_CHECK(w.attached());
    }

    BOOST_CHECK(!w.attached());
}

BOOST_AUTO_TEST_CASE( test_watch_attached_detached2 )
{
    WatchesT<std::string> watches;

    {
        auto w = watches.add();
        BOOST_CHECK(w.attached());
        BOOST_CHECK_EQUAL(watches.size(), 1);
        BOOST_CHECK(w.attached());
    }

    BOOST_CHECK(watches.empty());
}

BOOST_AUTO_TEST_CASE( test_cant_assign_wrong_type )
{
    WatchT<std::string> w;
    WatchesT<int> watches;

    BOOST_CHECK_EQUAL(watches.boundType(), &typeid(std::tuple<int>));
    BOOST_CHECK_EQUAL(watches.add().boundType(), &typeid(std::tuple<int>));
    BOOST_CHECK(watches.empty());

    // Can only have a bound type once it's assigned
    //BOOST_CHECK_EQUAL(w.boundType(), &typeid(std::tuple<std::string>));

    {
        MLDB_TRACE_EXCEPTIONS(false);
        BOOST_CHECK_THROW(w = watches.add(), std::exception);
    }
    BOOST_CHECK(!w.attached());
    BOOST_CHECK(watches.empty());
}

BOOST_AUTO_TEST_CASE( test_cant_assign_wrong_type_through_generic )
{
    WatchesT<int> watches;
    Watch w = watches.add();
    {
        MLDB_TRACE_EXCEPTIONS(false);
        BOOST_CHECK_THROW(WatchT<std::string> w2 = std::move(w), std::exception);
    }
}

BOOST_AUTO_TEST_CASE( test_generic_watches )
{
    Watches watches;
    WatchT<std::string> w = watches.add();
    {
        MLDB_TRACE_EXCEPTIONS(false);
        BOOST_CHECK_THROW(w.pop(), std::exception);
    }
    watches.trigger<std::string>("hello");
    BOOST_CHECK_EQUAL(w.pop(), "hello");
}

BOOST_AUTO_TEST_CASE( test_generic_watch )
{
    WatchesT<std::string> watches;
    Watch w = watches.add();
    {
        MLDB_TRACE_EXCEPTIONS(false);
        BOOST_CHECK_THROW(w.pop<std::string>(), std::exception);
    }
        
    watches.trigger("hello");
    BOOST_CHECK_EQUAL(w.pop<std::string>(), "hello");
}

BOOST_AUTO_TEST_CASE( test_generic_watch_late_bind )
{
    WatchesT<std::string> watches;
    Watch w = watches.add();
    watches.trigger("hello");

    BOOST_CHECK(w.any());

    vector<string> fired;

    auto gotEv = [&] (const Any & ev)
        {
            fired.push_back(std::get<0>(ev.as<std::tuple<string> >()));
        };

    w.bindGeneric(gotEv);

    BOOST_CHECK(!w.any());
    BOOST_REQUIRE_EQUAL(fired.size(), 1);
    BOOST_CHECK_EQUAL(fired[0], "hello");
}

BOOST_AUTO_TEST_CASE( test_generic_watch_late_bind2 )
{
    Watches watches;
    Watch w = watches.add();
    watches.trigger<std::string>("hello");

    BOOST_CHECK(w.any());

    vector<string> fired;

    auto gotEv = [&] (const Any & ev)
        {
            fired.push_back(std::get<0>(ev.as<std::tuple<string> >()));
        };

    w.bindGeneric(gotEv);

    BOOST_CHECK(!w.any());
    BOOST_REQUIRE_EQUAL(fired.size(), 1);
    BOOST_CHECK_EQUAL(fired[0], "hello");
}

BOOST_AUTO_TEST_CASE( test_generic_watch_and_watches )
{
    Watches watches;
    Watch w = watches.add();
    {
        MLDB_TRACE_EXCEPTIONS(false);
        BOOST_CHECK_THROW(w.pop<std::string>(), std::exception);
    }
    watches.trigger<std::string>("hello");
    BOOST_CHECK_EQUAL(w.pop<std::string>(), "hello");
}

BOOST_AUTO_TEST_CASE( test_multiplexing_watches_typed )
{
    WatchesT<std::string> watches1;
    WatchesT<std::string> watches2;

    WatchT<std::string> w;
    w.multiplex(watches1.add());
    w.multiplex(watches2.add());

    watches1.trigger("hello");
    watches2.trigger("world");

    BOOST_CHECK_EQUAL(w.pop(), "hello");
    BOOST_CHECK_EQUAL(w.pop(), "world");
}

BOOST_AUTO_TEST_CASE( test_multiplexing_watches_generic )
{
    Watches watches1;
    Watches watches2;

    WatchT<std::string> w;
    w.multiplex(watches1.add());
    w.multiplex(watches2.add());

    watches1.trigger<std::string>("hello");
    watches2.trigger<std::string>("world");

    BOOST_CHECK_EQUAL(w.pop(), "hello");
    BOOST_CHECK_EQUAL(w.pop(), "world");
}

BOOST_AUTO_TEST_CASE( test_multiplexing_watches_generic2 )
{
    WatchesT<std::string> watches1;
    WatchesT<std::string> watches2;

    Watch w;
    w.multiplex(watches1.add());
    w.multiplex(watches2.add());

    watches1.trigger("hello");
    watches2.trigger("world");

    BOOST_CHECK_EQUAL(w.pop<std::string>(), "hello");
    BOOST_CHECK_EQUAL(w.pop<std::string>(), "world");
}

BOOST_AUTO_TEST_CASE( test_multiplexing_watches_generic3 )
{
    Watches watches1;
    Watches watches2;

    Watch w;
    w.multiplex(watches1.add());
    w.multiplex(watches2.add());

    watches1.trigger<std::string>("hello");
    watches2.trigger<std::string>("world");

    BOOST_CHECK_EQUAL(w.pop<std::string>(), "hello");
    BOOST_CHECK_EQUAL(w.pop<std::string>(), "world");
}

BOOST_AUTO_TEST_CASE( test_multiplexing_watches_mixed_types )
{
    Watches watches1;
    Watches watches2;

    Watch w;
    w.multiplex(watches1.add());
    w.multiplex(watches2.add());

    watches1.trigger<std::string>("hello");
    watches2.trigger<int>(3);

    BOOST_CHECK_EQUAL(w.pop<std::string>(), "hello");
    BOOST_CHECK_EQUAL(w.pop<int>(), 3);
}

BOOST_AUTO_TEST_CASE( test_multiplexing_watches_incompatible_mixed_types )
{
    WatchesT<std::string> watches1;
    WatchesT<int> watches2;

    Watch w;
    w.multiplex(watches1.add());
    {
        MLDB_TRACE_EXCEPTIONS(false);
        BOOST_CHECK_THROW(w.multiplex(watches2.add()), std::exception);
    }

    watches1.trigger("hello");

    BOOST_CHECK_EQUAL(w.pop<std::string>(), "hello");
}

BOOST_AUTO_TEST_CASE( test_multiplexing_watches_typed_early_trigger )
{
    WatchesT<std::string> watches1;
    WatchesT<std::string> watches2;

    // Trigger before multiplexing to make sure that there is no
    // deadlock
    WatchT<std::string> w1 = watches1.add(), w2 = watches2.add();
    
    watches1.trigger("hello");
    watches2.trigger("world");

    WatchT<std::string> w;
    w.multiplex(std::move(w1));
    w.multiplex(std::move(w2));

    BOOST_CHECK_EQUAL(w.pop(), "hello");
    BOOST_CHECK_EQUAL(w.pop(), "world");
}

BOOST_AUTO_TEST_CASE( test_transform )
{
    WatchesT<int> watches;

    WatchT<std::string> w
        = transform<std::string>(watches.add(),
                                 [] (int i) { return std::to_string(i); });

    watches.trigger(1);
    watches.trigger(-1);

    BOOST_CHECK_EQUAL(w.pop(), "1");
    BOOST_CHECK_EQUAL(w.pop(), "-1");
}

BOOST_AUTO_TEST_CASE( test_transform_implicit )
{
    WatchesT<int> watches;

    // An int can be transformed implicitly into a double, so no transformer
    // function.
    WatchT<double> w = transform<double>(watches.add());

    watches.trigger(1);
    watches.trigger(-1);

    BOOST_CHECK_EQUAL(w.pop(), 1.0);
    BOOST_CHECK_EQUAL(w.pop(), -1.0);
}

BOOST_AUTO_TEST_CASE( test_transform_implicit2 )
{
    WatchesT<int, int> watches;

    // An int can be transformed implicitly into a double, so no transformer
    // function.
    auto w = transform<double, double>(watches.add());

    watches.trigger(1, 2);
    watches.trigger(-1, -2);

    tuple<double, double> val = w.pop();
    BOOST_CHECK_EQUAL(std::get<0>(val), 1.0);
    BOOST_CHECK_EQUAL(std::get<1>(val), 2.0);

    val = w.pop();

    BOOST_CHECK_EQUAL(std::get<0>(val), -1.0);
    BOOST_CHECK_EQUAL(std::get<1>(val), -2.0);
}

BOOST_AUTO_TEST_CASE( test_filtering )
{
    WatchesT<std::string> watches;

    // Trigger before multiplexing to make sure that there is no
    // deadlock
    auto filter = [] (const std::string & str)
        {
            return str.find("ll") != string::npos;
        };

    WatchT<std::string> w = watches.add(nullptr, filter);
    
    watches.trigger("hello");
    watches.trigger("world");

    BOOST_CHECK_EQUAL(w.pop(), "hello");

    BOOST_CHECK(!w.any());
}

BOOST_AUTO_TEST_CASE( test_free_filter )
{
    WatchesT<int> watches;

    WatchT<int> w = filter(watches.add(), [] (int i) { return i >= 0; });
    
    watches.trigger(1);
    watches.trigger(-1);

    BOOST_CHECK_EQUAL(w.pop(), 1);

    BOOST_CHECK(!w.any());
}

BOOST_AUTO_TEST_CASE( test_generic_watches_type_safety )
{
    WatchesT<string> watches1;

    Watches watches2(watches1.boundType());

    BOOST_CHECK_EQUAL(watches1.boundType(), watches2.boundType());

    {
        MLDB_TRACE_EXCEPTIONS(false);
        BOOST_CHECK_THROW(watches2.trigger<int>(3), std::exception);
    }

    Watch w = watches2.add();

    BOOST_CHECK_EQUAL(w.boundType(), watches1.boundType());

    auto bound = watches1.add();
    bound.bind([&] (const std::string & str) { watches2.trigger<std::string>(str); });

    WatchT<string> w2 = watches2.add();
    WatchT<int> w3;
    {
        MLDB_TRACE_EXCEPTIONS(false);
        BOOST_CHECK_THROW(w3 = watches2.add(), std::exception);
    }

    watches1.trigger("hello");

    BOOST_CHECK(w.any());

    {
        MLDB_TRACE_EXCEPTIONS(false);
        BOOST_CHECK_THROW(w.pop<int>(), std::exception);
    }

    BOOST_CHECK(w.any());

    BOOST_CHECK_EQUAL(w.pop<std::string>(), "hello");
    BOOST_CHECK_EQUAL(w2.pop(), "hello");

    {
        MLDB_TRACE_EXCEPTIONS(false);
        BOOST_CHECK_THROW(w3.pop(), std::exception);
    }
}

BOOST_AUTO_TEST_CASE( test_all )
{
    WatchesT<string> watches1;
    WatchesT<int> watches2;

    auto a = all(watches1.add(), watches2.add());

    BOOST_CHECK_EQUAL(watches1.size(), 1);
    BOOST_CHECK_EQUAL(watches2.size(), 1);

    BOOST_CHECK(a.attached());

    BOOST_CHECK(!a.any());

    watches1.trigger("hello");

    BOOST_CHECK(!a.any());

    watches2.trigger(1);

    BOOST_CHECK(a.any());

    string r1;
    int r2;

    std::tie(r1, r2) = unpackAll<string, int>(a.pop());

    BOOST_CHECK_EQUAL(r1, "hello");
    BOOST_CHECK_EQUAL(r2, 1);
}

BOOST_AUTO_TEST_CASE( test_any_encode_decode )
{
    Any a = string("hello");
    
    cerr << a.type().name() << endl;

    Json::Value v = jsonEncode(a);

    //cerr << v << endl;

    Any b = jsonDecode<Any>(v);

    BOOST_CHECK_EQUAL(a.as<string>(), b.convert<string>());
}

BOOST_AUTO_TEST_CASE( test_transport_watch_json )
{
    WatchesT<string, int> watches;

    auto w = watches.add();

    watches.trigger("hello", 42);

    Json::Value v = jsonEncode(w.popGeneric());

    //cerr << v << endl;

    Any b = jsonDecode<Any>(v);

    auto t = b.convert<std::tuple<string, int> >();

    BOOST_CHECK_EQUAL(std::get<0>(t), "hello");
    BOOST_CHECK_EQUAL(std::get<1>(t), 42);
}

BOOST_AUTO_TEST_CASE( test_info )
{
    WatchesT<string> watches;

    auto w1 = watches.add(string("w1"));
    auto ws = watches.active();
    BOOST_REQUIRE_EQUAL(ws.size(), 1);
    BOOST_CHECK_EQUAL(ws[0].first, &w1);
    BOOST_CHECK_EQUAL(ws[0].second.as<std::string>(), "w1");

    {
        auto w2 = watches.add(string("w2"));
        
        ws = watches.active();
        BOOST_REQUIRE_EQUAL(ws.size(), 2);
        BOOST_CHECK_EQUAL(ws[1].first, &w2);
        BOOST_CHECK_EQUAL(ws[1].second.as<std::string>(), "w2");
    }

    ws = watches.active();
    BOOST_REQUIRE_EQUAL(ws.size(), 1);
    BOOST_CHECK_EQUAL(ws[0].first, &w1);
    BOOST_CHECK_EQUAL(ws[0].second.as<std::string>(), "w1");
}

BOOST_AUTO_TEST_CASE( test_on_release )
{
    WatchesT<string> watches;

    vector<Any> releasedInfo;

    auto onRelease = [&] (Watch & toBeReleased, Any && info)
        {
            releasedInfo.emplace_back(std::move(info));
        };

    watches.onRelease = onRelease;

    {
        auto w = watches.add(string("w1"));
        BOOST_CHECK_EQUAL(releasedInfo.size(), 0);
    }

    BOOST_REQUIRE_EQUAL(releasedInfo.size(), 1);

    BOOST_CHECK_EQUAL(releasedInfo[0].as<std::string>(), "w1");
}

BOOST_AUTO_TEST_CASE(test_detach)
{
    WatchesT<string> watches;
    BOOST_CHECK_EQUAL(watches.size(), 0);

    {
        auto w = watches.add();

        BOOST_CHECK_EQUAL(watches.size(), 1);

        w.detach();

        BOOST_CHECK_EQUAL(watches.size(), 0);
    }
    
    BOOST_CHECK_EQUAL(watches.size(), 0);
}

BOOST_AUTO_TEST_CASE(test_transform_detach)
{
    bool released = false;

    WatchesT<int> watches;

    watches.onRelease = [&] (Watch & w, Any && info)
        {
            released = true;
        };

    WatchT<std::string> w
        = transform<std::string>(watches.add(),
                                 [] (int i) { return std::to_string(i); });
        
    BOOST_CHECK(w.attached());
    w.detach();
    BOOST_CHECK(!w.attached());
    BOOST_CHECK(released);
}

BOOST_AUTO_TEST_CASE(test_bind_non_recursive)
{
    WatchesT<int> watches;

    vector<int> firedRecursive;
    vector<int> firedNonRecursive;

    auto onFireRecursive = [&] (int i)
        {
            firedRecursive.push_back(i);
        };

    auto onFireNonRecursive = [&] (int i)
        {
            firedNonRecursive.push_back(i);
        };

    WatchT<int> w = watches.add();

    BOOST_CHECK_EQUAL(w.tryBindNonRecursive(onFireNonRecursive), true);
    w.unbind();

    watches.trigger(1);
    watches.trigger(2);
    watches.trigger(3);

    BOOST_CHECK_EQUAL(w.tryBindNonRecursive(onFireNonRecursive), false);

    w.bindNonRecursive(onFireNonRecursive, onFireRecursive);

    BOOST_CHECK_EQUAL(firedRecursive.size(), 3);
    BOOST_CHECK_EQUAL(firedNonRecursive.size(), 0);

    watches.trigger(4);

    BOOST_CHECK_EQUAL(firedRecursive.size(), 3);
    BOOST_CHECK_EQUAL(firedNonRecursive.size(), 1);
}

BOOST_AUTO_TEST_CASE(test_disable_from_fire)
{
    // Make sure the test bombs if there is a deadlock rather than hanging
    ML::Watchdog watchdog(2 /* seconds */);

    WatchesT<int> watches;

    auto w = watches.add();

    w.bind([&] (int) { w = WatchT<int>(); });

    watches.trigger(1);
}
