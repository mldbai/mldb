// This file is part of MLDB. Copyright 2015 mldb.ai inc. All rights reserved.

/* epoll_test.cc
   Wolfgang Sourdeau, 17 November 2014
   Copyright (c) 2014 mldb.ai inc.  All rights reserved.

   Assumption tests for epoll
*/

#define BOOST_TEST_MAIN
#define BOOST_TEST_DYN_LINK

#include <boost/test/unit_test.hpp>

#include "mldb/io/epoller.h"
#include "mldb/io/timerfd.h"
#include <sys/poll.h>

using namespace std;
using namespace MLDB;


BOOST_AUTO_TEST_CASE( test_basics )
{
    Epoller epoller;
    BOOST_CHECK_EQUAL(epoller.selectFd(), -1);
    epoller.init(16);
    BOOST_CHECK_NE(epoller.selectFd(), -1);
    BOOST_CHECK_EQUAL(epoller.poll(), false);
    BOOST_CHECK_EQUAL(epoller.processOne(), false);

    // Make sure we don't have anything if we poll on its fd
    pollfd fd[1] = { { epoller.selectFd(), POLLIN, 0 } };
    int res = ::poll(fd, 1, 1 /* microsecond */);
    BOOST_CHECK_EQUAL(res, 0);

    epoller.close();
}

BOOST_AUTO_TEST_CASE( test_add_fd )
{
    Epoller epoller;
    epoller.init(16);

    BOOST_CHECK_EQUAL(epoller.poll(), false);

    int fds[2];  // read, write
    int res = pipe(fds);
    BOOST_REQUIRE_EQUAL(res, 0);

    // Add the reader
    epoller.addFd(fds[0], EPOLL_INPUT);

    // Check we can't read anything
    BOOST_CHECK_EQUAL(epoller.poll(), false);
    BOOST_CHECK_EQUAL(epoller.handleEvents(1 /* microsecond */), 0);

    // Write something to the pipe
    char data[] = "hello";
    BOOST_REQUIRE_EQUAL(write(fds[1], data, sizeof(data)), sizeof(data));

    BOOST_CHECK_EQUAL(epoller.poll(), true);

    bool done = false;
    auto handleEvent = [&] (EpollEvent & event)
    {
        BOOST_REQUIRE(!done);
        done = true;

        BOOST_CHECK_EQUAL(getFd(event), fds[0]);
        BOOST_CHECK_EQUAL(hasInput(event), true);
        BOOST_CHECK_EQUAL(hasOutput(event), false);
        BOOST_CHECK_EQUAL(hasHangup(event), false);

        char buf[128];
        int res = read(fds[0], buf, 128);
        BOOST_CHECK_EQUAL(res, sizeof(data));

        return Epoller::DONE;
    };

    BOOST_CHECK_EQUAL(epoller.handleEvents(1000 /* microsecond */, -1 /* num events */, handleEvent), 1);
    BOOST_CHECK_EQUAL(done, true);
    BOOST_CHECK_EQUAL(epoller.handleEvents(1000 /* microsecond */, -1 /* num events */, handleEvent), 0);

    BOOST_CHECK_EQUAL(epoller.poll(), false);
}

BOOST_AUTO_TEST_CASE( test_nested_epoll )
{
    Epoller outer;
    outer.init(16);

    Epoller inner;
    inner.init(16);

    outer.addFd(inner.selectFd(), EPOLL_INPUT);
    BOOST_CHECK_EQUAL(outer.poll(), false);
    BOOST_CHECK_EQUAL(inner.poll(), false);

    int fds[2];  // read, write
    int res = pipe(fds);
    BOOST_REQUIRE_EQUAL(res, 0);

    // Add the reader
    inner.addFd(fds[0], EPOLL_INPUT);

    // Write something to the pipe
    char data[] = "hello";
    BOOST_REQUIRE_EQUAL(write(fds[1], data, sizeof(data)), sizeof(data));

    BOOST_CHECK_EQUAL(inner.poll(), true);
    BOOST_CHECK_EQUAL(outer.poll(), true);

    char buf[128];
    res = read(fds[0], buf, 128);
    BOOST_CHECK_EQUAL(res, sizeof(data));

    auto handleOuterEvent = [&] (EpollEvent & event)
    {
        BOOST_CHECK(false);
        return Epoller::DONE;
    };

    auto handleInnerEvent = [&] (EpollEvent & event)
    {
        BOOST_CHECK(false);
        return Epoller::DONE;
    };

    BOOST_CHECK_EQUAL(inner.handleEvents(1000 /* microsecond */, -1, handleOuterEvent), 0);
    BOOST_CHECK_EQUAL(outer.handleEvents(1000 /* microsecond */, -1, handleInnerEvent), 0);

    BOOST_CHECK_EQUAL(inner.poll(), false);
    BOOST_CHECK_EQUAL(outer.poll(), false);

}

BOOST_AUTO_TEST_CASE( test_nested_epoll_add_with_event )
{
    Epoller outer;
    outer.init(16);

    Epoller inner;
    inner.init(16);

    BOOST_CHECK_EQUAL(outer.poll(), false);
    BOOST_CHECK_EQUAL(inner.poll(), false);

    int fds[2];  // read, write
    int res = pipe(fds);
    BOOST_REQUIRE_EQUAL(res, 0);

    // Add the reader
    inner.addFd(fds[0], EPOLL_INPUT);

    // Write something to the pipe
    char data[] = "hello";
    BOOST_REQUIRE_EQUAL(write(fds[1], data, sizeof(data)), sizeof(data));

    BOOST_CHECK_EQUAL(inner.poll(), true);
    BOOST_CHECK_EQUAL(outer.poll(), false);

    outer.addFd(inner.selectFd(), EPOLL_INPUT);

    BOOST_CHECK_EQUAL(outer.poll(), true);

    char buf[128];
    res = read(fds[0], buf, 128);
    BOOST_CHECK_EQUAL(res, sizeof(data));

    auto handleOuterEvent = [&] (EpollEvent & event)
    {
        BOOST_CHECK(false);
        return Epoller::DONE;
    };

    auto handleInnerEvent = [&] (EpollEvent & event)
    {
        BOOST_CHECK(false);
        return Epoller::DONE;
    };

    BOOST_CHECK_EQUAL(inner.handleEvents(1000 /* microsecond */, -1, handleOuterEvent), 0);
    BOOST_CHECK_EQUAL(outer.handleEvents(1000 /* microsecond */, -1, handleInnerEvent), 0);

    BOOST_CHECK_EQUAL(inner.poll(), false);
    BOOST_CHECK_EQUAL(outer.poll(), false);
}

BOOST_AUTO_TEST_CASE( test_triply_nested_epoll )
{
    Epoller outer;
    outer.init(16);

    Epoller middle;
    middle.init(16);

    Epoller inner;
    inner.init(16);

    outer.addFd(middle.selectFd(), EPOLL_INPUT);
    middle.addFd(inner.selectFd(), EPOLL_INPUT);

    BOOST_CHECK_EQUAL(outer.poll(), false);
    BOOST_CHECK_EQUAL(middle.poll(), false);
    BOOST_CHECK_EQUAL(inner.poll(), false);

    int fds[2];  // read, write
    int res = pipe(fds);
    BOOST_REQUIRE_EQUAL(res, 0);

    // Add the reader
    inner.addFd(fds[0], EPOLL_INPUT);

    // Write something to the pipe
    char data[] = "hello";
    BOOST_REQUIRE_EQUAL(write(fds[1], data, sizeof(data)), sizeof(data));

    BOOST_CHECK_EQUAL(inner.poll(), true);
    BOOST_CHECK_EQUAL(middle.poll(), true);
    BOOST_CHECK_EQUAL(outer.poll(), true);

    char buf[128];
    res = read(fds[0], buf, 128);
    BOOST_CHECK_EQUAL(res, sizeof(data));

    auto handleOuterEvent = [&] (EpollEvent & event)
    {
        BOOST_CHECK(false);
        return Epoller::DONE;
    };

    auto handleMiddleEvent = [&] (EpollEvent & event)
    {
        BOOST_CHECK(false);
        return Epoller::DONE;
    };

    auto handleInnerEvent = [&] (EpollEvent & event)
    {
        BOOST_CHECK(false);
        return Epoller::DONE;
    };

    BOOST_CHECK_EQUAL(inner.handleEvents(1000 /* microsecond */, -1, handleOuterEvent), 0);
    BOOST_CHECK_EQUAL(middle.handleEvents(1000 /* microsecond */, -1, handleMiddleEvent), 0);
    BOOST_CHECK_EQUAL(outer.handleEvents(1000 /* microsecond */, -1, handleInnerEvent), 0);

    BOOST_CHECK_EQUAL(inner.poll(), false);
    BOOST_CHECK_EQUAL(middle.poll(), false);
    BOOST_CHECK_EQUAL(outer.poll(), false);
}


