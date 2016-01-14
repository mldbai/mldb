// This file is part of MLDB. Copyright 2015 Datacratic. All rights reserved.

/* atomic_ops_test.cc
   Jeremy Barnes, 21 December 2009
   Copyright (c) 2009 Jeremy Barnes.  All rights reserved.

   Test of the bit operations class.
*/

#define BOOST_TEST_MAIN
#define BOOST_TEST_DYN_LINK

#include "mldb/arch/atomic_ops.h"
#include "mldb/arch/demangle.h"
#include "mldb/arch/exception.h"
#include "mldb/arch/tick_counter.h"

#include <boost/test/unit_test.hpp>
#include <vector>
#include <stdint.h>
#include <iostream>
#include <stdarg.h>
#include <errno.h>
#include <thread>
#include <mutex>
#include <condition_variable>


using namespace ML;
using namespace std;

using boost::unit_test::test_suite;

struct ThreadGroup {
    void create_thread(std::function<void ()> fn)
    {
        threads.emplace_back(std::move(fn));
    }

    void join_all()
    {
        for (auto & t: threads)
            t.join();
        threads.clear();
    }
    std::vector<std::thread> threads;
};

// http://stackoverflow.com/questions/24465533/implementing-boostbarrier-in-c11
class Barrier
{
private:
    std::mutex _mutex;
    std::condition_variable _cv;
    std::size_t _count;
public:
    explicit Barrier(std::size_t count) : _count{count} { }
    void wait()
    {
        std::unique_lock<std::mutex> lock{_mutex};
        if (--_count == 0) {
            _cv.notify_all();
        } else {
            _cv.wait(lock, [this] { return _count == 0; });
        }
    }
};

template<class X>
void test1_type()
{
    cerr << "testing type " << demangle(typeid(X).name()) << endl;
    
    X x = 0;
    BOOST_CHECK_EQUAL(x, 0);
    atomic_add(x, 1);
    BOOST_CHECK_EQUAL(x, 1);
    atomic_add(x, -1);
    BOOST_CHECK_EQUAL(x, 0);
}
 
BOOST_AUTO_TEST_CASE( test1 )
{
    test1_type<int>();
    test1_type<uint8_t>();
    test1_type<int8_t>();
    test1_type<uint16_t>();
    test1_type<int16_t>();
    test1_type<uint32_t>();
    test1_type<int32_t>();
    test1_type<uint64_t>();
    test1_type<int64_t>();
}

template<class X>
struct test_atomic_add2_thread {
    test_atomic_add2_thread(Barrier & barrier, X & val, int iter, int tnum)
        : barrier(barrier), val(val), iter(iter), tnum(tnum)
    {
    }

    Barrier & barrier;
    X & val;
    int iter;
    int tnum;

    void operator () ()
    {
        //cerr << "thread " << tnum << " waiting" << endl;

        barrier.wait();
        
        //cerr << "started thread" << tnum << endl;
        
        for (unsigned i = 0;  i < iter;  ++i)
            atomic_add(val, 1);

        //cerr << "finished thread" << tnum << endl;
    }
};

template<class X>
void test_atomic_add2_type()
{
    cerr << "testing type " << demangle(typeid(X).name()) << endl;
    int nthreads = 8, iter = 1000000;
    Barrier barrier(nthreads);
    X val = 0;
    ThreadGroup tg;

    for (unsigned i = 0;  i < nthreads;  ++i)
        tg.create_thread(test_atomic_add2_thread<X>(barrier, val, iter, i));

    tg.join_all();

    BOOST_CHECK_EQUAL(val, (iter * nthreads) & (X)-1);
}

BOOST_AUTO_TEST_CASE( test_atomic_add2 )
{
    cerr << "atomic add" << endl;
    test_atomic_add2_type<uint8_t>();
    test_atomic_add2_type<uint16_t>();
    test_atomic_add2_type<uint32_t>();
    test_atomic_add2_type<uint64_t>();
}

template<class X>
struct test_atomic_max_thread {
    test_atomic_max_thread(Barrier & barrier, X & val, int iter,
                           int tnum, size_t & num_errors)
        : barrier(barrier), val(val), iter(iter), tnum(tnum),
          num_errors(num_errors)
    {
    }

    Barrier & barrier;
    X & val;
    int iter;
    int tnum;
    size_t num_errors;

    void operator () ()
    {
        for (unsigned i = 0;  i < iter;  ++i) {
            atomic_max(val, i);
            if (val < i)
                atomic_add(num_errors, 1);
        }
    }
};

template<class X>
void test_atomic_max_type()
{
    cerr << "testing type " << demangle(typeid(X).name()) << endl;
    int nthreads = 8, iter = 1000000;
    X iter2 = (X)-1;
    if (iter2 < iter) iter = iter2;
    Barrier barrier(nthreads);
    X val = 0;
    ThreadGroup tg;
    size_t num_errors = 0;
    for (unsigned i = 0;  i < nthreads;  ++i)
        tg.create_thread(test_atomic_max_thread<X>(barrier, val, iter, i,
                                                   num_errors));

    tg.join_all();

    BOOST_CHECK_EQUAL(num_errors, 0);
}

BOOST_AUTO_TEST_CASE( test_atomic_max )
{
    cerr << "atomic max" << endl;
    test_atomic_max_type<uint8_t>();
    test_atomic_max_type<uint16_t>();
    test_atomic_max_type<uint32_t>();
    test_atomic_max_type<uint64_t>();
}

BOOST_AUTO_TEST_CASE( test_atomic_set_bits )
{
    cerr << "atomic set bits" << endl;

    int i = 0;
    BOOST_CHECK_EQUAL(i, 0);
    atomic_set_bits(i, 1);
    BOOST_CHECK_EQUAL(i, 1);
    atomic_clear_bits(i, 1);
    BOOST_CHECK_EQUAL(i, 0);
}

BOOST_AUTO_TEST_CASE( test_atomic_test_and_set )
{
    cerr << "atomic test and set" << endl;

    int i = 0;
    BOOST_CHECK_EQUAL(i, 0);
    BOOST_CHECK_EQUAL(atomic_test_and_set(i, 1), 0);
    BOOST_CHECK_EQUAL(i, 2);
    BOOST_CHECK_EQUAL(atomic_test_and_clear(i, 1), 1);
    BOOST_CHECK_EQUAL(i, 0);
}
