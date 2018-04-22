/* sigsegv_test.cc
   Jeremy Barnes, 23 February 2010
   Copyright (c) 2010 Jeremy Barnes.  Public domain.
   This file is part of MLDB. Copyright 2018 mldb.ai inc. All rights reserved.


   Test of the segmentation fault handler functionality.
*/

#define BOOST_TEST_MAIN
#define BOOST_TEST_DYN_LINK

#include <boost/test/unit_test.hpp>
#include <iostream>
#include "mldb/arch/segv.h"
#include "mldb/arch/vm.h"
#include "mldb/jml/utils/string_functions.h"
#include <signal.h>

#include <sys/types.h>
#include <sys/stat.h>
#include <fcntl.h>
#include <errno.h>
#include <sys/mman.h>
#include <sys/wait.h>
#include "mldb/jml/utils/guard.h"
#include <fstream>
#include <vector>
#include <atomic>
#include <thread>


using namespace MLDB;
using namespace std;


void * mmap_addr = 0;

std::atomic<int> num_handled(0);

volatile siginfo_t handled_info;
volatile ucontext_t handled_context;

struct Barrier {
    Barrier(int ninit)
        : n(ninit)
    {
    }

    void wait()
    {
        --n;
        while (n.load() > 0);
    }
    
    std::atomic<int> n;
};

void test1_segv_handler(int signum, siginfo_t * info, void * context)
{
    cerr << "handled" << endl;

    ++num_handled;
    ucontext_t * ucontext = (ucontext_t *)context;

    (ucontext_t &)handled_context = *ucontext;
    (siginfo_t &)handled_info = *info;

    // Make the memory writeable
    int res = mprotect(mmap_addr, page_size, PROT_READ | PROT_WRITE);
    if (res == -1)
        cerr << "error in mprotect: " << strerror(errno) << endl;

    // Return to the trapping statement, which will perform the call
}

BOOST_AUTO_TEST_CASE ( test1_segv_restart )
{
    // Create a memory mapped page, read only
    mmap_addr = mmap(0, page_size, PROT_READ, MAP_PRIVATE | MAP_ANONYMOUS,
                       -1, 0);

    BOOST_REQUIRE(mmap_addr != MAP_FAILED);

    // Install a segv handler
    struct sigaction action;
    action.sa_sigaction = test1_segv_handler;
    action.sa_flags = SA_SIGINFO | SA_RESETHAND;

    int res = sigaction(SIGSEGV, &action, 0);
    
    BOOST_REQUIRE_EQUAL(res, 0);

    char * mem = (char *)mmap_addr;
    BOOST_CHECK_EQUAL(*mem, 0);

    cerr << "before handler" << endl;
    cerr << "addr = " << mmap_addr << endl;

    // write to the memory address; this will cause a SEGV
 dowrite:
    *mem = 'x';

    cerr << "after handler" << endl;

    void * x = &&dowrite;
    cerr << "x = " << x << endl;

    cerr << "signal info:" << endl;
    cerr << "  errno:   " << strerror(handled_info.si_errno) << endl;
    cerr << "  code:    " << handled_info.si_code << endl;
    cerr << "  si_addr: " << handled_info.si_addr << endl;
    cerr << "  status:  " << strerror(handled_info.si_status) << endl;
    cerr << "  RIP:     " << format("%12p", (void *)handled_context.uc_mcontext.gregs[16]) << endl;

    // Check that it was handled properly
    BOOST_CHECK_EQUAL(*mem, 'x');
    BOOST_CHECK_EQUAL(num_handled, 1);
}

void test2_segv_handler_thread(char * addr)
{
    *addr = 'x';
}

BOOST_AUTO_TEST_CASE ( test2_segv_handler )
{
    // Create a memory mapped page, read only
    void * vaddr = mmap(0, page_size, PROT_READ, MAP_PRIVATE | MAP_ANONYMOUS,
                       -1, 0);
    
    char * addr = (char *)vaddr;

    BOOST_REQUIRE(addr != MAP_FAILED);

    installSegvHandler();

    int region = registerSegvRegion(addr, addr + page_size);

    int nthreads = 1;

    std::vector<std::thread> tg;
    for (unsigned i = 0;  i < nthreads;  ++i)
        tg.emplace_back(std::bind(&test2_segv_handler_thread, addr));

    sleep(1);

    int res = mprotect(vaddr, page_size, PROT_READ | PROT_WRITE);

    BOOST_CHECK_EQUAL(res, 0);

    unregisterSegvRegion(region);

    for (auto & t: tg)
        t.join();

    BOOST_CHECK_EQUAL(*addr, 'x');

    BOOST_CHECK_EQUAL(getNumSegvFaultsHandled(), 1);
}

// Thread to continually modify the memory
void test2_segv_handler_stress_thread1(int * addr,
                                       int npages,
                                       Barrier & barrier,
                                       std::atomic<bool> & finished)
{
    barrier.wait();

    cerr << "m";

    int * end = addr + npages * page_size / sizeof(int);

    while (!finished) {
        for (int * p = addr;  p != end;  ++p)
            *(std::atomic<int> *)(p) += 1;
        std::atomic_thread_fence(std::memory_order_seq_cst);
    }

    cerr << "M";
}

// Thread to continually unmap and remap the memory
void test2_segv_handler_stress_thread2(int * addr,
                                       Barrier & barrier,
                                       std::atomic<bool> & finished)
{
    barrier.wait();

    cerr << "p";

    while (!finished) {
        int region = registerSegvRegion(addr, addr + page_size);
        int res = mprotect(addr, page_size, PROT_READ);
        if (res == -1) {
            cerr << "mprotect(PROT_READ) returned " << strerror(errno)
                 << endl;
            abort();
        }
        res = mprotect(addr, page_size, PROT_READ | PROT_WRITE);
        if (res == -1) {
            cerr << "mprotect(PROT_WRITE) returned " << strerror(errno)
                 << endl;
            abort();
        }

        unregisterSegvRegion(region);
    }

    cerr << "P";
}

BOOST_AUTO_TEST_CASE ( test2_segv_handler_stress )
{
    int npages = 128;

    // Create a memory mapped page, read only
    void * vaddr = mmap(0, npages * page_size, PROT_WRITE | PROT_READ,
                        MAP_PRIVATE | MAP_ANONYMOUS,
                       -1, 0);
    
    int * addr = (int *)vaddr;

    BOOST_REQUIRE(addr != MAP_FAILED);

    installSegvHandler();

    // 8 threads simultaneously causing faults, with 8 threads writing to
    // pages

    int nthreads = 8;

    std::atomic<bool> finished(false);

    Barrier barrier(nthreads + npages);

    std::vector<std::thread> tg;
    for (unsigned i = 0;  i < nthreads;  ++i)
        tg.emplace_back(std::bind(&test2_segv_handler_stress_thread1,
                                  addr, npages, std::ref(barrier),
                                  std::ref(finished)));
    
    for (unsigned i = 0;  i < npages;  ++i)
        tg.emplace_back(std::bind(&test2_segv_handler_stress_thread2,
                                  addr + i * page_size / sizeof(*addr),
                                  std::ref(barrier),
                                  std::ref(finished)));

    sleep(2);

    finished = true;

    for (auto & t: tg)
        t.join();

    cerr << endl;

    // All values in all of the pages should be the same value
    int val = *addr;

    cerr << "val = " << val << endl;
    
    for (unsigned i = 0;  i < npages;  ++i)
        for (unsigned j = 0;  j < page_size / sizeof(int);  ++j)
            BOOST_CHECK_EQUAL(addr[i * page_size / sizeof(int) + j], val);

    cerr << getNumSegvFaultsHandled() << " segv faults handled" << endl;

    BOOST_CHECK(getNumSegvFaultsHandled() > 1);
}

BOOST_AUTO_TEST_CASE ( test3_normal_segv_still_works )
{
    // Don't make boost::test think that processes exiting is a problem
    signal(SIGCHLD, SIG_DFL);

    pid_t pid = fork();

    if (pid == 0) {
        installSegvHandler();

        *(char *)0 = 12;

        raise(SIGKILL);
    }

    // Give it one second to exit with a SIGSEGV
    sleep(1);

    // Force it to exit anyway with a SIGKILL
    kill(pid, SIGKILL);

    int status = -1;
    pid_t res = waitpid(pid, &status, 0);

    // If it exited properly with a SIGSEGV, then we'll get that in the status.
    // If it didn't exit, then we'll get a SIGKILL in the status instead.

    BOOST_CHECK_EQUAL(res, pid);
    BOOST_CHECK(WIFSIGNALED(status));
    BOOST_CHECK_EQUAL(WTERMSIG(status), SIGSEGV);
}
