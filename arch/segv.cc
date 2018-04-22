/* sigsegv.cc
   Jeremy Barnes, 24 February 2010.
   Copyright (c) 2010 Jeremy Barnes.  Public domain.
   Copyright (c) 2018 Element AI Inc.  All rights reserved.
   This file is part of MLDB. Copyright 2016 mldb.ai inc. All rights reserved.
    
   Segmentation fault handlers.
*/

#include "segv.h"
#include <mutex>
#include "spinlock.h"
#include <signal.h>
#include "exception.h"
#include <iostream>


using namespace std;

extern "C" {
    void doRunHandler();
    void runHandler();
};

//asm (".globl doRunHandler\n.globl runHandler\ndoRunHandler:\tcall runHandler\n");

void runHandler()
{
    cerr << "runHandler" << endl;
    throw MLDB::Exception("hello");
}


namespace MLDB {

namespace {

struct SegvDescriptor {
    SegvDescriptor()
        : active(false), ref(0), start(nullptr), end(nullptr)
    {
    }

    bool matches(const void * addr) const
    {
        if (!active || ref == 0)
            return false;
        const char * addr2 = (const char *)addr;

        return addr2 >= start && addr2 < end;
    }

    std::atomic<bool> active;
    std::atomic<int> ref;
    const char * start;
    const char * end;
    std::function<bool (const void * addr)> handler;
};


enum { NUM_SEGV_DESCRIPTORS = 256 };

SegvDescriptor SEGV_DESCRIPTORS[NUM_SEGV_DESCRIPTORS];

Spinlock segvLock(false /* don't yield in spinlock; sched_yield() not valid in signal handlers */);

// Number that increments each time a change is made to the segv regions
static std::atomic<uint64_t> segvEpoch(0);

static std::atomic<unsigned long long> numFaultsHandled(0);

} // file scope

unsigned long long getNumSegvFaultsHandled()
{
    return numFaultsHandled;
}

int registerSegvRegion(const void * start, const void * end,
                       std::function<bool (const void *)> handler)
{
    std::unique_lock<Spinlock> guard(segvLock);
    
    // Busy wait until we get one
    int idx = -1;
    while (idx == -1) {
        for (unsigned i = 0;  i < NUM_SEGV_DESCRIPTORS && idx == -1;  ++i)
            if (SEGV_DESCRIPTORS[i].ref == 0) idx = i;
        
        if (idx == -1) sched_yield();
    }

    SegvDescriptor & descriptor = SEGV_DESCRIPTORS[idx];
    descriptor.start  = (const char *)start;
    descriptor.end    = (const char *)end;
    descriptor.handler= std::move(handler);

    std::atomic_thread_fence(std::memory_order_seq_cst);
    
    descriptor.active = true;

    ++segvEpoch;

    std::atomic_thread_fence(std::memory_order_seq_cst);
    
    descriptor.ref = 1;

    return idx;
}

void unregisterSegvRegion(int region)
{
    std::unique_lock<Spinlock> guard(segvLock);

    if (region < 0 || region >= NUM_SEGV_DESCRIPTORS)
        throw Exception("unregister_segv_region(): invalid region");

    SegvDescriptor & descriptor = SEGV_DESCRIPTORS[region];
    
    if (descriptor.ref == 0 || !descriptor.active)
        throw Exception("segv region is not active");
    
    descriptor.active = false;
    descriptor.start = 0;
    descriptor.end = 0;

    ++segvEpoch;

    std::atomic_thread_fence(std::memory_order_seq_cst);

    descriptor.ref -= 1;
}

// Race condition:
// If the region was removed between when the signal happened and when the
// lock is obtained, then region will be -1.
//
// There are two possibilities when that happens:
//
// 1.  It was a real segfault, which should be turned into a real signal, or
// 2.  The descriptor was removed due to this race condition
// In this case, we need to retry *one* time

struct ThreadSignalInfo {
    bool in_retry;
    uint64_t old_epoch;
    void * old_ip;
    void * old_addr;
};

__thread ThreadSignalInfo threadSignalInfo = { 0, 0, 0, 0 };

void defaultHandleSignal(int signum)
{
    struct sigaction action;
    action.sa_handler = SIG_DFL;
    action.sa_flags = 0;
    sigaction(signum, &action, 0);
    raise(signum);
}

void segvHandler(int signum, siginfo_t * info, void * context)
{
    if (signum != SIGSEGV
        || info->si_code != SEGV_ACCERR) {
        defaultHandleSignal(signum);
        return;
    }
    // We could do various things here to filter out the signal

    ucontext_t * ucontext = (ucontext_t *)context;

    const char * addr = (const char *)info->si_addr;

    int region = -1;
    {
        std::unique_lock<Spinlock> guard(segvLock);

        for (unsigned i = 0;  i < NUM_SEGV_DESCRIPTORS && region == -1;  ++i) {
            if (SEGV_DESCRIPTORS[i].matches(addr)) {
                region = i;
                threadSignalInfo.in_retry = false;

                if (SEGV_DESCRIPTORS[i].handler) {
                    SEGV_DESCRIPTORS[i].ref += 1;
                    numFaultsHandled += 1;
                    cerr << "setting up return ip to handler"
                         << endl;
                    ucontext->uc_mcontext.gregs[16] = (ptrdiff_t)&runHandler;
                    return;
                    
                    if (!SEGV_DESCRIPTORS[i].handler(addr)) {
                        return;
                    }
                }
                SEGV_DESCRIPTORS[i].ref += 1;
                break;
            }
        }
    }

    if (region == -1) {
        // Not found.  We need to determine if it's because of a real segfault
        // or due to a race between the memory access and the region
        // disappearing.

        void * sig_addr = info->si_addr;

        // TODO: not very elegant; find a way to get this programatically
        void * sig_ip   = (void *)ucontext->uc_mcontext.gregs[16];

        // Is it exactly the same instruction reading exactly the same
        // address without any intervening changes in the descriptors?
        if (threadSignalInfo.in_retry
            && threadSignalInfo.old_epoch == segvEpoch
            && threadSignalInfo.old_ip == sig_ip
            && threadSignalInfo.old_addr == sig_addr) {

            // Must be a real sigsegv; re-raise it with the default handler
            threadSignalInfo.in_retry = false;

            defaultHandleSignal(signum);
        }

        threadSignalInfo.in_retry = true;
        threadSignalInfo.old_epoch = segvEpoch;
        threadSignalInfo.old_ip = sig_ip;
        threadSignalInfo.old_addr = sig_addr;

        // Now return, to retry it
        return;
    }

    SegvDescriptor & descriptor = SEGV_DESCRIPTORS[region];

    // busy wait for it to become inactive
    //timespec zero_point_one_ms = {0, 100000};

    // TODO: should we call nanosleep in a signal handler?
    // NOTE: doesn't do the right thing in linux 2.4; small nanosleeps are busy
    // waits

    while (descriptor.active) {
        //nanosleep(&zero_point_one_ms, 0);
    }

    descriptor.ref -= 1;
    numFaultsHandled += 1;
}

static bool isSegvHandlerInstalled()
{
    struct sigaction action;

    int res = sigaction(SIGSEGV, 0, &action);
    if (res == -1)
        throw Exception(errno, "isSegvHandlerInstalled()", "sigaction");

    return action.sa_sigaction == segvHandler;
}

void installSegvHandler()
{
    if (isSegvHandlerInstalled())
        return;

    // Install a segv handler
    struct sigaction action;

    action.sa_sigaction = segvHandler;
    action.sa_flags = SA_SIGINFO;

    int res = sigaction(SIGSEGV, &action, 0);

    if (res == -1)
        throw Exception(errno, "installSegvHandler()", "sigaction");
}


} // namespace MLDB
