/* gc_lock_test_common.h                                           -*- C++ -*-
   Jeremy Barnes, 23 February 2010
   Copyright (c) 2010 mldb.ai inc.  All rights reserved.

   This file is part of MLDB. Copyright 2015 mldb.ai inc. All rights reserved.
*/

#include "mldb/arch/gc_lock.h"
#include <vector>
#include <functional>
#include <thread>
#include <atomic>
#include <iostream>
#include <chrono>
#include "mldb/arch/tick_counter.h"
#include "mldb/arch/spinlock.h"

#pragma once

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

#define USE_MALLOC 1

template<typename T>
struct Allocator {
    Allocator(int nblocks, T def = T())
        : def(def)
    {
        init(nblocks);
        highestAlloc = nallocs = ndeallocs = 0;
    }

    ~Allocator()
    {
#if ( ! USE_MALLOC )
        delete[] blocks;
        delete[] free;
#endif
    }

    T def;
    T * blocks;
    int * free;
    int nfree;
    std::atomic<int> highestAlloc;
    std::atomic<int> nallocs;
    std::atomic<int> ndeallocs;
    MLDB::Spinlock lock;

    void init(int nblocks)
    {
#if ( ! USE_MALLOC )
        blocks = new T[nblocks];
        free = new int[nblocks];

        std::fill(blocks, blocks + nblocks, def);

        nfree = 0;
        for (int i = nblocks - 1;  i >= 0;  --i)
            free[nfree++] = i;
#endif
    }

    T * alloc()
    {
#if USE_MALLOC
        nallocs += 1;

        // Atomic max operation
        {
            int current = highestAlloc;
            for (;;) {
                auto newValue = std::max(current,
                                         nallocs.load() - ndeallocs.load());
                if (newValue == current)
                    break;
                if (highestAlloc.compare_exchange_weak(current, newValue))
                    break;
            }
        }

        return new T(def);
#else
        std::lock_guard<MLDB::Spinlock> guard(lock);
        if (nfree == 0)
            throw MLDB::Exception("none free");
        int i = free[nfree - 1];
        highestAlloc = std::max(highestAlloc, i);
        T * result = blocks + i;
        --nfree;
        ++nallocs;
        return result;
#endif
    }

    void dealloc(T * value)
    {
        if (!value) return;
        *value = def;
#if USE_MALLOC
        delete value;
        ndeallocs += 1;
        return;
#else
        std::lock_guard<MLDB::Spinlock> guard(lock);
        int i = value - blocks;
        free[nfree++] = i;
        ++ndeallocs;
#endif
    }

    static void doDealloc(void * thisptr, void * blockPtr_, void * blockVar_)
    {
        int * & blockVar = *reinterpret_cast<int **>(blockVar_);
        int * blockPtr = reinterpret_cast<int *>(blockPtr_);
        ExcAssertNotEqual(blockVar, blockPtr);
        //blockVar = 0;
        //std::atomic_thread_fence(std::memory_order_seq_cst);
        //cerr << "blockPtr = " << blockPtr << endl;
        //int * blockPtr = reinterpret_cast<int *>(block);
        reinterpret_cast<Allocator *>(thisptr)->dealloc(blockPtr);
    }

    static void doDeallocAll(void * thisptr, void * blocksPtr_, void * numBlocks_)
    {
        size_t numBlocks = reinterpret_cast<size_t>(numBlocks_);
        int ** blocksPtr = reinterpret_cast<int **>(blocksPtr_);
        Allocator * alloc = reinterpret_cast<Allocator *>(thisptr);

        for (unsigned i = 0;  i != numBlocks;  ++i) {
            if (blocksPtr[i])
                alloc->dealloc(blocksPtr[i]);
        }

        delete[] blocksPtr;
    }
};

struct BlockHolder {
    BlockHolder(int ** p = nullptr)
        : block(p)
    {
    }
    
    BlockHolder & operator = (const BlockHolder & other)
    {
        block = other.block.load();
        return *this;
    }

    std::atomic<int **> block;

    int ** load() const { return block.load(); }

    operator int ** const () { return load(); }
};

template<typename Lock>
struct TestBase {
    TestBase(int nthreads, int nblocks, int nSpinThreads = 0)
        : finished(false),
          nthreads(nthreads),
          nblocks(nblocks),
          nSpinThreads(nSpinThreads),
          allocator(1024 * 1024, -1),
          nerrors(0),
          allBlocks(nthreads)
    {
        for (unsigned i = 0;  i < nthreads;  ++i) {
            allBlocks[i] = new int *[nblocks];
            std::fill(allBlocks[i].load(), allBlocks[i].load() + nblocks, (int *)0);
        }
    }

    ~TestBase()
    {
        for (unsigned i = 0;  i < nthreads;  ++i)
            delete[] allBlocks[i].load();
    }

    std::atomic<bool> finished;
    std::atomic<int> nthreads;
    int nblocks;
    std::atomic<int> nSpinThreads;
    Allocator<int> allocator;
    Lock gc;
    uint64_t nerrors;

    /* All of the blocks are published here.  Any pointer which is read from
       here by another thread should always refer to exactly the same
       value.
    */
    std::vector<BlockHolder> allBlocks;

    void checkVisible(int threadNum, unsigned long long start)
    {
        using namespace std;
        // We're reading from someone else's pointers, so we need to lock here
        //gc.enterCS();
        gc.lockShared();

        for (unsigned i = 0;  i < nthreads;  ++i) {
            for (unsigned j = 0;  j < nblocks;  ++j) {
                //int * val = allBlocks[i][j];
                int * val = allBlocks[i].load()[j];
                if (val) {
                    int atVal = *val;
                    if (atVal != i) {
                        cerr << MLDB::format("%.6f thread %d: invalid value read "
                                "from thread %d block %d: %d\n",
                                           (MLDB::ticks() - start)
                                             / MLDB::ticks_per_second, threadNum,
                                i, j, atVal);
                        nerrors += 1;
                        //abort();
                    }
                }
            }
        }

        //gc.exitCS();
        gc.unlockShared();
    }

    void doReadThread(int threadNum)
    {
        gc.getEntry();
        unsigned long long start = MLDB::ticks();
        while (!finished) {
            checkVisible(threadNum, start);
        }
    }

    void doSpinThread()
    {
        while (!finished) {
        }
    }

    void allocThreadDefer(int threadNum)
    {
        using namespace std;
        gc.getEntry();
        try {
            uint64_t nErrors = 0;

            int ** blocks = allBlocks[threadNum];

            while (!finished) {

                int ** oldBlocks = new int * [nblocks];

                //gc.enterCS();

                for (unsigned i = 0;  i < nblocks;  ++i) {
                    int * block = allocator.alloc();
                    if (*block != -1) {
                        cerr << "old block was allocated" << endl;
                        ++nErrors;
                    }
                    *block = threadNum;
                    std::atomic_thread_fence(std::memory_order_seq_cst);
                    //rcu_set_pointer_sym((void **)&blocks[i], block);
                    int * oldBlock = blocks[i];
                    blocks[i] = block;
                    std::atomic_thread_fence(std::memory_order_seq_cst);
                    oldBlocks[i] = oldBlock;
                }

                gc.defer(Allocator<int>::doDeallocAll, &allocator, oldBlocks,
                         (void *)(size_t)nblocks);

                //gc.exitCS();
            }


            int * oldBlocks[nblocks];

            for (unsigned i = 0;  i < nblocks;  ++i) {
                oldBlocks[i] = blocks[i];
                blocks[i] = 0;
            }

            gc.visibleBarrier();

            //cerr << "at end" << endl;

            for (unsigned i = 0;  i < nblocks;  ++i)
                allocator.dealloc(oldBlocks[i]);

            //cerr << "nErrors = " << nErrors << endl;
        } catch (...) {
            static MLDB::Spinlock lock;
            lock.acquire();
            //cerr << "threadnum " << threadNum << " inEpoch "
            //     << gc.getEntry().inEpoch << endl;
            gc.dump();
            abort();
        }
    }

    void allocThreadSync(int threadNum)
    {
        using namespace std;
        gc.getEntry();
        try {
            uint64_t nErrors = 0;

            int ** blocks = allBlocks[threadNum];
            int * oldBlocks[nblocks];

            while (!finished) {

                for (unsigned i = 0;  i < nblocks;  ++i) {
                    int * block = allocator.alloc();
                    if (*block != -1) {
                        cerr << "old block was allocated" << endl;
                        ++nErrors;
                    }
                    *block = threadNum;
                    int * oldBlock = blocks[i];
                    blocks[i] = block;
                    oldBlocks[i] = oldBlock;
                }

                std::atomic_thread_fence(std::memory_order_seq_cst);
                gc.visibleBarrier();

                for (unsigned i = 0;  i < nblocks;  ++i)
                    if (oldBlocks[i]) *oldBlocks[i] = 1234;

                for (unsigned i = 0;  i < nblocks;  ++i)
                    if (oldBlocks[i]) allocator.dealloc(oldBlocks[i]);
            }

            for (unsigned i = 0;  i < nblocks;  ++i) {
                oldBlocks[i] = blocks[i];
                blocks[i] = 0;
            }

            gc.visibleBarrier();

            for (unsigned i = 0;  i < nblocks;  ++i)
                allocator.dealloc(oldBlocks[i]);

            //cerr << "nErrors = " << nErrors << endl;
        } catch (...) {
            static MLDB::Spinlock lock;
            lock.acquire();
            //cerr << "threadnum " << threadNum << " inEpoch "
            //     << gc.getEntry().inEpoch << endl;
            gc.dump();
            abort();
        }
    }

    void run(std::function<void (int)> allocFn,
             int runTime = 1)
    {
        using namespace std;
        gc.getEntry();
        ThreadGroup tg;

        for (unsigned i = 0;  i < nthreads;  ++i)
            tg.create_thread(std::bind<void>(&TestBase::doReadThread, this, i));

        for (unsigned i = 0;  i < nthreads;  ++i)
            tg.create_thread(std::bind<void>(allocFn, i));

        for (unsigned i = 0;  i < nSpinThreads;  ++i)
            tg.create_thread(std::bind<void>(&TestBase::doSpinThread, this));

        std::this_thread::sleep_for(std::chrono::seconds(runTime));

        finished = true;

        tg.join_all();

        gc.deferBarrier();

        gc.dump();

        cerr << "allocs " << allocator.nallocs
             << " deallocs " << allocator.ndeallocs << endl;
        cerr << "highest " << allocator.highestAlloc << endl;

        cerr << "gc.currentEpoch() = " << gc.currentEpoch() << endl;

        ExcAssertEqual(allocator.nallocs, allocator.ndeallocs);
        ExcAssertEqual(nerrors, 0);
    }
};

