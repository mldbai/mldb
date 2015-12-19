/** thread_pool_impl.h                                             -*- C++ -*-
    Jeremy Barnes, 18 December 2015
    Copyright (c) 2015 Datacratic Inc.  All rights reserved.

    This file is part of MLDB. Copyright 2015 Datacratic. All rights reserved.

    Implementation of the thread pool abstraction for when work needs to be
    spread over multiple threads.
*/

#pragma once

#include "thread_pool.h"
#include <atomic>
#include <vector>

namespace Datacratic {

template<typename Item>
struct ThreadQueue {
    static constexpr unsigned int NUMBER_OF_JOBS = 4096u;
    static constexpr unsigned int MASK = NUMBER_OF_JOBS - 1u;

    ThreadQueue()
        : bottom(0), top(0)
    {
        for (auto & i: items)
            i = nullptr;
    }
    
    std::atomic<long long> bottom, top;
    std::atomic<Item *> items[NUMBER_OF_JOBS];
    
    bool push(Item & item)
    {
        long long t = top.load(std::memory_order_consume);
        long long b = bottom;
        if (b - t == NUMBER_OF_JOBS)
            return false;

        // Switch the new item in
        items[b & MASK] = new Item(std::move(item));
        
        // ensure the item is written before b+1 is published to other threads.
        // on x86/64, a compiler barrier is enough.
        bottom.store(b+1, std::memory_order_release);

        return true;
    }

    bool steal(Item & item)
    {
        long long t = top.load(std::memory_order_consume);
 
        // ensure that top is always read before bottom.
        long long b = bottom.load(std::memory_order_acquire);
        if (t >= b)
            return nullptr;  // no work to be taken

        // non-empty queue
        Item* ptr = items[t & MASK];
 
        // the interlocked function serves as a compiler barrier, and guarantees that the read happens before the CAS.
        if (!top.compare_exchange_strong(t, t + 1)) {
            // a concurrent steal or pop operation removed an element from the deque in the meantime.
            return false;
        }
        
        item = std::move(*ptr);

        delete ptr;
        
        return true;
    }

    bool pop(Item & item)
    {
        long long b = bottom - 1;
        bottom.exchange(b);
 
        long long t = top.load(std::memory_order_acquire);

        if (t <= b) {
            // non-empty queue
            Item* ptr = items[b & MASK];
            if (t != b) {
                // there's still more than one item left in the queue
                item = std::move(*ptr);
                return true;
            }
 
            // this is the last item in the queue
            if (!top.compare_exchange_strong(t, t+1)) {
                // failed race against steal operation
                return false;
            }
            
            bottom = t+1;
            item = std::move(*ptr);
            delete ptr;
            return true;
        }
        else {
            // deque was already empty
            bottom = t;
            return false;
        }
    }
};

} // namespace Datacratic

