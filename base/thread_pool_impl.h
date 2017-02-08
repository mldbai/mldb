/** thread_pool_impl.h                                             -*- C++ -*-
    Jeremy Barnes, 18 December 2015
    Copyright (c) 2015 mldb.ai inc.  All rights reserved.

    This file is part of MLDB. Copyright 2015 mldb.ai inc. All rights reserved.

    Implementation of the thread pool abstraction for when work needs to be
    spread over multiple threads.
*/

#pragma once

#include "thread_pool.h"
#include "mldb/base/exc_assert.h"
#include <atomic>
#include <vector>

namespace MLDB {

template<typename Item>
struct ThreadQueue {
    static constexpr unsigned int NUMBER_OF_ITEMS = 4096u;
    static constexpr unsigned int MASK = NUMBER_OF_ITEMS - 1u;

    ThreadQueue(bool initialize = false)
        : bottom_(0), top_(0), num_queued_(0)
    {
        if (initialize)
            for (auto & i: items)
                i.store(nullptr, std::memory_order_relaxed);
    }
    
    std::atomic_uint_fast32_t bottom_, top_;
    std::atomic<Item *> items[NUMBER_OF_ITEMS];
    std::atomic_uint_fast32_t num_queued_;

    Item * push(Item * item)
    {
        ExcAssert(item);
        
        if (num_queued_ == NUMBER_OF_ITEMS)
            return item;

        uint_fast32_t b = bottom_.load(std::memory_order_relaxed);

        // Switch the new item in
        items[b & MASK] = item;
        
        // ensure the item is written before b+1 is published to other threads.
        // on x86/64, a compiler barrier is enough.
        bottom_.store(b+1, std::memory_order_release);

        ++num_queued_;

        return nullptr;
    }

    Item * steal()
    {
        uint_fast32_t t = top_.load(std::memory_order_acquire);
 
        // ensure that top_ is always read before bottom_.
        uint_fast32_t b = bottom_.load(std::memory_order_acquire);
        if (t == b || !topLessThanBottom(t, b))
            return nullptr;  // no work to be taken

        // non-empty queue
        Item * item = items[t & MASK];
 
        // the interlocked function serves as a compiler barrier, and guarantees that the read happens before the CAS.
        if (!top_.compare_exchange_strong(t, t + 1)) {
            // a concurrent steal or pop operation removed an element from the deque in the meantime.
            return nullptr;
        }
        
        ExcAssert(item);
        --num_queued_;

        return item;
    }

    Item * pop(int * path = nullptr)
    {
        if (path)
            *path = 0;

        while (num_queued_.load(std::memory_order_relaxed)) {
            // We speculatively decrement bottom_ here, and put it back to
            // its old value if we couldn't pop anything.  This means that
            // on a failure we do extra work, but normally that will mean
            // wasting time only when there is nothing to do, which is
            // preferable to wasting it when we have work to do.
            // The pairing acquire is in steal().
            uint_fast32_t b1 = bottom_.fetch_sub(1, std::memory_order_release);
            uint_fast32_t b = b1 - 1;

            uint_fast32_t t = top_.load(std::memory_order_acquire);

            if (t == b || topLessThanBottom(t, b)) {
                // non-empty queue
                Item* ptr = items[b & MASK];
                if (t != b) {
                    // there's still more than one item left in the queue
                    ExcAssert(ptr);
                    --num_queued_;
                    return ptr;
                }
 
                // this is the last item in the queue
                if (!top_.compare_exchange_strong(t, t+1)) {
                    // failed race against steal operation
                    bottom_.store(b1, std::memory_order_relaxed);
                    continue;
                }
                
                // Won race against steal operation
                bottom_.store(t+1, std::memory_order_relaxed);
                ExcAssert(ptr);
                --num_queued_;
                return ptr;
            }
            else {
                // deque was already empty
                bottom_.store(b1, std::memory_order_relaxed);
                continue;
            }
        }
        return nullptr;
    }

    bool topLessThanBottom(uint_fast32_t t, uint_fast32_t b)
    {
        // Simple case has no wrap-around
        // Complex case has wrap-around; we detect that by noticing
        // that there are implicitly more items in the queue than
        // there are slots.
        return t < b || (t - b > NUMBER_OF_ITEMS);
    }
};

} // namespace MLDB

