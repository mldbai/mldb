/** atomic_shared_ptr.h                                            -*- C++ -*-
    Jeremy Barnes, 5 April 2016
    This file is part of MLDB. Copyright 2015 mldb.ai inc. All rights reserved.
*/    

#pragma once

#include "mldb/arch/spinlock.h"
#include <memory>
#include <mutex>

namespace MLDB {

// Note: in GCC 4.9+, we can use the std::atomic_xxx overloads for
// std::shared_ptr.  Once the Concurrency TR is available, we can
// replace with those classes.  For the moment we use a simple,
// spinlock protected implementation that is a lowest common
// denominator.
template<typename T>
struct atomic_shared_ptr {

    atomic_shared_ptr(std::shared_ptr<T> ptr = nullptr)
        : ptr(std::move(ptr))
    {
    }
    
    std::shared_ptr<T> load() const
    {
        std::unique_lock<Spinlock> guard(lock);
        return ptr;
    }

    void store(std::shared_ptr<T> newVal)
    {
        std::unique_lock<Spinlock> guard(lock);
        ptr = std::move(newVal);
    }

    std::shared_ptr<T> exchange(std::shared_ptr<T> newVal)
    {
        std::unique_lock<Spinlock> guard(lock);
        std::shared_ptr<T> result = std::move(ptr);
        ptr = std::move(newVal);
        return result;
    }

    bool compare_exchange_strong(std::shared_ptr<T> & expected,
                                 std::shared_ptr<T> desired)
    {
        std::unique_lock<Spinlock> guard(lock);
        if (ptr == expected) {
            expected = std::move(ptr);
            ptr = std::move(desired);
            return true;
        }
        else {
            expected = ptr;
            return false;
        }
    }

private:
    mutable Spinlock lock;
    std::shared_ptr<T> ptr;
};

} // namespace MLDB
