/* possibly_dynamic_buffer.h                                                          -*- C++ -*-
   Mich, 24 August 2016

   This file is part of MLDB. Copyright 2016 mldb.ai inc. All rights reserved.
*/

#pragma once

#include <memory>
#include <span>

namespace MLDB {

// Made to init and hold a non POD array of dynamic size in a memory safe
// way.
template<typename T, size_t MAX_STACK_ENTRIES=1024/sizeof(T)>
struct PossiblyDynamicBuffer {
    PossiblyDynamicBuffer(size_t sz)
        : size_(sz)
    {
        if (onStack()) {
            std::uninitialized_fill(stackEntries, stackEntries + size_, T());
        } 
        else {
            heapEntries = new T[size_];
        }
    }

    ~PossiblyDynamicBuffer()
    {
        if (onStack()) {
            for (size_t i = 0; i < size_;  ++i) {
                stackEntries[i].~T();
            }
        }
        else {
            delete[] heapEntries;
        }
    }

    bool onStack() const { return size_ <= MAX_STACK_ENTRIES; }

    T * data() { return onStack() ? stackEntries: heapEntries; }
    const T * data() const { return onStack() ? stackEntries: heapEntries; }
    size_t size() const { return size_; }

    operator std::span<T> () const { return { data(), size() }; }

    const T * begin() const { return data(); }
    const T * end() const { return data() + size(); }
    T * begin() { return data(); }
    T * end() { return data() + size(); }

    operator T * () { return data(); }

    T & operator [] (size_t el)
    {
        return data()[el];
    }

    const T & operator [] (size_t el) const
    {
        return data()[el];
    }

    size_t size_;

    union {
        T stackEntries[MAX_STACK_ENTRIES];
        T * heapEntries;
    };

private:
    PossiblyDynamicBuffer(const PossiblyDynamicBuffer & other);
    PossiblyDynamicBuffer & operator=(PossiblyDynamicBuffer & other);
};

} // namespace MLDB
