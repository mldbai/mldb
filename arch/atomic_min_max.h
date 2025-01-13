// public domain

#pragma once

#include <atomic>

namespace MLDB {

template<typename T, typename T2>
void atomic_min(std::atomic<T> & val, T2 other)
{
    T current = val.load(std::memory_order_relaxed);
    for (;;) {
        if (other >= current)
            return;
        if (val.compare_exchange_weak(current, other, std::memory_order_relaxed))
            return;
    }
}

template<typename T, typename T2>
void atomic_max(std::atomic<T> & val, T2 other)
{
    T current = val.load(std::memory_order_relaxed);
    for (;;) {
        if (other <= current)
            return;
        if (val.compare_exchange_weak(current, other, std::memory_order_relaxed))
            return;
    }
}

} // namespace MLDB