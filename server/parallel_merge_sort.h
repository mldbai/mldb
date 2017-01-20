/* parallel_merge_sort.h                                           -*- C++ -*-
   Jeremy Barnes, 12 August 2015
   Copyright (c) 2015 mldb.ai inc.  All rights reserved.

   This file is part of MLDB. Copyright 2015 mldb.ai inc. All rights reserved.
*/

#pragma once

#include <thread>
#include <algorithm>
#include <vector>
#include <memory>

#include "mldb/base/thread_pool.h"


namespace MLDB {

template<class VecOfVecs, typename SortFn, typename MergeFn, typename SizeFn>
void parallelMergeSortRecursive(VecOfVecs & range, unsigned first, unsigned last,
                                const SortFn & sort,
                                const MergeFn & merge,
                                const SizeFn & size,
                                size_t threadThreshold)
{
    if (first == last)
        return;

    if (first == last - 1) {
        sort(range[first]);
    }
    else {
        int mid = (first + last) / 2;

        auto doSide = [&] (int first, int last)
            {
                size_t totalToDo = 0;
                for (int i = first;  i < last;  ++i)
                    totalToDo += size(range[i]);

                std::thread t;

                auto run = [=,&range,&sort,&merge,&size] ()
                {
                    parallelMergeSortRecursive(range, first, last, sort, merge, size,
                                               threadThreshold);
                };

                if (totalToDo > threadThreshold) {
                    t = std::thread(run);
                }
                else run();

                return std::move(t);
            };

        std::thread t1(doSide(first, mid));
        std::thread t2(doSide(mid, last));

        if (t1.joinable())
            t1.join();
        if (t2.joinable())
            t2.join();
                    
        merge(range[first], range[mid]);
    }
}

template<class T, class Compare = std::less<T> >
std::vector<T>
parallelMergeSort(std::vector<std::vector<T> > & range,
                  const Compare & cmp = std::less<T>(),
                  size_t threadThreshold = 10000)
{
    if (range.empty())
        return {};

    auto sort = [&] (const std::vector<T> & v)
        {
            std::sort(v.begin(), v.end(), cmp);
        };

    auto size = [] (const std::vector<T> & v)
        {
            return v.size();
        };

    auto merge = [&] (std::vector<T> & v1, std::vector<T> & v2)
        {
            size_t split = v1.size();

            v1.insert(v1.end(), 
                      std::make_move_iterator(v2.begin()),
                      std::make_move_iterator(v2.end()));
            v2.clear();

            std::inplace_merge(v1.begin(),
                               v1.begin() + split,
                               v1.end(),
                               cmp);
        };

    parallelMergeSortRecursive(range, 0, range.size(), sort, merge, size, threadThreshold);

    return std::move(range[0]);
}

template<typename T, typename Compare>
std::vector<T> uniqueMerge(std::vector<T> & v1, std::vector<T> & v2,
                           const Compare & cmp = Compare())
{
    std::vector<T> result;
    result.reserve(v1.size() + v2.size());

    auto it1 = v1.begin(), e1 = v1.end(), it2 = v2.begin(), e2 = v2.end();
    while (it1 < e1 && it2 < e2) {
        if (cmp(*it1, *it2)) {
            result.emplace_back(std::move(*it1));
            ++it1;
        }
        else if (cmp(*it2, *it1)) {
            result.emplace_back(std::move(*it2));
            ++it2;
        }
        else {
            // Equal; keep only one
            result.emplace_back(std::move(*it1));
            ++it1;
            ++it2;
        }
    }
    result.insert(result.end(),
                  std::make_move_iterator(it1),
                  std::make_move_iterator(e1));
    result.insert(result.end(),
                  std::make_move_iterator(it2),
                  std::make_move_iterator(e2));
    v1.clear();
    v2.clear();
    return result;
}

template<class T, class Compare = std::less<T> >
std::vector<T>
parallelMergeSortUnique(std::vector<std::vector<T> > & range,
                        const Compare & cmp = std::less<T>(),
                        size_t threadThreshold = 10000)
{
    if (range.empty())
        return {};

    auto sort = [&cmp] (std::vector<T> & v)
        {
            std::sort(v.begin(), v.end(), cmp);
            v.erase(std::unique(v.begin(), v.end()),
                    v.end());
        };

    auto size = [] (const std::vector<T> & v)
        {
            return v.size();
        };

    auto merge = [&cmp] (std::vector<T> & v1, std::vector<T> & v2)
        {
            v1 = uniqueMerge(v1, v2, cmp);
        };
    
    parallelMergeSortRecursive(range, 0, range.size(), sort, merge, size, threadThreshold);

    return std::move(range[0]);
}

template<class T, class Compare = std::less<T> >
std::vector<T>
parallelMergeSort(std::vector<std::shared_ptr<std::vector<T> > > & range,
                  const Compare & cmp = std::less<T>(),
                  size_t threadThreshold = 10000)
{
    if (range.empty())
        return {};

    auto sort = [&] (const std::shared_ptr<std::vector<T> > & v)
        {
            std::sort((*v).begin(), v->end(), cmp);
        };
    
    auto size = [] (const std::shared_ptr<std::vector<T> > & v) -> size_t
        {
            return v->size();
        };

    auto merge = [&] (std::shared_ptr<std::vector<T> > & v1,
                      std::shared_ptr<std::vector<T> > & v2)
        {
            size_t split = v1->size();

            v1->insert(v1->end(), 
                       std::make_move_iterator(v2->begin()),
                       std::make_move_iterator(v2->end()));
            v2->clear();

            std::inplace_merge(v1->begin(),
                               v1->begin() + split,
                               v1->end(),
                               cmp);
        };

    parallelMergeSortRecursive(range, 0, range.size(), sort, merge, size, threadThreshold);

    return std::move(*range[0]);
}

/* taken from http://demin.ws/blog/english/2012/04/28/multithreaded-quicksort/ */

template<class T, class Compare >
void
parallelQuickSortRecursive(typename std::vector<T>::iterator begin, typename std::vector<T>::iterator end, Compare less = Compare(), int depth = 0)
{
    size_t numElements = end - begin;
    if (numElements <= 1)
        return;

    if (depth > 8 || numElements < 1024) {
        std::sort(begin, end, less);
        return;
    }

    auto pivot = begin + numElements / 2;
    auto const pivotValue = *pivot;

    std::swap(*pivot, *(end-1));
    auto p = std::partition(begin, end, [&](const T& a) { return less(a, pivotValue); } );
    std::swap(*p, *(end - 1));

    ThreadPool tp;

    auto runLeft = [&] () { parallelQuickSortRecursive<T, Compare>(begin, p, less, depth+1); };
    auto runRight = [&] () { parallelQuickSortRecursive<T, Compare>(p + 1, end, less, depth+1); };
    
    // Put the smallest one on the thread pool, so that we have the highest
    // probability of running both on our thread in case of lots of work.
    if (p-begin < end-p){
        tp.add(runLeft);
        runRight();
    }
    else {
        tp.add(runRight);
        runLeft();
    }
  
    tp.waitForAll();
}

template<class T, class Compare = std::less<T> >
void 
parallelQuickSortRecursive(std::vector<T> & vec)
{
    return parallelQuickSortRecursive<T, Compare>(vec.begin(), vec.end(),
                                                  Compare());
}


} // namespace MLDB

