// This file is part of MLDB. Copyright 2015 mldb.ai inc. All rights reserved.

/* multi_array_utils.h                                             -*- C++ -*-
   Jeremy Barnes, 1 February 2005
   Copyright (c) 2005 Jeremy Barnes.  All rights reserved.
   


   ---
   
   Utilities to do with multi arrays.
*/

#ifndef __utils__multi_array_utils_h__
#define __utils__multi_array_utils_h__

#include <boost/multi_array.hpp>
#include <algorithm>

namespace boost {

/* Multi arrays don't have a swap function, so we make one for them.  This
   is a nasty, nasty hack.
*/

template<typename T, std::size_t NumDims, typename Allocator>
void swap(multi_array<T, NumDims, Allocator> & a1,
          multi_array<T, NumDims, Allocator> & a2)
{
    int * p1 = (int *)&a1;
    int * p2 = (int *)&a2;

    for (unsigned i = 0;
         i < sizeof(multi_array<T, NumDims, Allocator>)
             / sizeof(int);
         ++i)
        std::swap(*p1++, *p2++);
}

template<typename T, std::size_t NumDims>
void swap(multi_array_ref<T, NumDims> & a1,
          multi_array_ref<T, NumDims> & a2)
{
    int * p1 = (int *)&a1;
    int * p2 = (int *)&a2;

    for (unsigned i = 0;
         i < sizeof(multi_array_ref<T, NumDims>)
             / sizeof(int);
         ++i)
        std::swap(*p1++, *p2++);
}

template<typename T, std::size_t NumDims>
void swap(const_multi_array_ref<T, NumDims> & a1,
          const_multi_array_ref<T, NumDims> & a2)
{
    int * p1 = (int *)&a1;
    int * p2 = (int *)&a2;

    for (unsigned i = 0;
         i < sizeof(const_multi_array_ref<T, NumDims>)
             / sizeof(int);
         ++i)
        std::swap(*p1++, *p2++);
}


} // namespace boost

namespace ML {

template<typename T, typename T2, std::size_t NumDims, typename Allocator>
void fill_multi_array(boost::multi_array<T, NumDims, Allocator> & a, T2 val)
{
    throw 1;
}

} // namespace ML

#endif /* __utils__multi_array_utils_h__ */
