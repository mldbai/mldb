/* multi_array_utils.h                                             -*- C++ -*-
   Jeremy Barnes, 1 February 2005
   Copyright (c) 2005 Jeremy Barnes.  All rights reserved.
   This file is part of MLDB. Copyright 2015 mldb.ai inc. All rights reserved.   
   ---
   
   Utilities to do with multi arrays.
*/

#pragma once

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

template<typename Val, std::size_t Dims, class Allocator>
void swap_multi_arrays(boost::multi_array<Val, Dims, Allocator> & a1,
                       boost::multi_array<Val, Dims, Allocator> & a2)
{
    /* Since we know there is nothing self-referential, we do a bit by
       bit copy.  Note that this might not work for some allocator
       types.
 
       This is a hack needed to get around the lack of a swap function
       in the boost multi array types.
   */

    volatile char * p1 = (volatile char *)(&a1);
    volatile char * p2 = (volatile char *)(&a2);

    for (unsigned i = 0;  i < sizeof(a1);  ++i)
        std::swap(p1[i], p2[i]);
}

} // namespace ML
