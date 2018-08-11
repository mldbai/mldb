/* smart_ptr_utils.h                                               -*- C++ -*-
   Jeremy Barnes, 1 February 2005
   Copyright (c) 2005 Jeremy Barnes.  All rights reserved.
   This file is part of MLDB. Copyright 2015 mldb.ai inc. All rights reserved.  
   ---

   Utilities to help with smart pointers.
*/

#pragma once

#include <memory>

namespace MLDB {

template<class T>
std::shared_ptr<T> make_sp(T * val)
{
    return std::shared_ptr<T>(val);
}

struct Dont_Delete {
    template<class X> void operator () (const X & x) const
    {
    }
};

template<class T>
std::shared_ptr<T> make_unowned_sp(T & val)
{
    return std::shared_ptr<T>(&val, Dont_Delete());
}

template<class T>
std::shared_ptr<const T> make_unowned_sp(const T & val)
{
    return std::shared_ptr<const T>(&val, Dont_Delete());
}

} // namespace MLDB
