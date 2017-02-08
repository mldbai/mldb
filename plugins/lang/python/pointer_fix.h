/** python_converters.h                                 -*- C++ -*-
    Adrian Max McCrea, 22 May 2013
    Copyright (c) 2012 mldb.ai inc.  All rights reserved.

    This file is part of MLDB. Copyright 2015 mldb.ai inc. All rights reserved.

    This has to be included before boost/python.hpp
    otherwise, boost python can't handle the std::shared_ptr.
    Also, boost/python.hpp explodes when memory (and possibly
    other things) are included first.

*/

#pragma once

#include <memory>
#include <boost/get_pointer.hpp>

#ifdef __clang__

// clang 3.4 and boost 1.52 don't detect std::shared_ptr properly
namespace std {
template<typename T>
T * get_pointer(const std::shared_ptr<T> & ptr)
{
    return ptr.get();
}

} // namespace std

#endif // __clang__

