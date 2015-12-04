// This file is part of MLDB. Copyright 2015 Datacratic. All rights reserved.

/** python_converters.h                                 -*- C++ -*-
    Adrian Max McCrea, 22 May 2013
    Copyright (c) 2012 Datacratic.  All rights reserved.

    This has to be included before boost/python.hpp
    otherwise, boost python can't handle the std::shared_ptr.
    Also, boost/python.hpp explodes when memory (and possibly
    other things) are included first.

*/

#pragma once

#include <boost/version.hpp>

#if (BOOST_VERSION <= 105200)

#include <memory>

namespace boost {

template<typename T>
T* get_pointer(std::shared_ptr<T>& p) { return p.get(); }

template<typename T>
T* get_pointer(const std::shared_ptr<T>& p) { return p.get(); }

}

#endif
