/* demangle.h                                                      -*- C++ -*-
   Jeremy Barnes, 27 January 2005
   Copyright (c) 2005 Jeremy Barnes.  All rights reserved.
   This file is part of MLDB. Copyright 2015 mldb.ai inc. All rights reserved.

   Interface to the C++ demangler.
*/

#pragma once


#include <string>
#include <typeinfo>

namespace MLDB {

/* returns a null-terminated string allocated on the heap */
char * char_demangle(const char * name);

std::string demangle(const std::string & name);
std::string demangle(const std::type_info & type);

template<typename T>
std::string type_name(const T & val)
{
    return demangle(typeid(val));
}

template<typename T>
std::string type_name()
{
    return demangle(typeid(T));
}

} // namespace MLDB

