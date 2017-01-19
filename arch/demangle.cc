// This file is part of MLDB. Copyright 2015 mldb.ai inc. All rights reserved.

/* demangle.cc
   Jeremy Barnes, 17 March 2005
   Copyright (c) 2005 Jeremy Barnes.  All rights reserved.


   Demangler.  Just calls the ABI one, but does the memory management for
   us.
*/

#include <string.h>
#include <string>
#include <cxxabi.h>
#include <stdlib.h>

#include "mldb/jml/utils/guard.h"

#include "demangle.h"


using namespace std;

namespace MLDB {

char * char_demangle(const char * name)
{
    int status;
    char * result = abi::__cxa_demangle(name, nullptr, 0, &status);

    if (status != 0)
        result = ::strdup(name);

    return result;
}

std::string demangle(const std::string & name)
{
    string result;
    char * ptr = char_demangle(name.c_str());

    if (ptr) {
        ML::Call_Guard guard([&] { free(ptr); });
        result = ptr;
    }
    else {
        result = name;
    }

    return result;
}

std::string demangle(const std::type_info & type)
{
    return demangle(type.name());
}

} // namespace MLDB

