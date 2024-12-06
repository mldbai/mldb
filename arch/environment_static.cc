// This file is part of MLDB. Copyright 2015 mldb.ai inc. All rights reserved.

/* environment.cc
   Jeremy Barnes, 14 March 2005
   Copyright (c) 2005 Jeremy Barnes.  All rights reserved.
   

   Functions to deal with the environment.
*/

#include "mldb/utils/environment.h"
#include <iostream>
#include <memory>

using namespace std;


extern char ** environ;

namespace MLDB {

//template<>
std::ostream &
EnvOptionTracer<true>::
start_trace(const std::string & var_name)
{
    return std::cerr << "Environment option " << var_name << " set to ";
}

//template<>
std::ostream &
EnvOptionTracer<true>::
end_trace(std::ostream & stream)
{
   return stream << std::endl;
}

Environment::Environment()
{
    char ** e = environ;
    base_type my_map;

    //cerr << "e = " << e << endl;

    while (e) {
        const char * p = *e++;
        if (!p) break;

        const char * s = p;
        while (p && *p != '=')
            ++p;
        const char * e __attribute__((unused)) = p;
        while (p && *p)
            ++p;

        string key(s, e);
        string val(*e == '=' ? e + 1 : e, p);

        my_map.emplace(std::move(key), std::move(val));
    }

    swap(my_map);
}

const Environment &
Environment::instance()
{
    static const auto result = std::make_unique<Environment>();
    return *result;
}

} // namespace MLDB
