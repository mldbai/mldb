// This file is part of MLDB. Copyright 2015 mldb.ai inc. All rights reserved.

/* environment.cc
   Jeremy Barnes, 14 March 2005
   Copyright (c) 2005 Jeremy Barnes.  All rights reserved.
   

   Functions to deal with the environment.
*/

#include "mldb/jml/utils/environment.h"
#include <iostream>

using namespace std;


extern char ** environ;

namespace MLDB {

Environment::Environment()
{
    char ** e = environ;

    //cerr << "e = " << e << endl;

    while (e) {
        const char * p = *e++;
        if (!p) break;
        //cerr << "p = " << (void *)p << endl;
        //cerr << "p = " << p << endl;
        const char * s = p;
        while (p && *p != '=')
            ++p;
        const char * e = p;
        while (p && *p)
            ++p;

        string key(s, e);
        string val(*e == '=' ? e + 1 : e, p);

        this->emplace(std::move(key), std::move(val));
    }
}

const Environment &
Environment::instance()
{
    static const Environment result;
    return result;
}

} // namespace MLDB
