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
        string s = p;
        //cerr << "s = " << s << endl;
        string::size_type equal_pos = s.find('=');
        if (equal_pos == string::npos) operator [] (s) = "";
        else {
            string key(s, 0, equal_pos);
            string val(s, equal_pos + 1);
            operator [] (key) = val;
        }
    }
}

const Environment &
Environment::instance()
{
    static const Environment result;
    return result;
}

} // namespace MLDB
