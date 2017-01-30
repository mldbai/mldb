// This file is part of MLDB. Copyright 2015 mldb.ai inc. All rights reserved.

/* sgi_functional.h                                                -*- C++ -*-
   Jeremy Barnes, 2 February 2005
   Copyright (c) 2005 Jeremy Barnes.  All rights reserved.
   


   ---
   
   Move SGI's <functional> algorithms into the std:: namespace.
*/

#ifndef __utils__sgi_functional_h__
#define __utils__sgi_functional_h__

#include <ext/functional>

namespace std {
    
using namespace __gnu_cxx;

};

#endif /* __utils__sgi_functional_h__ */
