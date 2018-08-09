/* sgi_numeric.h                                                    -*- C++ -*-
   Jeremy Barnes, 2 February 2005
   Copyright (c) 2005 Jeremy Barnes.  All rights reserved.
   This file is part of MLDB. Copyright 2015 mldb.ai inc. All rights reserved.

   ---
   
   Move SGI's <numeric> algorithms into the std:: namespace.
*/

#pragma once

#include <ext/numeric>

namespace std {
    
using namespace __gnu_cxx;

};
