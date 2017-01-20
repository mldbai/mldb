// This file is part of MLDB. Copyright 2015 mldb.ai inc. All rights reserved.

/* float_traits.h                                                  -*- C++ -*-
   Jeremy Barnes, 30 January 2005
   Copyright (c) 2005 Jeremy Barnes.  All rights reserved.
   


   ---
   
   Floating point traits.
*/

#ifndef __utils__float_traits_h__
#define __utils__float_traits_h__

#include "mldb/compiler/compiler.h"

namespace ML {

template<typename F1, typename F2>
struct float_traits {
    typedef jml_typeof(F1() + F2()) return_type;
    typedef jml_typeof(F1() / F2(1)) fraction_type;
};

template <typename F>
struct float_traits<F, F> {
    typedef F return_type;
    typedef jml_typeof(F() / F(1)) fraction_type;
};

template<typename F1, typename F2, typename F3>
struct float_traits3 {
    typedef jml_typeof(*((F1*)(0)) + (*((F2*)(0))) + (*((F3*)(0)))) return_type;
};

template <typename F>
struct float_traits3<F, F, F> {
    typedef F return_type;
};

} // namespace ML

#endif /* __utils__float_traits_h__ */

