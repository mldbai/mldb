/* guard.h                                                         -*- C++ -*-
   Jeremy Barnes, 13 February 2007
   Copyright (c) 2007 Jeremy Barnes.  All rights reserved.
   This file is part of MLDB. Copyright 2015 mldb.ai inc. All rights reserved.


   ---
*/

#pragma once

#include "mldb/compiler/compiler.h"
#include <functional>

namespace ML {

struct Call_Guard {
    
    typedef std::function<void ()> Fn;

    Call_Guard(const Fn & fn, bool condition = true)
        : fn(condition ? fn : Fn())
    {
    }

    Call_Guard()
    {
    }
    
#if MLDB_HAS_RVALUE_REFERENCES
    Call_Guard(Call_Guard && other)
        : fn(other.fn)
    {
        other.clear();
    }

    Call_Guard & operator = (Call_Guard && other)
    {
        if (fn) fn();
        fn = other.fn;
        other.clear();
        return *this;
    }
#endif

    ~Call_Guard()
    {
        if (fn) fn();
    }

    void clear() { fn = Fn(); }

    void set(const Fn & fn) { this->fn = fn; }

    std::function<void ()> fn;

private:
    Call_Guard(const Call_Guard & other);
    void operator = (const Call_Guard & other);
};

#if MLDB_HAS_RVALUE_REFERENCES
template<typename Fn>
Call_Guard call_guard(Fn fn, bool condition = true)
{
    return Call_Guard(fn, condition);
}
#endif


} // namespace MLDB

