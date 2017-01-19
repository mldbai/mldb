// This file is part of MLDB. Copyright 2015 mldb.ai inc. All rights reserved.

/* unnamed_bool.h                                                  -*- C++ -*-
   Jeremy Barnes, 30 January 2005
   Copyright (c) 2005 Jeremy Barnes.  All rights reserved.
   


   ---

   An unnamed bool type.  Used to enable operator bool without having the
   object be convertable to int.
*/

#pragma once

#include "mldb/compiler/compiler.h"

namespace ML {

struct unnamed_bool_t {
    unnamed_bool_t(bool val)
        : val(val) {}

    int fn() const { return 0; }
    
    typedef int (unnamed_bool_t::* type)() const;

    operator type () const
    {
        static const type true_val = &unnamed_bool_t::fn;
        static const type false_val = 0;
        return val ? true_val : false_val;
    }

    bool val;
};

typedef unnamed_bool_t::type unnamed_bool;
static const unnamed_bool unnamed_true = &unnamed_bool_t::fn;
static const unnamed_bool unnamed_false = 0;

MLDB_ALWAYS_INLINE unnamed_bool make_unnamed_bool(bool val)
{
    return (val ? unnamed_true : unnamed_false);
}

#define MLDB_IMPLEMENT_OPERATOR_BOOL(expr) \
    operator ML::unnamed_bool () const { return ML::make_unnamed_bool(expr); }

} // namespace ML

