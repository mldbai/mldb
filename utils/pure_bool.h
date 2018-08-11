// This file is part of MLDB. Copyright 2015 mldb.ai inc. All rights reserved.

/* pure_bool.h                                                  -*- C++ -*-
   Jeremy Barnes, 30 January 2005
   Copyright (c) 2005 Jeremy Barnes.  All rights reserved.
   


   ---

   An unnamed bool type.  Used to enable operator bool without having the
   object be convertable to int.
*/

#pragma once

#include "mldb/compiler/compiler.h"

namespace MLDB {

struct pure_bool_t {
    pure_bool_t(bool val)
        : val(val) {}

    int fn() const { return 0; }
    
    typedef int (pure_bool_t::* type)() const;

    operator type () const
    {
        static const type true_val = &pure_bool_t::fn;
        static const type false_val = 0;
        return val ? true_val : false_val;
    }

    bool val;
};

typedef pure_bool_t::type pure_bool;
static const pure_bool pure_true = &pure_bool_t::fn;
static const pure_bool pure_false = 0;

MLDB_ALWAYS_INLINE pure_bool make_pure_bool(bool val)
{
    return (val ? pure_true : pure_false);
}

#define MLDB_IMPLEMENT_OPERATOR_BOOL(expr) \
    operator ::MLDB::pure_bool () const { return ::MLDB::make_pure_bool(expr); }

} // namespace MLDB

