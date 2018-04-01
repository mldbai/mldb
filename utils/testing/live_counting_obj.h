/* live_counting_obj.h                                             -*- C++ -*-
   Jeremy Barnes, 10 December 2009
   Copyright (c) 2009 Jeremy Barnes.  All rights reserved.
   This file is part of MLDB. Copyright 2015 mldb.ai inc. All rights reserved.

   Test object that counts the number that are live.  Used to test that a
   container properly constructs and destroys its contents.
*/

#pragma once

#include "mldb/arch/exception.h"
#include <atomic>

namespace MLDB {

volatile size_t constructed = 0, destroyed = 0;

int GOOD = 0xfeedbac4;
int BAD  = 0xdeadbeef;

struct Obj {
    Obj()
        : val(0)
    {
        //cerr << "default construct at " << this << endl;
        constructed += 1;
        magic = GOOD;
    }

    Obj(int val)
        : val(val)
    {
        //cerr << "value construct at " << this << endl;
        constructed += 1;
        magic = GOOD;
    }

    ~Obj() noexcept(false)
    {
        //cerr << "destroying at " << this << endl;
        destroyed += 1;
        if (magic == BAD)
            throw Exception("object destroyed twice");

        if (magic != GOOD)
            throw Exception("object never initialized in destructor");

        magic = BAD;
    }

    Obj(const Obj & other)
        : val(other.val)
    {
        //cerr << "copy construct at " << this << endl;
        constructed += 1;
        magic = GOOD;
    }

    Obj & operator = (int val)
    {
        if (magic == BAD)
            throw Exception("assigned to destroyed object");

        if (magic != GOOD)
            throw Exception("assigned to object never initialized in assign");

        this->val = val;
        return *this;
    }

    Obj & operator += (int amt)
    {
        if (magic == BAD)
            throw Exception("assigned to destroyed object");

        if (magic != GOOD)
            throw Exception("assigned to object never initialized in assign");

        val += amt;

        return *this;
    }

    Obj & operator -= (int amt)
    {
        if (magic == BAD)
            throw Exception("assigned to destroyed object");

        if (magic != GOOD)
            throw Exception("assigned to object never initialized in assign");

        val -= amt;

        return *this;
    }

    bool operator == (int val) const
    {
        if (magic == BAD)
            throw Exception("assigned to destroyed object");
        
        if (magic != GOOD)
            throw Exception("assigned to object never initialized in assign");
        
        return this->val == val;
    }

    bool operator != (int val) const
    {
        return ! operator == (val);
    }

    int val;
    int magic;

    operator int () const
    {
        if (magic == BAD)
            throw Exception("read destroyed object");

        if (magic != GOOD)
            throw Exception("read from uninitialized object");

        return val;
    }
};

std::ostream & operator << (std::ostream & stream, const Obj & obj)
{
    return stream << obj.val;
}

} // namespace MLDB
