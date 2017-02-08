// This file is part of MLDB. Copyright 2015 mldb.ai inc. All rights reserved.

/** maybe.h                                                        -*- C++ -*-
    Jeremy Barnes, 3 July 2014
    Copyright (C) 2014 mldb.ai inc.  All rights reserved.


*/

#pragma once

#include "mldb/types/value_description_fwd.h"

namespace MLDB {

struct Dummy {
    void dummy() {};
};

template<typename Val, typename None = void>
struct MaybeT {
    MaybeT()
    {
    }

    MaybeT(const Val & val)
        : val_(new Val(val))
    {
    }

    MaybeT(Val && val)
        : val_(new Val(std::move(val)))
    {
    }

    MaybeT(const None & err)
        : err_(new None(err))
    {
    }

    MaybeT(None && err)
        : err_(new None(std::move(err)))
    {
    }

    /** A maybe is null if there is neither a value nor an error, ie
        it was created using the default constructor.
    */
    bool isNull() const
    {
        return !val_ && !err_;
    }

    /** Operator bool support to know if there is a value there. */
    typedef void (Dummy::* UnnamedBool) ();
    operator UnnamedBool() const
    {
        return val_ ? &Dummy::dummy : nullptr;
    }
    
    const Val & val() const
    {
        ExcAssert(val_);
        return *val_;
    }

    const None & err() const
    {
        ExcAssert(err_);
        return *err_;
    }

    Val & val()
    {
        ExcAssert(val_);
        return *val_;
     }

    None & err()
    {
        ExcAssert(err_);
        return *err_;
    }

    std::unique_ptr<Val> val_;
    std::unique_ptr<None> err_;
};

template<typename Val, typename None>
ValueDescriptionT<MaybeT<Val, None> > *
getDefaultDescription(MaybeT<Val, None> * = 0);

} // namespace MLDB
