/** jml_serialization.h                                            -*- C++ -*-
    Jeremy Barnes, 26 November 2015
    Copyright (c) 2015 mldb.ai inc.  All rights reserved.

    This file is part of MLDB. Copyright 2015 mldb.ai inc. All rights reserved.

    Serialization JML-style for types.  Header-only library; shouldn't
    be included in a .h file.
*/

#include "mldb/jml/db/persistent.h"
#include "mldb/jml/db/compact_size_types.h"
#include "string.h"
#include "date.h"
#include "hash_wrapper.h"
#include <memory>

#pragma once

namespace MLDB {

inline ML::DB::Store_Writer & operator << (ML::DB::Store_Writer & store, const Utf8String & str)
{
    return store << str.rawString();
}

inline ML::DB::Store_Reader & operator >> (ML::DB::Store_Reader & store, Utf8String & str)
{
    std::string s;
    store >> s;
    str = std::move(s);
    return store;
}

inline ML::DB::Store_Writer & operator << (ML::DB::Store_Writer & store, Date date)
{
    return store << date.secondsSinceEpoch();
}

inline ML::DB::Store_Reader & operator >> (ML::DB::Store_Reader & store, Date & date)
{
    double d;
    store >> d;
    date = Date::fromSecondsSinceEpoch(d);
    return store;
}

template<int Domain>
inline
ML::DB::Store_Writer & operator << (ML::DB::Store_Writer & store,
                                    const HashWrapper<Domain> & h)
{
    ML::DB::compact_size_t v(h);
    return store << v;
}

template<int Domain>
inline
ML::DB::Store_Reader & operator >> (ML::DB::Store_Reader & store,
                                    HashWrapper<Domain> & h)
{
    ML::DB::compact_size_t sz(store);
    h = HashWrapper<Domain>(sz);
    return store;
}


#if 0
void
Utf32String::
serialize(ML::DB::Store_Writer & store) const
{
    std::string utf8Str;
    utf8::utf32to8(std::begin(data_), std::end(data_), std::back_inserter(utf8Str));
    store << utf8Str;
}

void
Utf32String::
reconstitute(ML::DB::Store_Reader & store)
{
    std::string utf8Str;
    store >> utf8Str;

    std::u32string utf32Str;
    utf8::utf8to32(std::begin(utf8Str), std::end(utf8Str), std::back_inserter(utf32Str));

    data_ = std::move(utf32Str);
}

#endif


} // namespace MLDB
