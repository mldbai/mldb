/** jml_serialization.h                                            -*- C++ -*-
    Jeremy Barnes, 26 November 2015
    Copyright (c) 2015 Datacratic Inc.  All rights reserved.

    This file is part of MLDB. Copyright 2015 Datacratic. All rights reserved.

    Serialization JML-style for types.  Header-only library; shouldn't
    be included in a .h file.
*/

#include "mldb/jml/db/persistent.h"
#include "mldb/jml/db/compact_size_types.h"
#include "string.h"
#include "date.h"
#include "id.h"
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

inline ML::DB::Store_Writer & operator << (ML::DB::Store_Writer & store, const Id & id)
{
    unsigned typeToSerialize = id.type;
    if (typeToSerialize == Id::SHORTSTR)
        typeToSerialize = Id::STR;

    store << (char)1 << (char)typeToSerialize;

    switch (id.type) {
    case Id::NONE: break;
    case Id::NULLID: break;
    case Id::UUID:
    case Id::UUID_CAPS:
    case Id::GOOG128:
    case Id::BIGDEC:
        store.save_binary(&id.val1, 8);
        store.save_binary(&id.val2, 8);
        break;
    case Id::BASE64_96:
        store.save_binary(&id.val1, 8);
        store.save_binary(&id.val2, 4);
        break;
    case Id::HEX128LC:
        store.save_binary(&id.val1, 8);
        store.save_binary(&id.val2, 8);
        break;
    case Id::STR:
        store << std::string(id.str->data, id.str->data + id.len);
        break;
    case Id::SHORTSTR:
        store << std::string(id.shortStr, strnlen(id.shortStr, 16));
        break;
    case Id::COMPOUND2:
        store << id.compoundId1() << id.compoundId2();
        break;
    default:
        throw ML::Exception("unknown Id type");
    }

    return store;
}

inline ML::DB::Store_Reader & operator >> (ML::DB::Store_Reader & store, Id & id)
{
    using namespace std;
    Id r;

    char v, tp;
    store >> v;
    if (v < 0 || v > 1)
        throw ML::Exception("unknown Id version reconstituting");
    store >> tp;
    r.type = tp;

    // Fix up from earlier reconstitution version
    if (v == 0 && tp == 5)
        r.type = Id::STR;

    if (v == 0) {
        // old domain field; no longer used
        int d;
        store >> d;
        //r.domain = d;
    }

    switch (r.type) {
    case Id::NONE: break;
    case Id::NULLID: break;
    case Id::UUID:
    case Id::UUID_CAPS:
    case Id::GOOG128:
    case Id::BIGDEC: {
        store.load_binary(&r.val1, 8);
        store.load_binary(&r.val2, 8);
        break;
    }
    case Id::INT64DEC: {
        store.load_binary(&r.val1, 8);
        r.type = Id::BIGDEC;
        break;
    }
    case Id::BASE64_96: {
        store.load_binary(&r.val1, 8);
        store.load_binary(&r.val2, 4);
        break;
    }
    case Id::HEX128LC: {
        store.load_binary(&r.val1, 8);
        store.load_binary(&r.val2, 8);
        break;
    }
    case Id::STR: {
        std::string s;
        store >> s;
        if (s.length() <= 16) {
            r.type = Id::SHORTSTR;
            r.val1 = r.val2 = 0;
            std::copy(s.c_str(), s.c_str() + s.length(), r.shortStr);
        }
        else {
            r.len = s.size();
            r.ownstr = true;
            char * s2 = new char[s.size() + 4];
            Id::StringRep * sr = new (s2) Id::StringRep(1);
            r.str = sr;
            std::copy(s.begin(), s.end(), sr->data);
        }
        break;
    }
    case Id::COMPOUND2: {
        std::unique_ptr<Id> id1(new Id()), id2(new Id());
        store >> *id1 >> *id2;
        r.cmp1 = id1.release();
        r.cmp2 = id2.release();
        break;
    }
    default:
        throw ML::Exception("unknown Id type %d reconstituting",
                            tp);
    }

    id = std::move(r);

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
