// This file is part of MLDB. Copyright 2015 mldb.ai inc. All rights reserved.

/** hash_wrapper_description.h                                     -*- C++ -*-
    Jeremy Barnes, 28 November 2015
    Copyright (c) 2015 mldb.ai inc.  All rights reserved.

    Value description of hash_wrapper class.
*/

#pragma once

#include "hash_wrapper.h"
#include "value_description.h"

namespace MLDB {

template<int Domain>
struct HashWrapperDescription: public ValueDescriptionT<HashWrapper<Domain> > {
    HashWrapperDescription()
        : ValueDescriptionT<HashWrapper<Domain> >(ValueKind::ATOM)
    {
    }

    virtual void parseJson(void * val, JsonParsingContext & context) const
    {
        HashWrapper<Domain> * val2 = reinterpret_cast<HashWrapper<Domain> *>(val);
        return parseJsonTyped(val2, context);
    }

    virtual void parseJsonTyped(HashWrapper<Domain> * val,
                                JsonParsingContext & context) const
    {
        auto str = context.expectStringAscii();
        *val = HashWrapper<Domain>::fromString(str);
    }

    virtual void printJson(const void * val, JsonPrintingContext & context) const
    {
        const HashWrapper<Domain> * val2 = reinterpret_cast<const HashWrapper<Domain> *>(val);
        return printJsonTyped(val2, context);
    }

    virtual void printJsonTyped(const HashWrapper<Domain> * val,
                                JsonPrintingContext & context) const
    {
        context.writeString(val->toString());
    }

    virtual bool isDefaultTyped(const HashWrapper<Domain> * val) const
    {
        return *val == HashWrapper<Domain>();
    }

};

template<int Domain>
ValueDescriptionT<HashWrapper<Domain> > *
getDefaultDescription(const HashWrapper<Domain> * = 0)
{
    return new HashWrapperDescription<Domain>();
}

// Allow it to be used as part of a REST interface

template<int Domain>
HashWrapper<Domain> restDecode(const std::string & str, HashWrapper<Domain> * = 0)
{
    return HashWrapper<Domain>::fromString(str);
}

template<int Domain>
std::string restEncode(HashWrapper<Domain> val)
{
    return val.toString();
}

template<int Domain>
std::string keyToString(const HashWrapper<Domain> & val)
{
    return val.toString();
}

template<int Domain>
HashWrapper<Domain> stringToKey(const std::string & str,
                                HashWrapper<Domain> *)
{
    return HashWrapper<Domain>::fromString(str);
}

template<typename Int, int Domain>
struct IntWrapperDescription: public ValueDescriptionT<IntWrapper<Int, Domain> > {
    IntWrapperDescription()
        : ValueDescriptionT<IntWrapper<Int, Domain> >(ValueKind::ATOM)
    {
    }

    virtual void parseJsonTyped(IntWrapper<Int, Domain> * val,
                                JsonParsingContext & context) const
    {
        *val = IntWrapper<Int, Domain>::fromString(context.expectStringAscii());
    }

    virtual void printJsonTyped(const IntWrapper<Int, Domain> * val,
                                JsonPrintingContext & context) const
    {
        context.writeString(val->toString());
    }

    virtual bool isDefaultTyped(const IntWrapper<Int, Domain> * val) const
    {
        return val == IntWrapper<Int, Domain>();
    }

};

template<typename Int, int Domain>
ValueDescriptionT<IntWrapper<Int, Domain> > *
getDefaultDescription(const IntWrapper<Int, Domain> * = 0)
{
    return new IntWrapperDescription<Int, Domain>();
}

} // namespace MLDB
