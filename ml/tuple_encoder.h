// This file is part of MLDB. Copyright 2015 mldb.ai inc. All rights reserved.

/* tuple_encoder.h                                                 -*- C++ -*-
   Jeremy Barnes, 5 September 2012
   Copyright (c) 2012 mldb.ai inc.  All rights reserved.

   Base class to encode/decode tuples.
*/

#ifndef __ml__tuple_encoder_h__
#define __ml__tuple_encoder_h__

#include <boost/any.hpp>
#include <tuple>
#include "mldb/jml/utils/positioned_types.h"

namespace MLDB {

/*****************************************************************************/
/* CALL WITH UNPACKED TUPLE                                                  */
/*****************************************************************************/

/** Given a tuple, unpacks the arguments and calls the function forwarding
    the tuple values to the arguments.
*/

using ML::TypeList;
using ML::InPosition;
using ML::MakeInPositionList;

// Template that, given an InPosition<Arg, Index> argument, will actually
// extract the argument from a JS::JsArgs and pass it on
template<class Tuple, typename Param>
struct ForwardTupleElement {
};

// Implementation of that template with the InPosition argument unpacked
template<class Tuple, typename Arg, int Index>
struct ForwardTupleElement<Tuple, InPosition<Arg, Index> > {
    static auto
    get(const Tuple & tuple) -> decltype(std::get<Index>(tuple))
    {
        return std::get<Index>(tuple);
    }
};

// Given a std::function type Fn and a TypeList of InPosition values,
// this calls the function with the JS arguments unpacked
template<typename List>
struct CallPmfWithTupleTypePositionList {
};

// Implementation of that template with the List argument unpacked
template<typename... ArgsWithPosition>
struct CallPmfWithTupleTypePositionList<TypeList<ArgsWithPosition...> > {

    template<typename R, typename... Args, typename Obj, typename Tuple>
    static R call(R (Obj::* pmf) (Args...) const, const Obj & obj,
                  const Tuple & tuple)
    {
        return (obj.*pmf)(ForwardTupleElement<Tuple, ArgsWithPosition>
                          ::get(tuple)...);
    }
};

template<typename R, typename Obj, typename... Args, typename Tuple>
R
callPmfWithTuple(R (Obj::*pmf) (Args...) const, const Obj & obj,
                 const Tuple & tuple)
{
    typedef typename MakeInPositionList<0, Args...>::List TypePositionList;
    return CallPmfWithTupleTypePositionList<TypePositionList>
        ::call(pmf, obj, tuple);
}


/*****************************************************************************/
/* TUPLE ENCODER                                                             */
/*****************************************************************************/

template<typename... Args>
struct TupleEncoder {

    typedef decltype(std::make_tuple(std::declval<Args>()...)) tuple_type;

    virtual const std::type_info & paramType() const
    {
        return typeid(tuple_type);
    }

    /** Record the features in a boost::any that can be later passed on
        into featuresGeneric and properly decoded.
    */
    boost::any
    encode(Args... args) const
    {
        return encodeStatic(args...);
    }

    tuple_type
    decode(const boost::any & args)
    {
        return decodeStatic(args);
    }

    static boost::any encodeStatic(Args... args)
    {
        return tuple_type(args...);
    }

    static tuple_type decodeStatic(const boost::any & arg)
    {
        return boost::any_cast<tuple_type>(arg);
    }
};

} // namespace MLDB


#endif /* __ml__tuple_encoder_h__ */
