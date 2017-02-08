// This file is part of MLDB. Copyright 2015 mldb.ai inc. All rights reserved.

/* pair_utils.h                                                  -*- C++ -*-
   Jeremy Barnes, 1 February 2005
   Copyright (c) 2005 Jeremy Barnes.  All rights reserved.
   


   ---
  
   Helpful functions in dealing with pairs.
*/

#pragma once

#include "mldb/jml/utils/boost_fixes.h"
#include "mldb/jml/utils/sgi_functional.h"
#include <type_traits>
#include <boost/iterator/transform_iterator.hpp>
#include <boost/iterator/zip_iterator.hpp>
#include <boost/iterator/iterator_facade.hpp>
#include <utility>

namespace ML {


/*****************************************************************************/
/* FIRST_EXTRACT_ITERATOR                                                    */
/*****************************************************************************/

template<class Iterator>
boost::transform_iterator<std::select1st<typename Iterator::value_type>,
                          Iterator>
first_extractor(const Iterator & it)
{
    return boost::transform_iterator
        <std::select1st<typename Iterator::value_type>, Iterator>(it);
}


/*****************************************************************************/
/* SECOND_EXTRACT_ITERATOR                                                   */
/*****************************************************************************/

template<class Iterator>
boost::transform_iterator<std::select2nd<typename Iterator::value_type>,
                          Iterator>
second_extractor(const Iterator & it)
{
    return boost::transform_iterator
        <std::select2nd<typename Iterator::value_type>, Iterator>(it);
}


/*****************************************************************************/
/* PAIR_MERGER                                                               */
/*****************************************************************************/

#if 0

template<class X, class Y>
struct tuple_to_pair {

    typedef std::pair<X, Y> result_type;

    result_type operator () (const std::tuple<X, Y> & t) const
    {
        return std::make_pair(t.template get<0>(), t.template get<1>());
    }
};

template<class Iterator1, class Iterator2>
boost::transform_iterator<tuple_to_pair<typename Iterator1::value_type,
                                        typename Iterator2::value_type>,
                          boost::zip_iterator<std::tuple<Iterator1,
                                                           Iterator2> > >
pair_merger(const Iterator1 & it1, const Iterator2 & it2)
{
    return boost::make_transform_iterator
        <tuple_to_pair<typename Iterator1::value_type,
                       typename Iterator2::value_type> >
            (boost::make_zip_iterator(std::make_tuple(it1, it2)));
}

#else

template<typename It1, typename It2>
struct Pair_Merger
    : public boost::iterator_facade<Pair_Merger<It1, It2>,
                                    std::pair<typename std::iterator_traits<It1>::value_type,
                                              typename std::iterator_traits<It2>::value_type>,
                                    boost::forward_traversal_tag> {
    Pair_Merger()
    {
    }
    
    Pair_Merger(const It1 & it1, const It2 & it2)
        : it1(it1), it2(it2)
    {
    }

    It1 it1;
    It2 it2;

    std::pair<typename std::iterator_traits<It1>::value_type,
              typename std::iterator_traits<It2>::value_type>
    operator * () const
    {
        return std::make_pair(*it1, *it2);
    }

    bool equal(const Pair_Merger & other) const
    {
        return it1 == other.it1 && it2 == other.it2;
    }

    void increment()
    {
        ++it1;
        ++it2;
    }
};

template<typename It1, typename It2>
Pair_Merger<It1, It2>
pair_merger(const It1 & it1, const It2 & it2)
{
    return Pair_Merger<It1, It2>(it1, it2);
}

#endif

} // namespace ML

namespace std {

template<typename T1, typename T2>
std::ostream & operator << (std::ostream & stream,
                            const std::pair<T1, T2> & p)
{
    return stream << "(" << p.first << "," << p.second << ")";
}

} // namespace std
