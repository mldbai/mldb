// This file is part of MLDB. Copyright 2015 mldb.ai inc. All rights reserved.

/* sorted_vector.h                                             -*- C++ -*-
   Jeremy Barnes, 6 March 2005
   Copyright (c) 2005 Jeremy Barnes.  All rights reserved.
   $Source:$
   $Id:$
   


   ---

   Sorted vector<pair>, that acts like a memory-efficient map.
*/

#ifndef __utils__sorted_vector_h__
#define __utils__sorted_vector_h__


#include <vector>
#include <algorithm>
#include <utility>


namespace ML {


/*****************************************************************************/
/* SORTED_VECTOR                                                             */
/*****************************************************************************/

/** Class that looks like a map, but in fact stores its contents as a sorted
    vector, where searches are binary searches.
*/
template<class Key, class Data,
         class Compare = std::less<std::pair<Key, Data> > >
class sorted_vector {
    typedef std::vector<std::pair<Key, Data> > base_type;
    /* Immutable map interface, but lives in a sorted vector. */

public:
    sorted_vector()
    {
    }

    template<class Iterator>
    sorted_vector(Iterator first, Iterator last)
        : base(first, last)
    {
        std::sort(base.begin(), base.end(), Compare());
    }

    typedef std::pair<const Key, Data> value_type;
    typedef value_type & reference;
    typedef value_type * pointer;

    typedef typename base_type::iterator iterator;
    typedef typename base_type::const_iterator const_iterator;

    iterator begin() { return base.begin(); }
    iterator end() { return base.end(); }
    
    const_iterator begin() const { return base.begin(); }
    const_iterator end() const { return base.end(); }

    iterator find(const Key & key)
    {
        return std::binary_search(begin(), end(), key, Compare());
    }
    
    const_iterator find(const Key & key) const
    {
        return std::binary_search(begin(), end(), key, Compare());
    }

    iterator lower_bound(const Key & key)
    {
        return std::lower_bound(begin(), end(), key, Compare());
    }

    const_iterator lower_bound(const Key & key) const
    {
        return std::lower_bound(begin(), end(), key, Compare());
    }

    iterator upper_bound(const Key & key)
    {
        return std::upper_bound(begin(), end(), key, Compare());
    }

    const_iterator upper_bound(const Key & key) const
    {
        return std::upper_bound(begin(), end(), key, Compare());
    }

    size_t size() const { return base.size(); }

    bool empty() const { return base.empty(); }

private:
    base_type base;
};

} // namespace ML


#endif /* __utils__sorted_vector_h__ */

