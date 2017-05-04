// This file is part of MLDB. Copyright 2015 mldb.ai inc. All rights reserved.

/* tranches.h                                                      -*- C++ -*-
   Jeremy Barnes, 1 August 2013
   Copyright (c) 2013 mldb.ai inc.  All rights reserved.

   Logic for manipulating sets of tranches.
*/

#pragma once

#include "behavior_domain.h"
#include "mldb/types/value_description_fwd.h"
#include <boost/iterator/iterator_facade.hpp>

using namespace std;

namespace MLDB {


/*****************************************************************************/
/* TRANCHE SPEC                                                              */
/*****************************************************************************/

/** Structure that contains a set of tranches which represents a subset of
    user IDs.  The particular peculiarity is that this is a fractal set, ie
    you can always subdivide it finer.
*/

struct TrancheSpec {
    /** Default constructor creates one that contains all data. */
    TrancheSpec();

    /** Initialize for (eg) file 0 of 32 with (0,32) */
    TrancheSpec(int num, int totalTranches);

    /** Initialize for all of totalTranchs. */
    explicit TrancheSpec(int totalTranches);

    /** Initialize for (eg) file 0 of 32 with (0,32) */
    TrancheSpec(const std::vector<int> & set, int totalTranches);

    /** Initialize from a string */
    TrancheSpec(const std::string & str);

    /** Initialize from a string */
    TrancheSpec(const char * str);

    std::vector<int> set;
    int modulusShift;

    int totalTranches() const { return (1 << modulusShift); }

    /** Parse the output of the toString() function. */
    static TrancheSpec parse(const std::string & val, char separator = '_');

    void infer(const BehaviorDomain & behs,
               int maxModulusShift = 8);

    bool tryCollapseOneLevel();

    void expandOneLevel();

    bool empty() const
    {
        return set.empty();
    }
    
    // Return a new object with the given modulus shift
    TrancheSpec atModulus(int newModulusShift) const;

    // Create the maximally specific modulus shift for the given tranche
    // spec.
    void collapse(int newModulusShift = -1);

    TrancheSpec collapsed() const
    {
        TrancheSpec result = *this;
        result.collapse();
        return result;
    }

    bool matchesAll() const
    {
        return set.size() == 1 << modulusShift;
    }

    // TODO: make it linear...
    bool matches(SH subject) const
    {
        int mask = ((1 << (modulusShift)) - 1);

        int bucket = subject.hash() & mask;
        //using namespace std;
        //cerr << "hash = " << subject.hash()
        //     << " bucket = " << bucket
        //     << " mask = " << mask << endl;

        return count(bucket);
    }

    bool count(int bucket) const
    {
        return std::binary_search(set.begin(), set.end(), bucket);
    }

    /** Returns if there is any overlap between the two. */
    bool intersects(const TrancheSpec & other) const;

    /** Returns a TrancheSpec that represents the overlap between the
        two tranches.  It will have the same number of tranches as
        the highest of the two inputs.
    */
    TrancheSpec intersect(const TrancheSpec & other) const;

    TrancheSpec complement() const;

    std::string toString(char separator = '_') const;

    struct iterator;

    iterator begin() const;
    iterator end() const;

    bool operator==(const TrancheSpec & other) const;
    bool operator!=(const TrancheSpec & other) const
        { return !(*this == other); }
    bool operator < (const TrancheSpec & other) const
    {
        return totalTranches() < other.totalTranches()
            || (totalTranches() == other.totalTranches()
                && this->set < other.set);
    }
};

std::ostream &
operator << (std::ostream & stream, const TrancheSpec & spec);


struct TrancheSpec::iterator
    : public boost::iterator_facade<iterator, TrancheSpec,
                                    std::random_access_iterator_tag,
                                    TrancheSpec> {
    friend class boost::iterator_core_access;

    iterator(std::vector<int>::const_iterator it,
             int numTranches)
        : it(it), numTranches(numTranches)
    {
    }

    std::vector<int>::const_iterator it;
    int numTranches;

    bool equal(const iterator & other) const
    {
        return it == other.it;
    }
    
    TrancheSpec dereference() const
    {
        return TrancheSpec(*it, numTranches);
    }
    
    void increment()
    {
        ++it;
    }
    
    void decrement()
    {
        --it;
    }

    void advance(ssize_t amount)
    {
        it += amount;
    }

    ssize_t distance_to(const iterator & other) const
    {
        return other.it - it;
    }
};

inline TrancheSpec::iterator
TrancheSpec::begin() const { return iterator(set.begin(), totalTranches()); }

inline TrancheSpec::iterator
TrancheSpec::end() const { return iterator(set.end(), totalTranches()); }


ValueDescriptionT<TrancheSpec> * getDefaultDescription(TrancheSpec *);

inline std::string keyToString(const TrancheSpec & tr)
{
    return tr.toString();
}

inline TrancheSpec stringToKey(const std::string & str, TrancheSpec *)
{
    return TrancheSpec(str);
}

} // namespace MLDB
