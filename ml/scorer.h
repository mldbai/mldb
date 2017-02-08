// This file is part of MLDB. Copyright 2015 mldb.ai inc. All rights reserved.

/* scorer.h                                                        -*- C++ -*-
   Jeremy Barnes, 5 Spetember 2012
   Copyright (c) 2012 mldb.ai inc.  All rights reserved.

   Generic class to score a bunch of things.
*/

#ifndef __ml__scorer_h__
#define __ml__scorer_h__

#include "separation_stats.h"
#include "data_partition.h"
#include "tuple_encoder.h"


namespace MLDB {


/*****************************************************************************/
/* SCORER                                                                    */
/*****************************************************************************/


struct Scorer {

    virtual ~Scorer()
    {
    }

    /** Test the given classifier over the selected partition. */
    ScoredStats test(const DataPartition & partition) const;

    virtual float scoreGeneric(const boost::any & args) const = 0;

    virtual float probabilityGeneric(const boost::any & args) const = 0;

    virtual bool isCallableFromMultipleThreads() const
    {
        return true;  // default
    }
};


/*****************************************************************************/
/* SCORER TEMPLATE                                                           */
/*****************************************************************************/

template<typename... Args>
struct ScorerT
    : virtual public Scorer,
      virtual public TupleEncoder<Args...> {

    virtual ~ScorerT()
    {
    }

    virtual float scoreGeneric(const boost::any & args) const
    {
        return callPmfWithTuple(&ScorerT::score,
                                *this,
                                this->decodeStatic(args));
        
    }

    virtual float probabilityGeneric(const boost::any & args) const
    {
        return callPmfWithTuple(&ScorerT::probability,
                                *this,
                                this->decodeStatic(args));
    }

    virtual float score(Args... args) const = 0;

    virtual float probability(Args... args) const
    {
        return score(args...);
    }
};

} // namespace MLDB

#endif /* __ml__scorer_h__ */
