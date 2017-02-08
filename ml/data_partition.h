// This file is part of MLDB. Copyright 2015 mldb.ai inc. All rights reserved.

/* data_partition.h                                                -*- C++ -*-
   Jeremy Barns, 26 August 2012
   Copyright (c) 2012 mldb.ai inc.  All rights reserved.

   Abstract data partition used for training.
*/

#ifndef __recoset__ml__data_partition_h__
#define __recoset__ml__data_partition_h__

#include <tuple>
#include <functional>
#include <vector>
#include <boost/any.hpp>
#include <stdint.h>
#include "tuple_encoder.h"

namespace MLDB {


/*****************************************************************************/
/* DATA PARTITION                                                            */
/*****************************************************************************/

struct DataPartition {

    virtual ~DataPartition()
    {
    }

    virtual size_t exampleCount() const = 0;

    virtual std::tuple<bool, boost::any, double>
    getExample(size_t exampleNum) const = 0;

    typedef void (ForEachExampleCallbackFn) (bool label,
                                             const boost::any & features,
                                             double weight,
                                             size_t exampleNum);

    // Callback to be called to iterate over all examples
    typedef std::function<ForEachExampleCallbackFn>
        ForEachExampleCallback;

    virtual void forEachExample(const ForEachExampleCallback & cb,
                                bool inParallel) const = 0;
};


/*****************************************************************************/
/* ADHOC DATA PARTITION                                                      */
/*****************************************************************************/

struct AdHocDataPartition : public DataPartition {
    // Callback to be called to get the number of examples
    std::function<size_t ()> exampleCountImpl;

    virtual size_t exampleCount() const
    {
        return exampleCountImpl();
    }

    // Callback to be called to get a specific example
    std::function<std::tuple<bool, boost::any, double> (int exampleNum)>
        getExampleImpl;

    virtual std::tuple<bool, boost::any, double>
    getExample(size_t exampleNum) const
    {
        return getExampleImpl(exampleNum);
    }

    std::function<void (ForEachExampleCallback, bool inParallel)>
        forEachExampleImpl;

    virtual void forEachExample(const ForEachExampleCallback & cb,
                                bool inParallel) const
    {
        return forEachExampleImpl(cb, inParallel);
    }
};


/*****************************************************************************/
/* STORED DATA PARTITION                                                     */
/*****************************************************************************/

/** Implementation of a DataPartition that stores its data in a vector. */

struct StoredDataPartition : public DataPartition {

    virtual ~StoredDataPartition();

    virtual size_t exampleCount() const;

    virtual std::tuple<bool, boost::any, double>
    getExample(size_t exampleNum) const;

    virtual void forEachExample(const ForEachExampleCallback & cb,
                                bool inParallel) const;

    void add(bool label, const boost::any & key, double weight);

    std::vector<std::tuple<bool, boost::any, double> > examples;
};


/*****************************************************************************/
/* STORED DATA PARTITION TEMPLATE                                            */
/*****************************************************************************/

/** Implementation of a DataPartition that stores its data in a vector. */

template<typename... Args>
struct StoredDataPartitionT : public StoredDataPartition {

    TupleEncoder<Args...> encoder;

    virtual ~StoredDataPartitionT()
    {
    }

    void add(bool label, Args... args, double weight)
    {
        StoredDataPartition::add(label, encoder.encode(args...), weight);
    }
};

} // namespace MLDB


#endif /* __recoset__ml__data_partition_h__ */
