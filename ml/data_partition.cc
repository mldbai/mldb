// This file is part of MLDB. Copyright 2015 mldb.ai inc. All rights reserved.

/* data_partition.cc
   Jeremy Barnes, 26 August 2012
   Copyright (c) 2012 mldb.ai inc.  All rights reserved.

   Functions to implement our generic data partitions.
*/

#include "data_partition.h"
#include "mldb/base/parallel.h"

using namespace std;

namespace MLDB {


/*****************************************************************************/
/* STORED DATA PARTITION                                                     */
/*****************************************************************************/


StoredDataPartition::
~StoredDataPartition()
{
}

size_t
StoredDataPartition::
exampleCount() const
{
    return examples.size();
}

std::tuple<bool, boost::any, double>
StoredDataPartition::
getExample(size_t exampleNum) const
{
    return examples.at(exampleNum);
}

void
StoredDataPartition::
forEachExample(const ForEachExampleCallback & cb,
               bool inParallel) const
{
    if (inParallel) {
        auto onExample = [&] (size_t i)
            {
                const auto & ex = examples[i];
                cb(std::get<0>(ex), std::get<1>(ex), std::get<2>(ex), i);
            };

        parallelMap(0, examples.size(), onExample);
    }
    else {
        for (unsigned i = 0;  i < examples.size();  ++i) {
            const auto & ex = examples[i];
            cb(std::get<0>(ex), std::get<1>(ex), std::get<2>(ex), i);
        }
    }
}

void
StoredDataPartition::
add(bool label, const boost::any & key, double weight)
{
    examples.push_back(make_tuple(label, key, weight));
}

} // namespace MLDB
