/** randomforest_kernels.h                                     -*- C++ -*-
    Jeremy Barnes, 13 October 2018
    Copyright (c) 2018 mldb.ai inc.  All rights reserved.
    This file is part of MLDB. Copyright 2016 mldb.ai inc. All rights reserved.

    Kernels for random forest algorithm.
*/

#pragma once

#define OPENCL_ENABLED 1


#include "randomforest_types.h"

namespace MLDB {
namespace RF {

#if OPENCL_ENABLED

std::pair<bool, int>
testFeatureKernelOpencl(Rows::RowIterator rowIterator,
                        size_t numRows,
                        const BucketList & buckets,
                        W * w /* buckets.numBuckets entries */);

std::tuple<double, int, int, W, W, std::vector<uint8_t> >
testAllOpenCL(int depth,
              const std::span<const Feature> & features,
              const Rows & rows,
              FrozenMemoryRegionT<uint32_t> bucketMemory);

ML::Tree::Ptr
trainPartitionedEndToEndOpenCL(int depth, int maxDepth,
                               ML::Tree & tree,
                               MappedSerializer & serializer,
                               const Rows & rows,
                               const std::span<const Feature> & features,
                               FrozenMemoryRegionT<uint32_t> bucketMemory,
                               const DatasetFeatureSpace & fs);

#endif // OPENCL_ENABLED

}  // namespace RF
}  // namespace MLDB