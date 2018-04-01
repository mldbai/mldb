/** memory_region_impl.h                                           -*- C++ -*-
    Jeremy Barnes, 27 March 2016
    This file is part of MLDB. Copyright 2016 mldb.ai inc. All rights reserved.

    Common memory region code for implementation.
*/

#pragma once

#include "memory_region.h"

namespace MLDB {


/*****************************************************************************/
/* SERIALIZER STREAM HANDLER                                                 */
/*****************************************************************************/

/** Helper class for implementations of serializers that write to streams.

    Simply caches the data in a stream and writes the contents to a frozen
    block from the destructor.

    Better versions are possible for when the data to be written is very
    large.
*/

struct SerializerStreamHandler {
    MappedSerializer * owner = nullptr;
    std::ostringstream stream;
    std::shared_ptr<void> baggage;  /// anything we need to keep to stay alive

    ~SerializerStreamHandler()
    {
        // we now need to write our stream to the serializer
        auto mem = owner->allocateWritable(stream.str().size(),
                                           1 /* alignment */);
        std::memcpy(mem.data(), stream.str().data(), stream.str().size());
        mem.freeze();
    }
};

} // namespace MLDB

