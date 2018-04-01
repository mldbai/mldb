/** file_serializer.h                                               -*- C++ -*-
    Jeremy Barnes, 27 March 2016
    This file is part of MLDB. Copyright 2016 mldb.ai inc. All rights reserved.

    Structured serializer/reconstituter for plain files.
*/

#pragma once

#include "memory_region.h"


namespace MLDB {


/*****************************************************************************/
/* FILE SERIALIZER                                                           */
/*****************************************************************************/

/** Mapped serializer that allocates things from a file that is then memory
    mapped.  This allows for unused data to be paged out.
*/

struct FileSerializer: public MappedSerializer {
    FileSerializer(Utf8String filename);

    virtual ~FileSerializer();

    virtual void commit();

    virtual MutableMemoryRegion
    allocateWritable(uint64_t bytesRequired,
                     size_t alignment);

    virtual FrozenMemoryRegion freeze(MutableMemoryRegion & region);

private:
    struct Itl;
    std::unique_ptr<Itl> itl;
};



} // namespace MLDB
