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

    /** Create a new serializer that writes to a file.  The file at filename
        will be deleted unless exclusive is true (in which case an exception
        will be thrown if the file already exists).
        
        If sequential is true, then the file will be set up to write
        sequential, contiguous blocks that allow for merging of memory
        regions and alignment must always be 1.

        If sync is true, then commit will not return until the OS has confirmed
        that all of the data has been synced to disk.  Otherwise, the OS
        will be allowed to sync on its own schedule.
    */
    FileSerializer(Utf8String filename,
                   bool exclusive = false,
                   bool sync = false);

    virtual ~FileSerializer();

    /** Commit the file.  If sync is true, then this may take a long time
        as it will wait for the OS to confirm that all of the data is
        written to disk.
    */
    virtual void commit();

    virtual MutableMemoryRegion
    allocateWritable(uint64_t bytesRequired,
                     size_t alignment);

    virtual FrozenMemoryRegion freeze(MutableMemoryRegion & region);

private:
    struct Itl;
    std::unique_ptr<Itl> itl;
};


/*****************************************************************************/
/* TEMPORARY FILE SERIALIZER                                                 */
/*****************************************************************************/

/** Serializer that allocates sequential blocks from a memory mapped region.
    When the serializer is destroyed, the blocks disappear as well.
*/

struct TemporaryFileSerializer: public MappedSerializer {
    TemporaryFileSerializer(Utf8String directory,
                            Utf8String reason);

    virtual ~TemporaryFileSerializer();

    virtual void commit();

    virtual MutableMemoryRegion
    allocateWritable(uint64_t bytesRequired,
                     size_t alignment);

    virtual FrozenMemoryRegion freeze(MutableMemoryRegion & region);

    /** Return a region covering previously frozen memory.  It does not
        have to be aligned to the same blocks that were allocated,
        due to the sequentual nature.
    */
    virtual std::pair<uint64_t, FrozenMemoryRegion>
    getRangeContaining(size_t offset, size_t length) const;

    /** Return the total number of bytes currently serialized. */
    virtual size_t getLength() const;
    
private:
    struct Itl;
    std::unique_ptr<Itl> itl;
};


} // namespace MLDB
