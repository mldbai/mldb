/** memory_region.h                                                -*- C++ -*-
    Jeremy Barnes, 27 March 2016
    This file is part of MLDB. Copyright 2016 mldb.ai inc. All rights reserved.

    Basic primitives around memory regions.  Once frozen, these are the
    representation that covers CPU memory, device memory and remote
    memory and implements the primitives that allow data to be made
    available and brought to the compute resources required.
*/

#pragma once

#include <memory>
#include "mldb/types/string.h"
#include "mldb/base/exc_assert.h"
#include "mldb/types/value_description_fwd.h"
#include "mldb/types/path.h"

namespace MLDB {

struct filter_ostream;
struct MappedSerializer;
struct StructuredSerializer;

/*****************************************************************************/
/* FROZEN MEMORY REGION                                                      */
/*****************************************************************************/

struct FrozenMemoryRegion {
    FrozenMemoryRegion() = default;

#if 0
    FrozenMemoryRegion(FrozenMemoryRegion&&) = default;
    FrozenMemoryRegion & operator = (FrozenMemoryRegion&&) = default;
#endif

    FrozenMemoryRegion(std::shared_ptr<void> handle,
                       const char * data,
                       size_t length);


    const char * data() const
    {
        return data_;
    }

    size_t length() const
    {
        return length_;
    }

    size_t memusage() const
    {
        return length();
    }

#if 0
    /** Re-serialize the block to the other serializer. */
    void reserialize(MappedSerializer & serializer) const;

    /** Re-serialize the block to the structured serializer, in the root
        of the current path. */
    void reserialize(StructuredSerializer & serializer) const;
#endif

private:
    const char * data_ = nullptr;
    size_t length_ = 0;
    std::shared_ptr<void> handle_;
};

/*****************************************************************************/
/* FROZEN MEMORY REGION                                                      */
/*****************************************************************************/

template<typename T>
struct FrozenMemoryRegionT {
    FrozenMemoryRegionT() = default;
    
    FrozenMemoryRegionT(FrozenMemoryRegion raw)
        : data_((const T *)raw.data()),
          length_(raw.length() / sizeof(T)),
          raw_(std::move(raw))
    {
    }

    const T * data() const
    {
        return data_;
    }

    size_t length() const
    {
        return length_;
    }
    
    size_t memusage() const
    {
        return length() * sizeof(T);
    }

    const FrozenMemoryRegion & raw() const
    {
        return raw_;
    }

    operator const FrozenMemoryRegion & () const
    {
        return raw();
    }

#if 0
    /** Re-serialize the block to the other serializer. */
    void reserialize(MappedSerializer & serializer) const
    {
        raw.reserialize(serializer);
    }

    /** Re-serialize the block to the structured serializer, in the root
        of the current path. */
    void reserialize(StructuredSerializer & serializer) const
    {
        raw.reserialize(serializer);
    }
#endif

    const T & operator [] (size_t index) const
    {
        ExcAssertLess(index, length_);
        return data()[index];
    }

private:
    const T * data_;
    size_t length_;
    FrozenMemoryRegion raw_;
};


/*****************************************************************************/
/* MUTABLE MEMORY REGION                                                     */
/*****************************************************************************/

struct MutableMemoryRegion {
    MutableMemoryRegion()
    {
    }

    MutableMemoryRegion(std::shared_ptr<void> handle,
                        char * data,
                        size_t length,
                        MappedSerializer * owner);

    char * data() const
    {
        return data_;
    }

    size_t length() const
    {
        return length_;
    }
    
    FrozenMemoryRegion freeze();

    std::shared_ptr<void> handle() const;

private:
    struct Itl;
    std::shared_ptr<Itl> itl;
    char * const data_ = nullptr;
    size_t const length_ = 0;
};

template<typename T>
struct MutableMemoryRegionT {
    MutableMemoryRegionT()
    {
    }

    MutableMemoryRegionT(MutableMemoryRegion raw)
        : raw(std::move(raw)),
          data_(reinterpret_cast<T *>(this->raw.data())),
          length_(this->raw.length() / sizeof(T))
    {
    }

    T * data() const
    {
        return data_;
    }

    size_t length() const
    {
        return length_;
    }
    
    FrozenMemoryRegionT<T> freeze()
    {
        return FrozenMemoryRegionT<T>(raw.freeze());
    }

private:
    MutableMemoryRegion raw;
    T * data_ = nullptr;
    size_t length_ = 0;
};


/*****************************************************************************/
/* MAPPED SERIALIZER                                                         */
/*****************************************************************************/

struct MappedSerializer {
    virtual ~MappedSerializer()
    {
    }

    /** Commit all changes; this normally means that the current block
        is finished writing.  It is guaranteed that no more changes
        will be made to any allocated blocks after this method is
        called.
    */
    virtual void commit() = 0;

    /** Allocate a writable block of memory with the given size and
        alignment.  The memory in the block can be written until the
        freeze() method is called.
    */
    virtual MutableMemoryRegion
    allocateWritable(uint64_t bytesRequired, size_t alignment) = 0;

    template<typename T>
    MutableMemoryRegionT<T>
    allocateWritableT(size_t numItems)
    {
        return allocateWritable(numItems * sizeof(T), alignof(T));
    }

    /** Freeze the given block of writable memory into a fixed, frozen
        representation of the same data.  For memory that is backed by
        disk, this may also mean writing it out in whatever is its
        customary form.  The memory may be moved around, etc.
        
        All references to the given region will become invalid once this is
        completed.
    */
    virtual FrozenMemoryRegion
    freeze(MutableMemoryRegion & region) = 0;

    /** Return a stream, that can be used to write an (unknown) number of
        bytes to the serializer.

        The stream must have close() called once it's finished being
        written to.

        Note that the filter_ostream is also a std::ostream, and so anything
        that can be done to one of those can be done to it.
    */
    virtual filter_ostream getStream();
};


/*****************************************************************************/
/* MEMORY SERIALIZER                                                         */
/*****************************************************************************/

/** Mapped serializer that puts things in memory. */

struct MemorySerializer: public MappedSerializer {
    virtual ~MemorySerializer()
    {
    }

    virtual void commit();

    virtual FrozenMemoryRegion freeze(MutableMemoryRegion & region);

    virtual MutableMemoryRegion
    allocateWritable(uint64_t bytesRequired,
                     size_t alignment);
};

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


/*****************************************************************************/
/* STRUCTURED SERIALIZER                                                     */
/*****************************************************************************/

/** Serializer that structures its entries (like a Zip file).  This broadly
    corresponds to a directory (it can contain other entries, but not
    data).
*/

struct StructuredSerializer {
    virtual std::shared_ptr<StructuredSerializer>
    newStructure(const PathElement & name) = 0;

    virtual std::shared_ptr<MappedSerializer>
    newEntry(const PathElement & name) = 0;

    virtual filter_ostream
    newStream(const PathElement & name) = 0;

    virtual void addRegion(const FrozenMemoryRegion & region,
                           const PathElement & name);

    //virtual void addValue(const PathElement & name);

    template<typename T>
    void newObject(const PathElement & name,
                   const T & val,
                   const std::shared_ptr<const ValueDescriptionT<T> > & desc
                       = getDefaultDescriptionSharedT<T>())
    {
        newObject(name, &val, *desc);
    }

    virtual void newObject(const PathElement & name,
                           const void * val,
                           const ValueDescription & desc);

    virtual void commit() = 0;
};


/*****************************************************************************/
/* ZIP STRUCTURED SERIALIZER                                                 */
/*****************************************************************************/

/** Structured serializer that writes a zip file. */

struct ZipStructuredSerializer: public StructuredSerializer {
    ZipStructuredSerializer(Utf8String filename);
    ~ZipStructuredSerializer();

    virtual std::shared_ptr<StructuredSerializer>
    newStructure(const PathElement & name);

    virtual std::shared_ptr<MappedSerializer>
    newEntry(const PathElement & name);

    virtual filter_ostream
    newStream(const PathElement & name);

    virtual void commit();

    ZipStructuredSerializer(ZipStructuredSerializer * parent,
                            PathElement relativePath);
private:
    struct Itl;
    struct BaseItl;
    struct RelativeItl;
    struct EntrySerializer;
    std::unique_ptr<Itl> itl;
};


} // namespace MLDB
