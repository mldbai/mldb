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
#include <functional>
#include <span>
#include "mldb/types/string.h"
#include "mldb/base/exc_assert.h"
#include "mldb/types/value_description_fwd.h"
#include "mldb/arch/demangle.h"
#include "mldb/types/path.h"
#include "mldb/types/url.h"
#include "memory_region_fwd.h"

namespace MLDB {

struct filter_ostream;
struct filter_istream;
struct MappedSerializer;
struct StructuredSerializer;


/*****************************************************************************/
/* DIRECTORY                                                                 */
/*****************************************************************************/

/** This is where we define a block naming scheme.

    To access a block, there is:
    - A path schema
    - A set of parameters to that path (for example, numbers for arrays, etc);
      these can be ranges
    - A version for the block

    Blocks are named as follows:
    - <scheme>://path/to/object (simple block)
    - <scheme>://path/to/object?ver=<version> (versioned block)
    - <scheme>://path/to/object?range=123-456 (range of bytes)
    - <scheme1>+<scheme2>://path/in/object2#path/in/object1 (nested path)
    - op://function(block1,block2) (output of function)

    Each time a block gets created, it gets a version.  That can either be
    a hash (deterministic) of the contents, or a guaranteed globally unique
    identifier.

*/


/*****************************************************************************/
/* ADDRESS SPACE                                                             */
/*****************************************************************************/

/** Handle to a memory pool, which is a set of memory attached by some kind
    of a bus to one or more devices.
*/

struct AddressSpace {
};


/*****************************************************************************/
/* MEMORY REGION HANDLE                                                      */
/*****************************************************************************/

enum MemoryRegionState : uint8_t {
    NEW,         ///< Newly created, does not yet have a size etc
    ALLOCATED,   ///< Memory is allocated and can be written from one place
    FROZEN,      ///< Memory is frozen and is now immutable, cacheable
    ZOMBIE,      ///< Memory has been deleted but is still referenced
    DELETED      ///< Memory has been disposed of
};


/** This is a handle to an abstract memory region.  It can be used to refer
    to the memory region and to perform operations on it.

    Note that, in general, operations need to be queued on a device's work
    queue in order to run; they cannot be run imperatively in general.
*/

struct MemoryRegionHandleInfo {
    virtual ~MemoryRegionHandleInfo() = default;  // so we can safely upcast
    const std::type_info * type = nullptr;  //< non-CV qualified type in the array
    bool isConst = true;              //< Is the referred to memory constant or mutable?
    ssize_t lengthInBytes = -1;
};

struct MemoryRegionHandle {
    MemoryRegionHandle() = default;
    MemoryRegionHandle(std::shared_ptr<const MemoryRegionHandleInfo> handle)
        : handle(std::move(handle))
    {
        ExcAssert(this->handle);
        ExcAssertGreaterEqual(this->handle->lengthInBytes, 0);
    }

    template<typename T>
    void checkTypeAccessibleAs() const
    {
        if (!handle || !handle->type)
            return;
        if (*handle->type != typeid(std::remove_const_t<T>)) {
            throw MLDB::Exception("Attempt to cast to wrong type: from " + demangle(handle->type->name())
                                  + " to " + type_name<T>());
        }
    }

    size_t lengthInBytes() const
    {
        ExcAssertGreaterEqual(this->handle->lengthInBytes, 0);
        return this->handle->lengthInBytes;        
    }

    std::shared_ptr<const MemoryRegionHandleInfo> handle;  // opaque; upcast by context
};

template<> struct ValueDescriptionT<MemoryRegionHandle>;
DECLARE_VALUE_DESCRIPTION(MemoryRegionHandle);

template<typename T>
struct MemoryArrayHandleT: public MemoryRegionHandle {
    MemoryArrayHandleT() = default;
    MemoryArrayHandleT(std::shared_ptr<const MemoryRegionHandleInfo> handle)
        : MemoryRegionHandle(std::move(handle))
    {
    }

    size_t length() const
    {
        return this->lengthInBytes() / sizeof(T);
    }
};

template<typename T>
ValueDescriptionT<MemoryRegionHandle> *
getDefaultDescription(MemoryArrayHandleT<T> * p);

template<typename T>
std::shared_ptr<const ValueDescriptionT<MemoryRegionHandle>>
getDefaultDescriptionShared(MemoryArrayHandleT<T> * p);

/*****************************************************************************/
/* BLOCK CONTEXT                                                             */
/*****************************************************************************/

struct BlockContext {
    /** Allocate a memory region for the given number of bytes. */
    MemoryRegionHandle allocate(size_t bytes);

    /** Allocate and initialize the given region from the host.  It remains
        writeable.
    */
    MemoryRegionHandle fromHost(std::shared_ptr<char> data,
                                size_t bytes);

    /** Allocate and freeze the given region from the host.  It is no longer
        writeable.
    */
    MemoryRegionHandle fromHost(std::shared_ptr<const char> data,
                                size_t bytes);
};


/*****************************************************************************/
/* FROZEN MEMORY REGION                                                      */
/*****************************************************************************/

struct FrozenMemoryRegion {
    FrozenMemoryRegion() = default;

#if 0
    FrozenMemoryRegion(FrozenMemoryRegion&&) = default;
    FrozenMemoryRegion & operator = (FrozenMemoryRegion&&) = default;
#endif

    FrozenMemoryRegion(std::shared_ptr<const void> handle,
                       const char * data,
                       size_t length) noexcept;

    const char * data() const noexcept
    {
        return data_;
    }

    size_t length() const noexcept
    {
        return length_;
    }

    size_t memusage() const noexcept
    {
        return length();
    }

    /** Return another frozen memory region that points to a subset
        range of the current one.
    */
    FrozenMemoryRegion range(size_t start, size_t end) const;

    /** Return the given number of bytes at the start. */
    FrozenMemoryRegion rangeAtStart(size_t length) const;

    /** Return the given number of bytes at the end. */
    FrozenMemoryRegion rangeAtEnd(size_t length) const;
    
    /** This tests whether the region is initialized or not.  A
        non-initialized region has either been constructed from the
        default constructor or moved from.
    */
    explicit operator bool () const noexcept { return !!handle_; }
    
    /// Return a combined, contiguous memory region.  In the general case
    /// this may require reallocation of a new region and copying; however
    /// the implementation will attempt to minimise the amount of copying
    /// required.
    static FrozenMemoryRegion
    combined(const FrozenMemoryRegion & region1,
             const FrozenMemoryRegion & region2);

    static FrozenMemoryRegion
    combined(const std::vector<FrozenMemoryRegion> & regions);

#if 0
    /** Re-serialize the block to the other serializer. */
    void reserialize(MappedSerializer & serializer) const;

    /** Re-serialize the block to the structured serializer, in the root
        of the current path. */
    void reserialize(StructuredSerializer & serializer) const;
#endif

    std::shared_ptr<const void> handle() const { return handle_; }

private:
    const char * data_ = nullptr;
    size_t length_ = 0;
    std::shared_ptr<const void> handle_;
};


/*****************************************************************************/
/* FROZEN MEMORY REGION TYPED                                                */
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

    std::span<const T> getConstSpan(size_t start = 0, ssize_t length = -1) const
    {
        if (start > this->length()) {
            throw MLDB::Exception("getConstSpan: start is out of range");
        }
        if (length == -1)
            length = this->length() - start;
        if (length < 0 || start + length > this->length()) {
            throw MLDB::Exception("getConstSpan: length is out of range");
        }
        return { data() + start, (size_t)length };
    }

    operator std::span<const T> () const
    {
        return getConstSpan();
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
    const T * data_ = nullptr;
    size_t length_ = 0;
    FrozenMemoryRegion raw_;
};


FrozenMemoryRegion
mapFile(const Url & filename, size_t startOffset = 0,
        ssize_t length = -1);


/*****************************************************************************/
/* MUTABLE MEMORY REGION                                                     */
/*****************************************************************************/

struct MutableMemoryRegion {
    MutableMemoryRegion()
    {
    }

    MutableMemoryRegion(std::shared_ptr<const void> handle,
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

    MutableMemoryRegion range(size_t startByte, size_t endByte) const;
    
    FrozenMemoryRegion freeze();

    std::shared_ptr<const void> handle() const;

    // Reset the region (stealing the handle) to be null.  This is mostly
    // designed to be used by the MappedSerializer class and its descendents.
    std::shared_ptr<const void> reset();

private:
    struct Itl;
    std::shared_ptr<Itl> itl;
    char * data_ = nullptr;
    size_t length_ = 0;
};


/*****************************************************************************/
/* MUTABLE MEMORY REGION TYPED                                               */
/*****************************************************************************/

template<typename T>
struct MutableMemoryRegionT {
    MutableMemoryRegionT()
    {
    }

    MutableMemoryRegionT(MutableMemoryRegion raw)
        : raw_(std::move(raw)),
          data_(reinterpret_cast<T *>(this->raw_.data())),
          length_(this->raw_.length() / sizeof(T))
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

    MutableMemoryRegionT<T> rangeBytes(size_t start, size_t end) const
    {
        MutableMemoryRegionT result(raw_.range(start, end));
        return result;
    }
    
    FrozenMemoryRegionT<T> freeze()
    {
        return FrozenMemoryRegionT<T>(raw_.freeze());
    }

    const MutableMemoryRegion & raw() const { return raw_; }

    std::span<T> getSpan(size_t start = 0, ssize_t length = -1) const
    {
        if (start > this->length()) {
            throw MLDB::Exception("getSpan: start is out of range");
        }
        if (length == -1)
            length = this->length() - start;
        if (length < 0 || start + length > this->length()) {
            throw MLDB::Exception("getSpan: length is out of range");
        }
        return { data() + start, (size_t)length };
    }

    std::span<const T> getConstSpan(size_t start = 0, ssize_t length = -1) const
    {
        if (start > this->length()) {
            throw MLDB::Exception("getConstSpan: start is out of range");
        }
        if (length == -1)
            length = this->length() - start;
        if (length < 0 || start + length > this->length()) {
            throw MLDB::Exception("getConstSpan: length is out of range");
        }
        return { data() + start, (size_t)length };
    }

private:
    MutableMemoryRegion raw_;
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
        freeze() method is called.  The memory is uninitialized.
    */
    virtual MutableMemoryRegion
    allocateWritable(uint64_t bytesRequired, size_t alignment) = 0;

    template<typename T>
    MutableMemoryRegionT<T>
    allocateWritableT(size_t numItems,
                      size_t alignment = alignof(T))
    {
        return allocateWritable(numItems * sizeof(T), alignment);
    }

    /** Allocate a writable block of memory with the given size and
        alignment.  The memory in the block can be written until the
        freeze() method is called.  It is guaranteed filled with zeros.
    */
    virtual MutableMemoryRegion
    allocateZeroFilledWritable(uint64_t bytesRequired, size_t alignment);

    template<typename T>
    MutableMemoryRegionT<T>
    allocateZeroFilledWritableT(size_t numItems,
                                size_t alignment = alignof(T))
    {
        return allocateZeroFilledWritable(numItems * sizeof(T), alignment);
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

    /** Copy the given memory region into the current serializer.  In certain
        circumstances (between two files or two memory buffers aligned on
        page boundaries), this can be very efficient as the filsystem or
        virtual memory subsystem can do part of the work.

        Default implementation simply creates a new region, copies the
        memory, and writes it back.
    */
    virtual FrozenMemoryRegion
    copy(const FrozenMemoryRegion & region);
    
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
/* STRUCTURED SERIALIZER                                                     */
/*****************************************************************************/

/** Serializer that structures its entries (like a Zip file).  This broadly
    corresponds to a directory (it can contain other entries, but not
    data).
*/

struct StructuredSerializer {
    virtual ~StructuredSerializer() = default;
    
    virtual std::shared_ptr<StructuredSerializer>
    newStructure(const PathElement & name) = 0;

    virtual std::shared_ptr<MappedSerializer>
    newEntry(const PathElement & name) = 0;

    virtual filter_ostream
    newStream(const PathElement & name) = 0;

    virtual void
    addRegion(const FrozenMemoryRegion & region,
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
/* STRUCTURED RECONSTITUTER                                                  */
/*****************************************************************************/

struct StructuredReconstituter {
    virtual ~StructuredReconstituter();

    struct Entry {
        PathElement name;
        std::function<FrozenMemoryRegion ()> getBlock;
        std::function<std::shared_ptr<StructuredReconstituter> ()> getStructure;
    };

    virtual std::vector<Entry> getDirectory() const = 0;

    virtual Utf8String getContext() const = 0;
    
    virtual filter_istream
    getStream(const PathElement & name) const;

    virtual filter_istream
    getStreamRecursive(const Path & name) const;
    
    virtual std::shared_ptr<StructuredReconstituter>
    getStructure(const PathElement & name) const = 0;

    virtual std::shared_ptr<StructuredReconstituter>
    getStructureRecursive(const Path & name) const;

    template<typename T>
    void getObject(const PathElement & name, T & obj,
                   std::shared_ptr<const ValueDescription> desc
                   = getDefaultDescriptionSharedT<T>()) const
    {
        this->getObjectHelper(name, &obj, desc);
    }

    template<typename T>
    T getObject(const PathElement & name,
                std::shared_ptr<const ValueDescription> desc
                   = getDefaultDescriptionSharedT<T>()) const
    {
        T result;
        this->getObjectHelper(name, &result, desc);
        return result;
    }

    virtual FrozenMemoryRegion
    getRegion(const PathElement & name) const = 0;

    virtual FrozenMemoryRegion
    getRegionRecursive(const Path & name) const;

    template<typename T>
    FrozenMemoryRegionT<T>
    getRegionT(const PathElement & name) const
    {
        return getRegion(name);
    }

    template<typename T>
    FrozenMemoryRegionT<T>
    getRegionRecursiveT(const Path & name) const
    {
        return getRegionRecursive(name);
    }

private:
    void
    getObjectHelper(const PathElement & name, void * obj,
                    const std::shared_ptr<const ValueDescription> & desc) const;
};

} // namespace MLDB
