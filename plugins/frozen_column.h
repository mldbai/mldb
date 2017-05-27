/** frozen_column.h                                                -*- C++ -*-
    Jeremy Barnes, 27 March 2016
    This file is part of MLDB. Copyright 2016 mldb.ai inc. All rights reserved.

    Frozen (immutable), compressed column representations and methods
    to operate on them.
*/

#pragma once

#include "column_types.h"
#include "mldb/plugins/tabular_dataset.h"
#include <memory>


namespace MLDB {

struct TabularDatasetColumn;
struct filter_ostream;
struct MappedSerializer;

/*****************************************************************************/
/* MAPPED DEVICE                                                             */
/*****************************************************************************/

/** Represents a single device onto which data structures can be mapped. */

struct MappedDevice {
};


/*****************************************************************************/
/* MAPPED OBJECT                                                             */
/*****************************************************************************/

struct MappedObject {

    virtual ~MappedObject()
    {
    }

};


/*****************************************************************************/
/* FROZEN MEMORY REGION                                                      */
/*****************************************************************************/

struct FrozenMemoryRegion {
    FrozenMemoryRegion() = default;

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

    /** Re-serialize the block to the other serializer. */
    void reserialize(MappedSerializer & serializer) const;

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
          raw(std::move(raw))
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

    /** Re-serialize the block to the other serializer. */
    void reserialize(MappedSerializer & serializer) const
    {
        raw.reserialize(serializer);
    }

private:
    const T * data_;
    size_t length_;
    FrozenMemoryRegion raw;
};


/*****************************************************************************/
/* MUTABLE MEMORY REGION                                                     */
/*****************************************************************************/

struct MappedSerializer;

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

/** Serializer that structures its entries (like a Zip file). */

struct StructuredSerializer {
    virtual std::shared_ptr<StructuredSerializer>
    newStructure(const Utf8String & name) = 0;

    virtual std::shared_ptr<MappedSerializer>
    newEntry(const Utf8String & name) = 0;

    virtual filter_ostream
    newStream(const Utf8String & name) = 0;

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
    newStructure(const Utf8String & name);

    virtual std::shared_ptr<MappedSerializer>
    newEntry(const Utf8String & name);

    virtual filter_ostream
    newStream(const Utf8String & name);

    virtual void commit();

    ZipStructuredSerializer(ZipStructuredSerializer * parent,
                            Utf8String relativePath);
private:
    struct Itl;
    struct BaseItl;
    struct RelativeItl;
    struct EntrySerializer;
    std::unique_ptr<Itl> itl;
};


/*****************************************************************************/
/* MAPPED RECONSTITUTER                                                      */
/*****************************************************************************/

struct MappedReconstituter {
    virtual ~MappedReconstituter();
};


/*****************************************************************************/
/* COLUMN FREEZE PARAMETERS                                                  */
/*****************************************************************************/

/** Parameters used to control the freeze operation. */
struct ColumnFreezeParameters {
};


/*****************************************************************************/
/* FROZEN COLUMN                                                             */
/*****************************************************************************/

/// Base class for a frozen column
struct FrozenColumn: public MappedObject {
    FrozenColumn();

    virtual ~FrozenColumn()
    {
    }

    virtual std::string format() const = 0;

    virtual CellValue get(uint32_t rowIndex) const = 0;

    virtual size_t size() const = 0;

    virtual size_t memusage() const = 0;

    typedef std::function<bool (size_t rowNum, const CellValue & val)> ForEachRowFn;

    virtual bool forEach(const ForEachRowFn & onRow) const = 0;

    virtual bool forEachDense(const ForEachRowFn & onRow) const = 0;

    virtual bool
    forEachDistinctValue(std::function<bool (const CellValue &)> fn)
        const = 0;

    virtual ColumnTypes getColumnTypes() const = 0;

    virtual size_t nonNullRowCount() const = 0;

    /** Freeze the given column into the best fitting frozen column type. */
    static std::shared_ptr<FrozenColumn>
    freeze(TabularDatasetColumn & column,
           MappedSerializer & serializer,
           const ColumnFreezeParameters & params);

    virtual void serialize(MappedSerializer & serializer) const = 0;
};


/*****************************************************************************/
/* FROZEN COLUMN FORMAT                                                      */
/*****************************************************************************/

/** This describes a format of frozen column.  These can be registered to
    provide cusomized compression for given formats of columns.
*/

struct FrozenColumnFormat {

    virtual ~FrozenColumnFormat();

    /** Return the name of this frozen column format. */
    virtual std::string format() const = 0;

    /** Quick test that tells us whether this particular column format can
        store the given column.  It may update cachedInfo to store information
        that is useful in the columnSize() and freeze() functions.
    */
    virtual bool isFeasible(const TabularDatasetColumn & column,
                            const ColumnFreezeParameters & params,
                            std::shared_ptr<void> & cachedInfo) const = 0;

    static constexpr const size_t CANT_STORE = -2;
    static constexpr const size_t NOT_BEST = -1;
    
    /** Calculate how much data will be required to store the given column
        in this column format.  If it's unable to, or can't beat the previousBest,
        then it should return CANT_STORE or NOT_BEST.

        It can both read and update the cachedInfo, which has been provided
        by the isFeasible method.

        If this method returns a positive value, it must be able to serialize
        the data using the freeze() method.
    */
    virtual ssize_t columnSize(const TabularDatasetColumn & column,
                               const ColumnFreezeParameters & params,
                               ssize_t previousBest,
                               std::shared_ptr<void> & cachedInfo) const = 0;
    
    static std::pair<ssize_t,
                     std::function<std::shared_ptr<FrozenColumn> (TabularDatasetColumn & column, MappedSerializer & serializer)> >
    preFreeze(const TabularDatasetColumn & column,
              const ColumnFreezeParameters & params);

    /** Freeze the given column as this particular column format.  It has access
        to the cachedInfo that isFeasible and columnSize have provided.  This
        method should not fail unless there is an error in the underlying
        layers, eg a memory allocation error, in which case it should
        throw an exception.
    */
    virtual FrozenColumn *
    freeze(TabularDatasetColumn & column,
           MappedSerializer & serializer,
           const ColumnFreezeParameters & params,
           std::shared_ptr<void> cachedInfo) const = 0;
    
    /** Reconstitute a mapped version of the given frozen column. */
    virtual FrozenColumn *
    reconstitute(MappedReconstituter & reconstituter) const = 0;
    
    /** Register a new column format.  Returns a handle that, once released,
        will de-register the column format.
    */
    static std::shared_ptr<void>
    registerFormat(std::shared_ptr<FrozenColumnFormat> format);
};


/*****************************************************************************/
/* REGISTER FROZEN COLUMN FORMAT TEMPLATE                                    */
/*****************************************************************************/

/** Helper class that allows a frozen column format to be registered. */

template<typename Format>
struct RegisterFrozenColumnFormatT {
    RegisterFrozenColumnFormatT()
    {
        handle = FrozenColumnFormat
            ::registerFormat(std::make_shared<Format>());
    }
    
    std::shared_ptr<void> handle;
};

} // namespace MLDB

