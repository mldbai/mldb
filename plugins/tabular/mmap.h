/* mmap.h                                              -*- C++ -*-
   Jeremy Barnes, 6 November 2015
   Copyright (c) 2015 mldb.ai inc.  All rights reserved.

   This file is part of MLDB. Copyright 2015 mldb.ai inc. All rights reserved.
*/

#pragma once

#include <iostream>
#include <iomanip>
#include <cassert>
#include <span>
#include <any>
#include <cstddef>
#include <unordered_map>
#include "mldb/arch/demangle.h"
#include "mldb/utils/min_max.h"
#include "mldb/types/value_description_fwd.h"
#include "mldb/base/exc_assert.h"

namespace MLDB {

struct MemUsageOptions;

template<typename T>
std::shared_ptr<const ValueDescription> getDescriptionMaybeT()
{
    return getDefaultDescriptionSharedGenericMaybe((T*)0);
}

struct MappingOptionBase {
    MappingOptionBase(std::string name, std::any defValue);

    // Don't allow misuse of the option
    MappingOptionBase(const MappingOptionBase &) = delete;
    MappingOptionBase(MappingOptionBase &&) = delete;
    void operator = (const MappingOptionBase &) = delete;
    void operator = (MappingOptionBase &&) = delete;
 
    std::string name_;
    std::any defValue_;
    const std::any & defValue() const { return defValue_; }

    struct Value {
        const MappingOptionBase * option = nullptr;
        std::any value;
        operator std::pair<const MappingOptionBase *, std::any> () const { return { option, value }; }
    };
};

template<typename ValueType>
struct MappingOption: public MappingOptionBase {
    explicit MappingOption(const char * name, ValueType defValue = ValueType())
        : MappingOptionBase(name, defValue)
    {
    }

    template<typename T, typename Enable = std::enable_if_t<std::is_convertible_v<T, ValueType>>>
    Value operator = (const T & val) const
    {
        return Value{this, (ValueType)val};
    }
};

// Standard options
extern const MappingOption<bool> TRACE;  // Do we trace this serialization or not?
extern const MappingOption<bool> DEBUG;  // Do we debug the mapping or not?
extern const MappingOption<bool> VALIDATE;  // Do we validate that our output = our input?

extern const MappingOption<bool> DUMP_MEMORY_MAP;  // Do we dump a map of the whole mapping?
extern const MappingOption<bool> DUMP_TYPE_STATS;  // Do we dump stats on the types?

// Context used to track the memory we're using to freeze a structure in place for
// memory mapping.  This is used to allocate the original memory block and to allocate
// chunks of memory from it in an arena allocator.
//
// Actual freezing occurs using the freeze(context &, output &, const input &) function.  That
// function will allocate whatever indirect memory is required, and initialize the
// (uninitialized) output object with pointers to it.
//
// Looking deeper, a memory mapped object has data members (which have a fixed size),
// and is mapped directly in place (where its this pointer is pointing to the place
// in memory where it's mapped).  But objects may also have indirect memory, which
// is accessed via pointers; for this to happen the object will include pointers.
//
// For memory mapped objects, we don't ever know where the memory will be mapped, so
// we can't use pointers.  But we can use relative offsets, which provide the distance
// (which never changes due to fixed layout) from the address of the pointer.  This enables
// us to find the object.
//
// The other alternative would be to have a "base pointer" which we pass around to give
// the beginning of the memory block we're interested in, and store the offset from the
// base.  But that would require a very unnatural programming style to pass that pointer
// through, as it would need to be an argument to every function (or stored in thread
// local storage or another hack like that).
//
// For more concreteness, let's say that we had an object that contains two pointers,
// each of which points to a 1k block of memory:
//
// struct TwoPointers {
//     MappedPtr<int> p1; // points to 1000 byte block
//     MappedPtr<int> p2; // points to a different 1000 byte block
// } obj;
// 
// The memory will be laid out like this, with the value 8 in p1, and 1004 in p2
// addr
//    0  +-----------+             <- this pointer of obj, and obj.p1, at address base + 0
//       |     8     |--+   points to address 0 + 8 = 8, which is first 1k block
//    4  +-----------+  |          <- this pointer of obj.p1, at address base + 4
//       |  1004     |--|--+
//    8  +-----------+<-+  |
//       |   first   |     |
//       :    1 k    :     | points to address 4 + 1004 = 1008, which is second 1k block
//       |   block   |     |
// 1008  +-----------+<----+
//       |   second  |     
//       :    1 k    :     
//       |   block   |     
// 2008  +-----------+
//
// Similar ideas are explored in the boost::serialization framework.

struct MappingContext {
    static constexpr size_t SCALAR = MAX_LIMIT;

    enum Directness {
        DIRECT = 0,   // Storage of the object itself
        INDIRECT = 1  // Storage of memory pointed to by the object
    };

    // Initialize the context, with a maximum mapped size.  If we attempt to freeze objects
    // that don't fit within that size, we will get an exception.
    MappingContext(size_t maxSize = 1 << 30);

    template<typename... Options>
    MappingContext(size_t maxSize, Options&&... options)
        : MappingContext(maxSize)
    {
        addOptions(localState_->options, std::forward<Options>(options)...);
    }

    template<typename... Options>
    MappingContext(MappingOptionBase::Value option1, Options&&... options)
        : MappingContext()
    {
        addOptions(localState_->options, std::move(option1), std::forward<Options>(options)...);
    }

    // Constructor for a child context with the given options set
    MappingContext(MappingContext * parent, std::unordered_map<const MappingOptionBase *, std::any> options = {});

    // Constructor for a child context with the given addition to the path, and the given options set
    MappingContext(MappingContext * parent, std::string name, const std::type_info * type,
                   std::shared_ptr<const ValueDescription> desc,
                   size_t arrayLen = SCALAR, Directness directness = DIRECT,
                   std::unordered_map<const MappingOptionBase *, std::any> options = {});

    ~MappingContext();

    struct Stats;  // Internal structure holding statistics of mapped objects

    // This structure describes the shared state of the memory mapping: basically how much memory we've
    // allocated so far.
    struct SharedState {
        std::shared_ptr<std::byte> mem_;  // Memory block we've allocated
        size_t offset_ = 0;               // How much of it we've used so far
        size_t len_ = 0;                  // How much memory we have in total

        std::shared_ptr<Stats> stats;     // For when statistics are enabled
    };
    std::shared_ptr<SharedState> sharedState_;           // Shared state for the whole context

    struct LocalState {
        ~LocalState();                          // used to implement a scope exit hook
        MappingContext * parent = nullptr;      // Parent context (or null of this is the root context)
        size_t startOffset = 0;                 // Offset where this scope starts
        std::unordered_map<const MappingOptionBase *, std::any> options;  // Options for this level of the scope
        std::string name;                       // Name of the field of this object (if there is one)
        const std::type_info * type = nullptr;  // Type of object we're serializing (if there is one)
        std::shared_ptr<const ValueDescription> info; // Value description for contained type
        Directness directness = DIRECT;         // Is this the object itself, or the memory pointed to by the object?
        size_t arrayLen = SCALAR;               // Length of array when it's an array allocation
    };

    std::shared_ptr<LocalState> localState_;

    // Called once a child scope exits.  Can be used to gather statistics.
    void childExitScope(const LocalState & state);

    // Path of objects to get here
    std::vector<std::string> path() const;

    void setOption(MappingOptionBase::Value value);
    void unsetOption(const MappingOptionBase & option);
    const std::any & getOption(const MappingOptionBase & option) const;

    template<typename T>
    const T & getOption(const MappingOption<T> & option) const
    {
        return std::any_cast<const T &>(getOption((const MappingOptionBase &)option));
    }

    // Allocate an (uninitialized) type T
    template<typename T>
    T & alloc()
    {
        return *array<T>(1);
    }

    // Allocate an (uninitialized) type T as a field
    template<typename T>
    T & alloc_field(std::string name)
    {
        align<alignof(T)>();
        auto newContext = child<T>(name);
        return *newContext.template array<T>(1);
    }

    // Allocate an initialized array of n objects of type T, taking care of alignment,
    // initializd form an input array
    template<typename T>
    T * array(const T * data, size_t n)
    {
        T * result = (T *)malloc_aligned<alignof(T)>(n * sizeof(T));
        for (size_t i = 0;  i < n;  ++i) {
            freeze(*this, result[i], data[i]);
        }
        return result;
    }

    template<typename T>
    T * array_field(std::string name, const T * data, size_t n)
    {
        this->align<alignof(T)>();
        auto newContext = child_array<T>(name, n);
        return newContext.array(data, n);
    }

    // Allocate an uninitialized array of n objects of type T, taking care of alignment.
    template<typename T>
    T * array(size_t n)
    {
        return (T *)malloc_aligned<alignof(T)>(n * sizeof(T));
    }

    template<typename T>
    T * array_field(std::string name, size_t n)
    {
        auto newContext = child_array<T>(name, n);
        return newContext.template array<T>(n);
    }

    // Align to the given alignment
    template<int Align>
    void align()
    {
        static_assert(Align > 0, "can't align to zero");
        int aoffset = sharedState_->offset_ % Align; // current offset from alignment
        int padding = (Align - aoffset) % Align;  // how many we need to skip to get the right alignment
        ExcAssert((sharedState_->offset_ + padding) % Align == 0);
        if (padding > 0)
            malloc(padding);  // skip to ensure we're aligned
    }

    void align(size_t n)
    {
        if (n == 0)
            MLDB_THROW_RUNTIME_ERROR("can't alignt to zero");
        int aoffset = sharedState_->offset_ % n; // current offset from alignment
        int padding = (n - aoffset) % n;  // how many we need to skip to get the right alignment
        ExcAssert((sharedState_->offset_ + padding) % n == 0);
        if (padding > 0)
            malloc(padding);  // skip to ensure we're aligned
    }

    // Allocate a block with the given alignment
    template<int AlignTo>
    std::byte * malloc_aligned(size_t size)
    {
        align<AlignTo>();
        return malloc(size);
    }

    // Allocate an unaligned block of the given size
    std::byte * malloc(size_t size);

    // Assert that the given object is insize the arena.  Since the pointers
    // don't work when outside, this ensures that we don't make any mistakes
    // by pointing to objects allocated in a different arena or on the stack.
    //
    // Note that we allow things to be at the end of the arena, as zero-length
    // objects need to be able to be somewhere.
    void assertInArena(const void * obj);

    // Assert that this object is in the arena and is writable (in other words,
    // the object it refers to hasn't been finalized yet).
    void assertWritable(void * obj);

    // For debugging, tag with a string, which will be visible inside the mapped file
    // to understand its content.  This won't affect the functionality of the file
    // as the string will be inserted into "lost space" in the file.
    //void tag(const char * str);

    // Return the current offset in the array.  Mostly used for debugging or calculting
    // the storage efficiency.
    size_t getOffset() const;

    // Return the memory at the given offset in the array.  Mostly used for
    // debugging or calculating the storage efficiency.
    const std::byte * getMemory(size_t offset = 0) const;

    static void addOptions(std::unordered_map<const MappingOptionBase *, std::any> & /* optMap */)
    {
    }

    template<typename... Rest>
    static void addOptions(std::unordered_map<const MappingOptionBase *, std::any> & optMap,
                           const MappingOptionBase::Value & first,
                           Rest&&... rest)
    {
        optMap[first.option] = first.value;
        addOptions(optMap, std::forward<Rest>(rest)...);
    }

    template<typename... Options>
    MappingContext with(Options&&... options)
    {
        std::unordered_map<const MappingOptionBase *, std::any> optMap;
        addOptions(optMap, std::forward<Options>(options)...);
        return MappingContext(this, std::move(optMap));
    }

    template<typename ChildType, typename... Options>
    MappingContext child(const std::string & name, Options&&... options)
    {
        std::unordered_map<const MappingOptionBase *, std::any> optMap;
        addOptions(optMap, std::forward<Options>(options)...);
        return MappingContext(this, name, &typeid(ChildType), getDescriptionMaybeT<ChildType>(), SCALAR /* arrayLen */, DIRECT, std::move(optMap));
    }

    template<typename ChildType, typename... Options>
    MappingContext indirect_child(const std::string & name, Options&&... options)
    {
        std::unordered_map<const MappingOptionBase *, std::any> optMap;
        addOptions(optMap, std::forward<Options>(options)...);
        return MappingContext(this, name, &typeid(ChildType), getDescriptionMaybeT<ChildType>(), SCALAR /* arrayLen */, INDIRECT, std::move(optMap));
    }

    template<typename ChildType, typename... Options>
    MappingContext child_array(const std::string & name, size_t arrayLen, Options&&... options)
    {
        std::unordered_map<const MappingOptionBase *, std::any> optMap;
        addOptions(optMap, std::forward<Options>(options)...);
        return MappingContext(this, name, &typeid(ChildType), getDescriptionMaybeT<ChildType>(), arrayLen, DIRECT, std::move(optMap));
    }
};

// We can freeze any type which is convertible to another type.  TODO: this should
// only work for POD types.
template<typename T0, typename T1, typename E = typename std::enable_if<std::is_convertible_v<T1, T0>>::type>
void freeze(MappingContext & /* context */, T0 & output, const T1 & input)
{
    output = input;
}

// We can freeze any type which is convertible to another type.  TODO: this should
// only work for POD types.
template<typename T, typename E = typename std::enable_if<std::is_trivially_copyable_v<T>>::type>
void freeze(MappingContext & /* context */, T & output, const T & input)
{
    output = input;
}

// Convenience function to freeze a child field
template<typename Output, typename Input>
inline void freeze_field(MappingContext & context, std::string field, Output & output, Input && input)
{
    auto childContext = context.indirect_child<Output>(field);
    freeze(childContext, output, input);
}

void setMappedPtr(uint32_t * ofs, const void * ptr, size_t align);
void setMappedPtr(int32_t * ofs, const void * ptr, size_t align);
void setMappedPtr(uint64_t * ofs, const void * ptr, size_t align);
void setMappedPtr(int64_t * ofs, const void * ptr, size_t align);

// Mapped pointer structure.  This is a relative pointer that takes an offset from
// the address of the pointer's object rather than containing the actual address,
// which allows it to work in a mapped context.
//
// There are some tricks we use to emable smaller offsets to be used:
//
// * We store the offset as a multiple of the alignment, not a number of bytes.
// * We store the offset as a signed number by default, not an unsigned number.
//   This is because the vast majority of use-cases have the owning object
//   allocated before its child objects, and so the pointers all point forwards.
//   For use-cases involving cyclic or shared structures, it is possible to
//   override the template parameter to use a signed integer.
// * By default, we use a 32 bit offset.  This enables us to save space, but limits
//   the size of the file that can be written.  A 64 bit offset can be used by
//   modifying the template parameter.
//
// The result of all of this is that by default, pointers to 4 byte aligned objects
// can work up to 16GB files, for 8 byte aligned objects up to 32GB files, and for
// 16 byte aligned objects up to 64GB files.  If a very large mapped file is required,
// it's best to have a directory at the start to multiple root objects with a large
// alignment (eg, 256 ot 4096 bytes) and have each of these contain chunks of less
// than 16GB of data; that way it's possible to work with 32 bit pointers but have
// terabytes of data mapped.

template<typename T, typename Ofs = uint32_t>
struct MappedPtr {
    Ofs ofs = 0;
    std::byte data_[0];

    static_assert(sizeof(T) % alignof(T) == 0, "size must be a multiple of alignment");

    const std::byte * getBase() const
    {
        size_t s = reinterpret_cast<size_t>(data_);
        s = (s + alignof(T) - 1) / alignof(T) * alignof(T);
        ExcAssert(s % alignof(T) == 0);
        return reinterpret_cast<const std::byte *>(s);
    }

    const T * get() const
    {
        return reinterpret_cast<const T *>(getBase() + ofs * alignof(T));
    }

    const T * operator * () const { return get(); }

    const T & operator [] (size_t idx) const
    {
        return get()[idx];
    }

    // Set the pointer to the given value, checking it's in range first
    void set(const T * ptr)
    {
        setMappedPtr(&ofs, ptr, alignof(T));
    }
};

static_assert(std::is_default_constructible_v<MappedPtr<uint32_t>>);

// Ensure that it's the right size and alignment
static_assert(sizeof(MappedPtr<int>) == sizeof(int));
static_assert(alignof(MappedPtr<int>) == alignof(int));

// We can freeze a pointer into a MappedPtr
template<typename T, typename Ofs>
void freeze(MappingContext & context, MappedPtr<T, Ofs> & output, const T * input)
{
    // Make sure they are both in the arena
    context.assertInArena(&output);
    context.assertInArena(input);
    output.set(input);
}

template<typename T, typename Ofs> struct MappedPtrDescription;
DECLARE_TEMPLATE_VALUE_DESCRIPTION_2(MappedPtrDescription, MappedPtr, typename, T, typename, Ofs, true /* enable */);

// We freeze a string into a pointer by simply copying it in and returning the pointer.  Note that
// we don't store null terminators; it is assumed that the length of the string is known from
// elsewhere.
void freeze(MappingContext & context, MappedPtr<char> & output, const std::string & input);

// A mapped string is serialized as a length followed by a pointer to the data.  Note that this
// is an inefficient representation for an array of strings; use StringTable instead of
// MappedVector<MappedString>.  This one can only map strings up to 4GB in length.
struct MappedString {
    uint32_t length_;
    MappedPtr<char> data_;
};

// Freeze the data and then the length
void freeze(MappingContext & context, MappedString & output, const std::string & input);

// MappedVector is used to memory map an array of things that are not encodable as integers.
// It creates a list of mappable objects in memory; these may of course point to memory
// outside of the current object.
//
// The representation is simple: a length and an array of objects.
template<typename T, typename Ofs = uint32_t>
struct MappedVector {
    Ofs size_;
    MappedPtr<T, Ofs> data_;

    size_t memUsageIndirect(const MemUsageOptions & /* opt */) const
    {
        return 0;
    }

    const T & at(Ofs pos) const
    {
        //using namespace std;
        //cerr << "getting element " << pos << " of " << size_ << " for MappedVector of " << demangleTypeName(typeid(T).name())
        //     << endl;
        if (pos >= size_)
            MLDB_THROW_RANGE_ERROR("MappedVector");
        ExcAssert(pos < size_);
        return data_[pos];
    }

    bool empty() const
    {
        return size() == 0;
    }

    Ofs size() const
    {
        return size_;
    }

    const T * data() const { return data_.get(); }
};

template<typename T, typename Ofs> struct MappedVectorDescription;
DECLARE_TEMPLATE_VALUE_DESCRIPTION_2(MappedVectorDescription, MappedVector, typename, T, typename, Ofs, true /* enable */);

template<typename T>
struct WritableMappedVector: public MappedVector<T> {
    // Can only do once...
    void initialize(MappingContext & context, size_t n)
    {
        freeze(context, this->size_, n);
        context.align<alignof(T)>();
        T * p = context.array_field<T>("data_", n);
        freeze(context, this->data_, p);
    }

    // Used for freezing only
    T & atMutable(MappingContext & context, uint32_t pos)
    {
        ExcAssert(pos < this->size_);
        context.assertWritable(this);
        return this->data()[pos];
    }

    T * data() { return const_cast<T *>(this->data_.get()); }
};

// Freeze the length, and an array of objects.
template<typename T0, typename Container>
void freeze(MappingContext & context, MappedVector<T0> & outputImmutable, const Container & input)
{
    WritableMappedVector<T0> & output = const_cast<WritableMappedVector<T0> &>(reinterpret_cast<const WritableMappedVector<T0> &>(outputImmutable));
    output.initialize(context, input.size());
    for (size_t i = 0;  i < input.size();  ++i) {
        T0 & outputElement = output.atMutable(context, i);
        freeze_field(context, "[" + std::to_string(i) + "]", outputElement, input[i]);
    }
}

} // namespace MLDB
