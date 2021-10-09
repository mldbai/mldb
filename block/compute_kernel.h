/** compute_kernel.h                                                -*- C++ -*-
    Jeremy Barnes, 27 March 2016
    This file is part of MLDB. Copyright 2016 mldb.ai inc. All rights reserved.

    Basic primitives around memory regions.  Once frozen, these are the
    representation that covers CPU memory, device memory and remote
    memory and implements the primitives that allow data to be made
    available and brought to the compute resources required.
*/

#pragma once

#include "memory_region.h"
#include <string>
#include "mldb/types/annotated_exception.h"
#include "mldb/arch/demangle.h"
#include "mldb/utils/command_expression.h"
#include "mldb/arch/timers.h"
#include <any>
#include <iostream>
#include <compare>

namespace MLDB {

/// Gives a broad characterization of the devices that a compute runtime supports
enum ComputeRuntimeId : uint8_t {
    NONE = 0,
    HOST = 1,    ///< Runs on the local host CPU
    MULTI = 2,   ///< Runs on multiple devices
    OPENCL = 3,  ///< Runs on the OpenCL runtime
    METAL = 4,   ///< Runs on the Apple Metal runtime
    CUDA = 5,    ///< Runs on the Nvidia CUDA runtime
    ROCM = 6,    ///< Runs on the AMD ROCm runtime
    CUSTOM = 128
};

DECLARE_ENUM_DESCRIPTION(ComputeRuntimeId);

struct ComputeDevice {
    static constexpr ComputeDevice host() { return { HOST, 0 }; }
    uint64_t runtime:8;   ///< Which runtime this device belongs to
    uint64_t device:56;   ///< Which device ID (or number) in the runtime
};

struct ComputeContext;

// Basic abstract interface to a compute runtime.
struct ComputeRuntime {
    virtual ~ComputeRuntime() = default;

    virtual ComputeRuntimeId getId() const = 0;

    // Enumerate the devices available for this runtime
    virtual std::vector<ComputeDevice> enumerateDevices() const = 0;

    // Get a compute context for this runtime
    virtual std::shared_ptr<ComputeContext> getContext() const = 0;

    // Register a new compute runtime
    static void registerRuntime(ComputeRuntimeId id, const std::string & name,
                                std::function<ComputeRuntime *()> create);

    static std::shared_ptr<ComputeRuntime> getRuntimeForDevice(ComputeDevice device);
    static std::shared_ptr<ComputeRuntime> getRuntimeForId(ComputeRuntimeId device);
    static std::shared_ptr<ComputeRuntime> getDefault();
};

// Profiling information for a compute operation
struct ComputeProfilingInfo {

};

struct ComputeEvent {
    virtual ~ComputeEvent() = default;
    virtual ComputeProfilingInfo getProfilingInfo() const = 0;
    virtual void await() const = 0;
};

struct ComputeKernelDimension {
    std::shared_ptr<CommandExpression> bound; // or nullptr if no bounds
};

DECLARE_STRUCTURE_DESCRIPTION(ComputeKernelDimension);

struct ComputeKernelType {
    ComputeKernelType() = default;

    ComputeKernelType(std::string str,
                      std::shared_ptr<const ValueDescription> baseType)
        : str(std::move(str)), baseType(std::move(baseType))
    {
    }

    std::string str;
    std::shared_ptr<const ValueDescription> baseType;
    std::string access;

    std::string print() const
    {
        auto result = str;
        for (auto [bound]: dims) {
            result += "[" + bound->surfaceForm + "]";
        }
        return result;
    }
    
    std::vector<ComputeKernelDimension> dims;  // if empty, scalar, otherwise n-dimensional array
};

DECLARE_STRUCTURE_DESCRIPTION(ComputeKernelType);

ComputeKernelType parseType(const std::string & type);

struct ComputeContext;
struct BoundComputeKernel;

using GetRange = std::function<std::tuple<void *, size_t, std::shared_ptr<const void>>(const std::any & val, ComputeContext & context)>;
using GetConstRange = std::function<std::tuple<const void *, size_t, std::shared_ptr<const void>>(const std::any & val, ComputeContext & context)>;
using GetHandle = std::function<MemoryRegionHandle (const std::any & val, ComputeContext & context)>;

struct ComputeKernelArgument {
    std::any value;
    ComputeKernelType abstractType;
    GetRange getRange;
    GetConstRange getConstRange;
    GetHandle getHandle;

    const std::type_info & type() const { return value.type(); }
    bool has_value() const { return value.has_value(); }
};

struct ComputeKernelGridRange {
    ComputeKernelGridRange() = default;

    ComputeKernelGridRange(uint32_t range)
        : first_(0), last_(range), range_(range)
    {
    }

    struct Iterator {
        using iterator_category = std::forward_iterator_tag;
        using value_type = uint32_t;
        using difference_type = ssize_t;
        using pointer = const uint32_t*;
        using reference = const uint32_t&;

        auto operator <=> (const Iterator & other) const = default;

        Iterator operator++()
        {
            ++current;
            return *this;
        }

        value_type operator * () const
        {
            return current;
        }

        uint32_t current = 0;
    };

    uint32_t first_ = 0;  // Where this part of the grid starts; first <= last <= range
    uint32_t last_ = 0;   // Where this part of the grid finishes;
    uint32_t range_ = 0;  // Overall grid range (goes from 0 to range)

    uint32_t range() const { return range_; };

    Iterator begin() { return { first_ }; }
    Iterator end() { return { last_ }; }
};

struct ComputeKernel {
    using Callable = std::function<void (ComputeContext & context, std::span<ComputeKernelGridRange> idx)>;

    std::string kernelName;
    ComputeDevice device;
    ComputeContext * context = nullptr;

    struct ParameterInfo {
        std::string name;
        std::string access;
        ComputeKernelType type;
    };

    std::vector<ParameterInfo> params;
    std::map<std::string, int> paramIndex;

    struct DimensionInfo {
        std::string name;
        std::string range;
    };

    std::vector<DimensionInfo> dims;

    void addParameter(const std::string & parameterName, const std::string & access, const std::string & typeStr)
    {
        if (!paramIndex.emplace(parameterName, params.size()).second) {
            throw AnnotatedException(500, "Duplicate kernel parameter name: '" + parameterName + "'");
        }
        params.push_back({parameterName, access, parseType(typeStr)});
    }

    void addDimension(const std::string & dimensionName, const std::string & range)
    {
        dims.push_back({dimensionName, range});
    }

    template<typename... NamesAndArgs>
    BoundComputeKernel bind(NamesAndArgs&&... namesAndArgs);

    // This function extracts the latest values of the bound parameters and sets them to
    // be marshaled by the calling infrastructure.  It should be called once for each time
    // that the callable is used (in other words, the callable should be called only once).
    std::function<Callable (ComputeContext & context, std::vector<ComputeKernelArgument> & params)> createCallable;
};

struct BoundComputeKernel {
    const ComputeKernel * owner = nullptr;
    std::vector<ComputeKernelArgument> arguments;
    ComputeKernel::Callable call;

    void operator () (ComputeContext & context, std::span<ComputeKernelGridRange> grid) const
    {
        try {
            this->call(context, grid);
        } MLDB_CATCH_ALL {
            rethrowException(500, "Error launching kernel " + owner->kernelName);
        }
    }
};

namespace details {

// Forward definition as ComputeContext is defined later on
template<typename T>
FrozenMemoryRegionT<std::remove_const_t<T>>
transferToHostSync(ComputeContext & context, const MemoryArrayHandleT<T> & handle);

template<typename T>
MutableMemoryRegionT<T>
transferToHostMutableSync(ComputeContext & context, const MemoryArrayHandleT<T> & handle);

template<typename T>
std::tuple<ComputeKernelType, GetRange, GetConstRange, GetHandle>
getAbstractType(MemoryArrayHandleT<T> *)
{
    ComputeKernelType result(type_name<MemoryArrayHandleT<T>>(),
                             getDefaultDescriptionSharedT<T>());
    result.dims.push_back({nullptr});
    result.access = "rw";

    GetRange getRange = [] (const std::any & val, ComputeContext & context) -> std::tuple<void *, size_t, std::shared_ptr<const void>>
    {
        const auto & handle = std::any_cast<const MemoryArrayHandleT<T> &>(val);
        MutableMemoryRegionT<T> region = transferToHostMutableSync(context, handle);
        return { region.data(), region.length(), region.raw().handle() };
    };

    GetConstRange getConstRange = [] (const std::any & val, ComputeContext & context) -> std::tuple<const void *, size_t, std::shared_ptr<const void>>
    {
        const auto & handle = std::any_cast<const MemoryArrayHandleT<T> &>(val);
        ExcAssert(handle.handle);
        FrozenMemoryRegionT<T> region = transferToHostSync(context, handle);
        return { region.data(), region.length(), region.raw().handle() };
    };

    GetHandle getHandle = [] (const std::any & val, ComputeContext & context) -> MemoryRegionHandle
    {
        const auto & handle = std::any_cast<const MemoryArrayHandleT<T> &>(val);
        return handle;
    };

    return { result, getRange, getConstRange, getHandle };
}

template<typename T>
std::tuple<ComputeKernelType, GetRange, GetConstRange, GetHandle>
getAbstractType(MemoryArrayHandleT<const T> *)
{
    ComputeKernelType result(type_name<MemoryArrayHandleT<T>>(),
                             getDefaultDescriptionSharedT<T>());
    result.dims.push_back({nullptr});
    result.access = "r";

    GetConstRange getConstRange = [] (const std::any & val, ComputeContext & context) -> std::tuple<const void *, size_t, std::shared_ptr<const void>>
    {
        const auto & handle = std::any_cast<const MemoryArrayHandleT<const T> &>(val);
        FrozenMemoryRegionT<T> region = transferToHostSync(context, handle);
        return { region.data(), region.length(), region.raw().handle() };
    };

    GetHandle getHandle = [] (const std::any & val, ComputeContext & context) -> MemoryRegionHandle
    {
        const auto & handle = std::any_cast<const MemoryArrayHandleT<const T> &>(val);
        return handle;
    };

    return { result, nullptr /* getRange */, getConstRange, getHandle };
}

template<typename T>
std::tuple<ComputeKernelType, GetRange, GetConstRange, GetHandle>
getAbstractType(T *)
{
    ComputeKernelType result(type_name<T>(),
                             getDefaultDescriptionSharedT<std::remove_const_t<T>>());

    if constexpr (std::is_const_v<T>)
        result.access = "r";
    else
        result.access = "rw";

    return { result, nullptr, nullptr, nullptr };
}

template<typename T>
std::tuple<ComputeKernelType, GetRange, GetConstRange, GetHandle>
getAbstractType()
{
    return getAbstractType((T *)nullptr);
}

template<typename Arg>
void bindOne(const ComputeKernel * owner, std::vector<ComputeKernelArgument> & arguments, const std::string & argName, Arg&& arg)
{
    ExcAssert(owner);

    auto argIndexIt = owner->paramIndex.find(argName);
    if (argIndexIt == owner->paramIndex.end())
        throw MLDB::Exception("Couldn't bind arg: argument " + argName
                                + " is not an argument of kernel " + owner->kernelName);

    size_t argIndex = argIndexIt->second;
    //using namespace std;
    //cerr << "binding " << argName << " at index " << argIndex << " to value of type " << type_name<Arg>() << endl;

    if (arguments.at(argIndex).has_value())
        throw MLDB::Exception("Attempt to double bind argument " + argName
                                + " of kernel " + owner->kernelName);

    auto [type, getRange, getConstRange, getHandle] = getAbstractType<std::remove_reference_t<Arg>>();
    arguments[argIndex] = { std::forward<Arg>(arg), type, getRange, getConstRange, getHandle };
}

inline void bind(const ComputeKernel * owner, std::vector<ComputeKernelArgument> & arguments) // end of recursion
{
    for (size_t i = 0;  i < arguments.size();  ++i) {
        if (!arguments[i].has_value()) {
            throw MLDB::Exception("kernel " + owner->kernelName + " didn't set argument "
                                    + owner->params.at(i).name);
        }
    }
}

template<typename Arg, typename... Rest>
void bind(const ComputeKernel * owner, std::vector<ComputeKernelArgument> & arguments, const std::string & argName, Arg&& arg, Rest&&... rest)
{
    details::bindOne(owner, arguments, argName, std::forward<Arg>(arg));
    details::bind(owner, arguments, std::forward<Rest>(rest)...);
}

} // namespace details

template<typename... NamesAndArgs>
BoundComputeKernel ComputeKernel::bind(NamesAndArgs&&... namesAndArgs)
{
    // These are bound to the values in NamesAndArgs
    std::vector<ComputeKernelArgument> arguments(this->params.size());
    details::bind(this, arguments, std::forward<NamesAndArgs>(namesAndArgs)...);

    BoundComputeKernel result;
    result.owner = this;
    result.call = this->createCallable(*context, arguments);

    return result;
}

struct ComputeQueue {
    ComputeQueue(ComputeContext * owner)
        : owner(owner)
    {
    }

    ComputeContext * owner = nullptr;

    std::mutex kernelWallTimesMutex;
    std::map<std::string, double> kernelWallTimes; // in milliseconds

    std::shared_ptr<ComputeEvent> launch(const BoundComputeKernel & kernel,
                                         const std::vector<uint32_t> & grid,
                                         const std::vector<std::shared_ptr<ComputeEvent>> & prereqs = {})
    {
        ExcAssertEqual(kernel.owner->dims.size(), grid.size());

        Timer timer;
        std::vector<ComputeKernelGridRange> ranges(grid.begin(), grid.end());
        kernel(*owner, ranges);
        auto wallTime = timer.elapsed_wall();
        using namespace std;
        cerr << "calling " << kernel.owner->kernelName << " took " << timer.elapsed() << endl;
        {
            std::unique_lock guard(kernelWallTimesMutex);
            kernelWallTimes[kernel.owner->kernelName] += wallTime * 1000.0;
        }

        return std::shared_ptr<ComputeEvent>();
    }

    template<typename T>
    std::shared_ptr<ComputeEvent>
    enqueueFillArray(const MemoryArrayHandleT<T> & region, const T & val,
                     size_t start = 0, ssize_t length = -1)
    {
        auto mapped = details::transferToHostMutableSync(*owner, region);
        if (start > mapped.length()) {
            throw MLDB::Exception("enqueueFillArray: array start index out of bounds");
        }
        if (length == -1)
            length = mapped.length() - start;
        if (start + length > mapped.length()) {
            throw MLDB::Exception("enqueueFillArray: array end index out of bounds");
        }

        std::fill_n(mapped.data() + start, length, val);

        return std::shared_ptr<ComputeEvent>();
    }

    void flush()
    {
    }
};

enum MemoryRegionAccess {
    ACC_NONE = 0,
    ACC_READ = 1,
    ACC_WRITE = 2,
    ACC_READ_WRITE = 3
};

enum MemoryRegionInitialization {
    INIT_NONE,         //< Random contents (but no sensitive data)
    INIT_ZERO_FILLED,  //< Zero-filled
    INIT_BLOCK_FILLED, //< Filled with copies of the given block
    INIT_KERNEL        //< Filled by running the given kernel
};

struct ComputeContext {

    virtual ~ComputeContext() = default;

    virtual MemoryRegionHandle
    allocateImpl(size_t length, size_t align,
                 MemoryRegionInitialization initialization,
                 std::any initWith = std::any()) = 0;

    virtual std::tuple<MemoryRegionHandle, std::shared_ptr<ComputeEvent>>
    transferToDeviceImpl(FrozenMemoryRegion region,
                         const std::type_info & type, bool isConst) = 0;

    virtual std::tuple<FrozenMemoryRegion, std::shared_ptr<ComputeEvent>>
    transferToHostImpl(MemoryRegionHandle handle) = 0;

    virtual std::tuple<MutableMemoryRegion, std::shared_ptr<ComputeEvent>>
    transferToHostMutableImpl(MemoryRegionHandle handle) = 0;

    virtual std::shared_ptr<ComputeKernel>
    getKernel(const std::string & kernelName) = 0;

    virtual MemoryRegionHandle
    managePinnedHostRegion(std::span<const std::byte> region, size_t align) = 0;

    virtual std::shared_ptr<ComputeQueue>
    getQueue() = 0;

    // Return the MappedSerializer that owns the memory allocated on the host for this
    // device.  It's needed for the generic MemoryRegion functions to know how to manipulate
    // memory handles.  In practice it probably means that each runtime needs to define a
    // MappedSerializer derivitive.
    virtual MappedSerializer * getSerializer() = 0;

    template<typename T>
    auto transferToDeviceImmutable(const FrozenMemoryRegionT<T> & obj, const char * what)
        -> std::tuple<MemoryArrayHandleT<const T>, std::shared_ptr<ComputeEvent>, size_t>
    {
        auto [handle, event]
            = transferToDeviceImpl(obj, typeid(std::remove_const_t<T>), true /* isConst */);
        return { {std::move(handle.handle)}, std::move(event), obj.memusage() };
    }

    template<typename T>
    std::tuple<FrozenMemoryRegionT<std::remove_const_t<T>>, std::shared_ptr<ComputeEvent>>
    transferToHost(MemoryArrayHandleT<T> array)
    {
        array.template checkTypeAccessibleAs<std::remove_const_t<T>>();
        auto [handle, event] = transferToHostImpl(array);
        FrozenMemoryRegion raw(handle.handle(), (char *)handle.data(), handle.length());
        return { std::move(raw), std::move(event) };
    }

    template<typename T>
    auto transferToHostMutable(MemoryArrayHandleT<T> array)
        -> std::tuple<MutableMemoryRegionT<std::remove_const_t<T>>, std::shared_ptr<ComputeEvent>>
    {
        static_assert(!std::is_const_v<T>, "mutable transfer requires non-const type");
        array.template checkTypeAccessibleAs<std::remove_const_t<T>>();
        auto [handle, event] = transferToHostMutableImpl(array);
        MutableMemoryRegion raw(handle.handle(), (char *)handle.data(), handle.length(), getSerializer());
        return { std::move(raw), std::move(event) };
    }

    template<typename T>
    auto transferToHostSync(MemoryArrayHandleT<T> array) -> FrozenMemoryRegionT<std::remove_const_t<T>>
    {
        auto [result, event] = transferToHost(std::move(array));
        if (event)
            event->await();
        return result;
    }

    template<typename T>
    auto transferToHostMutableSync(MemoryArrayHandleT<T> array) -> MutableMemoryRegionT<T>
    {
        static_assert(!std::is_const_v<T>, "mutable transfer requires non-const type");
        auto [result, event] = transferToHostMutable(std::move(array));
        if (event)
            event->await();
        return result;
    }

    /** Transfers the array to the CPU so that it can be written from the CPU... but
     *  promises that the initial contents will never be read, which enables us to
     *  not copy any data to the CPU. */
    template<typename T>
    auto transferToHostUninitialized(MemoryArrayHandleT<T> array) -> MutableMemoryRegionT<T>
    {
        return transferToHostMutableSync(std::move(array));
    }

    template<typename T>
    auto allocArray(size_t size) -> MemoryArrayHandleT<T>
    {
        return {allocateImpl(size * sizeof(T), alignof(T), INIT_NONE).handle};
    }

    template<typename T>
    auto allocZeroInitializedArray(size_t size) -> MemoryArrayHandleT<T>
    {
        return {allocateImpl(size * sizeof(T), alignof(T), INIT_ZERO_FILLED).handle};
    }

    template<typename T>
    auto manageMemoryRegion(const std::vector<T> & obj) -> MemoryArrayHandleT<T>
    {
        return manageMemoryRegion(static_cast<std::span<const T>>(obj));
    }

    template<typename T, size_t N>
    auto manageMemoryRegion(const std::span<const T, N> & obj) -> MemoryArrayHandleT<T>
    {
        return {managePinnedHostRegion(std::as_bytes(obj), alignof(T)).handle};
    }
};

namespace details {
template<typename T>
FrozenMemoryRegionT<std::remove_const_t<T>>
transferToHostSync(ComputeContext & context, const MemoryArrayHandleT<T> & handle)
{
    return context.transferToHostSync(handle);
}

template<typename T>
MutableMemoryRegionT<T>
transferToHostMutableSync(ComputeContext & context, const MemoryArrayHandleT<T> & handle)
{
    return context.transferToHostMutableSync(handle);
}

} // namespace details

} // namespace MLDB
