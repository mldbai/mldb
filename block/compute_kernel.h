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
enum class ComputeRuntimeId : uint8_t {
    NONE = 0,
    HOST = 1,    ///< Runs on the local host CPU
    MULTI = 2,   ///< Runs on multiple devices
    OPENCL = 3,  ///< Runs on the OpenCL runtime
    METAL = 4,   ///< Runs on the Apple Metal runtime
    CUDA = 5,    ///< Runs on the Nvidia CUDA runtime
    ROCM = 6,    ///< Runs on the AMD ROCm runtime
    CUSTOM = 32
};

DECLARE_ENUM_DESCRIPTION(ComputeRuntimeId);

enum MemoryRegionAccess {
    ACC_NONE = 0,
    ACC_READ = 1,
    ACC_WRITE = 2,
    ACC_READ_WRITE = 3
};

DECLARE_ENUM_DESCRIPTION(MemoryRegionAccess);

enum MemoryRegionInitialization {
    INIT_NONE,         //< Random contents (but no sensitive data)
    INIT_ZERO_FILLED,  //< Zero-filled
    INIT_BLOCK_FILLED, //< Filled with copies of the given block
    INIT_KERNEL        //< Filled by running the given kernel
};

DECLARE_ENUM_DESCRIPTION(MemoryRegionInitialization);

struct ComputeDevice {
    static constexpr ComputeDevice none() { return { ComputeRuntimeId::NONE, 0, 0, 0, 0 }; }
    static constexpr ComputeDevice host() { return { ComputeRuntimeId::HOST, 0, 0, 0, 0 }; }

    // Return the default device for the given runtime (as chosen by the runtime
    // itself).
    static ComputeDevice defaultFor(ComputeRuntimeId runtime);
    ComputeRuntimeId runtime = ComputeRuntimeId::NONE;   ///< Which runtime this device belongs to
    uint8_t  runtimeInstance = 0;                        ///< Which runtime instance we belong to
    uint16_t deviceInstance = 0;                         ///< Which device instance we belong to
    uint32_t opaque1 = 0;                                ///< Runtime controls
    uint64_t opaque2 = 0;                                ///< Runtime controls

    // Return human-readable information about the device
    std::string info() const;

    // Allow comparisons
    auto operator <=> (const ComputeDevice & other) const = default;
};

PREDECLARE_VALUE_DESCRIPTION(ComputeDevice);
std::ostream & operator << (std::ostream & stream, const ComputeDevice & device);

struct ComputeContext;

// Basic abstract interface to a compute runtime.
struct ComputeRuntime {
    virtual ~ComputeRuntime() = default;

    virtual ComputeRuntimeId getId() const = 0;

    // Enumerate the devices available for this runtime
    virtual std::vector<ComputeDevice> enumerateDevices() const = 0;

    // Get a compute context for this runtime targeting the given device(s)
    virtual std::shared_ptr<ComputeContext>
    getContext(std::span<const ComputeDevice> devices) const = 0;

    // Print the rest of a device ID, in a machine readable (parseable) format
    virtual std::string printRestOfDevice(ComputeDevice device) const = 0;

    // Provide human-readable info about a device
    virtual std::string printHumanReadableDeviceInfo(ComputeDevice device) const = 0;

    // Return the default compute device for the given runtime
    virtual ComputeDevice getDefaultDevice() const = 0;

    // Register a new compute runtime
    static void registerRuntime(ComputeRuntimeId id, const std::string & name,
                                std::function<ComputeRuntime *()> create);

    // List the registered runtimes.. each of these allows a getRuntimeForId() call
    static std::vector<ComputeRuntimeId> enumerateRegisteredRuntimes();

    // Try to get the runtime for the given device, throwing an exception if it fails
    static std::shared_ptr<ComputeRuntime> getRuntimeForDevice(ComputeDevice device);

    // Try to get the runtime for the given ID, throwing an exception if it fails
    static std::shared_ptr<ComputeRuntime> getRuntimeForId(ComputeRuntimeId id);

    // Try to get the runtime for the given ID, returning a null pointer if it fails
    static std::shared_ptr<ComputeRuntime> tryGetRuntimeForId(ComputeRuntimeId id);

    // Get the default compute runtime
    static std::shared_ptr<ComputeRuntime> getDefault();
};

// Profiling information for a compute operation
struct ComputeProfilingInfo {
    virtual ~ComputeProfilingInfo() = default;
};

struct ComputeEvent {
    virtual ~ComputeEvent() = default;
    virtual std::shared_ptr<ComputeProfilingInfo> getProfilingInfo() const = 0;
    virtual void await() const = 0;
};

struct ComputeKernelDimension {
    std::shared_ptr<CommandExpression> bound; // or nullptr if no bounds
};

DECLARE_STRUCTURE_DESCRIPTION(ComputeKernelDimension);

struct ComputeKernelType {
    ComputeKernelType() = default;

    ComputeKernelType(std::shared_ptr<const ValueDescription> baseType,
                      std::string access)
        : baseType(std::move(baseType)), access(std::move(access))
    {
    }

    std::shared_ptr<const ValueDescription> baseType;
    std::string access;

    std::string print() const;
    
    std::vector<ComputeKernelDimension> dims;  // if empty, scalar, otherwise n-dimensional array
};

DECLARE_STRUCTURE_DESCRIPTION(ComputeKernelType);

ComputeKernelType parseType(const std::string & type);

struct ComputeContext;
struct BoundComputeKernel;

/// Class used to handle arguments
struct AbstractArgumentHandler {
    virtual ~AbstractArgumentHandler() = default;

    ComputeKernelType type;
    bool isConst = false;

    virtual bool canGetPrimitive() const;

    virtual std::span<const std::byte>
    getPrimitive(ComputeContext & context) const;

    virtual bool canGetRange() const;

    virtual std::tuple<void *, size_t, std::shared_ptr<const void>>
    getRange(ComputeContext & context) const;

    virtual bool canGetConstRange() const;

    virtual std::tuple<const void *, size_t, std::shared_ptr<const void>>
    getConstRange(ComputeContext & context) const;

    virtual bool canGetHandle() const;

    virtual MemoryRegionHandle getHandle(ComputeContext & context) const;

    virtual std::string info() const;
};

struct ComputeKernelArgument {
    std::shared_ptr<const AbstractArgumentHandler> handler;
    bool has_value() const { return !!handler; }
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
        uint32_t defaultDimension = 0;
    };

    std::vector<DimensionInfo> dims;

    void addParameter(const std::string & parameterName, const std::string & access, const std::string & typeStr)
    {
        if (!paramIndex.emplace(parameterName, params.size()).second) {
            throw AnnotatedException(500, "Duplicate kernel parameter name: '" + parameterName + "'");
        }
        params.push_back({parameterName, access, parseType(typeStr)});
    }

    void addParameter(const std::string & parameterName, const std::string & access, ComputeKernelType type)
    {
        if (!paramIndex.emplace(parameterName, params.size()).second) {
            throw AnnotatedException(500, "Duplicate kernel parameter name: '" + parameterName + "'");
        }
        params.push_back({parameterName, access, std::move(type)});
    }

    void addDimension(const std::string & dimensionName, const std::string & range,
                      uint32_t defaultDimension = 0)
    {
        dims.push_back({dimensionName, range, defaultDimension});
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

struct MemoryArrayAbstractArgumentHandler: public AbstractArgumentHandler {

    template<typename T>
    MemoryArrayAbstractArgumentHandler(MemoryArrayHandleT<T> handle)
        : MemoryArrayAbstractArgumentHandler(std::move(handle),
                                         getDefaultDescriptionSharedT<std::remove_const_t<T>>(),
                                         std::is_const_v<T>)
    {
    }

    MemoryArrayAbstractArgumentHandler(MemoryRegionHandle handle,
                                   std::shared_ptr<const ValueDescription> containedType,
                                   bool isConst);

    virtual ~MemoryArrayAbstractArgumentHandler() = default;

    MemoryRegionHandle handle;

    virtual bool canGetRange() const override;

    virtual std::tuple<void *, size_t, std::shared_ptr<const void>>
    getRange(ComputeContext & context) const override;

    virtual bool canGetConstRange() const override;

    virtual std::tuple<const void *, size_t, std::shared_ptr<const void>>
    getConstRange(ComputeContext & context) const override;

    virtual bool canGetHandle() const override;

    virtual MemoryRegionHandle
    getHandle(ComputeContext & context) const override;

    virtual std::string info() const override;
};

template<typename T>
MemoryArrayAbstractArgumentHandler *
getArgumentHandler(MemoryArrayHandleT<T> handle)
{
    return new MemoryArrayAbstractArgumentHandler(std::move(handle));
}

struct PrimitiveAbstractArgumentHandler: public AbstractArgumentHandler {

    template<typename T>
    PrimitiveAbstractArgumentHandler(T value)
    {
        val = std::move(value);
        const T & finalVal = std::any_cast<const T &>(val);
        mem = { (const std::byte *)&finalVal, sizeof(T) };
        this->isConst = true;
        this->type.baseType = getDefaultDescriptionSharedT<T>();
        this->type.access = "r";
    }

    std::any val;
    std::span<const std::byte> mem;

    virtual bool canGetPrimitive() const override;

    virtual std::span<const std::byte>
    getPrimitive(ComputeContext & context) const override;

    virtual std::string info() const override;
};

template<typename T>
PrimitiveAbstractArgumentHandler *
getArgumentHandler(T value)
{
    return new PrimitiveAbstractArgumentHandler(std::move(value));
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

    arguments[argIndex].handler.reset(getArgumentHandler(std::forward<Arg>(arg)));
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

    virtual ~ComputeQueue() = default;

    ComputeContext * owner = nullptr;

    std::mutex kernelWallTimesMutex;
    std::map<std::string, double> kernelWallTimes; // in milliseconds
    double totalKernelTime = 0.0;

    virtual std::shared_ptr<ComputeEvent>
    launch(const BoundComputeKernel & kernel,
           const std::vector<uint32_t> & grid,
           const std::vector<std::shared_ptr<ComputeEvent>> & prereqs = {});

    virtual std::shared_ptr<ComputeEvent>
    enqueueFillArrayImpl(MemoryRegionHandle region, MemoryRegionInitialization init,
                         size_t startOffsetInBytes, ssize_t lengthInBytes,
                         const std::any & arg);

    template<typename T>
    std::shared_ptr<ComputeEvent>
    enqueueFillArray(const MemoryArrayHandleT<T> & region, const T & val,
                     size_t start = 0, ssize_t length = -1)
    {
        const char * valBytes = (const char *)&val;
 
        // If all bytes in the initialization are zero, we can do it more efficiently
        bool allZero = true;
        for (size_t i = 0;  allZero && i < sizeof(T);  allZero = valBytes[i++] == 0) ;
 
        if (allZero) {
            return enqueueFillArrayImpl(region, MemoryRegionInitialization::INIT_ZERO_FILLED,
                                        start * sizeof(T), length == -1 ? length : length * sizeof(T),
                                        {});
        }
        else {
            std::span<const std::byte> fillWith((const std::byte *)&val, sizeof(val));
            return enqueueFillArrayImpl(region, MemoryRegionInitialization::INIT_BLOCK_FILLED,
                                        start * sizeof(T), length == -1 ? length : length * sizeof(T),
                                        fillWith);
        }
    }

    virtual void flush()
    {
    }
};

struct ComputeContext {

    virtual ~ComputeContext() = default;

    virtual MemoryRegionHandle
    allocateImpl(size_t length, size_t align,
                 const std::type_info & type, bool isConst,
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
    managePinnedHostRegion(std::span<const std::byte> region, size_t align,
                           const std::type_info & type, bool isConst) = 0;

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
        return {allocateImpl(size * sizeof(T), alignof(T), typeid(T), std::is_const_v<T>, INIT_NONE).handle};
    }

    template<typename T>
    auto allocZeroInitializedArray(size_t size) -> MemoryArrayHandleT<T>
    {
        return {allocateImpl(size * sizeof(T), alignof(T), typeid(T), std::is_const_v<T>, INIT_ZERO_FILLED).handle};
    }

    template<typename T>
    auto manageMemoryRegion(const std::vector<T> & obj) -> MemoryArrayHandleT<T>
    {
        return manageMemoryRegion(static_cast<std::span<const T>>(obj));
    }

    template<typename T, size_t N>
    auto manageMemoryRegion(const std::span<const T, N> & obj) -> MemoryArrayHandleT<T>
    {
        return { managePinnedHostRegion(std::as_bytes(obj), alignof(T),
                 typeid(std::remove_const_t<T>), std::is_const_v<T>).handle };
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
