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
#include <future>

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
struct ComputeQueue;
struct ComputeEvent;
struct ComputeRuntime;
struct ComputeKernel;
struct BoundComputeKernel;
struct ComputePromise;
template<typename T> struct ComputePromiseT;


// ComputeRuntime

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

struct ComputePromise {
    ComputePromise() = default;  // Cannot be used; methods will throw
    ComputePromise(std::shared_ptr<ComputeEvent> event,
                   const std::type_info & type,
                   std::shared_ptr<std::promise<std::any>> promise = std::make_shared<std::promise<std::any>>())
        : event_(std::move(event)),
          promise_(std::move(promise)),
          future_(std::make_shared<std::shared_future<std::any>>(promise_->get_future().share())),
          type_(&type)
    {
        ExcAssert(this->event_);
        ExcAssert(this->promise_);
        ExcAssert(this->future_);
        ExcAssert(this->type_);
    }

    ComputePromise(std::shared_ptr<ComputeEvent> event, std::any val)
        : ComputePromise(std::move(event), val.type())
    {
        ExcAssert(val.has_value());
        ExcAssert(*type_ != typeid(void));
        promise_->set_value(std::move(val));
    }

    bool valid() const { return !!event_; }

    std::shared_ptr<ComputeEvent> event_;
    std::shared_ptr<std::promise<std::any>> promise_;
    std::shared_ptr<std::shared_future<std::any>> future_;
    const std::type_info * type_ = &typeid(void);

    std::shared_ptr<ComputeEvent> event() const { return event_; }

    // Wait for the event to complete
    void await() const;

    template<typename T>
    ComputePromiseT<T> moveToType();

    template<typename T>
    ComputePromiseT<T> asType() const;
};

template<typename T>
struct ComputePromiseT: public ComputePromise {
    ComputePromiseT() = default;  // Cannot be used; methods will throw

    void verifyType() const
    {
        if (typeid(T) != *type_) {
            throw MLDB::Exception("Attempt to convert ComputePromiseT<" + demangle(*type_)
                                  + "> into ComputePromiseT<" + type_name<T>() + ">");
        }
    }

    // Cast the type up
    ComputePromiseT(ComputePromise&& untyped)
        : ComputePromise(std::move(untyped))
    {
        verifyType();
    }

    ComputePromiseT(const ComputePromise & untyped)
        : ComputePromise(untyped)
    {
        verifyType();
    }

    // Constructor for an already resolved promise
    ComputePromiseT(T value, std::shared_ptr<ComputeEvent> event)
        : ComputePromise(std::move(event), std::move(value))
    {
    }

    // Constructor for when we're awaiting an event
    ComputePromiseT(std::shared_ptr<std::promise<std::any>> promise,
                    std::shared_ptr<ComputeEvent> event)
        : ComputePromise(std::move(event), typeid(T), std::move(promise))
    {
    }

    T move()
    {
        await();
        return std::any_cast<T>(future_->get());
    }

    T get() const
    {
        await();
        return std::any_cast<T>(future_->get());
    }

    template<typename Fn, typename Return = std::invoke_result_t<Fn, T>, typename Enable = std::enable_if_t<!std::is_same_v<Return, void>>>
    ComputePromiseT<Return> then(Fn && fn);

    template<typename Fn, typename Return = std::invoke_result_t<Fn, T>, typename Enable = std::enable_if_t<std::is_same_v<Return, void>>>
    ComputePromise then(Fn && fn);
};

template<typename T>
ComputePromiseT<T>
ComputePromise::
moveToType()
{
    return ComputePromiseT<T>(std::move(*this));
}

template<typename T>
ComputePromiseT<T>
ComputePromise::
asType() const
{
    return ComputePromiseT<T>(*this);
}

struct ComputeEvent {
    virtual ~ComputeEvent() = default;
    virtual std::shared_ptr<ComputeProfilingInfo> getProfilingInfo() const = 0;
    virtual void await() const = 0;

    // Once the event is resolved, run the function, and return another event that will
    // fire once the chained function has returned
    virtual std::shared_ptr<ComputeEvent> thenImpl(std::function<void ()> fn) = 0;

    template<typename Fn, typename Return = std::invoke_result_t<Fn>, typename Enable = std::enable_if_t<!std::is_same_v<Return, void>>>
    ComputePromiseT<Return> then(Fn && fn)
    {
        auto promise = std::make_shared<std::promise<std::any>>();

        auto doPromise = [fn = std::move(fn), promise] ()
        {
            try {
                auto result = fn();
                promise->set_value(std::move(result));
            } MLDB_CATCH_ALL {
                promise->set_exception(std::current_exception());
            }
        };

        auto event = thenImpl(std::move(doPromise));
        return { std::move(promise), std::move(event) };
    }

    template<typename Fn, typename Return = std::invoke_result_t<Fn>, typename Enable = std::enable_if_t<std::is_same_v<Return, void>>>
    ComputePromise then(Fn && fn)
    {
        return { thenImpl(std::move(fn)), typeid(void) };
    }
};

template<typename T>
template<typename Fn, typename Return, typename Enable>
ComputePromise
ComputePromiseT<T>::
then(Fn && fn)
{
    // New promise to trigger on
    auto newPromise = std::make_shared<std::promise<std::any>>();

    auto newFn = [oldFuture = this->future_, newPromise, fn = std::move(fn)] () -> void
    {
        try {
            T input = std::any_cast<T>(oldFuture->get());
            fn(std::move(input));
            newPromise->set_value(std::any());
        } MLDB_CATCH_ALL {
            newPromise->set_exception(std::current_exception());
        }
    };

    event_->then(std::move(newFn));

    return { event_, typeid(void), std::move(newPromise) };
}

template<typename T>
template<typename Fn, typename Return, typename Enable>
ComputePromiseT<Return>
ComputePromiseT<T>::
then(Fn && fn)
{
    // New promise to hold the value
    auto newPromise = std::make_shared<std::promise<std::any>>();

    auto newFn = [oldFuture = this->future_, newPromise, fn = std::move(fn)] () -> void
    {
        try {
            T input = std::any_cast<T>(oldFuture->get());
            newPromise->set_value(fn(std::move(input)));
        } MLDB_CATCH_ALL {
            newPromise->set_exception(std::current_exception());
        }
    };

    event_->then(std::move(newFn));

    return { std::move(newPromise), event_ };
}

inline void
ComputePromise::
await() const
{
    ExcAssert(event_);
    event_->await();
}


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

    bool isCompatibleWith(const ComputeKernelType & otherType, std::string * reason = nullptr) const;
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
    getPrimitive(const std::string & opName, ComputeContext & context) const;

    virtual bool canGetRange() const;

    virtual std::tuple<void *, size_t, std::shared_ptr<const void>>
    getRange(const std::string & opName, ComputeContext & context) const;

    virtual bool canGetConstRange() const;

    virtual std::tuple<const void *, size_t, std::shared_ptr<const void>>
    getConstRange(const std::string & opName, ComputeContext & context) const;

    virtual bool canGetHandle() const;

    virtual MemoryRegionHandle getHandle(const std::string & opName, ComputeContext & context) const;

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


/// Opaque structure subclassed by each ComputeKernel implementation to store
/// information on how it's bound.  This is used rather than an anonymous shared
/// ptr so that it can be checked with dynamic_cast to catch programming errors
/// in mixing kernels across contexts.
struct ComputeKernelBindInfo {
    virtual ~ComputeKernelBindInfo() = default;
};

// ComptuteKernel

struct ComputeKernel {
    virtual ~ComputeKernel() = default;

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

    // bind() is called with (name, value) pairs to set the value of particular parameters
    template<typename... NamesAndArgs>
    BoundComputeKernel bind(NamesAndArgs&&... namesAndArgs);

    // Perform the abstract bind() operation, returning a BoundComputeKernel
    virtual BoundComputeKernel bindImpl(std::vector<ComputeKernelArgument> arguments) const = 0;
};

// BoundComputeKernel

struct BoundComputeKernel {
    const ComputeKernel * owner = nullptr;
    std::vector<ComputeKernelArgument> arguments;
    std::shared_ptr<ComputeKernelBindInfo> bindInfo;
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
    getRange(const std::string & opName, ComputeContext & context) const override;

    virtual bool canGetConstRange() const override;

    virtual std::tuple<const void *, size_t, std::shared_ptr<const void>>
    getConstRange(const std::string & opName, ComputeContext & context) const override;

    virtual bool canGetHandle() const override;

    virtual MemoryRegionHandle
    getHandle(const std::string & opName, ComputeContext & context) const override;

    virtual std::string info() const override;
};

template<typename T>
MemoryArrayAbstractArgumentHandler *
getArgumentHandler(MemoryArrayHandleT<T> handle)
{
    return new MemoryArrayAbstractArgumentHandler(std::move(handle));
}

struct PromiseAbstractArgumentHandler: public AbstractArgumentHandler {

    template<typename T>
    PromiseAbstractArgumentHandler(ComputePromiseT<T> value)
        : subImpl(getArgumentHandler(value.get()))
    {
        this->isConst = subImpl->isConst;
        this->type = subImpl->type;
        // TODO: bind this later so we don't wait on it until we actually need it
        // TODO: add the promise to the list of events we need to await
    }

    std::unique_ptr<AbstractArgumentHandler> subImpl;

    virtual bool canGetPrimitive() const override;

    virtual std::span<const std::byte>
    getPrimitive(const std::string & opName, ComputeContext & context) const override;

    virtual bool canGetRange() const override;

    virtual std::tuple<void *, size_t, std::shared_ptr<const void>>
    getRange(const std::string & opName, ComputeContext & context) const override;

    virtual bool canGetConstRange() const override;

    virtual std::tuple<const void *, size_t, std::shared_ptr<const void>>
    getConstRange(const std::string & opName, ComputeContext & context) const override;

    virtual bool canGetHandle() const override;

    virtual MemoryRegionHandle
    getHandle(const std::string & opName, ComputeContext & context) const override;

    virtual std::string info() const override;
};

template<typename T>
PromiseAbstractArgumentHandler *
getArgumentHandler(ComputePromiseT<T> promise)
{
    return new PromiseAbstractArgumentHandler(std::move(promise));
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
    getPrimitive(const std::string & opName, ComputeContext & context) const override;

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
    ExcAssert(arguments[argIndex].handler->type.baseType);

    auto & expectedType = owner->params.at(argIndex).type;
    auto & passedType = arguments[argIndex].handler->type;

    std::string reason;
    if (!expectedType.isCompatibleWith(passedType, &reason)) {
        throw MLDB::Exception("Couldn't bind arg: argument " + argName
                              + " of kernel " + owner->kernelName
                              + ": " + reason + "( Arg is " + type_name<Arg>() + ")");
    }
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

    return this->bindImpl(std::move(arguments));
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
    launch(const std::string & opName,
           const BoundComputeKernel & kernel,
           const std::vector<uint32_t> & grid,
           const std::vector<std::shared_ptr<ComputeEvent>> & prereqs = {}) = 0;

    virtual ComputePromiseT<MemoryRegionHandle>
    enqueueFillArrayImpl(const std::string & opName,
                         MemoryRegionHandle region, MemoryRegionInitialization init,
                         size_t startOffsetInBytes, ssize_t lengthInBytes,
                         const std::any & arg,
                         std::vector<std::shared_ptr<ComputeEvent>> prereqs);

    // Create an already resolved event (this is abstract so that the subclass may create an)
    // event of the correct type).
    virtual std::shared_ptr<ComputeEvent>
    makeAlreadyResolvedEvent() const = 0;

    template<typename T>
    ComputePromiseT<MemoryArrayHandleT<T>>
    enqueueFillArray(const std::string & opName,
                     const MemoryArrayHandleT<T> & region, const T & val,
                     size_t start = 0, ssize_t length = -1)
    {
        const char * valBytes = (const char *)&val;
 
        // If all bytes in the initialization are zero, we can do it more efficiently
        bool allZero = true;
        for (size_t i = 0;  allZero && i < sizeof(T);  allZero = valBytes[i++] == 0) ;
 
        auto convert = [] (MemoryRegionHandle handle) -> MemoryArrayHandleT<T>
        {
            return { std::move(handle.handle) };
        };

        if (allZero) {
            return enqueueFillArrayImpl(opName, region, MemoryRegionInitialization::INIT_ZERO_FILLED,
                                        start * sizeof(T), length == -1 ? length : length * sizeof(T),
                                        {}, {} /* prereqs */)
                .then(std::move(convert));
        }
        else {
            std::span<const std::byte> fillWith((const std::byte *)&val, sizeof(val));
            return enqueueFillArrayImpl(opName, region, MemoryRegionInitialization::INIT_BLOCK_FILLED,
                                        start * sizeof(T), length == -1 ? length : length * sizeof(T),
                                        fillWith, {} /* prereqs */)
                .then(std::move(convert));
        }
    }

    virtual void flush()
    {
    }

    virtual void finish()
    {
    }
};

struct ComputeContext {

    virtual ~ComputeContext() = default;

    virtual ComputePromiseT<MemoryRegionHandle>
    allocateImpl(const std::string & regionName,
                 size_t length, size_t align,
                 const std::type_info & type, bool isConst,
                 MemoryRegionInitialization initialization,
                 std::any initWith = std::any()) = 0;

    virtual MemoryRegionHandle
    allocateSyncImpl(const std::string & regionName,
                     size_t length, size_t align,
                     const std::type_info & type, bool isConst,
                     MemoryRegionInitialization initialization,
                     std::any initWith = std::any());

    virtual ComputePromiseT<MemoryRegionHandle>
    transferToDeviceImpl(const std::string & opName,
                         FrozenMemoryRegion region,
                         const std::type_info & type, bool isConst) = 0;

    virtual MemoryRegionHandle
    transferToDeviceSyncImpl(const std::string & opName,
                             FrozenMemoryRegion region,
                             const std::type_info & type, bool isConst);

    virtual ComputePromiseT<FrozenMemoryRegion>
    transferToHostImpl(const std::string & opName,
                       MemoryRegionHandle handle) = 0;

    virtual FrozenMemoryRegion
    transferToHostSyncImpl(const std::string & opName,
                           MemoryRegionHandle handle);

    virtual ComputePromiseT<MutableMemoryRegion>
    transferToHostMutableImpl(const std::string & opName,
                              MemoryRegionHandle handle) = 0;

    virtual MutableMemoryRegion
    transferToHostMutableSyncImpl(const std::string & opName,
                                  MemoryRegionHandle handle);

    virtual std::shared_ptr<ComputeKernel>
    getKernel(const std::string & kernelName) = 0;

    virtual ComputePromiseT<MemoryRegionHandle>
    managePinnedHostRegionImpl(const std::string & opName,
                               std::span<const std::byte> region, size_t align,
                               const std::type_info & type, bool isConst) = 0;

    virtual MemoryRegionHandle
    managePinnedHostRegionSyncImpl(const std::string & opName,
                                   std::span<const std::byte> region, size_t align,
                                   const std::type_info & type, bool isConst);

    virtual std::shared_ptr<ComputeQueue>
    getQueue() = 0;

    // Implement a slice operation (to get a subrange of a range)
    virtual MemoryRegionHandle
    getSliceImpl(const MemoryRegionHandle & handle, const std::string & regionName,
                 size_t startOffsetInBytes, size_t lengthInBytes,
                 size_t align, const std::type_info & type, bool isConst) = 0;

    template<typename T>
    ComputePromiseT<MemoryArrayHandleT<const T>>
    transferToDeviceImmutable(const std::string & opName, const FrozenMemoryRegionT<T> & obj)
    {
        auto convert = [] (MemoryRegionHandle handle) -> MemoryArrayHandleT<const T>
        {
            return { std::move(handle.handle) };
        };

        return transferToDeviceImpl(opName, obj, typeid(std::remove_const_t<T>), true /* isConst */)
            .then(std::move(convert));
    }

    template<typename T>
    ComputePromiseT<FrozenMemoryRegionT<std::remove_const_t<T>>>
    transferToHost(const std::string & opName, MemoryArrayHandleT<T> array)
    {
        array.template checkTypeAccessibleAs<std::remove_const_t<T>>();
        auto genericPromise = transferToHostImpl(opName, array);

        auto convert = [] (FrozenMemoryRegion region) -> FrozenMemoryRegionT<std::remove_const_t<T>>
        {
            return { std::move(region) };
        };

        return genericPromise.then(std::move(convert));
    }

    template<typename T>
    ComputePromiseT<MutableMemoryRegionT<std::remove_const_t<T>>>
    transferToHostMutable(const std::string & opName, MemoryArrayHandleT<T> array)
    {
        static_assert(!std::is_const_v<T>, "mutable transfer requires non-const type");
        array.template checkTypeAccessibleAs<std::remove_const_t<T>>();
        auto [handle, event] = transferToHostMutableImpl(array);
        MutableMemoryRegion raw(handle.handle(), (char *)handle.data(), handle.length() );
        return { std::move(raw), std::move(event) };
    }

    template<typename T>
    FrozenMemoryRegionT<std::remove_const_t<T>>
    transferToHostSync(const std::string & opName, MemoryArrayHandleT<T> array)
    {
        array.template checkTypeAccessibleAs<std::remove_const_t<T>>();
        return { transferToHostSyncImpl(opName, std::move(array)) };
    }

    template<typename T>
    MutableMemoryRegionT<T>
    transferToHostMutableSync(const std::string & opName, MemoryArrayHandleT<T> array)
    {
        static_assert(!std::is_const_v<T>, "mutable transfer requires non-const type");
        return transferToHostMutableSync(opName, std::move(array));
    }

    /** Transfers the array to the CPU so that it can be written from the CPU... but
     *  promises that the initial contents will never be read, which enables us to
     *  not copy any data to the CPU. */
    template<typename T>
    ComputePromiseT<MutableMemoryRegionT<T>>
    transferToHostUninitializedSync(const std::string & opName, MemoryArrayHandleT<T> array)
    {
        return transferToHostMutableSync(opName, std::move(array));
    }

    template<typename T>
    ComputePromiseT<MemoryArrayHandleT<T>>
    allocUninitializedArray(const std::string & regionName, size_t size)
    {
        auto convert = [] (MemoryRegionHandle handle) -> MemoryArrayHandleT<T>
        {
            return { std::move(handle.handle) };
        };

        return allocateImpl(regionName, size * sizeof(T), alignof(T), typeid(T), std::is_const_v<T>, INIT_NONE)
            .then(std::move(convert));
    }

    template<typename T>
    MemoryArrayHandleT<T>
    allocUninitializedArraySync(const std::string & regionName, size_t size)
    {
        auto convert = [] (MemoryRegionHandle handle) -> MemoryArrayHandleT<T>
        {
            return { std::move(handle.handle) };
        };

        return allocateSyncImpl(regionName, size * sizeof(T), alignof(T), typeid(T), std::is_const_v<T>, INIT_NONE);
    }

    template<typename T>
    ComputePromiseT<MemoryArrayHandleT<T>>
    allocZeroInitializedArray(const std::string & regionName, size_t size)
    {
        auto convert = [] (MemoryRegionHandle handle) -> MemoryArrayHandleT<T>
        {
            return { std::move(handle.handle) };
        };

        return allocateImpl(regionName, size * sizeof(T), alignof(T), typeid(T), std::is_const_v<T>, INIT_ZERO_FILLED)
            .then(std::move(convert));
    }

    template<typename T>
    ComputePromiseT<MemoryArrayHandleT<T>>
    manageMemoryRegion(const std::string & regionName, const std::vector<T> & obj)
    {
        return manageMemoryRegion(regionName, static_cast<std::span<const T>>(obj));
    }

    template<typename T>
    MemoryArrayHandleT<T>
    manageMemoryRegionSync(const std::string & regionName, const std::vector<T> & obj)
    {
        return manageMemoryRegionSync(regionName, static_cast<std::span<const T>>(obj));
    }

    template<typename T, size_t N>
    ComputePromiseT<MemoryArrayHandleT<T>>
    manageMemoryRegion(const std::string & regionName, const std::span<const T, N> & obj)
    {
        auto convert = [] (MemoryRegionHandle handle) -> MemoryArrayHandleT<T>
        {
            return { std::move(handle.handle) };
        };

        return managePinnedHostRegionImpl(regionName, std::as_bytes(obj), alignof(T),
                                      typeid(std::remove_const_t<T>), std::is_const_v<T>)
            .then(std::move(convert));
    }

    template<typename T, size_t N>
    MemoryArrayHandleT<T>
    manageMemoryRegionSync(const std::string & regionName, const std::span<const T, N> & obj)
    {
        return { managePinnedHostRegionSyncImpl(regionName, std::as_bytes(obj), alignof(T),
                                      typeid(std::remove_const_t<T>), std::is_const_v<T>).handle };
    }

    template<typename T>
    MemoryArrayHandleT<T>
    getArraySlice(const MemoryArrayHandleT<T> & array, const std::string & regionName, 
                  size_t startOffset, size_t length)
    {
        auto genericSlice = getSliceImpl(array, regionName,
                                         startOffset * sizeof(T), length * sizeof(T), alignof(T),
                                         typeid(std::remove_const_t<T>), std::is_const_v<T>);
        return { genericSlice.handle };
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
