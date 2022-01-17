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
#include <set>

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
    CPU = 7,     ///< Runs on the local CPU being used as a grid
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

MemoryRegionAccess parseAccess(const std::string & accessStr);
std::string printAccess(MemoryRegionAccess access);
std::ostream & operator << (std::ostream & stream, MemoryRegionAccess access);


enum MemoryRegionInitialization {
    INIT_NONE,         //< Random contents (but no sensitive data)
    INIT_ZERO_FILLED,  //< Zero-filled
    INIT_BLOCK_FILLED, //< Filled with copies of the given block
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

// Represents a tuneable parameter
struct ComputeTuneable {
    std::string name;
    int64_t defaultValue = 0;
};

DECLARE_STRUCTURE_DESCRIPTION(ComputeTuneable);


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


struct ComputeEvent {
    virtual ~ComputeEvent() = default;
    virtual std::shared_ptr<ComputeProfilingInfo> getProfilingInfo() const = 0;
    virtual void await() const = 0;

    // Returns a descriptive (debug) name for what's associated with this event
    virtual std::string label() const;

    // Once the event is resolved, run the function, and return another event that will
    // fire once the chained function has returned
    virtual std::shared_ptr<ComputeEvent> thenImpl(std::function<void ()> fn, const std::string & label) = 0;

};

enum class ComputeKernelOrdering: uint16_t {
    ORDERED,  //< Data is ordered along this dimension
    UNORDERED //< Data is unordered along this dimension
};

DECLARE_ENUM_DESCRIPTION(ComputeKernelOrdering);

// An array dimension
struct ComputeKernelDimension {
    bool tight = false;  ///< Is this a tight bound (we check equality not <= on the passed in array)

    std::shared_ptr<CommandExpression> bound; // or nullptr if no bounds

    // Do the indexes on this dimension of the array convey information (ordered) or not (unordered)
    ComputeKernelOrdering ordering = ComputeKernelOrdering::ORDERED;
};

DECLARE_STRUCTURE_DESCRIPTION(ComputeKernelDimension);

struct ComputeKernelType {
    ComputeKernelType() = default;

    ComputeKernelType(std::shared_ptr<const ValueDescription> baseType,
                      const std::string & access)
        : ComputeKernelType(std::move(baseType), parseAccess(access))
    {
    }

    ComputeKernelType(std::shared_ptr<const ValueDescription> baseType,
                      MemoryRegionAccess access)
        : baseType(std::move(baseType)), access(access)
    {
    }

    std::vector<uint16_t> simd;
    std::shared_ptr<const ValueDescription> baseType;
    MemoryRegionAccess access = ACC_NONE;

    std::string print() const;
    
    std::vector<ComputeKernelDimension> dims;  // if empty, scalar, otherwise n-dimensional array

    bool isCompatibleWith(const ComputeKernelType & otherType, std::string * reason = nullptr) const;
};

DECLARE_STRUCTURE_DESCRIPTION(ComputeKernelType);

ComputeKernelType parseType(const std::string & access, const std::string & type);
ComputeKernelType parseType(const std::string & accessAndType);
std::vector<ComputeKernelDimension> parseDimensions(const std::string & dimensionString);


/// Class used to handle arguments
struct AbstractArgumentHandler {
    virtual ~AbstractArgumentHandler() = default;

    ComputeKernelType type;
    bool isConst = false;

    virtual bool canGetPrimitive() const;

    virtual std::span<const std::byte>
    getPrimitive(const std::string & opName, ComputeQueue & queue) const;

    virtual bool canGetRange() const;

    virtual std::tuple<void *, size_t, std::shared_ptr<const void>>
    getRange(const std::string & opName, ComputeQueue & queue) const;

    virtual bool canGetConstRange() const;

    virtual std::tuple<const void *, size_t, std::shared_ptr<const void>>
    getConstRange(const std::string & opName, ComputeQueue & queue) const;

    virtual bool canGetHandle() const;

    virtual MemoryRegionHandle getHandle(const std::string & opName, ComputeQueue & queue) const;

    virtual std::string info() const;

    virtual Json::Value toJson() const = 0;

    // Return the given array element from the underlying storage, converting to the JSON representation
    virtual Json::Value getArrayElement(uint32_t index, ComputeQueue & queue) const = 0;

    // Set the argument to a new value from a reference source.  This is for debugging only,
    // for ensuring that two kernels get exactly the same input so that we can compare their
    // outputs.
    virtual void setFromReference(ComputeQueue & queue, std::span<const std::byte> reference) = 0;
};


// ComputeKernelArgument

struct ComputeKernelArgument {
    std::string name;
    std::shared_ptr<AbstractArgumentHandler> handler;
    bool has_value() const { return !!handler; }
};


// ComputeKernelConstraintScope

// Tells us when the constraint should hold
enum struct ComputeKernelConstraintScope {
    UNIVERSAL,
    PRE_RUN,
    POST_RUN
};

DECLARE_ENUM_DESCRIPTION(ComputeKernelConstraintScope);

struct ComputeKernelConstraintSolution;

// ComputeKernelConstraint

struct ComputeKernelConstraint {

    ComputeKernelConstraint() = default;

    ComputeKernelConstraint(std::shared_ptr<const CommandExpression> lhs,
                            std::string op,
                            std::shared_ptr<const CommandExpression> rhs,
                            std::string description);

    std::shared_ptr<const CommandExpression> lhs;
    std::string op;
    std::shared_ptr<const CommandExpression> rhs;
    std::string description;
    uint64_t hash;

    std::set<std::string> lhsVariables;
    std::set<std::string> lhsFunctions;
    std::set<std::string> rhsVariables;
    std::set<std::string> rhsFunctions;

    std::string print() const;

    // Attempt to satisfy the constraint.  Returns true if a change was,
    // made, false if no progress was made, and throws an
    // exception if it's not satisfiable.  Unsatisfied contains the list
    // variables with unknown values that will need to be determined
    // before the constraint can be found.
    bool attemptToSatisfy(ComputeKernelConstraintSolution & solution) const;

    // Is it satisfied?  Unsatisfiable will throw an exception.
    bool satisfied(const ComputeKernelConstraintSolution & solution) const;
};

DECLARE_STRUCTURE_DESCRIPTION(ComputeKernelConstraint);


// ComputeKernelConstraintSolution

struct ComputeKernelConstraintSolution {
    CommandExpressionVariables knowns;
    std::set<std::string> unknowns;
    std::set<std::string> unknownFunctions;

    // Set of hashes of constraints we know to be satisfied
    std::set<uint64_t> satisfied;

    bool hasValue(const std::string & variableName) const;
    void setValue(const std::string & variableName, const void * val, const ValueDescription & desc);
    void setValue(const std::string & variableName, const Json::Value & val);
    Json::Value getValue(const std::string & variableName) const;

    template<typename T>
    void setValue(const std::string & variableName, const T & val,
                  const std::shared_ptr<const ValueDescription> & desc = getDefaultDescriptionSharedT<T>())
    {
        setValue(variableName, &val, *desc);
    }

    bool hasFunction(const std::string & functionName) const;

    Json::Value evaluate(const CommandExpression & expr) const;
};

DECLARE_STRUCTURE_DESCRIPTION(ComputeKernelConstraintSolution);


// ComputeKernelConstraintSet

struct ComputeKernelConstraintSet {
    std::vector<ComputeKernelConstraint> constraints;

    void add(ComputeKernelConstraint constraint);

    void assertSolvedBy(const ComputeKernelConstraintSolution & solution) const;
};

DECLARE_STRUCTURE_DESCRIPTION(ComputeKernelConstraintSet);

ComputeKernelConstraintSolution solve(const ComputeKernelConstraintSolution & solutionIn,
                                      const ComputeKernelConstraintSet & constraints);

ComputeKernelConstraintSolution solve(const ComputeKernelConstraintSolution & solutionIn,
                                      const ComputeKernelConstraintSet & constraints1,
                                      const ComputeKernelConstraintSet & constraints2);
ComputeKernelConstraintSolution fullySolve(const ComputeKernelConstraintSolution & solutionIn,
                                           const ComputeKernelConstraintSet & constraints);

ComputeKernelConstraintSolution fullySolve(const ComputeKernelConstraintSolution & solutionIn,
                                           const ComputeKernelConstraintSet & constraints1,
                                           const ComputeKernelConstraintSet & constraints2);


// ComputeKernelConstraintManager

struct ComputeKernelConstraintManager {
    // Constraints that need to be met that are known before binding
    ComputeKernelConstraintSet constraints;

    // Constraints that need to be met, for the pre-kernel call.  Can only rely on
    // information known pre-call (kernel arguments, etc) but nothing calculated
    // by the kernel.  These are no longer active after the kernel call, allowing
    // things like dynamic lengths.
    ComputeKernelConstraintSet preConstraints;

    // Constraints that need to be met, post the kernel call.  Can rely on information
    // calculated by the kernel itself.
    ComputeKernelConstraintSet postConstraints;

    void addConstraint(const std::string lhs, const std::string & op, const std::string & rhs,
                       const std::string & description = "",
                       ComputeKernelConstraintScope scope = ComputeKernelConstraintScope::UNIVERSAL);
    void addConstraint(std::shared_ptr<const CommandExpression> lhs,
                       const std::string & op, const std::string & rhs,
                       const std::string & description = "",
                       ComputeKernelConstraintScope scope = ComputeKernelConstraintScope::UNIVERSAL);
    void addConstraint(std::shared_ptr<const CommandExpression> lhs,
                       const std::string & op,
                       std::shared_ptr<const CommandExpression> rhs,
                       const std::string & description = "",
                       ComputeKernelConstraintScope scope = ComputeKernelConstraintScope::UNIVERSAL);

    void addPreConstraint(const std::string lhs, const std::string & op, const std::string & rhs,
                          const std::string & description = "")
    {
        addConstraint(std::move(lhs), std::move(op), std::move(rhs), description, ComputeKernelConstraintScope::PRE_RUN);
    }

    void addPreConstraint(std::shared_ptr<const CommandExpression> lhs,
                          const std::string & op, const std::string & rhs,
                          const std::string & description = "")
    {
        addConstraint(std::move(lhs), std::move(op), std::move(rhs), description, ComputeKernelConstraintScope::PRE_RUN);
    }

    void addPreConstraint(std::shared_ptr<const CommandExpression> lhs,
                          const std::string & op,
                          std::shared_ptr<const CommandExpression> rhs,
                          const std::string & description = "")
    {
        addConstraint(std::move(lhs), std::move(op), std::move(rhs), description, ComputeKernelConstraintScope::PRE_RUN);
    }

    void addPostConstraint(const std::string lhs, const std::string & op, const std::string & rhs,
                           const std::string & description = "")
    {
        addConstraint(std::move(lhs), std::move(op), std::move(rhs), description, ComputeKernelConstraintScope::POST_RUN);
    }

    void addPostConstraint(std::shared_ptr<const CommandExpression> lhs,
                           const std::string & op, const std::string & rhs,
                           const std::string & description = "")
    {
        addConstraint(std::move(lhs), std::move(op), std::move(rhs), description, ComputeKernelConstraintScope::POST_RUN);
    }

    void addPostConstraint(std::shared_ptr<const CommandExpression> lhs,
                           const std::string & op,
                           std::shared_ptr<const CommandExpression> rhs,
                           const std::string & description = "")
    {
        addConstraint(std::move(lhs), std::move(op), std::move(rhs), description, ComputeKernelConstraintScope::POST_RUN);
    }

};

/// Opaque structure subclassed by each ComputeKernel implementation to store
/// information on how it's bound.  This is used rather than an anonymous shared
/// ptr so that it can be checked with dynamic_cast to catch programming errors
/// in mixing kernels across contexts.
struct ComputeKernelBindInfo {
    virtual ~ComputeKernelBindInfo() = default;
};

// ComptuteKernel

struct ComputeKernel: public ComputeKernelConstraintManager {
    virtual ~ComputeKernel() = default;

    std::string kernelName;
    ComputeDevice device;
    ComputeContext * context = nullptr;

    struct ParameterInfo {
        std::string name;
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

    // List of tuneable parameters
    std::vector<ComputeTuneable> tuneables;

    void addParameter(const std::string & parameterName, const std::string & access, const std::string & typeStr)
    {
        if (!paramIndex.emplace(parameterName, params.size()).second) {
            throw AnnotatedException(500, "Duplicate kernel parameter name: '" + parameterName + "'");
        }
        params.push_back({parameterName, parseType(access, typeStr)});
    }

    void addParameter(const std::string & parameterName, ComputeKernelType type)
    {
        if (!paramIndex.emplace(parameterName, params.size()).second) {
            throw AnnotatedException(500, "Duplicate kernel parameter name: '" + parameterName + "'");
        }
        params.push_back({parameterName, std::move(type)});
    }

    void addDimension(const std::string & dimensionName, const std::string & range,
                      uint32_t defaultDimension = 0)
    {
        dims.push_back({dimensionName, range, defaultDimension});
    }

    // Add a tuneable parameter to the set of constraints, with a default value
    void addTuneable(const std::string & variableName, int64_t defaultValue);

    // bind() is called with (name, value) pairs to set the value of particular parameters
    template<typename... NamesAndArgs>
    BoundComputeKernel bind(ComputeQueue & queue, NamesAndArgs&&... namesAndArgs);

    // Perform the abstract bind() operation, returning a BoundComputeKernel
    virtual BoundComputeKernel
    bindImpl(ComputeQueue & queue,
             std::vector<ComputeKernelArgument> arguments,
             ComputeKernelConstraintSolution knowns) const = 0;
};


// BoundComputeKernel

struct BoundComputeKernel: public ComputeKernelConstraintManager {
    const ComputeKernel * owner = nullptr;
    std::vector<ComputeKernelArgument> arguments;
    std::shared_ptr<ComputeKernelBindInfo> bindInfo;

    // Known quantities about the kernel
    ComputeKernelConstraintSolution knowns;

    // List of tuneable parameters and their parameters
    std::vector<ComputeTuneable> tuneables;

    void setKnownsFromArguments();
};

namespace details {

// Forward definition as ComputeContext is defined later on
template<typename T>
FrozenMemoryRegionT<std::remove_const_t<T>>
transferToHostSync(ComputeQueue & queue, const MemoryArrayHandleT<T> & handle);

template<typename T>
MutableMemoryRegionT<T>
transferToHostMutableSync(ComputeQueue & queue, const MemoryArrayHandleT<T> & handle);

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
    getRange(const std::string & opName, ComputeQueue & queue) const override;

    virtual bool canGetConstRange() const override;

    virtual std::tuple<const void *, size_t, std::shared_ptr<const void>>
    getConstRange(const std::string & opName, ComputeQueue & queue) const override;

    virtual bool canGetHandle() const override;

    virtual MemoryRegionHandle
    getHandle(const std::string & opName, ComputeQueue & queue) const override;

    virtual std::string info() const override;

    virtual Json::Value toJson() const override;

    virtual Json::Value getArrayElement(uint32_t index, ComputeQueue & queue) const override;

    virtual void setFromReference(ComputeQueue & queue, std::span<const std::byte> reference);
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
        this->type.access = ACC_READ;
    }

    std::any val;
    std::span<const std::byte> mem;

    virtual bool canGetPrimitive() const override;

    virtual std::span<const std::byte>
    getPrimitive(const std::string & opName, ComputeQueue & queue) const override;

    virtual std::string info() const override;

    virtual Json::Value toJson() const override;

    virtual Json::Value getArrayElement(uint32_t index, ComputeQueue & queue) const override;

    virtual void setFromReference(ComputeQueue & queue, std::span<const std::byte> reference);
};

template<typename T>
PrimitiveAbstractArgumentHandler *
getArgumentHandler(T value)
{
    return new PrimitiveAbstractArgumentHandler(std::move(value));
}

template<typename Arg>
void bindOne(const ComputeKernel * owner,
             ComputeQueue & queue,
             std::vector<ComputeKernelArgument> & arguments,
             ComputeKernelConstraintSolution & knowns,
             const std::string & argName,
             Arg&& arg)
{
    ExcAssert(owner);

    auto handler = getArgumentHandler(std::forward<Arg>(arg));

    knowns.setValue(argName, handler->toJson());

    auto argIndexIt = owner->paramIndex.find(argName);
    if (argIndexIt == owner->paramIndex.end()) {
        return;  // we don't require all passed arguments to be used; some arguments are not
        // needed by some kernels
        throw MLDB::Exception("Couldn't bind arg: argument " + argName
                                + " is not an argument of kernel " + owner->kernelName);
    }

    size_t argIndex = argIndexIt->second;
    //using namespace std;
    //cerr << "binding " << argName << " at index " << argIndex << " to value of type " << type_name<Arg>() << endl;

    if (arguments.at(argIndex).has_value())
        throw MLDB::Exception("Attempt to double bind argument " + argName
                                + " of kernel " + owner->kernelName);

    arguments[argIndex].name = argName;
    arguments[argIndex].handler.reset(handler);
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

inline void bind(const ComputeKernel * owner,
                 ComputeQueue & queue,
                 std::vector<ComputeKernelArgument> & arguments,
                 ComputeKernelConstraintSolution & knowns) // end of recursion
{
    // allow for other ways of setting arguments...
#if 0
    for (size_t i = 0;  i < arguments.size();  ++i) {
        if (!arguments[i].has_value()) {
            throw MLDB::Exception("kernel " + owner->kernelName + " didn't set argument "
                                    + owner->params.at(i).name);
        }
    }
#endif
}

template<typename Arg, typename... Rest>
void bind(const ComputeKernel * owner,
          ComputeQueue & queue,
          std::vector<ComputeKernelArgument> & arguments,
          ComputeKernelConstraintSolution & knowns,
          const std::string & argName, Arg&& arg, Rest&&... rest)
{
    details::bindOne(owner, queue, arguments, knowns, argName, std::forward<Arg>(arg));
    details::bind(owner, queue, arguments, knowns, std::forward<Rest>(rest)...);
}

} // namespace details

template<typename... NamesAndArgs>
BoundComputeKernel ComputeKernel::bind(ComputeQueue & queue, NamesAndArgs&&... namesAndArgs)
{
    // These are bound to the values in NamesAndArgs.  Note that we accumulate arguments in the
    // order of the parameters for the kernel, not in the calling order.  That's why arguments
    // has the same length as this->params; there is a 1-1 correspondence.
    std::vector<ComputeKernelArgument> arguments(this->params.size());

    ComputeKernelConstraintSolution knowns;

    // Set the names for debugging
    for (size_t i = 0;  i < this->params.size();  ++i)
        arguments[i].name = this->params[i].name;

    details::bind(this, queue, arguments, knowns, std::forward<NamesAndArgs>(namesAndArgs)...);

    return this->bindImpl(queue, std::move(arguments), std::move(knowns));
}

struct ComputeQueue {
    ComputeQueue(ComputeContext * owner, ComputeQueue * parent = nullptr)
        : owner(owner), parent(parent)
    {
    }

    virtual ~ComputeQueue() = default;

    ComputeContext * owner = nullptr;
    ComputeQueue * parent = nullptr;

    std::mutex kernelWallTimesMutex;
    std::map<std::string, double> kernelWallTimes; // in milliseconds
    double totalKernelTime = 0.0;

    // Return a queue that processes its sub-operations in parallel
    virtual std::shared_ptr<ComputeQueue> parallel(const std::string & opName) = 0;

    // Return a queue that processes its sub-operations in serial
    virtual std::shared_ptr<ComputeQueue> serial(const std::string & opName) = 0;

    virtual void
    enqueue(const std::string & opName,
            const BoundComputeKernel & kernel,
            const std::vector<uint32_t> & grid) = 0;

    virtual void
    enqueueFillArrayImpl(const std::string & opName,
                         MemoryRegionHandle region, MemoryRegionInitialization init,
                         size_t startOffsetInBytes, ssize_t lengthInBytes,
                         std::span<const std::byte> block);

    virtual void
    enqueueCopyFromHostImpl(const std::string & opName,
                            MemoryRegionHandle toRegion,
                            FrozenMemoryRegion fromRegion,
                            size_t deviceStartOffsetInBytes) = 0;

    virtual void
    copyFromHostSyncImpl(const std::string & opName,
                         MemoryRegionHandle toRegion,
                         FrozenMemoryRegion fromRegion,
                         size_t deviceStartOffsetInBytes) = 0;

    virtual FrozenMemoryRegion
    enqueueTransferToHostImpl(const std::string & opName,
                              MemoryRegionHandle handle) = 0;

    virtual FrozenMemoryRegion
    transferToHostSyncImpl(const std::string & opName,
                           MemoryRegionHandle handle);

    virtual MutableMemoryRegion
    enqueueTransferToHostMutableImpl(const std::string & opName,
                                     MemoryRegionHandle handle) = 0;

    virtual MutableMemoryRegion
    transferToHostMutableSyncImpl(const std::string & opName,
                                  MemoryRegionHandle handle);

    virtual MemoryRegionHandle
    enqueueManagePinnedHostRegionImpl(const std::string & opName,
                                      std::span<const std::byte> region, size_t align,
                                      const std::type_info & type, bool isConst) = 0;

    virtual MemoryRegionHandle
    managePinnedHostRegionSyncImpl(const std::string & opName,
                                   std::span<const std::byte> region, size_t align,
                                   const std::type_info & type, bool isConst) = 0;

    virtual void
    enqueueCopyBetweenDeviceRegionsImpl(const std::string & opName,
                                        MemoryRegionHandle from, MemoryRegionHandle to,
                                        size_t fromOffset, size_t toOffset,
                                        size_t length) = 0;

    virtual void
    copyBetweenDeviceRegionsSyncImpl(const std::string & opName,
                                     MemoryRegionHandle from, MemoryRegionHandle to,
                                     size_t fromOffset, size_t toOffset,
                                     size_t length) = 0;

    template<typename T>
    FrozenMemoryRegionT<std::remove_const_t<T>>
    enqueueTransferToHost(const std::string & opName, MemoryArrayHandleT<T> array)
    {
        array.template checkTypeAccessibleAs<std::remove_const_t<T>>();
        return { transferToHostImpl(opName, std::move(array)) };
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
        return { transferToHostMutableSyncImpl(opName, std::move(array)) };
    }

    template<typename T>
    MemoryArrayHandleT<const T>
    transferToDeviceImmutable(const std::string & opName, const FrozenMemoryRegionT<T> & obj)
    {
        return { managePinnedHostRegionSyncImpl(opName, std::as_bytes(obj.getConstSpan()), alignof(T), typeid(std::remove_const_t<T>), true /* isConst */).handle };
    }

    template<typename T>
    MemoryArrayHandleT<T>
    enqueueManageMemoryRegion(const std::string & regionName, const std::vector<T> & obj)
    {
        return enqueueManageMemoryRegion(regionName, static_cast<std::span<const T>>(obj));
    }

    template<typename T>
    MemoryArrayHandleT<T>
    manageMemoryRegionSync(const std::string & regionName, const std::vector<T> & obj)
    {
        return manageMemoryRegionSync(regionName, static_cast<std::span<const T>>(obj));
    }

    template<typename T, size_t N>
    MemoryArrayHandleT<T>
    enqueueManageMemoryRegion(const std::string & regionName, const std::span<const T, N> & obj)
    {
        auto convert = [] (MemoryRegionHandle handle) -> MemoryArrayHandleT<T>
        {
            return { std::move(handle.handle) };
        };

        return enqueueManagePinnedHostRegionImpl(regionName, std::as_bytes(obj), alignof(T),
                                      typeid(std::remove_const_t<T>), std::is_const_v<T>)
            .then(std::move(convert), regionName + " then manageMemoryRegion:convert");
    }

    template<typename T, size_t N>
    MemoryArrayHandleT<T>
    manageMemoryRegionSync(const std::string & regionName, const std::span<const T, N> & obj)
    {
        return { managePinnedHostRegionSyncImpl(regionName, std::as_bytes(obj), alignof(T),
                                      typeid(std::remove_const_t<T>), std::is_const_v<T>).handle };
    }

    template<typename T>
    void enqueueCopyFromHost(const std::string & opName, const MemoryArrayHandleT<T> & handle, const std::span<const T> & vals)
    {
        FrozenMemoryRegion region(nullptr, (const char *)vals.data(), vals.size_bytes());
        enqueueCopyFromHostImpl(opName, handle, region, 0 /* start offset in bytes */);
    }

    template<typename T>
    void enqueueCopyFromHost(const std::string & opName, const MemoryArrayHandleT<T> & handle, const T & val)
    {
        enqueueCopyFromHost(opName, handle, std::span{ &val, 1});
    }

    template<typename T>
    void enqueueCopyFromHost(const std::string & opName, const MemoryArrayHandleT<T> & handle, const std::vector<T> & vals)
    {
        enqueueCopyFromHost(opName, handle, std::span{vals});
    }

    // Create an already resolved event (this is abstract so that the subclass may create an)
    // event of the correct type).
    virtual std::shared_ptr<ComputeEvent>
    makeAlreadyResolvedEvent(const std::string & label) const = 0;

    template<typename T>
    void
    enqueueFillArray(const std::string & opName,
                     const MemoryArrayHandleT<T> & region, const T & val,
                     size_t start = 0, ssize_t length = -1)
    {
        const char * valBytes = (const char *)&val;
 
        // If all bytes in the initialization are zero, we can do it more efficiently
        bool allZero = true;
        for (size_t i = 0;  allZero && i < sizeof(T);  allZero = valBytes[i++] == 0) ;
 
        if (allZero) {
            enqueueFillArrayImpl(opName, region, MemoryRegionInitialization::INIT_ZERO_FILLED,
                                        start * sizeof(T), length == -1 ? length : length * sizeof(T),
                                        {});
        }
        else {
            std::span<const std::byte> fillWith((const std::byte *)&val, sizeof(val));
            enqueueFillArrayImpl(opName, region, MemoryRegionInitialization::INIT_BLOCK_FILLED,
                                        start * sizeof(T), length == -1 ? length : length * sizeof(T),
                                        fillWith);
        }
    }

    virtual void enqueueBarrier(const std::string & label) = 0;

    virtual std::shared_ptr<ComputeEvent> flush() = 0;

    virtual void finish() = 0;
};


// ComputeMarker
// Marks a set of operations, so that tracing and debugging are able to label and group
// actions.
struct ComputeMarker {
    virtual ~ComputeMarker() = default;

    virtual std::shared_ptr<ComputeMarker> enterScope(const std::string & scopeName);
};


// ComputeContext

struct ComputeContext {

    virtual ~ComputeContext() = default;

    // Get the primary device this context covers
    virtual ComputeDevice getDevice() const = 0;

    // Allow the context to take ownership of things that need to be cached for the
    // lifetime of the context.
    mutable std::mutex cacheMutex;
    std::map<std::string, std::any> cache;

    // Return the (generic) cache entry for the given key
    std::any getCacheEntry(const std::string & key) const;

    // Return a typed entry for the given key.  The type T must match that under which the
    // entry was cached.
    template<typename T>
    T getCacheEntry(const std::string & key) const
    {
        return std::any_cast<T>(getCacheEntry(key));
    }

    // Get the entry, using the given function to create it if it doesn't exist
    template<typename F, typename T = std::invoke_result_t<F>>
    T getCacheEntry(const std::string & key, F && createEntry)
    {
        std::unique_lock guard(cacheMutex);
        auto it = cache.find(key);
        if (it == cache.end()) {
            // TODO: release the cache mutex
            it = cache.emplace(key, createEntry()).first;
        }
        return std::any_cast<T>(it->second);
    }
 
    // Set an entry generically.  If the entry is already set, nothing happens.
    // In any case, the current value is returned.
    std::any setCacheEntry(const std::string & key, std::any value);

    // Return a marker that enters the named scope when created
    virtual std::shared_ptr<ComputeMarker> getScopedMarker(const std::string & scopeName);

    // Record a marker event, without creating a scope
    virtual void recordMarkerEvent(const std::string & event);

    virtual MemoryRegionHandle
    allocateSyncImpl(const std::string & regionName,
                     size_t length, size_t align,
                     const std::type_info & type, bool isConst) = 0;

    virtual std::shared_ptr<ComputeKernel>
    getKernel(const std::string & kernelName) = 0;

    virtual std::shared_ptr<ComputeQueue>
    getQueue(const std::string & queueName) = 0;

    // Implement a slice operation (to get a subrange of a range)
    virtual MemoryRegionHandle
    getSliceImpl(const MemoryRegionHandle & handle, const std::string & regionName,
                 size_t startOffsetInBytes, size_t lengthInBytes,
                 size_t align, const std::type_info & type, bool isConst) = 0;

    template<typename T>
    MemoryArrayHandleT<T>
    allocUninitializedArraySync(const std::string & regionName, size_t size)
    {
        auto convert = [] (MemoryRegionHandle handle) -> MemoryArrayHandleT<T>
        {
            return { std::move(handle.handle) };
        };

        return convert(allocateSyncImpl(regionName, size * sizeof(T), alignof(T), typeid(T), std::is_const_v<T>));
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
transferToHostSync(ComputeQueue & queue, const MemoryArrayHandleT<T> & handle)
{
    return queue.transferToHostSync(handle);
}

template<typename T>
MutableMemoryRegionT<T>
transferToHostMutableSync(ComputeQueue & queue, const MemoryArrayHandleT<T> & handle)
{
    return queue.transferToHostMutableSync(handle);
}

} // namespace details

} // namespace MLDB
