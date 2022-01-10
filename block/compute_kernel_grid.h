/** compute_kernel_grid.h                                                -*- C++ -*-
    Jeremy Barnes, 27 March 2016
    This file is part of MLDB. Copyright 2016 mldb.ai inc. All rights reserved.

    Compute kernel runtime for grid-based devices (normally GPU, but hopefully can be
    emulated on the CPU for correctness).  Grid, OpenCL, CUDA, ...
*/

#pragma once

#include "mldb/block/compute_kernel.h"
#include "mldb/arch/spinlock.h"
#include "mldb/utils/environment.h"

namespace MLDB {

struct GridComputeContext;
struct GridComputeKernel;
struct GridComputeQueue;
struct GridComputeKernelTemplate;
struct GridComputeKernelSpecialization;

// Tracing

enum class OperationType {
    GRID_COMPUTE = 1,
    METAL_COMPUTE = 2,
    OPENCL_COMPUTE = 3,
    HOST_COMPUTE = 4,
    USER = 1000
};

enum class OperationScope {
    ENTER = 1,
    EXIT = 2,
    EXIT_EXC = 3,
    EVENT = 4
};

extern EnvOption<int> GRID_TRACE_API_CALLS;

void traceOperationImpl(OperationScope opScope, OperationType opType, const std::string & opName,
                    const std::string & renderedArgs);

inline std::string renderArgs() { return {}; }

template<typename... Args>
inline std::string renderArgs(const std::string & arg1, Args&&... args)
{
    return MLDB::format(arg1.c_str(), std::forward<Args>(args)...);
}

template<typename... Args>
void traceOperation(OperationScope opScope, OperationType opType, const std::string & opName, Args&&... args)
{
    if (GRID_TRACE_API_CALLS.get()) {
        traceOperationImpl(opScope, opType, opName, renderArgs(std::forward<Args>(args)...));
    }
}

template<typename... Args>
void traceOperation(OperationType opType, const std::string & opName, Args&&... args)
{
    traceOperation(OperationScope::EVENT, opType, opName, std::forward<Args>(args)...);
}

template<typename... Args>
void traceGridOperation(const std::string & opName, Args&&... args)
{
    traceOperation(OperationScope::EVENT, OperationType::GRID_COMPUTE, opName, std::forward<Args>(args)...);
}

struct ScopedOperation {
    ScopedOperation(const ScopedOperation &) = delete;
    auto operator = (const ScopedOperation &) = delete;

    template<typename... Args>
    ScopedOperation(OperationType opType,
                    const std::string & opName, Args&&... args)
        : opType(opType), opName(opName)
    {
        if (GRID_TRACE_API_CALLS.get()) {
           traceOperation(OperationScope::ENTER, opType, opName, std::forward<Args>(args)...);
            incrementOpCount();
            timer.reset(new Timer);
        }
    }

    static void incrementOpCount();

    ~ScopedOperation();

    OperationType opType;
    std::string opName;
    std::unique_ptr<Timer> timer;
};

template<typename... Args>
ScopedOperation MLDB_WARN_UNUSED_RESULT scopedOperation(OperationType opType, const std::string & opName, Args&&... args) 
{
    return ScopedOperation(opType, opName, std::forward<Args>(args)...);
}


// Memory Handles

struct GridMemoryRegionHandleInfo: public MemoryRegionHandleInfo {
    GridMemoryRegionHandleInfo()
    {
        manager = std::make_shared<Manager>();
    };

    virtual ~GridMemoryRegionHandleInfo() = default;

    virtual GridMemoryRegionHandleInfo * clone() const = 0;

    size_t offset = 0;
    //std::any buffer;  ///< Really the underlying type

    // If we're managing host memory, this is where it is.
    const std::byte * backingHostMem = nullptr;

    struct Manager {
        Spinlock mutex;
        int numReaders = 0;
        int numWriters = 0;
        std::set<std::string> currentWriters;
        std::set<std::string> currentReaders;

        std::shared_ptr<const void>
        pinAccess(const std::string & opName, MemoryRegionHandleInfo * info, MemoryRegionAccess access);
    };

    std::shared_ptr<Manager> manager;

    std::tuple<FrozenMemoryRegion, int /* version */>
    getReadOnlyHostAccessSync(const GridComputeContext & context,
                              const std::string & opName,
                              size_t offset,
                              ssize_t length,
                              bool ignoreHazards);

    // Returns a pin that encodes the given access type
    std::shared_ptr<const void>
    pinAccess(const std::string & opName, MemoryRegionAccess access);
};

struct GridBindInfo: public ComputeKernelBindInfo {
    GridBindInfo(const GridComputeKernel * owner) : owner(owner) {}

    virtual ~GridBindInfo() = default;
    const GridComputeKernel * owner = nullptr;
    std::shared_ptr<StructuredSerializer> traceSerializer;

    // Pins that control the lifetime of the arguments and allow the system to know
    // when an argument is no longer needed
    std::vector<std::shared_ptr<const void>> argumentPins;
};

struct GridComputeProfilingInfo: public ComputeProfilingInfo {
    GridComputeProfilingInfo();
    virtual ~GridComputeProfilingInfo() = default;
};

// enable_shared_from_this is to ensure that we can pin lifetimes of events until the
// completion handlers have finished.
struct GridComputeEvent: public ComputeEvent, std::enable_shared_from_this<GridComputeEvent> {
    GridComputeEvent(const std::string & label, bool resolved,
                     const GridComputeQueue * owner);  // may or may not be already resolved

    virtual ~GridComputeEvent() = default;

    virtual std::shared_ptr<ComputeProfilingInfo> getProfilingInfo() const override;

    virtual std::shared_ptr<ComputeEvent> thenImpl(std::function<void ()> fn, const std::string & label);

    void resolve();

    virtual std::string label() const override { return label_; }
    std::string label_;

    const GridComputeQueue * owner = nullptr;

    std::mutex mutex;
    std::atomic<bool> isResolved = false;  // always starts this way, only resolve() can change
    std::promise<void> promise;
    std::shared_future<void> future;
    std::vector<std::function<void ()>> callbacks;
};

// GridDispatchType

enum class GridDispatchType {
    SERIAL,  ///< Wait for previous before dispatching next
    PARALLEL ///< Dispatch all at once with synchronization via explicit barriers
};

DECLARE_ENUM_DESCRIPTION(GridDispatchType);


// GridBindContext

// This is used to bind parameters and enqueue a grid launch
struct GridBindContext {
    virtual ~GridBindContext() = default;

    virtual void setPrimitive(const std::string & opName, int argNum, std::span<const std::byte> bytes) = 0;
    virtual void setBuffer(const std::string & opName, int argNum,
                           std::shared_ptr<GridMemoryRegionHandleInfo> handle,
                           MemoryRegionAccess access) = 0;
    virtual void setThreadGroupMemory(const std::string & opName, int argNum, size_t nBytes) = 0;

    // Subclasses override to launch the bound kernel
    virtual void launch(const std::string & opName, GridBindContext & context, std::vector<size_t> grid, std::vector<size_t> block) = 0;
};


// GridComputeQueue

struct GridComputeQueue: public ComputeQueue, std::enable_shared_from_this<GridComputeQueue> {
    GridComputeQueue(GridComputeContext * owner, GridComputeQueue * parent,
                      const std::string & label, GridDispatchType dispatchType);
    virtual ~GridComputeQueue();

    GridComputeContext * gridOwner = nullptr;
    std::weak_ptr<GridComputeQueue> weakParent;
    std::atomic<int> numChildren = 0;

    // What kind of dispatch (serial or parallel) do we do?
    GridDispatchType dispatchType;

    virtual void
    enqueue(const std::string & opName,
            const BoundComputeKernel & kernel,
            const std::vector<uint32_t> & grid) override;

    virtual void
    enqueueFillArrayImpl(const std::string & opName,
                         MemoryRegionHandle region, MemoryRegionInitialization init,
                         size_t startOffsetInBytes, ssize_t lengthInBytes,
                         std::span<const std::byte> block) override;

    virtual void
    enqueueCopyFromHostImpl(const std::string & opName,
                            MemoryRegionHandle toRegion,
                            FrozenMemoryRegion fromRegion,
                            size_t deviceStartOffsetInBytes) override;

    virtual void
    copyFromHostSyncImpl(const std::string & opName,
                         MemoryRegionHandle toRegion,
                         FrozenMemoryRegion fromRegion,
                         size_t deviceStartOffsetInBytes) override;

    virtual FrozenMemoryRegion
    enqueueTransferToHostImpl(const std::string & opName,
                              MemoryRegionHandle handle) override;

    virtual FrozenMemoryRegion
    transferToHostSyncImpl(const std::string & opName,
                           MemoryRegionHandle handle) override;

    virtual MemoryRegionHandle
    enqueueManagePinnedHostRegionImpl(const std::string & opName,
                                      std::span<const std::byte> region, size_t align,
                                      const std::type_info & type, bool isConst) override;

#if 0
    virtual MemoryRegionHandle
    managePinnedHostRegionSyncImpl(const std::string & opName,
                                   std::span<const std::byte> region, size_t align,
                                   const std::type_info & type, bool isConst) override;

    virtual void
    enqueueCopyBetweenDeviceRegionsImpl(const std::string & opName,
                                        MemoryRegionHandle from, MemoryRegionHandle to,
                                        size_t fromOffset, size_t toOffset,
                                        size_t length) override;

    virtual void
    copyBetweenDeviceRegionsSyncImpl(const std::string & opName,
                                     MemoryRegionHandle from, MemoryRegionHandle to,
                                     size_t fromOffset, size_t toOffset,
                                     size_t length) override;
#endif

    virtual void finish() override;

protected:
    // These methods are where the work is (concretely) done.  They are called by the implementations
    // above.  This enables tracing and argument checking to be handled in one single place, versus
    // needing to implement it for each.
    virtual void
    enqueueZeroFillArrayConcrete(const std::string & opName,
                                 MemoryRegionHandle region,
                                 size_t startOffsetInBytes, ssize_t lengthInBytes) = 0;
    virtual void
    enqueueBlockFillArrayConcrete(const std::string & opName,
                                  MemoryRegionHandle region,
                                  size_t startOffsetInBytes, ssize_t lengthInBytes,
                                  std::span<const std::byte> block) = 0;
    virtual void
    enqueueCopyFromHostConcrete(const std::string & opName,
                                MemoryRegionHandle toRegion,
                                FrozenMemoryRegion fromRegion,
                                size_t deviceStartOffsetInBytes) = 0;

    virtual FrozenMemoryRegion
    enqueueTransferToHostConcrete(const std::string & opName, MemoryRegionHandle handle) = 0;

    virtual FrozenMemoryRegion
    transferToHostSyncConcrete(const std::string & opName, MemoryRegionHandle handle) = 0;

    // Subclasses override to create a new bind context which binds and launches kernels
    virtual std::shared_ptr<GridBindContext>
    newBindContext(const std::string & opName, const GridComputeKernel * kernel, const GridBindInfo * bindInfo) = 0;
};


// GridComputeMarker

struct GridComputeMarker: public ComputeMarker {
    GridComputeMarker(const std::string & scopeName);
    virtual ~GridComputeMarker();

    virtual std::shared_ptr<ComputeMarker> enterScope(const std::string & scopeName);

    std::shared_ptr<void> scope;
};

// GridComputeFunctionDisposition

// Tells us what role the argument plays in the function (and ultimately, how we need
// to pass it to the function).
enum class GridComputeFunctionArgumentDisposition {
    BUFFER,      ///< Argument needs to be passed a buffer
    LITERAL,     ///< Argument needs to be passed a literal value
    THREADGROUP  ///< Argument requires a threadgroup allocation
};

DECLARE_ENUM_DESCRIPTION(GridComputeFunctionArgumentDisposition);


// GridComputeFunctionArgument

struct GridComputeFunctionArgument {
    std::string name;
    int computeFunctionArgIndex = -1;
    ComputeKernelType type;
    GridComputeFunctionArgumentDisposition disposition;
    Any implInfo;
};

DECLARE_STRUCTURE_DESCRIPTION(GridComputeFunctionArgument);


// GridComputeFunction

struct GridComputeFunction {
    
    virtual std::vector<GridComputeFunctionArgument> getArgumentInfo() const = 0;
};


// GridComputeFunctionLibrary

struct GridComputeFunctionLibrary {
    virtual ~GridComputeFunctionLibrary() = default;

    virtual std::shared_ptr<GridComputeFunction>
    getFunction(const std::string & kernelName) = 0;

    virtual std::string getId() const = 0;

    virtual Json::Value getMetadata() const = 0;
};


// GridComputeContext

struct GridComputeContext: public ComputeContext {

    GridComputeContext(ComputeDevice device);

    virtual ~GridComputeContext() = default;

    ComputeDevice device;

    virtual ComputeDevice getDevice() const override;

    // Return a marker that enters the named scope when created
    virtual std::shared_ptr<ComputeMarker> getScopedMarker(const std::string & scopeName) override;

    // Record a marker event, without creating a scope
    virtual void recordMarkerEvent(const std::string & event) override;

    // pin, region, length in bytes
    static std::tuple<std::shared_ptr<const void>, std::any, size_t>
    getMemoryRegion(const std::string & opName, MemoryRegionHandleInfo & handle,
                    MemoryRegionAccess access);

    std::tuple<FrozenMemoryRegion, int /* version */>
    getFrozenHostMemoryRegion(const std::string & opName,
                              MemoryRegionHandleInfo & handle,
                              size_t offset, ssize_t length,
                              bool ignoreHazards) const;

#if 0
    virtual MemoryRegionHandle
    allocateSyncImpl(const std::string & regionName,
                     size_t length, size_t align,
                     const std::type_info & type, bool isConst) override;
#endif

    virtual std::shared_ptr<ComputeKernel>
    getKernel(const std::string & kernelName) override;

    virtual std::shared_ptr<GridComputeFunctionLibrary>
    getLibrary(const std::string & name) = 0;

    virtual MemoryRegionHandle
    getSliceImpl(const MemoryRegionHandle & handle, const std::string & regionName,
                 size_t startOffsetInBytes, size_t lengthInBytes,
                 size_t align, const std::type_info & type, bool isConst) override;

protected:
    // Turn a template into a concrete specialization for this context
    virtual std::shared_ptr<GridComputeKernelSpecialization>
    specializeKernel(const GridComputeKernelTemplate & tmplate) = 0;
};

// GridComputeKernel helper types

enum class GridBindFieldActionType {
    SET_FIELD_FROM_PARAM,
    SET_FIELD_FROM_KNOWN
};

DECLARE_ENUM_DESCRIPTION(GridBindFieldActionType);

struct GridBindFieldAction {
    GridBindFieldActionType action;
    int fieldNumber = -1;
    int argNum = -1;
    std::shared_ptr<CommandExpression> expr;  // for setting from known

    void apply(void * object,
               const ValueDescription & desc,
               GridComputeQueue & queue,
               const std::vector<ComputeKernelArgument> & args,
               ComputeKernelConstraintSolution & knowns) const;
};

DECLARE_STRUCTURE_DESCRIPTION(GridBindFieldAction);

enum class GridBindActionType {
    SET_FROM_ARG,
    SET_FROM_STRUCT,
    SET_FROM_KNOWN,
    SET_THREAD_GROUP
};

DECLARE_ENUM_DESCRIPTION(GridBindActionType);

struct GridBindAction {
    GridBindActionType action;
    ComputeKernelType type;
    std::any arg;
    int computeFunctionArgIndex = -1;
    int argNum = -1;
    std::string argName;
    std::shared_ptr<CommandExpression> expr;  // for setting from known or size of thread group
    std::vector<GridBindFieldAction> fields; // for struct

    void apply(GridComputeQueue & queue,
               const std::vector<ComputeKernelArgument> & args,
               ComputeKernelConstraintSolution & knowns,
               bool setKnowns,
               GridBindContext & bindContext) const;

private:
    void applyArg(GridComputeQueue & queue,
                  const std::vector<ComputeKernelArgument> & args,
                  ComputeKernelConstraintSolution & knowns,
                  bool setKnowns,
                  GridBindContext & bindContext) const;
    void applyStruct(GridComputeQueue & queue,
                    const std::vector<ComputeKernelArgument> & args,
                    ComputeKernelConstraintSolution & knowns,
                    bool setKnowns,
                    GridBindContext & bindContext) const;
    void applyThreadGroup(GridComputeQueue & queue,
                          const std::vector<ComputeKernelArgument> & args,
                          ComputeKernelConstraintSolution & knowns,
                          bool setKnowns,
                          GridBindContext & bindContext) const;
};

DECLARE_STRUCTURE_DESCRIPTION(GridBindAction);


// GridComputeKernel

struct GridComputeKernelTemplate;


struct GridComputeKernel: public ComputeKernel {

    /// Block dimensions for launching the kernel
    //std::vector<size_t> block;

    /// Do we allow the grid to be padded out?
    bool allowGridPaddingFlag = false;

    /// Do we allow the grid to be expanded?
    bool allowGridExpansionFlag = false;

    // Expressions for the grid dimensions, if we override them
    std::shared_ptr<CommandExpression> gridExpression;
    std::shared_ptr<CommandExpression> blockExpression;

    GridComputeContext * gridContext = nullptr;

    std::vector<GridBindAction> bindActions;

    // For each Grid argument, which is the corresponding argument number in arguments passed in?
    std::vector<int> correspondingArgumentNumbers;

    // Add a tuneable parameter to the invocation
    void addTuneable(const std::string & variableName, int64_t defaultValue);

    // Get the expression for the grid.  This will run before modifyGrid.
    void setGridExpression(const std::string & gridExpr, const std::string & blockExpr);

    // This is called as internal documentation.  It says that we allow the runtime to pad
    // out the grid size so that it's a multiple of the block size.  This means however that
    // the kernel will be called with out of range values, and the kernel grid size may be
    // higher than the actual range, which will need to be passed in as an argument.  In
    // pratice, the kernel needs to check if its IDs are out of range and exit if so.
    void allowGridPadding();

    // This is also internal documention; it allows the grid we're called with to be expanded
    // to an extra dimension to accomodate the kernel which needs an extra dimension of
    // parallelism.
    void allowGridExpansion();

protected:
    // Implemented by subclass to create the GridBindInfo.  The generic version creates a
    // GridBindInfo directly, which works for most subclasses.
    virtual std::shared_ptr<GridBindInfo> getGridBindInfo() const;

    // Used only to construct a template
    GridComputeKernel(GridComputeContext * owner)
        : gridContext(owner)
    {
        this->context = owner;
    }
};


// GridComputeKernelTemplate

// This is used to register a GridComputeKernel that will later be specialized for a given
// runtime and device.  It can't be bound (getGridBindInfo will throw) but can be passed
// to the base constructor of a concrete implementation.

struct GridComputeKernelTemplate: public GridComputeKernel {
    // Used only to construct a template
    GridComputeKernelTemplate(GridComputeContext * owner)
        : GridComputeKernel(owner)
    {
    }

    std::string libraryName;
    std::string kernelName;

    void setComputeFunction(std::string libraryName, std::string kernelName);
    
protected:
    // Thest two both throw; they are not relevant for a template
    virtual std::shared_ptr<GridBindInfo> getGridBindInfo() const;
    virtual BoundComputeKernel bindImpl(ComputeQueue & queue,
                                        std::vector<ComputeKernelArgument> arguments,
                                        ComputeKernelConstraintSolution knowns) const override;
};


// GridComputeKernelSpecialization

// This is used to register a GridComputeKernel that will later be specialized for a given
// runtime and device.  It can't be bound (getGridBindInfo will throw) but can be passed
// to the base constructor of a concrete implementation.

struct GridComputeKernelSpecialization: public GridComputeKernel {
    // Used only to construct a template
    GridComputeKernelSpecialization(GridComputeContext * owner)
        : GridComputeKernel(owner)
    {
    }

    // Serializer for tracing this particular kernel
    std::shared_ptr<StructuredSerializer> traceSerializer;

    // Serializer for tracing the runs of this kernel
    std::shared_ptr<StructuredSerializer> runsSerializer;

    std::shared_ptr<GridComputeFunctionLibrary> gridLibrary;
    std::shared_ptr<GridComputeFunction> gridFunction;

    mutable std::atomic<int> numCalls = 0;

    // Perform the abstract bind() operation, returning a BoundComputeKernel
    virtual BoundComputeKernel bindImpl(ComputeQueue & queue,
                                        std::vector<ComputeKernelArgument> arguments,
                                        ComputeKernelConstraintSolution knowns) const override;

protected:
    // Used by constructors of subclasses to initialize from a template kernel
    GridComputeKernelSpecialization(GridComputeContext * owner, const GridComputeKernelTemplate & tmplate);
};


void registerGridComputeKernel(const std::string & kernelName,
                               std::function<std::shared_ptr<GridComputeKernelTemplate>(GridComputeContext &)> generator);

}  // namespace MLDB