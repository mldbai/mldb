/** compute_kernel_grid.h                                                -*- C++ -*-
    Jeremy Barnes, 27 March 2016
    This file is part of MLDB. Copyright 2016 mldb.ai inc. All rights reserved.

    Compute kernel runtime for grid-based devices (normally GPU, but hopefully can be
    emulated on the CPU for correctness).  Grid, OpenCL, CUDA, ...
*/

#pragma once

#include "mldb/block/compute_kernel.h"
#include "mldb/arch/spinlock.h"

namespace MLDB {

struct GridComputeContext;
struct GridComputeKernel;
struct GridComputeQueue;

struct GridMemoryRegionHandleInfo: public MemoryRegionHandleInfo {
    virtual ~GridMemoryRegionHandleInfo() = default;

    size_t offset = 0;
    std::any buffer;  ///< Really the underlying type

    // If we're managing host memory, this is where it is.
    const std::byte * backingHostMem = nullptr;

    Spinlock mutex;
    int numReaders = 0;
    int numWriters = 0;
    std::set<std::string> currentWriters;
    std::set<std::string> currentReaders;

    std::tuple<FrozenMemoryRegion, int /* version */>
    getReadOnlyHostAccessSync(const GridComputeContext & context,
                              const std::string & opName,
                              size_t offset,
                              ssize_t length,
                              bool ignoreHazards);

    // Pin, implementation buffer type
    std::tuple<std::shared_ptr<const void>, std::any>
    getGridAccess(const std::string & opName, MemoryRegionAccess access);
};

struct GridBindInfo: public ComputeKernelBindInfo {
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
                     GridComputeQueue * owner);  // may or may not be already resolved

    virtual ~GridComputeEvent() = default;

    virtual std::shared_ptr<ComputeProfilingInfo> getProfilingInfo() const override;

    virtual std::shared_ptr<ComputeEvent> thenImpl(std::function<void ()> fn, const std::string & label);

    void resolve();

    virtual std::string label() const override { return label_; }
    std::string label_;

    GridComputeQueue * owner = nullptr;

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

struct GridBindContext {
    virtual ~GridBindContext() = default;
};


// GridComputeQueue

struct GridComputeQueue: public ComputeQueue, std::enable_shared_from_this<GridComputeQueue> {
    GridComputeQueue(GridComputeContext * owner, GridComputeQueue * parent,
                      const std::string & label, GridDispatchType dispatchType);
    virtual ~GridComputeQueue();

    GridComputeContext * gridOwner = nullptr;

    // Subclasses override to create a new bind context
    virtual std::shared_ptr<GridBindContext> newBindContext() = 0;

    // Subclasses override to launch the bound kernel
    virtual void launch(GridBindContext & context, std::vector<size_t> grid, std::vector<size_t> block) = 0;

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

    virtual void finish() override;

protected:
    virtual void
    enqueueZeroFillArrayConcrete(const std::string & opName,
                                 MemoryRegionHandle region, MemoryRegionInitialization init,
                                 size_t startOffsetInBytes, ssize_t lengthInBytes) = 0;
    virtual void
    enqueueBlockFillArrayConcrete(const std::string & opName,
                                  MemoryRegionHandle region, MemoryRegionInitialization init,
                                  size_t startOffsetInBytes, ssize_t lengthInBytes,
                                  std::span<const std::byte> block) = 0;
};


// GridComputeMarker

struct GridComputeMarker: public ComputeMarker {
    GridComputeMarker(const std::string & scopeName);
    virtual ~GridComputeMarker();

    virtual std::shared_ptr<ComputeMarker> enterScope(const std::string & scopeName);

    std::shared_ptr<void> scope;
};


// GridComputeContext

struct GridComputeContext: public ComputeContext {

    GridComputeContext(ComputeDevice device);

    virtual ~GridComputeContext() = default;

    ComputeDevice device;
    std::shared_ptr<GridComputeQueue> queue;

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

    virtual MemoryRegionHandle
    allocateSyncImpl(const std::string & regionName,
                     size_t length, size_t align,
                     const std::type_info & type, bool isConst) override;

#if 0
    virtual MemoryRegionHandle
    transferToDeviceImpl(const std::string & opName,
                         FrozenMemoryRegion region,
                         const std::type_info & type, bool isConst) override;

    virtual MemoryRegionHandle
    transferToDeviceSyncImpl(const std::string & opName,
                             FrozenMemoryRegion region,
                             const std::type_info & type, bool isConst) override;

    virtual FrozenMemoryRegion
    transferToHostImpl(const std::string & opName, MemoryRegionHandle handle) override;

    virtual FrozenMemoryRegion
    transferToHostSyncImpl(const std::string & opName,
                           MemoryRegionHandle handle) override;

    virtual MutableMemoryRegion
    transferToHostMutableImpl(const std::string & opName, MemoryRegionHandle handle) override;

    virtual MutableMemoryRegion
    transferToHostMutableSyncImpl(const std::string & opName,
                                  MemoryRegionHandle handle) override;

    virtual void
    fillDeviceRegionFromHostImpl(const std::string & opName,
                                 MemoryRegionHandle deviceHandle,
                                 std::shared_ptr<std::span<const std::byte>> pinnedHostRegion,
                                 size_t deviceOffset = 0) override;

    virtual void
    fillDeviceRegionFromHostSyncImpl(const std::string & opName,
                                     MemoryRegionHandle deviceHandle,
                                     std::span<const std::byte> hostRegion,
                                     size_t deviceOffset = 0) override;
#endif

    virtual std::shared_ptr<ComputeKernel>
    getKernel(const std::string & kernelName) override;

    virtual std::shared_ptr<ComputeQueue>
    getQueue(const std::string & queueName) override;

    virtual MemoryRegionHandle
    getSliceImpl(const MemoryRegionHandle & handle, const std::string & regionName,
                 size_t startOffsetInBytes, size_t lengthInBytes,
                 size_t align, const std::type_info & type, bool isConst) override;
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
               GridComputeContext & context,
               const std::vector<ComputeKernelArgument> & args,
               ComputeKernelConstraintSolution & knowns) const;
};

DECLARE_STRUCTURE_DESCRIPTION(GridBindFieldAction);

enum class GridBindActionType {
    SET_BUFFER_FROM_ARG,
    SET_BUFFER_FROM_STRUCT,
    SET_BUFFER_THREAD_GROUP
};

DECLARE_ENUM_DESCRIPTION(GridBindActionType);

struct GridBindAction {
    GridBindActionType action;
    ComputeKernelType type;
    std::any arg;
    int argNum = -1;
    std::string argName;
    std::shared_ptr<CommandExpression> expr;  // for setting from known or size of thread group
    std::vector<GridBindFieldAction> fields; // for struct

    void apply(GridComputeContext & context,
               const std::vector<ComputeKernelArgument> & args,
               ComputeKernelConstraintSolution & knowns,
               bool setKnowns,
               GridBindContext & bindContext) const;

private:
    void applyArg(GridComputeContext & context,
                  const std::vector<ComputeKernelArgument> & args,
                  ComputeKernelConstraintSolution & knowns,
                  bool setKnowns,
                  GridBindContext & bindContext) const;
    void applyStruct(GridComputeContext & context,
                    const std::vector<ComputeKernelArgument> & args,
                    ComputeKernelConstraintSolution & knowns,
                    bool setKnowns,
                    GridBindContext & bindContext) const;
    void applyThreadGroup(GridComputeContext & context,
                          const std::vector<ComputeKernelArgument> & args,
                          ComputeKernelConstraintSolution & knowns,
                          bool setKnowns,
                          GridBindContext & bindContext) const;
};

DECLARE_STRUCTURE_DESCRIPTION(GridBindAction);


// GridComputeFunction

struct GridComputeFunction {

};


// GridComputeFunctionLibrary

struct GridComputeFunctionLibrary {

};


// GridComputeKernel

struct GridComputeKernel: public ComputeKernel {

    GridComputeKernel(GridComputeContext * owner)
        : gridContext(owner)
    {
    }

    /// Block dimensions for launching the kernel
    std::vector<size_t> block;

    /// Do we allow the grid to be padded out?
    bool allowGridPaddingFlag = false;

    /// Do we allow the grid to be expanded?
    bool allowGridExpansionFlag = false;

    // Expressions for the grid dimensions, if we override them
    std::shared_ptr<CommandExpression> gridExpression;
    std::shared_ptr<CommandExpression> blockExpression;

    GridComputeContext * gridContext = nullptr;
    std::shared_ptr<GridComputeFunctionLibrary> gridLibrary;
    std::shared_ptr<GridComputeFunction> gridFunction;

    std::vector<GridBindAction> bindActions;

    // Serializer for tracing this particular kernel
    std::shared_ptr<StructuredSerializer> traceSerializer;

    // Serializer for tracing the runs of this kernel
    std::shared_ptr<StructuredSerializer> runsSerializer;

    mutable std::atomic<int> numCalls;

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

    void setComputeFunction(std::shared_ptr<GridComputeFunctionLibrary> library,
                            std::string kernelName);

    // Perform the abstract bind() operation, returning a BoundComputeKernel
    virtual BoundComputeKernel bindImpl(std::vector<ComputeKernelArgument> arguments,
                                        ComputeKernelConstraintSolution knowns) const override;

protected:
    // Implemented by subclass to create the GridBindInfo
    virtual std::shared_ptr<GridBindInfo> getGridBindInfo() const = 0;

};

void registerGridComputeKernel(const std::string & kernelName,
                               std::function<std::shared_ptr<GridComputeKernel>(GridComputeContext &)> generator);

}  // namespace MLDB