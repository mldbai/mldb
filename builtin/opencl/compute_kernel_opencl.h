/** compute_kernel_opencl.h                                                -*- C++ -*-
    Jeremy Barnes, 27 March 2016
    This file is part of MLDB. Copyright 2016 mldb.ai inc. All rights reserved.

    Compute kernel runtime for CPU devices.
*/

#pragma once

#include "mldb/block/compute_kernel.h"
#include "opencl_types.h"

namespace MLDB {

struct OpenCLComputeContext;

struct OpenCLComputeProfilingInfo: public ComputeProfilingInfo {
    OpenCLComputeProfilingInfo(OpenCLProfilingInfo info);
    virtual ~OpenCLComputeProfilingInfo() = default;

    OpenCLProfilingInfo clInfo;
};

struct OpenCLComputeEvent: public ComputeEvent {
    OpenCLComputeEvent() = default;  // already resolved
    OpenCLComputeEvent(OpenCLEvent ev);

    virtual ~OpenCLComputeEvent() = default;

    virtual std::shared_ptr<ComputeProfilingInfo> getProfilingInfo() const override;

    virtual void await() const override;

    virtual std::shared_ptr<ComputeEvent> thenImpl(std::function<void ()> fn, const std::string & label);

    OpenCLEvent ev;
};

// OpenCLComputeQueue

struct OpenCLComputeQueue: public ComputeQueue {
    OpenCLComputeQueue(OpenCLComputeContext * owner);
    OpenCLComputeQueue(OpenCLComputeContext * owner, OpenCLCommandQueue queue);
    virtual ~OpenCLComputeQueue() = default;

    OpenCLComputeContext * clOwner = nullptr;
    OpenCLCommandQueue clQueue;

    virtual std::shared_ptr<ComputeEvent>
    launch(const std::string & opName,
           const BoundComputeKernel & kernel,
           const std::vector<uint32_t> & grid,
           const std::vector<std::shared_ptr<ComputeEvent>> & prereqs = {}) override;

    virtual ComputePromiseT<MemoryRegionHandle>
    enqueueFillArrayImpl(const std::string & opName,
                         MemoryRegionHandle region, MemoryRegionInitialization init,
                         size_t startOffsetInBytes, ssize_t lengthInBytes,
                         const std::any & arg,
                         std::vector<std::shared_ptr<ComputeEvent>> prereqs = {}) override;

    virtual ComputePromiseT<MemoryRegionHandle>
    enqueueCopyFromHostImpl(const std::string & opName,
                            MemoryRegionHandle toRegion,
                            FrozenMemoryRegion fromRegion,
                            size_t deviceStartOffsetInBytes,
                            std::vector<std::shared_ptr<ComputeEvent>> prereqs = {}) override;

    virtual std::shared_ptr<ComputeEvent> makeAlreadyResolvedEvent(const std::string & label) const override;

    virtual void flush() override;
    virtual void finish() override;
};

// OpenCLComputeContext

struct OpenCLComputeContext: public ComputeContext {

    OpenCLComputeContext(std::vector<OpenCLDevice> clDevices, std::vector<ComputeDevice> devices);

    virtual ~OpenCLComputeContext() = default;

    OpenCLContext clContext;
    std::shared_ptr<OpenCLComputeQueue> clQueue;  // for internal operations
    std::vector<OpenCLDevice> clDevices;
    std::vector<ComputeDevice> devices;

    virtual ComputeDevice getDevice() const override;

    // pin, region, length in bytes
    static std::tuple<std::shared_ptr<const void>, cl_mem, size_t>
    getMemoryRegion(const std::string & opName, MemoryRegionHandleInfo & handle,
                    MemoryRegionAccess access);

    std::tuple<FrozenMemoryRegion, int /* version */>
    getFrozenHostMemoryRegion(const std::string & opName,
                              MemoryRegionHandleInfo & handle,
                              size_t offset, ssize_t length,
                              bool ignoreHazards) const;

    virtual ComputePromiseT<MemoryRegionHandle>
    allocateImpl(const std::string & opName,
                 size_t length, size_t align,
                 const std::type_info & type,
                 bool isConst,
                 MemoryRegionInitialization initialization,
                 std::any initWith = std::any()) override;

    virtual MemoryRegionHandle
    allocateSyncImpl(const std::string & regionName,
                     size_t length, size_t align,
                     const std::type_info & type, bool isConst,
                     MemoryRegionInitialization initialization,
                     std::any initWith = std::any()) override;

    virtual ComputePromiseT<MemoryRegionHandle>
    transferToDeviceImpl(const std::string & opName,
                         FrozenMemoryRegion region,
                         const std::type_info & type, bool isConst) override;

    virtual MemoryRegionHandle
    transferToDeviceSyncImpl(const std::string & opName,
                             FrozenMemoryRegion region,
                             const std::type_info & type, bool isConst) override;

    virtual ComputePromiseT<FrozenMemoryRegion>
    transferToHostImpl(const std::string & opName, MemoryRegionHandle handle) override;

    virtual FrozenMemoryRegion
    transferToHostSyncImpl(const std::string & opName,
                           MemoryRegionHandle handle) override;

    virtual ComputePromiseT<MutableMemoryRegion>
    transferToHostMutableImpl(const std::string & opName, MemoryRegionHandle handle) override;

    virtual MutableMemoryRegion
    transferToHostMutableSyncImpl(const std::string & opName,
                                  MemoryRegionHandle handle) override;

    virtual std::shared_ptr<ComputeEvent>
    fillDeviceRegionFromHostImpl(const std::string & opName,
                                 MemoryRegionHandle deviceHandle,
                                 std::shared_ptr<std::span<const std::byte>> pinnedHostRegion,
                                 size_t deviceOffset = 0) override;

    virtual void
    fillDeviceRegionFromHostSyncImpl(const std::string & opName,
                                     MemoryRegionHandle deviceHandle,
                                     std::span<const std::byte> hostRegion,
                                     size_t deviceOffset = 0) override;

    virtual std::shared_ptr<ComputeEvent>
    copyBetweenDeviceRegionsImpl(const std::string & opName,
                                 MemoryRegionHandle from, MemoryRegionHandle to,
                                 size_t fromOffset, size_t toOffset,
                                 size_t length) override;

    virtual void
    copyBetweenDeviceRegionsSyncImpl(const std::string & opName,
                                     MemoryRegionHandle from, MemoryRegionHandle to,
                                     size_t fromOffset, size_t toOffset,
                                     size_t length) override;

    virtual std::shared_ptr<ComputeKernel>
    getKernel(const std::string & kernelName) override;

    virtual ComputePromiseT<MemoryRegionHandle>
    managePinnedHostRegionImpl(const std::string & opName,
                               std::span<const std::byte> region, size_t align,
                               const std::type_info & type, bool isConst) override;

    virtual MemoryRegionHandle
    managePinnedHostRegionSyncImpl(const std::string & opName,
                                   std::span<const std::byte> region, size_t align,
                                   const std::type_info & type, bool isConst) override;

    virtual std::shared_ptr<ComputeQueue>
    getQueue() override;

    virtual MemoryRegionHandle
    getSliceImpl(const MemoryRegionHandle & handle, const std::string & regionName,
                 size_t startOffsetInBytes, size_t lengthInBytes,
                 size_t align, const std::type_info & type, bool isConst) override;
};

// OpenCLComputeKernel

struct OpenCLComputeKernel: public ComputeKernel {

    /// Block dimensions for launching the kernel
    std::vector<size_t> block;

    /// Do we allow the grid to be padded out?
    bool allowGridPaddingFlag = false;

    /// Do we allow the grid to be expanded?
    bool allowGridExpansionFlag = false;

    using SetParameters = std::function<void (OpenCLKernel & kernel, OpenCLComputeContext & context)>;

    /// List of functions used to set arbitrary values on the kernel (especially for calculating
    /// sizes of local arrays or other bounds)
    std::vector<SetParameters> setters;

    // List of tuneable parameters
    std::vector<ComputeTuneable> tuneables;

    // Expressions for the grid dimensions, if we override them
    std::shared_ptr<CommandExpression> gridExpression;
    std::shared_ptr<CommandExpression> blockExpression;

    // Function to modify the grid dimensions
    std::function<void (std::vector<size_t> & grid, std::vector<size_t> & block)> modifyGrid;

    mutable OpenCLProgram clProgram;  // Mutable as createKernel is non-const
    OpenCLKernel clKernel;
    OpenCLKernelInfo clKernelInfo;

    // Serializer for tracing this particular kernel
    std::shared_ptr<StructuredSerializer> traceSerializer;

    // Serializer for tracing the runs of this kernel
    std::shared_ptr<StructuredSerializer> runsSerializer;

    mutable std::atomic<int> numCalls;

    // For each OpenCL argument, which is the corresponding argument number in arguments passed in?
    std::vector<int> correspondingArgumentNumbers;

    // Parses an OpenCL kernel argument info structure, and turns it into a ComputeKernel type
    static ComputeKernelType getKernelType(const OpenCLKernelArgInfo & info);

    void setParameters(SetParameters setter);

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

    void setComputeFunction(OpenCLProgram program,
                            std::string kernelName,
                            std::vector<size_t> block);

    // Perform the abstract bind() operation, returning a BoundComputeKernel
    virtual BoundComputeKernel bindImpl(std::vector<ComputeKernelArgument> arguments) const override;
};

void registerOpenCLComputeKernel(const std::string & kernelName,
                           std::function<std::shared_ptr<OpenCLComputeKernel>(OpenCLComputeContext &)> generator);

}  // namespace MLDB