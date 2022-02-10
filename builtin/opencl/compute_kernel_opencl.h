/** compute_kernel_opencl.h                                                -*- C++ -*-
    Jeremy Barnes, 27 March 2016
    This file is part of MLDB. Copyright 2016 mldb.ai inc. All rights reserved.

    Compute kernel runtime for CPU devices.
*/

#pragma once

#include "mldb/block/compute_kernel.h"
#include "mldb/block/compute_kernel_grid.h"
#include "opencl_types.h"

namespace MLDB {

template<typename... Args>
void traceOpenCLOperation(const std::string & opName, Args&&... args)
{
    traceOperation(OperationScope::EVENT, OperationType::OPENCL_COMPUTE, opName, std::forward<Args>(args)...);
}

struct OpenCLComputeContext;

struct OpenCLComputeProfilingInfo: public GridComputeProfilingInfo {
    OpenCLComputeProfilingInfo(OpenCLProfilingInfo info);
    virtual ~OpenCLComputeProfilingInfo() = default;

    OpenCLProfilingInfo clInfo;
};

struct OpenCLComputeEvent: public GridComputeEvent {
    OpenCLComputeEvent(const std::string & label, bool resolved,
                       const GridComputeQueue * owner, OpenCLEvent ev);

    virtual ~OpenCLComputeEvent() = default;

    virtual std::shared_ptr<ComputeProfilingInfo> getProfilingInfo() const override;

    virtual void await() const override;

    OpenCLEvent ev;
};

// OpenCLComputeQueue

struct OpenCLComputeQueue: public GridComputeQueue {
    OpenCLComputeQueue(OpenCLComputeContext * owner, OpenCLComputeQueue * parent,
                       const std::string & label,
                       OpenCLCommandQueue queue, GridDispatchType dispatchType);
    virtual ~OpenCLComputeQueue() = default;

    OpenCLComputeContext * clOwner = nullptr;
    OpenCLCommandQueue clQueue;

    virtual std::shared_ptr<ComputeQueue> parallel(const std::string & opName) override;
    virtual std::shared_ptr<ComputeQueue> serial(const std::string & opName) override;

#if 0
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
#endif

    virtual FrozenMemoryRegion
    enqueueTransferToHostImpl(const std::string & opName,
                              MemoryRegionHandle handle) override;

    virtual FrozenMemoryRegion
    transferToHostSyncImpl(const std::string & opName,
                           MemoryRegionHandle handle) override;

    virtual MutableMemoryRegion
    enqueueTransferToHostMutableImpl(const std::string & opName,
                                     MemoryRegionHandle handle) override;

    virtual MutableMemoryRegion
    transferToHostMutableSyncImpl(const std::string & opName,
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

    virtual std::shared_ptr<ComputeEvent> makeAlreadyResolvedEvent(const std::string & label) const override;

    virtual void enqueueBarrier(const std::string & label) override;
    virtual std::shared_ptr<ComputeEvent> flush(const std::string & opName) override;
    virtual void finish(const std::string & opName) override;

protected:
    virtual void
    enqueueZeroFillArrayConcrete(const std::string & opName,
                                 MemoryRegionHandle region,
                                 size_t startOffsetInBytes, ssize_t lengthInBytes) override;
    virtual void
    enqueueBlockFillArrayConcrete(const std::string & opName,
                                  MemoryRegionHandle region,
                                  size_t startOffsetInBytes, ssize_t lengthInBytes,
                                  std::span<const std::byte> block) override;
    virtual void
    enqueueCopyFromHostConcrete(const std::string & opName,
                                MemoryRegionHandle toRegion,
                                FrozenMemoryRegion fromRegion,
                                size_t deviceStartOffsetInBytes) override;

    virtual FrozenMemoryRegion
    enqueueTransferToHostConcrete(const std::string & opName, MemoryRegionHandle handle) override;

    virtual FrozenMemoryRegion
    transferToHostSyncConcrete(const std::string & opName, MemoryRegionHandle handle) override;

    // Subclasses override to create a new bind context
    virtual std::shared_ptr<GridBindContext>
    newBindContext(const std::string & opName,
                   const GridComputeKernel * kernel, const GridBindInfo * bindInfo) override;

private:
    std::shared_ptr<ComputeEvent>
    doEnqueueCopyBetweenDeviceRegionsImpl(const std::string & opName,
                                      MemoryRegionHandle from, MemoryRegionHandle to,
                                      size_t fromOffset, size_t toOffset,
                                      size_t length);
};

// OpenCLComputeContext

struct OpenCLComputeContext: public GridComputeContext {

    OpenCLComputeContext(OpenCLDevice clDevice, ComputeDevice device);

    virtual ~OpenCLComputeContext() = default;

    OpenCLContext clContext;
    OpenCLDevice clDevice;
    ComputeDevice device;

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

    virtual MemoryRegionHandle
    allocateSyncImpl(const std::string & regionName,
                     size_t length, size_t align,
                     const std::type_info & type, bool isConst) override;

    virtual std::shared_ptr<GridComputeFunctionLibrary>
    getLibrary(const std::string & name) override;

    virtual std::shared_ptr<ComputeQueue>
    getQueue(const std::string & queueName) override;

    // OpenCL has a special call to get a slice rather than just add an offset
    virtual MemoryRegionHandle
    getSliceImpl(const MemoryRegionHandle & handle, const std::string & regionName,
                 size_t startOffsetInBytes, size_t lengthInBytes,
                 size_t align, const std::type_info & type, bool isConst) override;

protected:
    virtual std::shared_ptr<GridComputeKernelSpecialization>
    specializeKernel(const GridComputeKernelTemplate & tmplate) override;
};


// OpenCLComputeFunction

struct OpenCLComputeFunction: public GridComputeFunction {
    OpenCLComputeFunction(OpenCLComputeContext & context, OpenCLProgram clProgram,
                          const std::string & kernelName);

    virtual ~OpenCLComputeFunction() = default;

    OpenCLProgram clProgram;
    std::string kernelName;

    OpenCLKernel generateKernel() const;

    virtual std::vector<GridComputeFunctionArgument> getArgumentInfo() const override;
};


// OpenCLComputeFunctionLibrary

struct OpenCLComputeFunctionLibrary: public GridComputeFunctionLibrary {
    OpenCLComputeFunctionLibrary(OpenCLComputeContext & context, OpenCLProgram clProgram);

    virtual ~OpenCLComputeFunctionLibrary() = default;

    OpenCLComputeContext & context;
    OpenCLProgram clProgram;

    virtual std::shared_ptr<GridComputeFunction>
    getFunction(const std::string & functionName) override;

    virtual std::string getId() const override;

    virtual Json::Value getMetadata() const override;

    // Return a version compiled from source read from the given filename
    static std::shared_ptr<OpenCLComputeFunctionLibrary>
    compileFromSourceFile(OpenCLComputeContext & context, const std::string & fileName);

    // Return a version compiled from source given in the sourceCode string
    static std::shared_ptr<OpenCLComputeFunctionLibrary>
    compileFromSource(OpenCLComputeContext & context, const Utf8String & sourceCode, const std::string & fileNameToAppearInErrorMessages);
};

// OpenCLComputeKernel

struct OpenCLComputeKernel: public GridComputeKernelSpecialization {

    OpenCLComputeKernel(OpenCLComputeContext * owner, const GridComputeKernelTemplate & tmplate);

    OpenCLComputeContext * clContext = nullptr;
    const OpenCLComputeFunction * clFunction = nullptr;

    // Parses an OpenCL kernel argument info structure, and turns it into a ComputeKernel type
    static ComputeKernelType getKernelType(const OpenCLKernelArgInfo & info);
};

void registerOpenCLLibrary(const std::string & libraryName,
                           std::function<std::shared_ptr<OpenCLComputeFunctionLibrary>(OpenCLComputeContext &)> generator);

}  // namespace MLDB