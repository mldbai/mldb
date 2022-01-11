/** compute_kernel_cpu.h                                                -*- C++ -*-
    Jeremy Barnes, 27 March 2016
    This file is part of MLDB. Copyright 2016 mldb.ai inc. All rights reserved.

    Compute kernel runtime for CPU devices.
*/

#pragma once

#include "mldb/block/compute_kernel.h"
#include "mldb/block/compute_kernel_grid.h"

namespace MLDB {

template<typename... Args>
void traceCPUOperation(const std::string & opName, Args&&... args)
{
    traceOperation(OperationScope::EVENT, OperationType::CPU_COMPUTE, opName, std::forward<Args>(args)...);
}

struct CPUComputeContext;
struct CPUComputeQueue;

using CPUComputeProfilingInfo = GridComputeProfilingInfo;

// enable_shared_from_this is to ensure that we can pin lifetimes of events until the
// completion handlers have finished.
struct CPUComputeEvent: public GridComputeEvent {
    CPUComputeEvent(const std::string & label, bool resolved, const CPUComputeQueue * owner);  // may or may not be already resolved

    virtual ~CPUComputeEvent() = default;

    virtual void await() const override;

    static std::shared_ptr<CPUComputeEvent>
    makeAlreadyResolvedEvent(const std::string & label, const CPUComputeQueue * owner);
    static std::shared_ptr<CPUComputeEvent>
    makeUnresolvedEvent(const std::string & label, const CPUComputeQueue * owner);
};


// CPUComputeQueue

struct CPUComputeQueue: public GridComputeQueue, std::enable_shared_from_this<CPUComputeQueue> {
    CPUComputeQueue(CPUComputeContext * owner, CPUComputeQueue * parent,
                    const std::string & label,
                    GridDispatchType dispatchType);
    virtual ~CPUComputeQueue();

    CPUComputeContext * cpuOwner = nullptr;

    virtual std::shared_ptr<ComputeQueue> parallel(const std::string & opName) override;
    virtual std::shared_ptr<ComputeQueue> serial(const std::string & opName) override;

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
    virtual std::shared_ptr<ComputeEvent> flush() override;
    virtual void finish() override;

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
};


// CPUComputeContext

struct CPUComputeContext: public GridComputeContext {

    CPUComputeContext();

    virtual ~CPUComputeContext() = default;

    virtual MemoryRegionHandle
    allocateSyncImpl(const std::string & regionName,
                     size_t length, size_t align,
                     const std::type_info & type, bool isConst) override;

    // pin, region, length in bytes
    static std::tuple<std::shared_ptr<const void>, const std::byte *, size_t>
    getMemoryRegion(const std::string & opName, MemoryRegionHandleInfo & handle,
                    MemoryRegionAccess access);

    std::tuple<FrozenMemoryRegion, int /* version */>
    getFrozenHostMemoryRegion(const std::string & opName,
                              MemoryRegionHandleInfo & handle,
                              size_t offset, ssize_t length,
                              bool ignoreHazards) const;

    virtual std::shared_ptr<GridComputeFunctionLibrary>
    getLibrary(const std::string & name) override;

    virtual std::shared_ptr<ComputeQueue>
    getQueue(const std::string & queueName) override;

protected:
    virtual std::shared_ptr<GridComputeKernelSpecialization>
    specializeKernel(const GridComputeKernelTemplate & tmplate) override;
};


// CPUComputeFunction

struct CPUComputeFunction: public GridComputeFunction {
    CPUComputeFunction(CPUComputeContext & context);

    virtual ~CPUComputeFunction() = default;

    virtual std::vector<GridComputeFunctionArgument> getArgumentInfo() const override;
};


// CPUComputeFunctionLibrary

struct CPUComputeFunctionLibrary: public GridComputeFunctionLibrary {
    CPUComputeFunctionLibrary(CPUComputeContext & context);

    virtual ~CPUComputeFunctionLibrary() = default;

    CPUComputeContext & context;

    virtual std::shared_ptr<GridComputeFunction>
    getFunction(const std::string & functionName) override;

    virtual std::string getId() const override;

    virtual Json::Value getMetadata() const override;

    // Return a version compiled from source read from the given filename
    static std::shared_ptr<CPUComputeFunctionLibrary>
    compileFromSourceFile(CPUComputeContext & context, const std::string & fileName);

    // Return a version compiled from source given in the sourceCode string
    static std::shared_ptr<CPUComputeFunctionLibrary>
    compileFromSource(CPUComputeContext & context, const Utf8String & sourceCode, const std::string & fileNameToAppearInErrorMessages);
};


// CPUComputeKernel

struct CPUComputeKernel: public GridComputeKernelSpecialization {

    CPUComputeKernel(CPUComputeContext * owner, const GridComputeKernelTemplate & tmplate);

    CPUComputeContext * cpuContext = nullptr;
    const CPUComputeFunction * cpuFunction = nullptr;
};


void registerCPULibrary(const std::string & libraryName,
                        std::function<std::shared_ptr<CPUComputeFunctionLibrary>(CPUComputeContext &)> generator);

}  // namespace MLDB