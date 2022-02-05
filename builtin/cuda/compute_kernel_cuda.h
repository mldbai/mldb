/** compute_kernel_cuda.h                                                -*- C++ -*-
    Jeremy Barnes, 27 March 2016
    This file is part of MLDB. Copyright 2016 mldb.ai inc. All rights reserved.

    Compute kernel runtime for CPU devices.
*/

#pragma once

#include "mldb/block/compute_kernel.h"
#include "mldb/block/compute_kernel_grid.h"
#include "cuda/runtime_api.hpp"

namespace MLDB {

template<typename... Args>
void traceCudaOperation(const std::string & opName, Args&&... args)
{
    traceOperation(OperationScope::EVENT, OperationType::CUDA_COMPUTE, opName, std::forward<Args>(args)...);
}

struct CudaComputeContext;
struct CudaComputeQueue;

using CudaComputeProfilingInfo = GridComputeProfilingInfo;

// enable_shared_from_this is to ensure that we can pin lifetimes of events until the
// completion handlers have finished.
struct CudaComputeEvent: public GridComputeEvent {
    CudaComputeEvent(const std::string & label, bool resolved, const CudaComputeQueue * owner);  // may or may not be already resolved

    virtual ~CudaComputeEvent() = default;

    virtual void await() const override;

    static std::shared_ptr<CudaComputeEvent>
    makeAlreadyResolvedEvent(const std::string & label, const CudaComputeQueue * owner);
    static std::shared_ptr<CudaComputeEvent>
    makeUnresolvedEvent(const std::string & label, const CudaComputeQueue * owner);
};


// CudaComputeQueue

struct CudaComputeQueue: public GridComputeQueue, std::enable_shared_from_this<CudaComputeQueue> {
    CudaComputeQueue(CudaComputeContext * owner, CudaComputeQueue * parent,
                      const std::string & label);
    virtual ~CudaComputeQueue();

    CudaComputeContext * mtlOwner = nullptr;

    // What kind of dispatch (serial or parallel) do we do?

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
#if 0
    template<typename CommandEncoder>
    void beginEncodingImpl(const std::string & opName, CommandEncoder & encoder, bool force);
    template<typename CommandEncoder>
    void endEncodingImpl(const std::string & opName, CommandEncoder & encoder, bool force);

    // Commands that are active and we haven't yet waited on (used to ensure serial execution)
    std::vector<mtlpp::Fence> activeCommands;

    // Performs the necessary fences, etc to implement the scheduling type on the queue
    void beginEncoding(const std::string & opName, mtlpp::ComputeCommandEncoder & encoder, bool force = false);
    void beginEncoding(const std::string & opName, mtlpp::BlitCommandEncoder & encoder, bool force = false);
    void endEncoding(const std::string & opName, mtlpp::ComputeCommandEncoder & encoder, bool force = false);
    void endEncoding(const std::string & opName, mtlpp::BlitCommandEncoder & encoder, bool force = false);
#endif

    friend struct CudaBindContext;
};


// CudaComputeContext

struct CudaComputeContext: public GridComputeContext {

#if 0
    CudaComputeContext(mtlpp::Device mtlDevice, ComputeDevice device);
#endif

    virtual ~CudaComputeContext() = default;

#if 0
    mtlpp::Device mtlDevice;
    mtlpp::Heap heap;
#endif

    virtual MemoryRegionHandle
    allocateSyncImpl(const std::string & regionName,
                     size_t length, size_t align,
                     const std::type_info & type, bool isConst) override;

#if 0
    // pin, region, length in bytes
    static std::tuple<std::shared_ptr<const void>, mtlpp::Buffer, size_t>
    getMemoryRegion(const std::string & opName, MemoryRegionHandleInfo & handle,
                    MemoryRegionAccess access);
#endif

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


// CudaComputeFunction

struct CudaComputeFunction: public GridComputeFunction {
    CudaComputeFunction(CudaComputeContext & context, mtlpp::Function mtlFunction);

    virtual ~CudaComputeFunction() = default;

#if 0
    mtlpp::Function mtlFunction;
    mtlpp::ComputePipelineState computePipelineState;
    mtlpp::ComputePipelineReflection reflection;
#endif

    virtual std::vector<GridComputeFunctionArgument> getArgumentInfo() const override;
};


// CudaComputeFunctionLibrary

struct CudaComputeFunctionLibrary: public GridComputeFunctionLibrary {
    CudaComputeFunctionLibrary(CudaComputeContext & context, mtlpp::Library mtlLibrary);

    virtual ~CudaComputeFunctionLibrary() = default;

#if 0
    CudaComputeContext & context;
    mtlpp::Library mtlLibrary;
#endif

    virtual std::shared_ptr<GridComputeFunction>
    getFunction(const std::string & functionName) override;

    virtual std::string getId() const override;

    virtual Json::Value getMetadata() const override;

    // Return a version compiled from source read from the given filename
    static std::shared_ptr<CudaComputeFunctionLibrary>
    compileFromSourceFile(CudaComputeContext & context, const std::string & fileName);

    // Return a version compiled from source given in the sourceCode string
    static std::shared_ptr<CudaComputeFunctionLibrary>
    compileFromSource(CudaComputeContext & context, const Utf8String & sourceCode, const std::string & fileNameToAppearInErrorMessages);

    // Load a binary library (.mtllib file)
    static std::shared_ptr<CudaComputeFunctionLibrary>
    loadMtllib(CudaComputeContext & context, const std::string & libraryFilename);
};


// CudaComputeKernel

struct CudaComputeKernel: public GridComputeKernelSpecialization {

    CudaComputeKernel(CudaComputeContext * owner, const GridComputeKernelTemplate & tmplate);

    CudaComputeContext * mtlContext = nullptr;
    const CudaComputeFunction * mtlFunction = nullptr;

    // Parses an Cuda kernel argument info structure, and turns it into a ComputeKernel type
    static ComputeKernelType getKernelType(const mtlpp::Argument & arg);
};


void registerCudaLibrary(const std::string & libraryName,
                          std::function<std::shared_ptr<CudaComputeFunctionLibrary>(CudaComputeContext &)> generator);

}  // namespace MLDB