/** compute_kernel_multi.h                                                -*- C++ -*-
    Jeremy Barnes, 27 March 2016
    This file is part of MLDB. Copyright 2016 mldb.ai inc. All rights reserved.

    Compute kernel runtime for CPU devices.
*/

#pragma once

#include "compute_kernel.h"

namespace MLDB {

enum class ComputeMultiMode : uint8_t {
    COMPARE  ///< Run kernel on multiple devices, comparing output
};

DECLARE_ENUM_DESCRIPTION(ComputeMultiMode);

struct MultiComputeContext;

// MultiComputeKernel

struct MultiComputeKernel: public ComputeKernel {
    MultiComputeKernel(MultiComputeContext * context, std::vector<std::shared_ptr<ComputeKernel>> kernelsIn);

    // Perform the abstract bind() operation, returning a BoundComputeKernel
    virtual BoundComputeKernel bindImpl(std::vector<ComputeKernelArgument> arguments,
                                        ComputeKernelConstraintSolution knowns) const override;

    MultiComputeContext * multiContext = nullptr;
    std::vector<std::shared_ptr<ComputeKernel>> kernels;

    void compareParameters(bool pre, const BoundComputeKernel & boundKernel, ComputeContext & context) const;
};

// MultiComputeEvent

struct MultiComputeEvent: public ComputeEvent {
    MultiComputeEvent(std::vector<std::shared_ptr<ComputeEvent>> events);

    virtual ~MultiComputeEvent() = default;

    std::vector<std::shared_ptr<ComputeEvent> > events;

    virtual std::shared_ptr<ComputeProfilingInfo> getProfilingInfo() const override;

    virtual void await() const override;

    virtual std::shared_ptr<ComputeEvent> thenImpl(std::function<void ()> fn, const std::string & label) override;
};

// MultiComputeQueue

struct MultiComputeQueue: public ComputeQueue {
    MultiComputeQueue(MultiComputeContext * owner,
                      MultiComputeQueue * parent,
                      std::vector<std::shared_ptr<ComputeQueue>> queues);

    virtual ~MultiComputeQueue() = default;

    MultiComputeContext * multiOwner;
    std::vector<std::shared_ptr<ComputeQueue>> queues;

    virtual std::shared_ptr<ComputeQueue> parallel(const std::string & opName) override;
    virtual std::shared_ptr<ComputeQueue> serial(const std::string & opName) override;

    virtual void
    enqueue(const std::string & opName,
            const BoundComputeKernel & kernel,
            const std::vector<uint32_t> & grid) override;

    virtual ComputePromiseT<MemoryRegionHandle>
    enqueueFillArrayImpl(const std::string & opName,
                         MemoryRegionHandle region, MemoryRegionInitialization init,
                         size_t startOffsetInBytes, ssize_t lengthInBytes,
                         const std::any & arg,
                         std::vector<std::shared_ptr<ComputeEvent>> prereqs) override;

    virtual ComputePromiseT<MemoryRegionHandle>
    enqueueCopyFromHostImpl(const std::string & opName,
                            MemoryRegionHandle toRegion,
                            FrozenMemoryRegion fromRegion,
                            size_t deviceStartOffsetInBytes,
                            std::vector<std::shared_ptr<ComputeEvent>> prereqs = {}) override;

    virtual ComputePromiseT<FrozenMemoryRegion>
    enqueueTransferToHostImpl(const std::string & opName,
                              MemoryRegionHandle handle) override;

    virtual FrozenMemoryRegion
    transferToHostSyncImpl(const std::string & opName,
                           MemoryRegionHandle handle) override;

    virtual std::shared_ptr<ComputeEvent>
    makeAlreadyResolvedEvent(const std::string & label) const override;

    virtual std::shared_ptr<ComputeEvent>  flush() override;

    virtual void finish() override;
};

// MultiComputeContext

struct MultiComputeContext: public ComputeContext {

    MultiComputeContext();

    virtual ~MultiComputeContext() = default;

    virtual ComputeDevice getDevice() const override;

    std::vector<std::shared_ptr<ComputeContext> > contexts;

    virtual ComputePromiseT<MemoryRegionHandle>
    allocateImpl(const std::string & regionName,
                 size_t length, size_t align,
                 const std::type_info & type, bool isConst,
                 MemoryRegionInitialization initialization,
                 std::any initWith) override;

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


    virtual MemoryRegionHandle
    getSliceImpl(const MemoryRegionHandle & handle, const std::string & regionName,
                 size_t startOffsetInBytes, size_t lengthInBytes,
                 size_t align, const std::type_info & type, bool isConst) override;

    virtual std::shared_ptr<ComputeQueue>
    getQueue(const std::string & queueName) override;
};

}  // namespace MLDB