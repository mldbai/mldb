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

    Callable createCallableImpl(ComputeContext & context, std::vector<ComputeKernelArgument> & params);

    MultiComputeContext * multiContext = nullptr;
    std::vector<std::shared_ptr<ComputeKernel>> kernels;
};

// MultiComputeEvent

struct MultiComputeEvent: public ComputeEvent {
    MultiComputeEvent(std::vector<std::shared_ptr<ComputeEvent>> events);

    virtual ~MultiComputeEvent() = default;

    std::vector<std::shared_ptr<ComputeEvent> > events;

    virtual std::shared_ptr<ComputeProfilingInfo> getProfilingInfo() const override;

    virtual void await() const override;
};

// MultiComputeQueue

struct MultiComputeQueue: public ComputeQueue {
    MultiComputeQueue(MultiComputeContext * owner,
                      std::vector<std::shared_ptr<ComputeQueue>> queues);

    virtual ~MultiComputeQueue() = default;

    MultiComputeContext * multiOwner;
    std::vector<std::shared_ptr<ComputeQueue>> queues;

    virtual std::shared_ptr<ComputeEvent>
    launch(const BoundComputeKernel & kernel,
           const std::vector<uint32_t> & grid,
           const std::vector<std::shared_ptr<ComputeEvent>> & prereqs = {}) override;

    virtual std::shared_ptr<ComputeEvent>
    enqueueFillArrayImpl(MemoryRegionHandle region, MemoryRegionInitialization init,
                         size_t startOffsetInBytes, ssize_t lengthInBytes,
                         const std::any & arg) override;
};

// MultiComputeContext

struct MultiComputeContext: public ComputeContext {

    MultiComputeContext();

    virtual ~MultiComputeContext() = default;

    std::vector<std::shared_ptr<ComputeContext> > contexts;

    virtual MemoryRegionHandle
    allocateImpl(size_t length, size_t align,
                 const std::type_info & type, bool isConst,
                 MemoryRegionInitialization initialization,
                 std::any initWith = std::any()) override;

    virtual std::tuple<MemoryRegionHandle, std::shared_ptr<ComputeEvent>>
    transferToDeviceImpl(FrozenMemoryRegion region,
                         const std::type_info & type, bool isConst) override;

    virtual std::tuple<FrozenMemoryRegion, std::shared_ptr<ComputeEvent>>
    transferToHostImpl(MemoryRegionHandle handle) override;

    virtual std::tuple<MutableMemoryRegion, std::shared_ptr<ComputeEvent>>
    transferToHostMutableImpl(MemoryRegionHandle handle) override;

    virtual std::shared_ptr<ComputeKernel>
    getKernel(const std::string & kernelName) override;

    virtual MemoryRegionHandle
    managePinnedHostRegion(std::span<const std::byte> region, size_t align,
                           const std::type_info & type, bool isConst) override;

    virtual std::shared_ptr<ComputeQueue>
    getQueue() override;

    // Return the MappedSerializer that owns the memory allocated on the host for this
    // device.  It's needed for the generic MemoryRegion functions to know how to manipulate
    // memory handles.  In practice it probably means that each runtime needs to define a
    // MappedSerializer derivitive.
    virtual MappedSerializer * getSerializer() override;
};

}  // namespace MLDB