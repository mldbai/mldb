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
    OpenCLComputeEvent(OpenCLEvent ev);

    virtual ~OpenCLComputeEvent() = default;

    virtual std::shared_ptr<ComputeProfilingInfo> getProfilingInfo() const override;

    virtual void await() const override;

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
    launch(const BoundComputeKernel & kernel,
           const std::vector<uint32_t> & grid,
           const std::vector<std::shared_ptr<ComputeEvent>> & prereqs = {}) override;

    virtual std::shared_ptr<ComputeEvent>
    enqueueFillArrayImpl(MemoryRegionHandle region, MemoryRegionInitialization init,
                         size_t startOffsetInBytes, ssize_t lengthInBytes,
                         const std::any & arg) override;
                         
    virtual void flush() override;
};

// OpenCLComputeContext

struct OpenCLComputeContext: public ComputeContext {

    OpenCLComputeContext(std::vector<OpenCLDevice> devices);

    virtual ~OpenCLComputeContext() = default;

    OpenCLContext clContext;
    OpenCLCommandQueue clQueue;  // for internal operations
    std::vector<OpenCLDevice> clDevices;

    std::shared_ptr<MappedSerializer> backingStore;

    mutable std::mutex cacheMutex;
    std::map<std::string, std::any> cache;

    std::any getCacheEntry(const std::string & key) const;

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
 
    std::any setCacheEntry(const std::string & key, std::any value);

    struct MemoryRegionInfo: public MemoryRegionHandleInfo {
        OpenCLMemObject mem;

        void init(OpenCLMemObject mem)
        {
            this->mem = std::move(mem);
        }
    };

    OpenCLMemObject getMemoryRegion(const MemoryRegionHandleInfo & handle) const;

    virtual MemoryRegionHandle
    allocateImpl(size_t length, size_t align,
                 const std::type_info & type,
                 bool isConst,
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
    virtual MappedSerializer * getSerializer();
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

    // Function to modify the grid dimensions
    std::function<void (std::vector<size_t> & grid)> modifyGrid;

    // Parses an OpenCL kernel argument info structure, and turns it into a ComputeKernel type
    std::pair<ComputeKernelType, std::string>
    getKernelType(const OpenCLKernelArgInfo & info);

    void setParameters(SetParameters setter);

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
};

void registerOpenCLComputeKernel(const std::string & kernelName,
                           std::function<std::shared_ptr<OpenCLComputeKernel>(OpenCLComputeContext &)> generator);

}  // namespace MLDB