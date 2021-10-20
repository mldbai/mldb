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

    virtual std::shared_ptr<ComputeEvent> thenImpl(std::function<void ()> fn);

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
                         
    virtual std::shared_ptr<ComputeEvent>
    makeAlreadyResolvedEvent() const;

    virtual void flush() override;
    virtual void finish() override;
};

// OpenCLComputeContext

struct OpenCLComputeContext: public ComputeContext {

    OpenCLComputeContext(std::vector<OpenCLDevice> devices);

    virtual ~OpenCLComputeContext() = default;

    OpenCLContext clContext;
    std::shared_ptr<OpenCLComputeQueue> clQueue;  // for internal operations
    std::vector<OpenCLDevice> clDevices;

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

    virtual ComputePromiseT<MemoryRegionHandle>
    allocateImpl(const std::string & opName,
                 size_t length, size_t align,
                 const std::type_info & type,
                 bool isConst,
                 MemoryRegionInitialization initialization,
                 std::any initWith = std::any()) override;

    virtual ComputePromiseT<MemoryRegionHandle>
    transferToDeviceImpl(const std::string & opName,
                         FrozenMemoryRegion region,
                         const std::type_info & type, bool isConst) override;

    virtual ComputePromiseT<FrozenMemoryRegion>
    transferToHostImpl(const std::string & opName, MemoryRegionHandle handle) override;

    virtual ComputePromiseT<MutableMemoryRegion>
    transferToHostMutableImpl(const std::string & opName, MemoryRegionHandle handle) override;

    virtual std::shared_ptr<ComputeKernel>
    getKernel(const std::string & kernelName) override;

    virtual ComputePromiseT<MemoryRegionHandle>
    managePinnedHostRegion(const std::string & opName,
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

    // Function to modify the grid dimensions
    std::function<void (std::vector<size_t> & grid)> modifyGrid;

    mutable OpenCLProgram clProgram;  // Mutable as createKernel is non-const
    std::string kernelName;
    OpenCLKernel clKernel;
    OpenCLKernelInfo clKernelInfo;

    // For each OpenCL argument, which is the corresponding argument number in arguments passed in?
    std::vector<int> correspondingArgumentNumbers;

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

    // Perform the abstract bind() operation, returning a BoundComputeKernel
    virtual BoundComputeKernel bindImpl(std::vector<ComputeKernelArgument> arguments) const override;
};

void registerOpenCLComputeKernel(const std::string & kernelName,
                           std::function<std::shared_ptr<OpenCLComputeKernel>(OpenCLComputeContext &)> generator);

}  // namespace MLDB