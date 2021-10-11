/** compute_kernel_opencl.h                                                -*- C++ -*-
    Jeremy Barnes, 27 March 2016
    This file is part of MLDB. Copyright 2016 mldb.ai inc. All rights reserved.

    Compute kernel runtime for CPU devices.
*/

#pragma once

#include "mldb/block/compute_kernel.h"
#include "opencl_types.h"

namespace MLDB {

struct OpenCLComputeContext: public ComputeContext {

    OpenCLComputeContext(std::vector<OpenCLDevice> devices)
        : clContext(devices),
          clQueue(clContext.createCommandQueue(devices[0])),
          clDevices(std::move(devices)),
          backingStore(new MemorySerializer())
    {
    }

    virtual ~OpenCLComputeContext() = default;

    OpenCLContext clContext;
    OpenCLCommandQueue clQueue;  // for internal operations
    std::vector<OpenCLDevice> clDevices;

    std::shared_ptr<MappedSerializer> backingStore;

    mutable std::mutex cacheMutex;
    std::map<std::string, std::any> cache;

    std::any getCacheEntry(const std::string & key) const
    {
        std::unique_lock guard(cacheMutex);
        auto it = cache.find(key);
        if (it == cache.end()) {
            return std::any();
        }
        return it->second;
    }

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
 
    std::any setCacheEntry(const std::string & key, std::any value)
    {
        std::unique_lock guard(cacheMutex);
        std::any oldValue;
        auto it = cache.find(key);
        if (it == cache.end()) {
            cache.emplace(key, std::move(value));
            return oldValue;
        }
        oldValue = std::move(it->second);
        it->second = std::move(value);
        return oldValue;
    }

    struct MemoryRegionInfo: public MemoryRegionHandleInfo {
        size_t lengthInBytes = 0;
        OpenCLMemObject mem;

        void init(OpenCLMemObject mem)
        {
            this->mem = std::move(mem);
        }
    };

    OpenCLMemObject getMemoryRegion(const MemoryRegionHandleInfo & handle) const
    {
        const MemoryRegionInfo * upcastHandle = dynamic_cast<const MemoryRegionInfo *>(&handle);
        if (!upcastHandle) {
            throw MLDB::Exception("TODO: get memory region from block handled from elsewhere");
        }
        return upcastHandle->mem;
    }

    virtual MemoryRegionHandle
    allocateImpl(size_t length, size_t align,
                 const std::type_info & type,
                 bool isConst,
                 MemoryRegionInitialization initialization,
                 std::any initWith = std::any()) override
    {
        // TODO: align...
        OpenCLMemObject mem = clContext.createBuffer(CL_MEM_READ_WRITE, length);

        switch (initialization) {
            case INIT_NONE:
                break;
            case INIT_ZERO_FILLED: {
                uint32_t pattern = 0;
                auto event = clQueue.enqueueFillBuffer(mem, &pattern, sizeof(pattern), 0, length, {});
                event.waitUntilFinished();
                break;
            }
            case INIT_BLOCK_FILLED: {
                auto [init, len] = std::any_cast<std::pair<const void *, size_t>>(initWith);
                auto event = clQueue.enqueueFillBuffer(mem, init, len, 0, length, {});
                event.waitUntilFinished();
                break;
            }
            case INIT_KERNEL: {
                throw MLDB::Exception("Kernel initialization not implemented yet");
            }
            default:
                throw MLDB::Exception("Unknown initialization in allocateImpl");
        }

        auto handle = std::make_shared<MemoryRegionInfo>();
        handle->mem = std::move(mem);
        handle->type = &type;
        handle->isConst = isConst;
        handle->lengthInBytes = length;
        MemoryRegionHandle result{std::move(handle)};
        return {result};
    }

    virtual std::tuple<MemoryRegionHandle, std::shared_ptr<ComputeEvent>>
    transferToDeviceImpl(FrozenMemoryRegion region,
                         const std::type_info & type, bool isConst) override
    {
        Timer timer;

        OpenCLMemObject mem;
        if (region.memusage() == 0) {
            // Create a valid pointer, which means non-zero length
            mem = clContext.createBuffer(CL_MEM_READ_ONLY, 4 /* size */);
        }
        else {
            mem = clContext.createBuffer(isConst ? CL_MEM_READ_ONLY : CL_MEM_READ_WRITE,
                                         region.data(), region.memusage());
        }

        auto handle = std::make_shared<MemoryRegionInfo>();
        handle->mem = std::move(mem);
        handle->type = &type;
        handle->isConst = isConst;
        handle->lengthInBytes = region.length();
        MemoryRegionHandle result{std::move(handle)};

        using namespace std;
        cerr << "transferring " << region.memusage() / 1000000.0 << " Mbytes of type "
             << demangle(type.name()) << " isConst " << isConst << " to device in "
             << timer.elapsed_wall() << " at "
             << region.memusage() / 1000000.0 / timer.elapsed_wall() << "MB/sec" << endl;

        return {std::move(result), nullptr};
    }

    virtual std::tuple<FrozenMemoryRegion, std::shared_ptr<ComputeEvent>>
    transferToHostImpl(MemoryRegionHandle handle) override
    {
        if (!handle.handle) {
            return { {}, nullptr };
        }

        OpenCLMemObject mem = getMemoryRegion(*handle.handle);
        auto [memPtr, event]
            = clQueue.enqueueMapBuffer(mem, CL_MAP_READ,
                                       0 /* offset */, mem.size());

        FrozenMemoryRegion result(memPtr, (char *)memPtr.get(), mem.size());
        
        event.waitUntilFinished();

        return { std::move(result), nullptr };
    }

    virtual std::tuple<MutableMemoryRegion, std::shared_ptr<ComputeEvent>>
    transferToHostMutableImpl(MemoryRegionHandle handle) override
    {
        if (!handle.handle) {
            return { {}, nullptr };
        }

        OpenCLMemObject mem = getMemoryRegion(*handle.handle);
        auto [memPtr, event]
            = clQueue.enqueueMapBuffer(mem, CL_MAP_READ | CL_MAP_WRITE,
                                       0 /* offset */, mem.size());

        // TODO: backingStore is WRONG... this is a hack
        MutableMemoryRegion result(memPtr, (char *)memPtr.get(), mem.size(), backingStore.get());
        
        event.waitUntilFinished();

        return { std::move(result), nullptr };
    }

    virtual std::shared_ptr<ComputeKernel>
    getKernel(const std::string & kernelName) override;

    virtual MemoryRegionHandle
    managePinnedHostRegion(std::span<const std::byte> region, size_t align,
                           const std::type_info & type, bool isConst) override
    {
        Timer timer;
        OpenCLMemObject mem;
        if (region.size() == 0) {
            // Create a valid pointer, which means non-zero length
            mem = clContext.createBuffer(CL_MEM_READ_ONLY, 4 /* size */);
        }
        else {
            if (isConst) {
                mem = clContext.createBuffer(CL_MEM_READ_ONLY,
                                             region.data(), region.size());
            } else {
                mem = clContext.createBuffer(CL_MEM_READ_WRITE,
                                             (void *)region.data(), region.size());
            }
        }

        using namespace std;
        cerr << "transferring " << region.size() / 1000000.0 << " Mbytes of pinned type "
             << demangle(type.name()) << " isConst " << isConst << " to device in "
             << timer.elapsed_wall() << " at "
             << region.size() / 1000000.0 / timer.elapsed_wall() << "MB/sec" << endl;

        auto handle = std::make_shared<MemoryRegionInfo>();
        handle->mem = std::move(mem);
        handle->type = &type;
        handle->isConst = isConst;
        handle->lengthInBytes = region.size();
        MemoryRegionHandle result{std::move(handle)};
        return result;
    }

    virtual std::shared_ptr<ComputeQueue>
    getQueue() override
    {
        return std::make_shared<ComputeQueue>(this);
    }

    // Return the MappedSerializer that owns the memory allocated on the host for this
    // device.  It's needed for the generic MemoryRegion functions to know how to manipulate
    // memory handles.  In practice it probably means that each runtime needs to define a
    // MappedSerializer derivitive.
    virtual MappedSerializer * getSerializer()
    {
        return backingStore.get();
    }
};

struct OpenCLComputeKernel: public ComputeKernel {

    /// Block dimensions for launching the kernel
    std::vector<size_t> block;

    /// Do we allow the grid to be padded out?
    bool allowGridPaddingFlag = false;

    using SetParameters = std::function<void (OpenCLKernel & kernel, OpenCLComputeContext & context)>;

    /// List of functions used to set arbitrary values on the kernel (especially for calculating
    /// sizes of local arrays or other bounds)
    std::vector<SetParameters> setters;

    // Parses an OpenCL kernel argument info structure, and turns it into a ComputeKernel type
    std::pair<ComputeKernelType, std::string>
    getKernelType(const OpenCLKernelArgInfo & info)
    {
        bool isConst = info.typeQualifier.test(OpenCLArgTypeQualifier::CONST);
        int arrayDim = 0;
        std::string clTypeName = info.typeName;
        while (!clTypeName.empty() && clTypeName.back() == '*') {
            ++arrayDim;
            clTypeName.pop_back();
        }

        ComputeKernelType type;

        if (clTypeName == "ulong") {
            type = parseType("u64");
        }
        else if (clTypeName == "uint") {
            type = parseType("u32");
        }
        else if (clTypeName == "ushort") {
            type = parseType("u16");
        }
        else if (clTypeName == "uchar") {
            type = parseType("u8");
        }
        else if (clTypeName == "long") {
            type = parseType("i64");
        }
        else if (clTypeName == "int") {
            type = parseType("i32");
        }
        else if (clTypeName == "short") {
            type = parseType("i16");
        }
        else if (clTypeName == "char") {
            type = parseType("i8");
        }
        else if (clTypeName == "float") {
            type = parseType("f32");
        }
        else if (clTypeName == "double") {
            type = parseType("f64");
        }
        else type = parseType(clTypeName);  // Must be a user defined type

        // Add back the dimensions
        for (size_t i = 0;  i < arrayDim;  ++i) {
            type.dims.push_back({nullptr});
        }

        return { std::move(type), isConst ? "r" : "rw" };
    }

    void setParameters(SetParameters setter)
    {
        setters.emplace_back(std::move(setter));
    }

    // This is called as internal documentation.  It says that we allow the runtime to pad
    // out the grid size so that it's a multiple of the block size.  This means however that
    // the kernel will be called with out of range values, and the kernel grid size may be
    // higher than the actual range, which will need to be passed in as an argument.  In
    // pratice, the kernel needs to check if its IDs are out of range and exit if so.
    void allowGridPadding()
    {
        allowGridPaddingFlag = true;
    }

    void setComputeFunction(OpenCLProgram program,
                            std::string kernelName,
                            std::vector<size_t> block)
    {
        this->block = std::move(block);

        OpenCLKernel kernel = program.createKernel(kernelName);

        auto kernelInfo = kernel.getInfo();

        using namespace std;
        cerr << jsonEncode(kernelInfo) << endl;

        std::vector<int> correspondingParameters(kernelInfo.numArgs, -1);

        for (auto & arg: kernelInfo.args) {
            if (arg.addressQualifier == OpenCLArgAddressQualifier::LOCAL) {
                if (this->setters.empty()) {
                    throw MLDB::Exception("Local parameter in kernel with no setters defined; "
                                          "implement a setter to avoid launch failure");
                }
            }
            else {
                auto [type, access] = getKernelType(arg);
                std::string argName = arg.name;
                auto it = paramIndex.find(argName);
                if (it == paramIndex.end()) {
                    if (this->setters.size() > 0)
                        continue;  // should be done in the setter...
                    throw MLDB::Exception("Kernel parameter " + std::to_string(arg.argNum)
                                          + " (" + argName + ") to OpenCL kernel " + kernelName
                                          + " has no counterpart in formal parameter list");
                }
                correspondingParameters.at(arg.argNum) = it->second;
            }
        }

        auto result = [program, kernelInfo, kernelName, correspondingParameters, this] (ComputeContext & context, std::vector<ComputeKernelArgument> & params) mutable -> Callable
        {
            auto & upcastContext = dynamic_cast<OpenCLComputeContext &>(context);
            OpenCLKernel kernel = program.createKernel(kernelName);

            for (size_t i = 0;  i < kernelInfo.args.size();  ++i) {
                int paramNum = correspondingParameters.at(i);
                cerr << "binding OpenCL parameter " << i << " from argument " << paramNum << endl;
                if (paramNum == -1) {
                    // local, or will be done via setter...
                }
                else {
                    const ComputeKernelArgument & param = params.at(paramNum);
                    if (param.getPrimitive) {
                        // TODO: bind it
                        auto bytes = param.getPrimitive(param.value, context);
                        kernel.bindArg(i, bytes.data(), bytes.size());
                    }
                    else if (param.getHandle) {
                        auto handle = param.getHandle(param.value, context);
                        OpenCLMemObject mem = upcastContext.getMemoryRegion(*handle.handle);
                        kernel.bindArg(i, std::move(mem));
                    }
                    else if (param.getConstRange) {
                        throw MLDB::Exception("param.getConstRange");
                    }
                    else {
                        throw MLDB::Exception("don't know how to handle passing parameter to OpenCL");
                    }
                }
            }

            // Run the setters to set the other parameters
            for (auto & s: this->setters) {
                s(kernel, upcastContext);
            }

            OpenCLKernelWorkgroupInfo info(kernel, upcastContext.clDevices[0]);
            cerr << jsonEncode(info) << endl;

            return [kernel, this] (ComputeContext & context, std::span<ComputeKernelGridRange> grid)
            {
                auto & upcastContext = dynamic_cast<OpenCLComputeContext &>(context);
                std::vector<size_t> clGrid;
                ExcAssertEqual(grid.size(), this->block.size());
                for (size_t i = 0;  i < grid.size();  ++i) {
                    // Pad out the grid so we cover the whole lot.  The kernel will need to be
                    // sure to no-op if it's out of bounds.
                    auto b = this->block[i];
                    auto range = grid[i].range();
                    auto rem = range % b;
                    if (rem > 0) {
                        if (this->allowGridPaddingFlag) {
                            range += (b - rem);
                            cerr << "padding out dimension " << i << " from " << grid[i].range()
                                << " to " << range << " due to block size of " << b << endl;
                        }
                        else {
                            throw MLDB::Exception("OpenCL kernel '" + this->kernelName + "' won't launch "
                                                  "due to grid dimension " + std::to_string(i)
                                                  + " (" + std::to_string(range) + ") not being a "
                                                  + "multple of the block size (" + std::to_string(b)
                                                  + ").  Consider using allowGridPadding() or modifying "
                                                  + "grid calculations");
                        }
                    }
                    clGrid.push_back(range);
                }
                cerr << "clGrid = " << jsonEncodeStr(clGrid) << endl;
                cerr << "this->block = " << jsonEncodeStr(this->block) << endl;
                auto event = upcastContext.clQueue.launch(kernel, clGrid, this->block);
                event.waitUntilFinished();
            };
        };

        createCallable = result;
    }
};

void registerOpenCLComputeKernel(const std::string & kernelName,
                           std::function<std::shared_ptr<OpenCLComputeKernel>(OpenCLComputeContext &)> generator);

}  // namespace MLDB