/** compute_kernel_opencl.cc                                                -*- C++ -*-
    Jeremy Barnes, 27 March 2016
    This file is part of MLDB. Copyright 2016 mldb.ai inc. All rights reserved.

    Compute kernel runtime for CPU devices.
*/

#include "compute_kernel_opencl.h"
#include "mldb/types/basic_value_descriptions.h"
#include "opencl_types.h"
#include "mldb/vfs/filter_streams.h"

using namespace std;

namespace MLDB {

namespace {

std::mutex kernelRegistryMutex;
struct KernelRegistryEntry {
    std::function<std::shared_ptr<ComputeKernel>(OpenCLComputeContext & context)> generate;
};

std::map<std::string, KernelRegistryEntry> kernelRegistry;

} // file scope

// OpenCLComputeProfilingInfo

OpenCLComputeProfilingInfo::
OpenCLComputeProfilingInfo(OpenCLProfilingInfo info)
    : clInfo(std::move(info))
{
}


// OpenCLComputeEvent

OpenCLComputeEvent::
OpenCLComputeEvent(OpenCLEvent ev)
    : ev(std::move(ev))
{
}

std::shared_ptr<ComputeProfilingInfo>
OpenCLComputeEvent::
getProfilingInfo() const
{
    return std::make_shared<OpenCLComputeProfilingInfo>(ev.getProfilingInfo());
}

void
OpenCLComputeEvent::
await() const
{
    return ev.waitUntilFinished();
}


// OpenCLComputeQueue

OpenCLComputeQueue::
OpenCLComputeQueue(OpenCLComputeContext * owner, OpenCLCommandQueue queue)
    : ComputeQueue(owner), clOwner(owner), clQueue(std::move(queue))
{
}

OpenCLComputeQueue::
OpenCLComputeQueue(OpenCLComputeContext * owner)
    : ComputeQueue(owner), clOwner(owner)
{
    ExcAssertEqual(clOwner->clDevices.size(), 1);
    clQueue = clOwner->clContext.createCommandQueue(clOwner->clDevices[0],
                                                    OpenCLCommandQueueProperties::PROFILING_ENABLE);
}

std::shared_ptr<ComputeEvent>
OpenCLComputeQueue::
launch(const BoundComputeKernel & kernel,
       const std::vector<uint32_t> & grid,
       const std::vector<std::shared_ptr<ComputeEvent>> & prereqs)
{
    return ComputeQueue::launch(kernel, grid, prereqs);
}

std::shared_ptr<ComputeEvent>
OpenCLComputeQueue::
enqueueFillArrayImpl(MemoryRegionHandle region, MemoryRegionInitialization init,
                     size_t startOffsetInBytes, ssize_t lengthInBytes,
                     const std::any & arg)
{
    OpenCLMemObject mem = clOwner->getMemoryRegion(*region.handle);
    
    if (startOffsetInBytes > region.lengthInBytes()) {
        throw MLDB::Exception("region is too long");
    }
    if (lengthInBytes == -1)
        lengthInBytes = region.lengthInBytes() - startOffsetInBytes;
    
    if (startOffsetInBytes + lengthInBytes > region.lengthInBytes()) {
        throw MLDB::Exception("overflowing memory region");
    }

    switch (init) {
        case INIT_NONE:
            throw MLDB::Exception("attempting to enqueue no-op OpenCL fill array operation");

        case INIT_ZERO_FILLED: {
            // Intel driver has a bug, so we need to fall back...
            cerr << "OpenCL fill: fallback to kernel" << endl;
            // Invoke the underlying method, which will launch a kernel
            return ComputeQueue::enqueueFillArrayImpl(region, init, startOffsetInBytes, lengthInBytes, arg);

            auto zeroFillWith = [&] (const auto pattern)
            {
                cerr << "zeroing " << lengthInBytes << " bytes with " << sizeof(pattern) << " bytes" << endl;
                ExcAssertLessEqual(startOffsetInBytes, region.lengthInBytes());
                if (lengthInBytes == -1)
                    lengthInBytes = region.lengthInBytes() - startOffsetInBytes;
                ExcAssertLessEqual(startOffsetInBytes + lengthInBytes, region.lengthInBytes());
                auto event = clQueue.enqueueFillBuffer(mem, pattern, startOffsetInBytes, lengthInBytes, {});
                if (event)
                    event.waitUntilFinished();  // needed for Intel
                return std::make_shared<OpenCLComputeEvent>(std::move(event));
            };

            if (lengthInBytes % 16 == 0)
                return zeroFillWith((std::array<uint64_t, 2>{0,0}));
            else if (lengthInBytes % 8 == 0)
                return zeroFillWith((uint64_t)0);
            else if (lengthInBytes % 4 == 0)
                return zeroFillWith((uint32_t)0);
            else if (lengthInBytes % 2 == 0)
                return zeroFillWith((uint16_t)0);
            else return zeroFillWith((uint8_t)0);
        }

        case INIT_BLOCK_FILLED: {
            auto block = std::any_cast<std::span<const std::byte>>(arg);
            const char * valBytes = (const char *)block.data();
 
            // If all bytes in the initialization are zero, we can do it more efficiently
            bool allZero = true;
            for (size_t i = 0;  allZero && i < block.size();  allZero = valBytes[i++] == 0) ;
            if (allZero)
                return enqueueFillArrayImpl(region, INIT_ZERO_FILLED, startOffsetInBytes, lengthInBytes, {});

            cerr << "block.size() = " << block.size() << endl;
            if (block.size() == 1 || block.size() == 2 || block.size() == 4 || block.size() == 8 || block.size() == 16 || block.size() == 32) {
                // The OpenCL fill array infrastructure only takes a few possible sizes
                auto event = clQueue.enqueueFillBuffer(mem, block.data(), block.size(),
                                                    startOffsetInBytes, lengthInBytes, {});
                return std::make_shared<OpenCLComputeEvent>(std::move(event));
            }
            else {
                cerr << "OpenCL fill: fallback to kernel" << endl;
                // Invoke the underlying method, which will launch a kernel
                return ComputeQueue::enqueueFillArrayImpl(region, init, startOffsetInBytes, lengthInBytes, arg);
            }
        }

        case INIT_KERNEL: {
            throw MLDB::Exception("Kernel initialization not implemented yet for OpenCL");
        }
        default:
            throw MLDB::Exception("Unknown initialization in allocateImpl");
    }
}
                        
void
OpenCLComputeQueue::
flush()
{
    clQueue.flush();
}

// OpenCLComputeContext

OpenCLComputeContext::
OpenCLComputeContext(std::vector<OpenCLDevice> devices)
    : clContext(devices),
        clQueue(clContext.createCommandQueue(devices[0], OpenCLCommandQueueProperties::PROFILING_ENABLE)),
        clDevices(std::move(devices)),
        backingStore(new MemorySerializer())
{
}

std::any
OpenCLComputeContext::
getCacheEntry(const std::string & key) const
{
    std::unique_lock guard(cacheMutex);
    auto it = cache.find(key);
    if (it == cache.end()) {
        return std::any();
    }
    return it->second;
}

std::any
OpenCLComputeContext::
setCacheEntry(const std::string & key, std::any value)
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

OpenCLMemObject
OpenCLComputeContext::
getMemoryRegion(const MemoryRegionHandleInfo & handle) const
{
    const MemoryRegionInfo * upcastHandle = dynamic_cast<const MemoryRegionInfo *>(&handle);
    if (!upcastHandle) {
        throw MLDB::Exception("TODO: get memory region from block handled from elsewhere");
    }
    return upcastHandle->mem;
}

MemoryRegionHandle
OpenCLComputeContext::
allocateImpl(size_t length, size_t align,
                const std::type_info & type,
                bool isConst,
                MemoryRegionInitialization initialization,
                std::any initWith)
{
    // TODO: align...
    OpenCLMemObject mem = clContext.createBuffer(CL_MEM_READ_WRITE, length);

#if 0 // buggy on Intel
    switch (initialization) {
        case INIT_NONE:
            break;
        case INIT_ZERO_FILLED: {


            auto zeroFillWith = [&] (const auto pattern)
            {
                auto event = clQueue.enqueueFillBuffer(mem, &pattern, sizeof(pattern), 0, length, {});
                event.waitUntilFinished();
            };

            if (length % 16 == 0)
                zeroFillWith((std::array<uint64_t, 2>{0,0}));
            else if (length % 12 == 0)
                zeroFillWith((std::array<uint32_t, 3>{0,0,0}));
            else if (length % 8 == 0)
                zeroFillWith((uint64_t)0);
            else if (length % 4 == 0)
                zeroFillWith((uint32_t)0);
            else if (length % 2 == 0)
                zeroFillWith((uint16_t)0);
            else zeroFillWith((uint8_t)0);
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
#endif

    auto handle = std::make_shared<MemoryRegionInfo>();
    handle->mem = std::move(mem);
    handle->type = &type;
    handle->isConst = isConst;
    handle->lengthInBytes = length;
    MemoryRegionHandle result{std::move(handle)};

#if 1
    OpenCLComputeQueue queue(this, clQueue);
    auto event = queue.ComputeQueue::enqueueFillArrayImpl(result, initialization,
                                       0 /* startOffsetInBytes */, -1 /*lengthinBytes*/, initWith);
    if (event)
        event->await();
#endif

    return {result};
}

std::tuple<MemoryRegionHandle, std::shared_ptr<ComputeEvent>>
OpenCLComputeContext::
transferToDeviceImpl(FrozenMemoryRegion region,
                        const std::type_info & type, bool isConst)
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

std::tuple<FrozenMemoryRegion, std::shared_ptr<ComputeEvent>>
OpenCLComputeContext::
transferToHostImpl(MemoryRegionHandle handle)
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

std::tuple<MutableMemoryRegion, std::shared_ptr<ComputeEvent>>
OpenCLComputeContext::
transferToHostMutableImpl(MemoryRegionHandle handle)
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

std::shared_ptr<ComputeKernel>
OpenCLComputeContext::
getKernel(const std::string & kernelName)
{
    std::unique_lock guard(kernelRegistryMutex);
    auto it = kernelRegistry.find(kernelName);
    if (it == kernelRegistry.end()) {
        throw AnnotatedException(400, "Unable to find OpenCL compute kernel '" + kernelName + "'",
                                        "kernelName", kernelName);
    }
    auto result = it->second.generate(*this);
    result->context = this;
    return result;
}

MemoryRegionHandle
OpenCLComputeContext::
managePinnedHostRegion(std::span<const std::byte> region, size_t align,
                        const std::type_info & type, bool isConst)
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

std::shared_ptr<ComputeQueue>
OpenCLComputeContext::
getQueue()
{
    return std::make_shared<OpenCLComputeQueue>(this);
}

// Return the MappedSerializer that owns the memory allocated on the host for this
// device.  It's needed for the generic MemoryRegion functions to know how to manipulate
// memory handles.  In practice it probably means that each runtime needs to define a
// MappedSerializer derivitive.
MappedSerializer *
OpenCLComputeContext::
getSerializer()
{
    return backingStore.get();
}

// OpenCLComputeKernel

// Parses an OpenCL kernel argument info structure, and turns it into a ComputeKernel type
std::pair<ComputeKernelType, std::string>
OpenCLComputeKernel::
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

void
OpenCLComputeKernel::
setParameters(SetParameters setter)
{
    setters.emplace_back(std::move(setter));
}

void
OpenCLComputeKernel::
allowGridPadding()
{
    allowGridPaddingFlag = true;
}

void
OpenCLComputeKernel::
allowGridExpansion()
{
    allowGridExpansionFlag = true;
}

void
OpenCLComputeKernel::
setComputeFunction(OpenCLProgram program,
                   std::string kernelName,
                   std::vector<size_t> block)
{
    this->block = std::move(block);

    OpenCLKernel kernel = program.createKernel(kernelName);

    auto kernelInfo = kernel.getInfo();

    using namespace std;
    //cerr << jsonEncode(kernelInfo) << endl;

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
            //cerr << "binding OpenCL parameter " << i << " from argument " << paramNum << endl;
            if (paramNum == -1) {
                // local, or will be done via setter...
            }
            else {
                const ComputeKernelArgument & param = params.at(paramNum);
                if (param.handler->canGetPrimitive()) {
                    // TODO: bind it
                    auto bytes = param.handler->getPrimitive(context);
                    kernel.bindArg(i, bytes.data(), bytes.size());
                }
                else if (param.handler->canGetHandle()) {
                    auto handle = param.handler->getHandle(context);
                    OpenCLMemObject mem = upcastContext.getMemoryRegion(*handle.handle);
                    kernel.bindArg(i, std::move(mem));
                }
                else if (param.handler->canGetConstRange()) {
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
        //cerr << jsonEncode(info) << endl;

        return [kernel, this] (ComputeContext & context, std::span<ComputeKernelGridRange> grid)
        {
            auto & upcastContext = dynamic_cast<OpenCLComputeContext &>(context);
            std::vector<size_t> clGrid;
            
            if (allowGridExpansionFlag)
                ExcAssertLessEqual(grid.size(), this->block.size());
            else
                ExcAssertEqual(grid.size(), this->block.size());

            for (size_t i = 0;  i < this->block.size();  ++i) {
                // Pad out the grid so we cover the whole lot.  The kernel will need to be
                // sure to no-op if it's out of bounds.
                auto b = this->block[i];
                auto range = i < grid.size() ? grid[i].range() : b;
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
            if (modifyGrid)
                modifyGrid(clGrid);
            
            cerr << "launching kernel " << this->kernelName << " with grid " << jsonEncodeStr(clGrid) << endl;
            //cerr << "this->block = " << jsonEncodeStr(this->block) << endl;
            auto event = upcastContext.clQueue.launch(kernel, clGrid, this->block);
            event.waitUntilFinished();

            cerr << "  submit    queue    start      end  elapsed name" << endl;
    
            auto info = event.getProfilingInfo();

            auto ms = [&] (int64_t ns) -> double
                {
                    return ns / 1000000.0;
                };
            
            cerr << format("%8.3f %8.3f %8.3f %8.3f %8.3f ",
                        ms(info.queued), ms(info.submit), ms(info.start),
                        ms(info.end),
                        ms(info.end - info.start))
                 << this->kernelName << endl;
        };
    };

    createCallable = result;
}


// OpenCLComputeRuntime

struct OpenCLComputeRuntime: public ComputeRuntime {

    std::vector<OpenCLPlatform> clPlatforms;
    std::vector<std::vector<OpenCLDevice>> clDevices;
    std::vector<ComputeDevice> devices;

    OpenCLComputeRuntime()
    {
        clPlatforms = getOpenCLPlatforms();
        clDevices.reserve(clPlatforms.size());

        for (size_t i = 0;  i < clPlatforms.size();  ++i) {
            clDevices.emplace_back(clPlatforms[i].getDevices());
            for (size_t j = 0;  j < clDevices[i].size();  ++j) {
                devices.push_back({ComputeRuntimeId::OPENCL, (uint8_t)i, (uint16_t)j, 0, 0});
            }
        }
    }

    OpenCLDevice convertDevice(ComputeDevice device) const
    {
        if (device.runtime != ComputeRuntimeId::OPENCL) {
            throw MLDB::Exception("Attempt to pass non-OpenCL device " + device.info() + " to OpenCL");
        }
        return clDevices.at(device.runtimeInstance).at(device.deviceInstance);
    }

    virtual ~OpenCLComputeRuntime()
    {
    }

    virtual ComputeRuntimeId getId() const
    {
        return ComputeRuntimeId::OPENCL;
    }

    virtual std::string printRestOfDevice(ComputeDevice device) const
    {
        return std::to_string(device.runtimeInstance) + ":" + std::to_string(device.deviceInstance);
    }

    virtual std::string printHumanReadableDeviceInfo(ComputeDevice device) const
    {
        if (device.runtimeInstance >= clDevices.size()
            || device.deviceInstance >= clDevices[device.runtimeInstance].size()) {
            return "<<INVALID OPENCL PLATFORM OR DEVICE INDEX>>";
        }
        std::string result = clPlatforms[device.runtimeInstance].getPlatformInfo().name
             + " " + clDevices[device.runtimeInstance][device.deviceInstance].getDeviceInfo().name;
        return result;
    }

    virtual ComputeDevice getDefaultDevice() const
    {
        if (clPlatforms.empty()) {
            return ComputeDevice::none();
        }

        // Look for a device that's a GPU with non-unified memory
        for (size_t i = 0;  i < clDevices.size();  ++i) {
            for (size_t j = 0;  j < clDevices[i].size();  ++j) {
                auto info = clDevices[i][j].getDeviceInfo();
                if (info.type.test(OpenCLDeviceType::GPU) && info.unifiedMemory == false ) {
                    return {ComputeRuntimeId::OPENCL, (uint8_t)i, (uint16_t)j, 0, 0};
                }
            }
        }

        // Look for a device that's a GPU
        for (size_t i = 0;  i < clDevices.size();  ++i) {
            for (size_t j = 0;  j < clDevices[i].size();  ++j) {
                auto info = clDevices[i][j].getDeviceInfo();
                if (info.type.test(OpenCLDeviceType::GPU)) {
                    return {ComputeRuntimeId::OPENCL, (uint8_t)i, (uint16_t)j, 0, 0};
                }
            }
        }

        // Fall back on the first device
        return devices[0];
    }

    // Enumerate the devices available for this runtime
    virtual std::vector<ComputeDevice> enumerateDevices() const
    {
        return devices;
    }

    // Get a compute context for this runtime
    virtual std::shared_ptr<ComputeContext>
    getContext(std::span<const ComputeDevice> devices) const
    {
        std::vector<OpenCLDevice> clDevices;
        for (auto & d: devices) {
            clDevices.emplace_back(convertDevice(d));
        }
        return std::make_shared<OpenCLComputeContext>(clDevices);
    }

};

void registerOpenCLComputeKernel(const std::string & kernelName,
                                 std::function<std::shared_ptr<OpenCLComputeKernel>(OpenCLComputeContext & context)> generator)
{
    kernelRegistry[kernelName].generate = generator;
}

namespace {

static struct Init {
    Init()
    {
        ComputeRuntime::registerRuntime(ComputeRuntimeId::OPENCL, "opencl",
                                        [] () { return new OpenCLComputeRuntime(); });

        auto getProgram = [] (OpenCLComputeContext & context) -> OpenCLProgram
        {

            auto compileProgram = [&] () -> OpenCLProgram
            {
                std::string fileName = "mldb/builtin/opencl/base_kernels.cl";
                filter_istream stream(fileName);
                Utf8String source = "#line 1 \"" + fileName + "\"\n" + stream.readAll();

                OpenCLProgram program = context.clContext.createProgram(source);
                string options = "-cl-kernel-arg-info";

                // Build for all devices
                auto buildInfo = program.build(context.clDevices, options);
                
                cerr << jsonEncode(buildInfo[0]) << endl;
                return program;
            };

            static const std::string cacheKey = "__base_kernels";
            OpenCLProgram program = context.getCacheEntry(cacheKey, compileProgram);
            return program;
        };
    
        auto createBlockFillArrayKernel = [getProgram] (OpenCLComputeContext& context) -> std::shared_ptr<OpenCLComputeKernel>
        {
            auto program = getProgram(context);
            auto result = std::make_shared<OpenCLComputeKernel>();
            result->kernelName = "__blockFillArray";
            result->allowGridExpansion();
            result->addParameter("region", "w", "u8[regionLength]");
            result->addParameter("startOffsetInBytes", "r", "u64");
            result->addParameter("lengthInBytes", "r", "u64");
            result->addParameter("blockData", "r", "u8[blockLengthInBytes]");
            result->addParameter("blockLengthInBytes", "r", "u64");
            auto setTheRest = [=] (OpenCLKernel & kernel, OpenCLComputeContext & context)
            {
            };
            result->setParameters(setTheRest);

            result->setComputeFunction(program, "__blockFillArrayKernel", { 256 });

            return result;
        };

        registerOpenCLComputeKernel("__blockFillArray", createBlockFillArrayKernel);

        auto createZeroFillArrayKernel = [getProgram] (OpenCLComputeContext& context) -> std::shared_ptr<OpenCLComputeKernel>
        {
            auto program = getProgram(context);
            auto result = std::make_shared<OpenCLComputeKernel>();
            result->kernelName = "__zeroFillArray";
            result->allowGridExpansion();
            result->addParameter("region", "w", "u8[regionLength]");
            result->addParameter("startOffsetInBytes", "r", "u64");
            result->addParameter("lengthInBytes", "r", "u64");
            auto setTheRest = [=] (OpenCLKernel & kernel, OpenCLComputeContext & context)
            {
            };
            result->setParameters(setTheRest);

            result->setComputeFunction(program, "__zeroFillArrayKernel", { 256 });

            return result;
        };

        registerOpenCLComputeKernel("__zeroFillArray", createZeroFillArrayKernel);

    }

} init;

} // file scope
} // namespace MLDB
