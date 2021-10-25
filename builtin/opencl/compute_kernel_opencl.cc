/** compute_kernel_opencl.cc                                                -*- C++ -*-
    Jeremy Barnes, 27 March 2016
    This file is part of MLDB. Copyright 2016 mldb.ai inc. All rights reserved.

    Compute kernel runtime for CPU devices.
*/

#include "compute_kernel_opencl.h"
#include "mldb/types/basic_value_descriptions.h"
#include "opencl_types.h"
#include "mldb/vfs/filter_streams.h"
#include "mldb/utils/environment.h"
#include "mldb/arch/ansi.h"

using namespace std;

namespace MLDB {

namespace {

std::mutex kernelRegistryMutex;
struct KernelRegistryEntry {
    std::function<std::shared_ptr<ComputeKernel>(OpenCLComputeContext & context)> generate;
};

std::map<std::string, KernelRegistryEntry> kernelRegistry;

struct OpenCLBindInfo: public ComputeKernelBindInfo {
    virtual ~OpenCLBindInfo() = default;

    OpenCLKernel clKernel;
    const OpenCLComputeKernel * owner = nullptr;
};

EnvOption<int> OPENCL_TRACE_API_CALLS("OPENCL_COMPUTE_TRACE_API_CALLS", 0);

__thread int opCount = 0;
Timer startTimer;

template<typename... Args>
void traceOperation(const std::string & opName, Args&&... args)
{
    if (OPENCL_TRACE_API_CALLS.get()) {
        using namespace MLDB::ansi;
        int tid = std::hash<std::thread::id>()(std::this_thread::get_id());
        double elapsed = startTimer.elapsed_wall();

        std::string header = format("%10.6f t%8x %2d  OPENCL COMPUTE: ", elapsed, tid, opCount);
        std::string indent(4 * opCount, ' ');
        std::string toDump = (string)ansi_str_cyan() + header + indent + ansi_str_bold() + opName
                           + ansi_str_reset() + "\n";
        cerr << toDump << flush;
    }
}

struct ScopedOperation {
    ScopedOperation(const ScopedOperation &) = delete;
    auto operator = (const ScopedOperation &) = delete;

    template<typename... Args>
    ScopedOperation(const std::string & opName, Args&&... args)
        : opName(opName)
    {
        traceOperation("BEGIN " + opName, std::forward<Args>(args)...);
        ++opCount;
    }

    ~ScopedOperation()
    {
        if (!opName.empty()) {
            --opCount;

            double elapsed = timer.elapsed_wall();
            std::string timerStr;
            if (elapsed < 0.000001) {
                timerStr = format("%.1fns", elapsed * 1000000000.0);
            }
            else if (elapsed < 0.001) {
                timerStr = format("%.1fus", elapsed * 1000000.0);
            }
            else if (elapsed < 1) {
                timerStr = format("%.1fms", elapsed * 1000.0);
            }
            else {
                timerStr = format("%.1fs", elapsed);
            }
            traceOperation("END " + opName + " [" + ansi::ansi_str_underline() + timerStr + "]");
        }
    }

    std::string opName;
    Timer timer;
};

template<typename... Args>
ScopedOperation MLDB_WARN_UNUSED_RESULT scopedOperation(const std::string & opName, Args&&... args) 
{
    return ScopedOperation(opName, std::forward<Args>(args)...);
}

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
    if (!ev) {
        traceOperation("await(): already satisfied");
        return;  // null event; already satisfied
    }

    auto tr = scopedOperation("await()");
    return ev.waitUntilFinished();
}

std::shared_ptr<ComputeEvent>
OpenCLComputeEvent::
thenImpl(std::function<void ()> fn)
{
    // No event means it's an already satisfied event; we simply run the callback
    if (!ev) {
        fn();
        return std::make_shared<OpenCLComputeEvent>();
    }

    // Otherwise, create a user event for the post-then part
    auto context = ev.getContext();
    OpenCLUserEvent userEvent(context);
    auto nextEvent = std::make_shared<OpenCLComputeEvent>(std::move(userEvent));

    auto cb = [fn=std::move(fn), nextEvent] (auto ev, auto status)
    {
        fn();
        nextEvent->ev.setUserEventStatus(OpenCLEventCommandExecutionStatus::COMPLETE);
    };

    ev.addCallback(std::move(cb));

    return nextEvent;
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
launch(const std::string & opName,
       const BoundComputeKernel & bound,
       const std::vector<uint32_t> & grid,
       const std::vector<std::shared_ptr<ComputeEvent>> & prereqs)
{
    auto tr = scopedOperation("launch kernel " + bound.owner->kernelName + " as " + opName);

    ExcAssert(bound.bindInfo);
    
    const OpenCLBindInfo * bindInfo
        = dynamic_cast<const OpenCLBindInfo *>(bound.bindInfo.get());
    ExcAssert(bindInfo);

    const OpenCLComputeKernel * kernel = bindInfo->owner;

    std::vector<size_t> clGrid;
    
    if (kernel->allowGridExpansionFlag)
        ExcAssertLessEqual(grid.size(), kernel->block.size());
    else
        ExcAssertEqual(grid.size(), kernel->block.size());

    for (size_t i = 0;  i < kernel->block.size();  ++i) {
        // Pad out the grid so we cover the whole lot.  The kernel will need to be
        // sure to no-op if it's out of bounds.
        auto b = kernel->block[i];
        auto range = i < grid.size() ? grid[i] : b;
        auto rem = range % b;
        if (rem > 0) {
            if (kernel->allowGridPaddingFlag) {
                range += (b - rem);
                //cerr << "padding out dimension " << i << " from " << grid[i]
                //    << " to " << range << " due to block size of " << b << endl;
            }
            else {
                throw MLDB::Exception("OpenCL kernel '" + kernel->kernelName + "' won't launch "
                                        "due to grid dimension " + std::to_string(i)
                                        + " (" + std::to_string(range) + ") not being a "
                                        + "multple of the block size (" + std::to_string(b)
                                        + ").  Consider using allowGridPadding() or modifying "
                                        + "grid calculations");
            }
        }
        clGrid.push_back(range);
    }
    if (kernel->modifyGrid)
        kernel->modifyGrid(clGrid);
    
    //cerr << "launching kernel " << kernel->kernelName << " with grid " << jsonEncodeStr(clGrid) << endl;
    //cerr << "this->block = " << jsonEncodeStr(this->block) << endl;
    auto timer = std::make_shared<Timer>();

    auto event = clQueue.launch(bindInfo->clKernel, clGrid, kernel->block);

    // Ensure it's submitted before we start using the event
    clQueue.flush();

    std::string kernelName = kernel->kernelName;

    auto execTimes = std::make_shared<std::map<OpenCLEventCommandExecutionStatus, double>>();

    auto doCallback = [this, kernelName, opName, execTimes, timer]
            (const OpenCLEvent & event, OpenCLEventCommandExecutionStatus status)
    {
        traceOperation("completion callback " + opName + " with status " + jsonEncodeStr(status));
        auto wallTime = timer->elapsed_wall();

        // TODO: lock?
        execTimes->emplace(status, timer->elapsed_wall());

        //std::string msg = format("kernel %s status %s wallTime %.2fms\n",
        //                         kernelName.c_str(), jsonEncodeStr(status).c_str(), wallTime * 1000.0);
        //cerr << msg;

        if (status != OpenCLEventCommandExecutionStatus::COMPLETE)
            return;
    
        if (true) {
            std::unique_lock guard(kernelWallTimesMutex);
            kernelWallTimes[kernelName] += wallTime * 1000.0;
            totalKernelTime += wallTime * 1000.0;
        }

        std::string toDump = "  submit    queue    start      end  elapsed name\n";

        return;

        auto info = event.getProfilingInfo();

        auto ms = [&] (int64_t ns) -> double
            {
                return ns / 1000000.0;
            };
        
        toDump += format("%8.3f %8.3f %8.3f %8.3f %8.3f %s\n",
                    ms(info.queued), ms(info.submit), ms(info.start),
                    ms(info.end),
                    ms(info.end - info.start), kernelName);
        cerr << toDump;
    };

    //event.addCallback(doCallback, OpenCLEventCommandExecutionStatus::QUEUED);
    //event.addCallback(doCallback, OpenCLEventCommandExecutionStatus::RUNNING);
    //event.addCallback(doCallback, OpenCLEventCommandExecutionStatus::SUBMITTED);
    event.addCallback(doCallback, OpenCLEventCommandExecutionStatus::COMPLETE);
    //event.addCallback(doCallback, OpenCLEventCommandExecutionStatus::ERROR);
    //clQueue.flush();  // TODO: remove, this is debug!!!

    // DEBUG
    //event.waitUntilFinished();



    //doCallback(event, 0);

    return std::make_shared<OpenCLComputeEvent>(std::move(event));
}

ComputePromiseT<MemoryRegionHandle>
OpenCLComputeQueue::
enqueueFillArrayImpl(const std::string & opName,
                     MemoryRegionHandle region, MemoryRegionInitialization init,
                     size_t startOffsetInBytes, ssize_t lengthInBytes,
                     const std::any & arg,
                     std::vector<std::shared_ptr<ComputeEvent>> prereqs)
{
    auto op = scopedOperation("enqueueFillArrayImpl " + opName);

    if (startOffsetInBytes > region.lengthInBytes()) {
        throw MLDB::Exception("region is too long");
    }
    if (lengthInBytes == -1)
        lengthInBytes = region.lengthInBytes() - startOffsetInBytes;
    
    if (startOffsetInBytes + lengthInBytes > region.lengthInBytes()) {
        throw MLDB::Exception("overflowing memory region");
    }

    return ComputeQueue::enqueueFillArrayImpl(opName, region, init, startOffsetInBytes, lengthInBytes, arg, prereqs);
}
                        
void
OpenCLComputeQueue::
flush()
{
    auto op = scopedOperation("OpenCLComputeQueue flush");
    clQueue.flush();
}

void
OpenCLComputeQueue::
finish()
{
    auto op = scopedOperation("OpenCLComputeQueue finish");
    clQueue.finish();
}

std::shared_ptr<ComputeEvent>
OpenCLComputeQueue::
makeAlreadyResolvedEvent() const
{
    return std::make_shared<OpenCLComputeEvent>();
}


// OpenCLComputeContext

OpenCLComputeContext::
OpenCLComputeContext(std::vector<OpenCLDevice> devices)
    : clContext(devices),
        clDevices(std::move(devices))
{
    clQueue = std::make_shared<OpenCLComputeQueue>(this);
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

std::tuple<cl_mem, size_t>
OpenCLComputeContext::
getMemoryRegion(const MemoryRegionHandleInfo & handle) const
{
    const MemoryRegionInfo * upcastHandle = dynamic_cast<const MemoryRegionInfo *>(&handle);
    if (!upcastHandle) {
        throw MLDB::Exception("TODO: get memory region from block handled from elsewhere: got " + demangle(typeid(handle)));
    }
    return { upcastHandle->memBase, upcastHandle->offset };
}

static MemoryRegionHandle
doOpenCLAllocate(OpenCLContext & clContext,
                 const std::string & regionName,
                 size_t length, size_t align,
                 const std::type_info & type,
                 bool isConst)
{
    // TODO: align...
    OpenCLMemObject mem = clContext.createBuffer(CL_MEM_READ_WRITE, length);

    auto handle = std::make_shared<OpenCLComputeContext::MemoryRegionInfo>();
    handle->memBase = std::move(mem);
    handle->offset = 0;
    handle->type = &type;
    handle->isConst = isConst;
    handle->lengthInBytes = length;
    handle->name = regionName;

    MemoryRegionHandle result{std::move(handle)};
    return result;
}

ComputePromiseT<MemoryRegionHandle>
OpenCLComputeContext::
allocateImpl(const std::string & regionName,
             size_t length, size_t align,
             const std::type_info & type,
             bool isConst,
             MemoryRegionInitialization initialization,
             std::any initWith)
{
    auto op = scopedOperation("OpenCLComputeContext allocateImpl " + regionName);
    auto result = doOpenCLAllocate(clContext, regionName, length, align, type, isConst);
    return clQueue->enqueueFillArrayImpl(regionName + " initialize", result, initialization,
                                       0 /* startOffsetInBytes */, -1 /*lengthinBytes*/, initWith);
}

MemoryRegionHandle
OpenCLComputeContext::
allocateSyncImpl(const std::string & regionName,
                 size_t length, size_t align,
                 const std::type_info & type, bool isConst,
                 MemoryRegionInitialization initialization,
                 std::any initWith)
{
    auto op = scopedOperation("OpenCLComputeContext allocateSyncImpl " + regionName);
    auto result = doOpenCLAllocate(clContext, regionName, length, align, type, isConst);
    if (initialization != MemoryRegionInitialization::INIT_NONE) {
        return result = clQueue->enqueueFillArrayImpl(regionName + " initialize", std::move(result), initialization,
                                        0 /* startOffsetInBytes */, -1 /*lengthinBytes*/, initWith).move();

    }

    return result;
}

static MemoryRegionHandle
doOpenCLTransferToDevice(OpenCLContext & clContext,
                         const std::string & opName, FrozenMemoryRegion region,
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

    auto handle = std::make_shared<OpenCLComputeContext::MemoryRegionInfo>();
    handle->memBase = std::move(mem);
    handle->offset = 0;
    handle->type = &type;
    handle->isConst = isConst;
    handle->lengthInBytes = region.length();
    MemoryRegionHandle result{std::move(handle)};

    using namespace std;
    cerr << "transferring " << region.memusage() / 1000000.0 << " Mbytes of type "
            << demangle(type.name()) << " isConst " << isConst << " to device in "
            << timer.elapsed_wall() << " at "
            << region.memusage() / 1000000.0 / timer.elapsed_wall() << "MB/sec" << endl;

    return result;
}

ComputePromiseT<MemoryRegionHandle>
OpenCLComputeContext::
transferToDeviceImpl(const std::string & opName, FrozenMemoryRegion region,
                     const std::type_info & type, bool isConst)
{
    auto op = scopedOperation("OpenCLComputeContext transferToDeviceImpl " + opName);
    auto result = doOpenCLTransferToDevice(clContext, opName, region, type, isConst);
    return {std::move(result), std::make_shared<OpenCLComputeEvent>()};
}

MemoryRegionHandle
OpenCLComputeContext::
transferToDeviceSyncImpl(const std::string & opName,
                         FrozenMemoryRegion region,
                         const std::type_info & type, bool isConst)
{
    auto op = scopedOperation("OpenCLComputeContext transferToDeviceSyncImpl " + opName);
    auto result = doOpenCLTransferToDevice(clContext, opName, region, type, isConst);
    return result;
}


ComputePromiseT<FrozenMemoryRegion>
OpenCLComputeContext::
transferToHostImpl(const std::string & opName, MemoryRegionHandle handle)
{
    auto op = scopedOperation("OpenCLComputeContext transferToHostImpl " + opName);

    ExcAssert(handle.handle);

    auto [mem, offset] = getMemoryRegion(*handle.handle);
    //OpenCLEvent clEvent;
    //std::shared_ptr<void> memPtr;
    auto res = clQueue->clQueue.enqueueMapBuffer(mem, CL_MAP_READ,
                                    offset /* offset */, handle.lengthInBytes());

    //cerr << "transferToHostImpl: opName " << opName << " bytes " << handle.lengthInBytes() << endl;

    auto & memPtr = std::get<0>(res);
    auto & clEvent = std::get<1>(res);

    //cerr << "clEvent is " << clEvent.event.operator cl_event() << endl;

    //cerr << jsonEncode(clEvent.getInfo()) << endl;

    auto event = std::make_shared<OpenCLComputeEvent>(clEvent);
    auto promise = std::make_shared<std::promise<std::any>>();
    auto data = (char *)memPtr.get();

    auto cb = [handle, promise, data, memPtr] (const OpenCLEvent & event, auto status)
    {
        //cerr << "transferToHostImpl callback" << endl;
        if (status == OpenCLEventCommandExecutionStatus::ERROR)
            promise->set_exception(std::make_exception_ptr(MLDB::Exception("OpenCL error mapping host memory")));
        else {
            promise->set_value(FrozenMemoryRegion(memPtr, data, handle.lengthInBytes()));
        }
    };

    clEvent.addCallback(cb);

    static const bool bugAsyncMapBufferDoesntComplete = true;

    if (bugAsyncMapBufferDoesntComplete) {
        // TODO: hack, somehow callback isn't being called if we leave this async...
        clEvent.waitUntilFinished();
    }

    //cerr << jsonEncode(clEvent.getInfo()) << endl;

    return { promise, event };
}

FrozenMemoryRegion
OpenCLComputeContext::
transferToHostSyncImpl(const std::string & opName,
                       MemoryRegionHandle handle)
{
    auto op = scopedOperation("OpenCLComputeContext transferToHostSyncImpl " + opName);

    ExcAssert(handle.handle);

    auto [mem, offset] = getMemoryRegion(*handle.handle);
    auto memPtr = clQueue->clQueue.enqueueMapBufferBlocking(mem, CL_MAP_READ,
                                                            offset, handle.lengthInBytes());
    const char * data = (const char *)memPtr.get();
    FrozenMemoryRegion result(std::move(memPtr), data, handle.lengthInBytes());
    return result;
}

ComputePromiseT<MutableMemoryRegion>
OpenCLComputeContext::
transferToHostMutableImpl(const std::string & opName, MemoryRegionHandle handle)
{
    auto op = scopedOperation("OpenCLComputeContext transferToHostMutableImpl " + opName);
    ExcAssert(handle.handle);

    auto [mem, offset] = getMemoryRegion(*handle.handle);
    OpenCLEvent clEvent;
    std::shared_ptr<void> memPtr;
    std::tie(memPtr, clEvent)
        = clQueue->clQueue.enqueueMapBuffer(mem, CL_MAP_READ | CL_MAP_WRITE,
                                    offset, handle.lengthInBytes());

    auto event = std::make_shared<OpenCLComputeEvent>(std::move(clEvent));
    auto promise = std::shared_ptr<std::promise<std::any>>();

    auto cb = [handle, promise, memPtr] (const OpenCLEvent & event, auto status)
    {
        if (status == OpenCLEventCommandExecutionStatus::ERROR)
            promise->set_exception(std::make_exception_ptr(MLDB::Exception("OpenCL error mapping host memory")));
        else {
            promise->set_value(MutableMemoryRegion(memPtr, (char *)memPtr.get(), handle.lengthInBytes()));
        }
    };

    clEvent.addCallback(cb);

    return { std::move(promise), std::move(event) };
}

MutableMemoryRegion
OpenCLComputeContext::
transferToHostMutableSyncImpl(const std::string & opName,
                              MemoryRegionHandle handle)
{
    auto op = scopedOperation("OpenCLComputeContext transferToHostMutableSyncImpl " + opName);

    ExcAssert(handle.handle);

    auto [mem, offset] = getMemoryRegion(*handle.handle);
    auto memPtr = clQueue->clQueue.enqueueMapBufferBlocking(mem, CL_MAP_READ | CL_MAP_WRITE,
                                                            offset, handle.lengthInBytes());
    MutableMemoryRegion result(std::move(memPtr), (char *)memPtr.get(), handle.lengthInBytes());
    return result;
}

std::shared_ptr<ComputeKernel>
OpenCLComputeContext::
getKernel(const std::string & kernelName)
{
    auto op = scopedOperation("OpenCLComputeContext getKernel " + kernelName);

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

ComputePromiseT<MemoryRegionHandle>
OpenCLComputeContext::
managePinnedHostRegionImpl(const std::string & opName, std::span<const std::byte> region, size_t align,
                           const std::type_info & type, bool isConst)
{
    auto op = scopedOperation("OpenCLComputeContext managePinnedHostRegionImpl " + opName);

    auto result = managePinnedHostRegionSyncImpl(opName, region, align, type, isConst);
    return ComputePromiseT<MemoryRegionHandle>(std::move(result), clQueue->makeAlreadyResolvedEvent());
}

MemoryRegionHandle
OpenCLComputeContext::
managePinnedHostRegionSyncImpl(const std::string & opName,
                               std::span<const std::byte> region, size_t align,
                               const std::type_info & type, bool isConst)
{
    auto op = scopedOperation("OpenCLComputeContext managePinnedHostRegionSyncImpl " + opName);
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

    //using namespace std;
    //cerr << "transferring " << region.size() / 1000000.0 << " Mbytes of pinned type "
    //        << demangle(type.name()) << " isConst " << isConst << " to device in "
    //        << timer.elapsed_wall() << " at "
    //        << region.size() / 1000000.0 / timer.elapsed_wall() << "MB/sec" << endl;

    // TODO: this is synchronous; it should become asynchronous

    auto handle = std::make_shared<MemoryRegionInfo>();
    handle->memBase = std::move(mem);
    handle->offset = 0;
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

MemoryRegionHandle
OpenCLComputeContext::
getSliceImpl(const MemoryRegionHandle & handle, const std::string & regionName,
             size_t startOffsetInBytes, size_t lengthInBytes,
             size_t align, const std::type_info & type, bool isConst)
{
    auto op = scopedOperation("OpenCLComputeContext getSliceImpl " + regionName);

    auto info = std::dynamic_pointer_cast<const MemoryRegionInfo>(std::move(handle.handle));
    ExcAssert(info);

    if (info->isConst && !isConst) {
        throw MLDB::Exception("getSliceImpl: attempt to take a non-const slice of a const region");
    }

    if (*info->type != type) {
        throw MLDB::Exception("getSliceImpl: attempt to cast a slice");
    }

    if (startOffsetInBytes % align != 0) {
        throw MLDB::Exception("getSliceImpl: unaligned start offset");
    }

    if (lengthInBytes % align != 0) {
        throw MLDB::Exception("getSliceImpl: unaligned length");
    }

    if (startOffsetInBytes > info->lengthInBytes) {
        throw MLDB::Exception("getSliceImpl: start offset past the end");
    }

    if (startOffsetInBytes + lengthInBytes > info->lengthInBytes) {
        throw MLDB::Exception("getSliceImpl: end offset past the end");
    }

    auto newInfo = std::make_shared<MemoryRegionInfo>();

    newInfo->memBase = OpenCLMemObject(info->memBase, false /* already retained */);
    newInfo->offset = info->offset + startOffsetInBytes;
    newInfo->isConst = isConst;
    newInfo->type = &type;
    newInfo->name = regionName;
    newInfo->lengthInBytes = lengthInBytes;
    newInfo->parent = info;
    newInfo->ownerOffset = startOffsetInBytes;

    return { newInfo };

#if 0
    cl_buffer_region region = { startOffsetInBytes, lengthInBytes };
    cl_int error = CL_NONE;
    OpenCLMemObject mem(clCreateSubBuffer(info->mem, isConst ? CL_MEM_READ_ONLY : CL_MEM_READ_WRITE,
                        CL_BUFFER_CREATE_TYPE_REGION, &region, &error),
                        true /* already retained */);
    checkOpenCLError(error, "clCreateSubBuffer");

    newInfo->mem = std::move(mem);
    newInfo->isConst = isConst;
    newInfo->type = &type;
    newInfo->name = regionName;
    newInfo->lengthInBytes = lengthInBytes;
    newInfo->parent = info;
    newInfo->ownerOffset = startOffsetInBytes;

    return { newInfo };
#endif
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

    if (clTypeName == "ulong" || clTypeName == "uint64_t") {
        type = parseType("u64");
    }
    else if (clTypeName == "uint" || clTypeName == "uint32_t") {
        type = parseType("u32");
    }
    else if (clTypeName == "ushort" || clTypeName == "uint16_t") {
        type = parseType("u16");
    }
    else if (clTypeName == "uchar" || clTypeName == "uint8_t") {
        type = parseType("u8");
    }
    else if (clTypeName == "long" || clTypeName == "int64_t") {
        type = parseType("i64");
    }
    else if (clTypeName == "int" || clTypeName == "int32_t") {
        type = parseType("i32");
    }
    else if (clTypeName == "short" || clTypeName == "int16_t") {
        type = parseType("i16");
    }
    else if (clTypeName == "char" || clTypeName == "int8_t") {
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
setComputeFunction(OpenCLProgram programIn,
                   std::string kernelName,
                   std::vector<size_t> block)
{
    this->block = std::move(block);
    this->clProgram = std::move(programIn);
    this->kernelName = kernelName;
    this->clKernel = clProgram.createKernel(kernelName);
    this->clKernelInfo = this->clKernel.getInfo();

    //using namespace std;
    //cerr << jsonEncode(clKernelInfo) << endl;

    correspondingArgumentNumbers.resize(clKernelInfo.numArgs, -1);

    for (auto & arg: clKernelInfo.args) {
        if (arg.addressQualifier == OpenCLArgAddressQualifier::LOCAL) {
            if (this->setters.empty()) {
                throw MLDB::Exception("Local parameter in kernel with no setters defined; "
                                        "implement a setter to avoid launch failure");
            }
        }
        else {
            //cerr << "doing arg " << jsonEncodeStr(arg) << endl;
            auto [type, access] = getKernelType(arg);
            //cerr << "type = " << type.print() << endl;
            std::string argName = arg.name;
            auto it = paramIndex.find(argName);
            if (it == paramIndex.end()) {
                if (this->setters.size() > 0)
                    continue;  // should be done in the setter...
                throw MLDB::Exception("Kernel parameter " + std::to_string(arg.argNum)
                                        + " (" + argName + ") to OpenCL kernel " + kernelName
                                        + " has no counterpart in formal parameter list");
            }
            correspondingArgumentNumbers.at(arg.argNum) = it->second;
            auto & param = params.at(it->second);
            std::string reason;
            if (!type.isCompatibleWith(param.type)) {
                throw MLDB::Exception("Kernel parameter " + std::to_string(arg.argNum)
                                        + " (" + argName + ") to OpenCL kernel " + kernelName
                                        + ": declared parameter type " + type.print()
                                        + " is not compatible with kernel type " + param.type.print()
                                        + ": " + reason);
            }
        }
    }
}

BoundComputeKernel
OpenCLComputeKernel::
bindImpl(std::vector<ComputeKernelArgument> argumentsIn) const
{
    auto op = scopedOperation("OpenCLComputeKernel bindImpl " + kernelName);

    ExcAssert(this->context);
    auto & upcastContext = dynamic_cast<OpenCLComputeContext &>(*this->context);
    auto kernel = this->clProgram.createKernel(this->kernelName);

    auto bindInfo = std::make_shared<OpenCLBindInfo>();
    bindInfo->clKernel = kernel;
    bindInfo->owner = this;

    BoundComputeKernel result;
    result.arguments = std::move(argumentsIn);
    result.owner = this;
    result.bindInfo = bindInfo;

    for (auto & arg: result.arguments) {
        result.knowns.setValue(arg.name, arg.handler->toJson());
    }

    for (size_t i = 0;  i < this->clKernelInfo.args.size();  ++i) {
        auto tr = scopedOperation("bind arg " + std::to_string(i) + " " + this->clKernelInfo.args[i].name);
        int argNum = correspondingArgumentNumbers.at(i);
        //cerr << "binding OpenCL parameter " << i << " from argument " << paramNum << endl;
        if (argNum == -1) {
            // local, or will be done via setter...
        }
        else {
            const ComputeKernelArgument & arg = result.arguments.at(argNum);
            std::string opName = "bind " + this->clKernelInfo.args[i].name;
            if (arg.handler->canGetPrimitive()) {
                auto bytes = arg.handler->getPrimitive(opName, upcastContext);
                traceOperation("binding handle with " + std::to_string(bytes.size()) + " bytes");
                kernel.bindArg(i, bytes.data(), bytes.size());
            }
            else if (arg.handler->canGetHandle()) {
                auto handle = arg.handler->getHandle(opName, upcastContext);
                traceOperation("binding handle with " + std::to_string(handle.lengthInBytes()) + " bytes");
                auto [mem, offset] = upcastContext.getMemoryRegion(*handle.handle);
                ExcAssertEqual(offset, 0);  // need to get sub buffer for non-zero offset to work
                kernel.bindArg(i, mem);
            }
            else if (arg.handler->canGetConstRange()) {
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

    // Look through for constraints from dimensions
    for (size_t i = 0;  i < this->dims.size();  ++i) {
        result.addConstraint(dims[i].range, "==", "grid[" + std::to_string(i) + "].range",
                             "Constraint implied by variables named in dimension " + std::to_string(0));
    }

    // Look through for constraints from parameters
    for (auto & p: this->params) {
        if (!p.type.dims.empty() && p.type.dims[0].bound) {
            result.addConstraint(p.type.dims[0].bound, "==", p.name + ".length",
                                 "Constraint implied by array bounds of parameter " + p.name + ": "
                                 + p.type.print());
        }
    }

    bool progress = true;

    while (progress) {
        progress = false;
        for (auto & c: result.constraints) {
            progress = progress || c.attemptToSatisfy(result.knowns, result.unknowns);
        }
    }

    cerr << "got " << result.constraints.size() << " constraints" << endl;
    for (auto & c: result.constraints) {
        cerr << "  " << c.print()
             << (c.satisfied(result.knowns) ? " [SATSIFIED]" : " [UNSATISFIED]") << endl;
    }

    return result;
}


// OpenCLComputeRuntime

EnvOption<int> OPENCL_DEFAULT_PLATFORM("OPENCL_DEFAULT_PLATFORM", 0);
EnvOption<int> OPENCL_DEFAULT_DEVICE("OPENCL_DEFAULT_DEVICE", -1);

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

        if (OPENCL_DEFAULT_PLATFORM.specified() || OPENCL_DEFAULT_DEVICE.specified()) {
            return {ComputeRuntimeId::OPENCL,
                    (uint8_t)OPENCL_DEFAULT_PLATFORM.get(),
                    (uint16_t)OPENCL_DEFAULT_DEVICE.get(), 0, 0};
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
