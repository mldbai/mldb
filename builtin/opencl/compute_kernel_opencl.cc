/** compute_kernel_opencl.cc                                                -*- C++ -*-
    Jeremy Barnes, 27 March 2016
    This file is part of MLDB. Copyright 2016 mldb.ai inc. All rights reserved.

    Compute kernel runtime for CPU devices.
*/

#include "compute_kernel_opencl.h"
#include "mldb/types/basic_value_descriptions.h"
#include "mldb/types/meta_value_description.h"
#include "mldb/types/set_description.h"
#include "opencl_types.h"
#include "mldb/vfs/filter_streams.h"
#include "mldb/utils/environment.h"
#include "mldb/arch/ansi.h"
#include "mldb/block/zip_serializer.h"
#include "mldb/utils/command_expression_impl.h"
#include <compare>

using namespace std;

namespace MLDB {

namespace {

std::mutex libraryRegistryMutex;
struct LibraryRegistryEntry {
    std::function<std::shared_ptr<OpenCLComputeFunctionLibrary>(OpenCLComputeContext & context)> generate;
};

std::map<std::string, LibraryRegistryEntry> libraryRegistry;

EnvOption<bool, false> OPENCL_ENABLED("OPENCL_ENABLED", true);

struct OpenCLMemoryRegionHandleInfo: public GridMemoryRegionHandleInfo {
    OpenCLMemObject memBase;

    virtual OpenCLMemoryRegionHandleInfo * clone() const
    {
        return new OpenCLMemoryRegionHandleInfo(*this);
    }

    void init(OpenCLMemObject mem, size_t offset)
    {
        this->memBase = std::move(mem);
        this->offset = offset;
        this->version = 0;
    }

    std::tuple<std::shared_ptr<const void>, OpenCLMemObject>
    getOpenCLAccess(const std::string & opName, MemoryRegionAccess access)
    {
        return { pinAccess(opName, access), memBase };
    }
};

} // file scope

// OpenCLComputeProfilingInfo

OpenCLComputeProfilingInfo::
OpenCLComputeProfilingInfo(OpenCLProfilingInfo info)
    : clInfo(std::move(info))
{
}


// OpenCLComputeEvent

OpenCLComputeEvent::
OpenCLComputeEvent(const std::string & label, bool resolved,
                   const GridComputeQueue * owner, OpenCLEvent ev)
    : GridComputeEvent(label, resolved, owner), ev(std::move(ev))
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
        traceOpenCLOperation("await(): already satisfied");
        return;  // null event; already satisfied
    }

    auto tr = scopedOperation(OperationType::OPENCL_COMPUTE, "await()");
    return ev.waitUntilFinished();
}


// OpenCLComputeQueue

OpenCLComputeQueue::
OpenCLComputeQueue(OpenCLComputeContext * owner, OpenCLComputeQueue * parent,
                   const std::string & label,
                   OpenCLCommandQueue queue, GridDispatchType dispatchType)
    : GridComputeQueue(owner, parent, label, dispatchType), clOwner(owner), clQueue(std::move(queue))
{
    ExcAssert(clQueue);
}

std::shared_ptr<ComputeQueue>
OpenCLComputeQueue::
parallel(const std::string & opName)
{
    // TODO: not really done yet; need to inject prereqs on parent
    return std::make_shared<OpenCLComputeQueue>(this->clOwner, this, opName, clQueue, GridDispatchType::PARALLEL);
}

std::shared_ptr<ComputeQueue>
OpenCLComputeQueue::
serial(const std::string & opName)
{
    // TODO: not really done yet; need to inject prereqs on previous event
    return std::make_shared<OpenCLComputeQueue>(this->clOwner, this, opName, clQueue, GridDispatchType::SERIAL);
}

#if 0
void
OpenCLComputeQueue::
enqueue(const std::string & opName,
       const BoundComputeKernel & bound,
       const std::vector<uint32_t> & grid)
{
    try {
        auto tr = scopedOperation(OperationType::OPENCL_COMPUTE, "enqueue kernel " + bound.owner->kernelName + " as " + opName);

        ExcAssert(bound.bindInfo);
        
        const OpenCLBindInfo * bindInfo
            = dynamic_cast<const OpenCLBindInfo *>(bound.bindInfo.get());
        ExcAssert(bindInfo);

        const OpenCLComputeKernel * kernel = bindInfo->owner;

        auto knowns = bound.knowns;

        for (size_t i = 0;  i < grid.size();  ++i) {
            auto & dim = kernel->dims[i];
            knowns.setValue(dim.range, grid[i]);
        }

        std::vector<size_t> clGrid, clBlock;
        
        //if (kernel->allowGridExpansionFlag)
        //    ExcAssertLessEqual(grid.size(), clBlock.size());
        //else
        //    ExcAssertEqual(grid.size(), clBlock.size());

        if (clGrid.empty()) {
            clGrid.push_back(1);
        }
        if (clBlock.empty()) {
            clBlock.push_back(1);
        }

        if (kernel->modifyGrid)
            kernel->modifyGrid(clGrid, clBlock);
        
        knowns.setValue("grid", grid);
        knowns.setValue("clGrid", clGrid);
        knowns.setValue("clBlock", clBlock);

        std::function<Json::Value (const std::vector<Json::Value> &)>
        readArrayElement = [&] (const std::vector<Json::Value> & args)
        {
            if (args.size() != 2)
                throw MLDB::Exception("readArrayElement takes two arguments");

            const Json::Value & array = args[0];
            const Json::Value & index = args[1];
            const std::string & arrayName = array["name"].asString();
            auto version = array["version"].asInt();
            // find the array in our arguments
            // We have to do a scan, since we don't index by the memory region name (only the parameter name)

            cerr << "needle: " << arrayName << " v " << version << endl;
            for (auto & arg: bound.arguments) {
                if (!arg.handler)
                    continue;
                if (!arg.handler->canGetHandle())
                    continue;
                auto handle = arg.handler->getHandle("readArrayElement", *this);
                ExcAssert(handle.handle);
                
                cerr << "  haystack: " << handle.handle->name << " v " << handle.handle->version << endl;

                if (handle.handle->name != arrayName || handle.handle->version != version)
                    continue;

                auto i = index.asUInt();
                return arg.handler->getArrayElement(i, *this);
            }

            throw MLDB::Exception("Couldn't find array named '" + arrayName + "' in kernel arguments");
        };

        knowns.knowns.addFunction("readArrayElement", readArrayElement);

        // figure out the values of the new constraints
        knowns = solve(knowns, bound.preConstraints, bound.constraints);

        if (kernel->gridExpression) {
            clGrid = jsonDecode<decltype(clGrid)>(knowns.evaluate(*kernel->gridExpression));
            knowns.setValue("clGrid", clGrid);
        }

        if (kernel->blockExpression) {
            clBlock = jsonDecode<decltype(clBlock)>(knowns.evaluate(*kernel->blockExpression));
            knowns.setValue("clBlock", clBlock);
        }

        if (bindInfo->traceSerializer) {
            bindInfo->traceSerializer->newObject("grid", clGrid);
            bindInfo->traceSerializer->newObject("block", clBlock);
            bindInfo->traceSerializer->newObject("knowns", knowns);
            bindInfo->traceSerializer->commit();
        }

        // OpenCL needs us to pass the full extent of the grid, not just the number of blocks,
        // so we multiply it out here
        for (size_t i = 0;  i < clGrid.size();  ++i) {
            clGrid[i] *= clBlock.at(i);
        }

        auto event = clQueue.launch(bindInfo->clKernel, clGrid, clBlock);

        constexpr bool solveAfter = false;

        if (solveAfter) {
            knowns = solve(knowns, bound.constraints, bound.preConstraints);
        }
        //knowns = fullySolve(knowns, bound.constraints, bound.preConstraints);

    #if 0
        std::string kernelName = kernel->kernelName;

        auto execTimes = std::make_shared<std::map<OpenCLEventCommandExecutionStatus, double>>();

        auto doCallback = [this, kernelName, opName, execTimes, timer]
                (const OpenCLEvent & event, OpenCLEventCommandExecutionStatus status)
        {
            traceOpenCLOperation("completion callback " + opName + " with status " + jsonEncodeStr(status));
            auto wallTime = timer->elapsed_wall();

            // TODO: lock?
            //execTimes->emplace(status, timer->elapsed_wall());

            //std::string msg = format("kernel %s status %s wallTime %.2fms\n",
            //                         kernelName.c_str(), jsonEncodeStr(status).c_str(), wallTime * 1000.0);
            //cerr << msg;

            if (status != OpenCLEventCommandExecutionStatus::COMPLETE)
                return;
        
            if (false) {
                // If there is an exception, this can happen after we've been destroyed
                std::unique_lock guard(kernelWallTimesMutex);
                kernelWallTimes[kernelName] += wallTime * 1000.0;
                totalKernelTime += wallTime * 1000.0;
            }

            return;

            std::string toDump = "  submit    queue    start      end  elapsed name\n";

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
    #endif

        // DEBUG
    #if 0
        event.waitUntilFinished();
        auto wallTime = timer->elapsed_wall();

        {
            std::unique_lock guard(kernelWallTimesMutex);
            kernelWallTimes[bound.owner->kernelName] += wallTime * 1000.0;
            totalKernelTime += wallTime * 1000.0;
        }
    #endif
        //doCallback(event, 0);
    } MLDB_CATCH_ALL {
        rethrowException(400, "Error launching OpenCL kernel " + bound.owner->kernelName);
    }
}
#endif
#if 0
void
OpenCLComputeQueue::
enqueueFillArrayImpl(const std::string & opName,
                     MemoryRegionHandle region, MemoryRegionInitialization init,
                     size_t startOffsetInBytes, ssize_t lengthInBytes,
                     std::span<const std::byte> block)
{
    auto op = scopedOperation(OperationType::OPENCL_COMPUTE, "enqueueFillArrayImpl " + opName);

    if (startOffsetInBytes > region.lengthInBytes()) {
        throw MLDB::Exception("region is too long");
    }
    if (lengthInBytes == -1)
        lengthInBytes = region.lengthInBytes() - startOffsetInBytes;
    
    if (startOffsetInBytes + lengthInBytes > region.lengthInBytes()) {
        throw MLDB::Exception("overflowing memory region");
    }

    ComputeQueue::enqueueFillArrayImpl(opName, region, init, startOffsetInBytes, lengthInBytes, block);
}

void
OpenCLComputeQueue::
enqueueCopyFromHostImpl(const std::string & opName,
                        MemoryRegionHandle toRegion,
                        FrozenMemoryRegion fromRegion,
                        size_t deviceStartOffsetInBytes)
{
    auto op = scopedOperation(OperationType::OPENCL_COMPUTE, "OpenCLComputeQueue enqueueCopyFromHostImpl " + opName);

    ExcAssert(toRegion.handle);

    auto [pin, mem, offset] = OpenCLComputeContext::getMemoryRegion(opName, *toRegion.handle, ACC_WRITE);
    clQueue.enqueueWriteBuffer(mem, offset, fromRegion.length(), fromRegion.data());
}

void
OpenCLComputeQueue::
copyFromHostSyncImpl(const std::string & opName,
                            MemoryRegionHandle toRegion,
                            FrozenMemoryRegion fromRegion,
                            size_t deviceStartOffsetInBytes)
{
    auto op = scopedOperation(OperationType::OPENCL_COMPUTE, "OpenCLComputeQueue copyFromHostSyncImpl " + opName);

    ExcAssert(toRegion.handle);

    auto [pin, mem, offset] = OpenCLComputeContext::getMemoryRegion(opName, *toRegion.handle, ACC_WRITE);
    clQueue.enqueueWriteBuffer(mem, offset, fromRegion.length(), fromRegion.data()).waitUntilFinished();
}
#endif

FrozenMemoryRegion
OpenCLComputeQueue::
enqueueTransferToHostImpl(const std::string & opName,
                          MemoryRegionHandle handle)
{
    auto op = scopedOperation(OperationType::OPENCL_COMPUTE, "OpenCLComputeQueue enqueueTransferToHostImpl " + opName);

    ExcAssert(handle.handle);

    auto [pin, mem, offset] = OpenCLComputeContext::getMemoryRegion(opName, *handle.handle, ACC_READ);
    //OpenCLEvent clEvent;
    //std::shared_ptr<void> memPtr;
    auto res = clQueue.enqueueMapBuffer(mem, CL_MAP_READ,
                                        offset /* offset */, handle.lengthInBytes());

    //cerr << "transferToHostImpl: opName " << opName << " bytes " << handle.lengthInBytes() << endl;

    auto & memPtr = std::get<0>(res);
    auto & clEvent = std::get<1>(res);

    static const bool bugAsyncMapBufferDoesntComplete = true;

    if (bugAsyncMapBufferDoesntComplete) {
        // TODO: hack, somehow callback isn't being called if we leave this async...
        clEvent.waitUntilFinished();
    }

    return FrozenMemoryRegion(memPtr, (const char *)memPtr.get(), handle.lengthInBytes());
}

FrozenMemoryRegion
OpenCLComputeQueue::
transferToHostSyncImpl(const std::string & opName,
                       MemoryRegionHandle handle)
{
    auto op = scopedOperation(OperationType::OPENCL_COMPUTE, "OpenCLComputeQueue transferToHostSyncImpl " + opName);

    ExcAssert(handle.handle);

    auto [pin, mem, offset] = OpenCLComputeContext::getMemoryRegion(opName, *handle.handle, ACC_READ);
    finish();  // TODO: instead of blocking here, put the last submitted command as a prereq
    auto memPtr = clQueue.enqueueMapBufferBlocking(mem, CL_MAP_READ,
                                                   offset, handle.lengthInBytes());
    const char * data = (const char *)memPtr.get();
    FrozenMemoryRegion result(std::move(memPtr), data, handle.lengthInBytes());
    return result;
}

MutableMemoryRegion
OpenCLComputeQueue::
enqueueTransferToHostMutableImpl(const std::string & opName, MemoryRegionHandle handle)
{
    auto op = scopedOperation(OperationType::OPENCL_COMPUTE, "OpenCLComputeQueue enqueueTransferToHostMutableImpl " + opName);
    ExcAssert(handle.handle);

    auto [pin, mem, offset] = OpenCLComputeContext::getMemoryRegion(opName, *handle.handle, ACC_READ_WRITE);
    OpenCLEvent clEvent;
    std::shared_ptr<void> memPtr;
    std::tie(memPtr, clEvent)
        = clQueue.enqueueMapBuffer(mem, CL_MAP_READ | CL_MAP_WRITE,
                                    offset, handle.lengthInBytes());

    return MutableMemoryRegion(memPtr, (char *)memPtr.get(), handle.lengthInBytes());
}

MutableMemoryRegion
OpenCLComputeQueue::
transferToHostMutableSyncImpl(const std::string & opName,
                              MemoryRegionHandle handle)
{
    auto op = scopedOperation(OperationType::OPENCL_COMPUTE, "OpenCLComputeQueue transferToHostMutableSyncImpl " + opName);

    ExcAssert(handle.handle);

    auto [pin, mem, offset] = OpenCLComputeContext::getMemoryRegion(opName, *handle.handle, ACC_READ_WRITE);
    auto memPtr = clQueue.enqueueMapBufferBlocking(mem, CL_MAP_READ | CL_MAP_WRITE,
                                                            offset, handle.lengthInBytes());
    MutableMemoryRegion result(std::move(memPtr), (char *)memPtr.get(), handle.lengthInBytes());
    return result;
}

MemoryRegionHandle
OpenCLComputeQueue::
enqueueManagePinnedHostRegionImpl(const std::string & opName, std::span<const std::byte> region, size_t align,
                           const std::type_info & type, bool isConst)
{
    auto op = scopedOperation(OperationType::OPENCL_COMPUTE, "OpenCLComputeContext managePinnedHostRegionImpl " + opName);

    return managePinnedHostRegionSyncImpl(opName, region, align, type, isConst);
}

MemoryRegionHandle
OpenCLComputeQueue::
managePinnedHostRegionSyncImpl(const std::string & opName,
                               std::span<const std::byte> region, size_t align,
                               const std::type_info & type, bool isConst)
{
    auto op = scopedOperation(OperationType::OPENCL_COMPUTE, "OpenCLComputeContext managePinnedHostRegionSyncImpl " + opName);
    Timer timer;
    OpenCLMemObject mem;
    if (region.size() == 0) {
        // Create a valid pointer, which means non-zero length
        mem = clOwner->clContext.createBuffer(CL_MEM_READ_ONLY, 4 /* size */);
    }
    else {
        if (isConst) {
            mem = clOwner->clContext.createBuffer(CL_MEM_READ_ONLY,
                                         region.data(), region.size());
        } else {
            mem = clOwner->clContext.createBuffer(CL_MEM_READ_WRITE,
                                         (void *)region.data(), region.size());
        }
    }

    //using namespace std;
    //cerr << "transferring " << region.size() / 1000000.0 << " Mbytes of pinned type "
    //        << demangle(type.name()) << " isConst " << isConst << " to device in "
    //        << timer.elapsed_wall() << " at "
    //        << region.size() / 1000000.0 / timer.elapsed_wall() << "MB/sec" << endl;

    // TODO: this is synchronous; it should become asynchronous

    auto handle = std::make_shared<OpenCLMemoryRegionHandleInfo>();
    handle->memBase = std::move(mem);
    handle->offset = 0;
    handle->type = &type;
    handle->isConst = isConst;
    handle->lengthInBytes = region.size();
    handle->version = 0;
    handle->name = opName;
    MemoryRegionHandle result{std::move(handle)};
    return result;
}

std::shared_ptr<ComputeEvent>
OpenCLComputeQueue::
doEnqueueCopyBetweenDeviceRegionsImpl(const std::string & opName,
                                      MemoryRegionHandle from, MemoryRegionHandle to,
                                      size_t fromOffset, size_t toOffset,
                                      size_t length)
{
    auto [fromPin, fromMem, fromBaseOffset] = OpenCLComputeContext::getMemoryRegion(opName, *from.handle, ACC_READ);
    auto [toPin, toMem, toBaseOffset] = OpenCLComputeContext::getMemoryRegion(opName, *to.handle, ACC_WRITE);

    auto event = this->clQueue.enqueueCopyBuffer(fromMem, toMem, fromBaseOffset + fromOffset, toBaseOffset + toOffset, length);
    return std::make_shared<OpenCLComputeEvent>(opName, false /* resolved */, this, std::move(event));
}

void
OpenCLComputeQueue::
enqueueCopyBetweenDeviceRegionsImpl(const std::string & opName,
                                MemoryRegionHandle from, MemoryRegionHandle to,
                                size_t fromOffset, size_t toOffset,
                                size_t length)
{
    doEnqueueCopyBetweenDeviceRegionsImpl(opName, from, to, fromOffset, toOffset, length);
}

void
OpenCLComputeQueue::
copyBetweenDeviceRegionsSyncImpl(const std::string & opName,
                                    MemoryRegionHandle from, MemoryRegionHandle to,
                                    size_t fromOffset, size_t toOffset,
                                    size_t length)
{
    doEnqueueCopyBetweenDeviceRegionsImpl(opName, from, to, fromOffset, toOffset, length)->await();
}

void
OpenCLComputeQueue::
enqueueBarrier(const std::string & label)
{
    auto op = scopedOperation(OperationType::OPENCL_COMPUTE, "OpenCLComputeQueue enqueueBarrier");
    // TODO: barrier should wait for the last event...
    clQueue.enqueueBarrier({});
}

std::shared_ptr<ComputeEvent>
OpenCLComputeQueue::
flush()
{
    auto op = scopedOperation(OperationType::OPENCL_COMPUTE, "OpenCLComputeQueue flush");
    // TODO: barrier should wait for the last event...
    auto ev = clQueue.enqueueBarrier({});
    clQueue.flush();
    return std::make_shared<OpenCLComputeEvent>("flush", false /* resolved */, this /* owner */, ev);
}

void
OpenCLComputeQueue::
finish()
{
    auto op = scopedOperation(OperationType::OPENCL_COMPUTE, "OpenCLComputeQueue finish");
    clQueue.finish();
}

std::shared_ptr<ComputeEvent>
OpenCLComputeQueue::
makeAlreadyResolvedEvent(const std::string & label) const
{
    return std::make_shared<OpenCLComputeEvent>(label, true /* resolved */, this /* owner */, OpenCLEvent() /* ev */);
}

static bool isValidOpenCLFillLength(size_t length)
{
    static const auto validLengths = set{1,2,4,8};
    return length % 4 == 0 && validLengths.count(length / 4);
}

void
OpenCLComputeQueue::
enqueueZeroFillArrayConcrete(const std::string & opName,
                                MemoryRegionHandle region,
                                size_t startOffsetInBytes, ssize_t lengthInBytes)
{

    if (lengthInBytes % 4 == 0 && startOffsetInBytes % 4 == 0 && lengthInBytes % 4 == 0) {
        auto [pin, buffer, offset] = OpenCLComputeContext::getMemoryRegion(opName, *region.handle, ACC_WRITE);
        ExcAssertEqual(lengthInBytes % 4, 0);
        ExcAssertEqual(startOffsetInBytes % 4, 0);
        ExcAssertEqual(offset % 4, 0);
        clQueue.enqueueFillBuffer<int>(buffer, 0, offset + startOffsetInBytes, lengthInBytes);
        return;
    }

    // Fall back to generic kernel
    ComputeQueue::enqueueFillArrayImpl(opName, region, MemoryRegionInitialization::INIT_ZERO_FILLED, startOffsetInBytes, lengthInBytes, {});
}                                
                                
void
OpenCLComputeQueue::
enqueueBlockFillArrayConcrete(const std::string & opName,
                              MemoryRegionHandle region,
                              size_t startOffsetInBytes, ssize_t lengthInBytes,
                              std::span<const std::byte> block)
{
    if (lengthInBytes % 4 == 0 && startOffsetInBytes % 4 == 0 && lengthInBytes % 4 == 0
        && isValidOpenCLFillLength(block.size())) {
        auto [pin, buffer, offset] = OpenCLComputeContext::getMemoryRegion(opName, *region.handle, ACC_WRITE);
        clQueue.enqueueFillBuffer(buffer, block.data(), block.size(), offset + startOffsetInBytes, lengthInBytes);
        return;
    }

    // Fall back to generic kernel
    ComputeQueue::enqueueFillArrayImpl(opName, region, MemoryRegionInitialization::INIT_BLOCK_FILLED, startOffsetInBytes, lengthInBytes, block);
}                                
                                
void
OpenCLComputeQueue::
enqueueCopyFromHostConcrete(const std::string & opName,
                            MemoryRegionHandle toRegion,
                            FrozenMemoryRegion fromRegion,
                            size_t deviceStartOffsetInBytes)
{
    auto op = scopedOperation(OperationType::OPENCL_COMPUTE, "OpenCLComputeQueue enqueueCopyFromHostImpl " + opName);

    ExcAssert(toRegion.handle);

    auto [pin, mem, offset] = OpenCLComputeContext::getMemoryRegion(opName, *toRegion.handle, ACC_WRITE);
    clQueue.enqueueWriteBuffer(mem, offset, fromRegion.length(), fromRegion.data());
}                            

FrozenMemoryRegion
OpenCLComputeQueue::
enqueueTransferToHostConcrete(const std::string & opName, MemoryRegionHandle handle)
{
    MLDB_THROW_UNIMPLEMENTED();
}

FrozenMemoryRegion
OpenCLComputeQueue::
transferToHostSyncConcrete(const std::string & opName, MemoryRegionHandle handle)
{
    MLDB_THROW_UNIMPLEMENTED();
}

struct OpenCLBindContext: public GridBindContext {
    OpenCLBindContext(OpenCLComputeQueue * queue,
                     const std::string & opName,
                     const GridComputeKernel * kernel,
                     const GridBindInfo * bindInfo)
        : kernel(dynamic_cast<const OpenCLComputeKernel *>(kernel)),
          queue(queue), opName(opName)
    {
        ExcAssert(this->kernel);

        clKernel = this->kernel->clFunction->generateKernel();
    }

    virtual ~OpenCLBindContext()
    {
    }

    const OpenCLComputeKernel * kernel = nullptr;
    OpenCLComputeQueue * queue = nullptr;
    std::string opName;
    OpenCLKernel clKernel;
    
    virtual void
    setPrimitive(const std::string & opName, int argNum, std::span<const std::byte> bytes) override
    {
        traceOpenCLOperation("setPrimitive " + opName, "argNum %d with %zd bytes", argNum, bytes.size_bytes());
        clKernel.bindArg(argNum, bytes.data(), bytes.size());
    }

    virtual void
    setBuffer(const std::string & opName, int argNum,
              std::shared_ptr<GridMemoryRegionHandleInfo> handle,
              MemoryRegionAccess access) override
    {
        traceOpenCLOperation("setBuffer " + opName, "argNum %d handle %s:%d with %zd bytes",
                            argNum, handle->name.c_str(), handle->version, handle->lengthInBytes);
        ExcAssert(handle);
        auto [pin, memBuffer, offset] = OpenCLComputeContext::getMemoryRegion(opName, *handle, access);
        // TODO: where do we put the pin?
        ExcAssertEqual(offset, 0);  // OpenCL doesn't allow us to specify an offset
        clKernel.bindArg(argNum, memBuffer);
    }

    virtual void
    setThreadGroupMemory(const std::string & opName, int argNum, size_t nBytes) override
    {
        traceOpenCLOperation("setPrimitive " + opName, "argNum %d with %zd bytes", argNum, nBytes);
        // Set it up in the command encoder
        clKernel.bindArg(argNum, LocalArray<uint32_t>((nBytes + 3) / 4));
    }

    virtual void launch(const std::string & opName,
                        GridBindContext & context, std::vector<size_t> grid,
                        std::vector<size_t> block) override
    {
        OpenCLBindContext & bindContext = dynamic_cast<OpenCLBindContext &>(context);
        auto * kernel = bindContext.kernel;

        try {
            auto op = scopedOperation(OperationType::OPENCL_COMPUTE, "launch kernel " + opName);
            grid.resize(3, 1);
            block.resize(3, 1);

            grid[0] *= block[0];
            grid[1] *= block[1];
            grid[2] *= block[2];

            auto invocations = grid[0] * grid[1] * grid[2];
            double memoryRequiredMb = invocations * 2048 / 1024.0 / 1024.0;

            traceOpenCLOperation("grid size " + jsonEncodeStr(grid));
            traceOpenCLOperation("block size " + jsonEncodeStr(block));
            traceOpenCLOperation(std::to_string(invocations) + " invocations requiring " + std::to_string(memoryRequiredMb)
                                + "MB of wired scratchpad memory");


            auto event = queue->clQueue.launch(clKernel, grid, block);
        } MLDB_CATCH_ALL {
            rethrowException(400, "Error launching OpenCL kernel " + kernel->kernelName);
        }
    }
};

std::shared_ptr<GridBindContext>
OpenCLComputeQueue::
newBindContext(const std::string & opName,
               const GridComputeKernel * kernel,
               const GridBindInfo * bindInfo)
{
    ExcAssert(bindInfo);
    return std::make_shared<OpenCLBindContext>(this, opName, kernel, bindInfo);
}


// OpenCLComputeContext

OpenCLComputeContext::
OpenCLComputeContext(OpenCLDevice clDevice, ComputeDevice device)
    : GridComputeContext(device),
      clContext(clDevice), clDevice(clDevice), device(device)
{
}

ComputeDevice
OpenCLComputeContext::
getDevice() const
{
    return device;
}

std::tuple<std::shared_ptr<const void>, cl_mem, size_t>
OpenCLComputeContext::
getMemoryRegion(const std::string & opName, MemoryRegionHandleInfo & handle, MemoryRegionAccess access)
{
    OpenCLMemoryRegionHandleInfo * upcastHandle = dynamic_cast<OpenCLMemoryRegionHandleInfo *>(&handle);
    if (!upcastHandle) {
        throw MLDB::Exception("TODO: get memory region from block handled from elsewhere: got " + demangle(typeid(handle)));
    }
    auto [pin, mem] = upcastHandle->getOpenCLAccess(opName, access);

    return { std::move(pin), mem, upcastHandle->offset };
}

std::tuple<FrozenMemoryRegion, int /* version */>
OpenCLComputeContext::
getFrozenHostMemoryRegion(const std::string & opName, MemoryRegionHandleInfo & handle,
                          size_t offset, ssize_t length,
                          bool ignoreHazards) const
{
    OpenCLMemoryRegionHandleInfo * upcastHandle = dynamic_cast<OpenCLMemoryRegionHandleInfo *>(&handle);
    if (!upcastHandle) {
        throw MLDB::Exception("TODO: get memory region from block handled from elsewhere: got " + demangle(typeid(handle)));
    }
    return upcastHandle->getReadOnlyHostAccessSync(*this, opName, offset, length, ignoreHazards);
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

    auto handle = std::make_shared<OpenCLMemoryRegionHandleInfo>();
    handle->memBase = std::move(mem);
    handle->offset = 0;
    handle->type = &type;
    handle->isConst = isConst;
    handle->lengthInBytes = length;
    handle->name = regionName;
    handle->version = 0;

    MemoryRegionHandle result{std::move(handle)};
    return result;
}

MemoryRegionHandle
OpenCLComputeContext::
allocateSyncImpl(const std::string & regionName,
                 size_t length, size_t align,
                 const std::type_info & type, bool isConst)
{
    auto op = scopedOperation(OperationType::OPENCL_COMPUTE, "OpenCLComputeContext allocateSyncImpl " + regionName);
    auto result = doOpenCLAllocate(clContext, regionName, length, align, type, isConst);
    return result;
}

#if 0
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

    auto handle = std::make_shared<OpenCLMemoryRegionHandleInfo>();
    handle->memBase = std::move(mem);
    handle->offset = 0;
    handle->type = &type;
    handle->isConst = isConst;
    handle->lengthInBytes = region.length();
    handle->name = opName;
    handle->version = 0;
    MemoryRegionHandle result{std::move(handle)};

    //using namespace std;
    //cerr << "transferring " << region.memusage() / 1000000.0 << " Mbytes of type "
    //        << demangle(type.name()) << " isConst " << isConst << " to device in "
    //        << timer.elapsed_wall() << " at "
    //        << region.memusage() / 1000000.0 / timer.elapsed_wall() << "MB/sec" << endl;

    return result;
}

MemoryRegionHandle
OpenCLComputeContext::
transferToDeviceImpl(const std::string & opName, FrozenMemoryRegion region,
                     const std::type_info & type, bool isConst)
{
    auto op = scopedOperation(OperationType::OPENCL_COMPUTE, "OpenCLComputeContext transferToDeviceImpl " + opName);
    auto result = doOpenCLTransferToDevice(clContext, opName, region, type, isConst);
    return {std::move(result), std::make_shared<OpenCLComputeEvent>()};
}

MemoryRegionHandle
OpenCLComputeContext::
transferToDeviceSyncImpl(const std::string & opName,
                         FrozenMemoryRegion region,
                         const std::type_info & type, bool isConst)
{
    auto op = scopedOperation(OperationType::OPENCL_COMPUTE, "OpenCLComputeContext transferToDeviceSyncImpl " + opName);
    auto result = doOpenCLTransferToDevice(clContext, opName, region, type, isConst);
    return result;
}

FrozenMemoryRegion
OpenCLComputeContext::
transferToHostImpl(const std::string & opName, MemoryRegionHandle handle)
{
    auto op = scopedOperation(OperationType::OPENCL_COMPUTE, "OpenCLComputeContext transferToHostImpl " + opName);
    return clQueue->enqueueTransferToHostImpl(opName, std::move(handle));
}

FrozenMemoryRegion
OpenCLComputeContext::
transferToHostSyncImpl(const std::string & opName,
                       MemoryRegionHandle handle)
{
    auto op = scopedOperation(OperationType::OPENCL_COMPUTE, "OpenCLComputeContext transferToHostSyncImpl " + opName);
    return clQueue->transferToHostSyncImpl(opName, std::move(handle));
}

#endif

#if 0
std::shared_ptr<ComputeKernel>
OpenCLComputeContext::
getKernel(const std::string & kernelName)
{
    auto op = scopedOperation(OperationType::OPENCL_COMPUTE, "OpenCLComputeContext getKernel " + kernelName);

    std::unique_lock guard(kernelRegistryMutex);
    auto it = kernelRegistry.find(kernelName);
    if (it == kernelRegistry.end()) {
        throw AnnotatedException(400, "Unable to find OpenCL compute kernel '" + kernelName + "'",
                                        "kernelName", kernelName);
    }
    auto result = it->second.generate(*this);
    result->context = this;
    if (traceSerializer) {
        result->traceSerializer = kernelsSerializer->newStructure(kernelName);
        result->runsSerializer = result->traceSerializer->newStructure("runs");
    }
    return result;
}
#endif

std::shared_ptr<ComputeQueue>
OpenCLComputeContext::
getQueue(const std::string & queueName)
{
    auto queue = clContext.createCommandQueue(clDevice, OpenCLCommandQueueProperties::PROFILING_ENABLE);
    return std::make_shared<OpenCLComputeQueue>(this, nullptr, queueName, std::move(queue), GridDispatchType::SERIAL);
}

MemoryRegionHandle
OpenCLComputeContext::
getSliceImpl(const MemoryRegionHandle & handle, const std::string & regionName,
             size_t startOffsetInBytes, size_t lengthInBytes,
             size_t align, const std::type_info & type, bool isConst)
{
    return GridComputeContext::getSliceImpl(handle, regionName, startOffsetInBytes, lengthInBytes, align, type, isConst);
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

std::shared_ptr<GridComputeFunctionLibrary>
OpenCLComputeContext::
getLibrary(const std::string & libraryName)
{
    auto op = scopedOperation(OperationType::OPENCL_COMPUTE, "OpenCLComputeContext getLibrary " + libraryName);

    std::unique_lock guard(libraryRegistryMutex);
    auto it = libraryRegistry.find(libraryName);
    if (it == libraryRegistry.end()) {
        throw AnnotatedException(400, "Unable to find OpenCL compute library '" + libraryName + "'",
                                        "libraryName", libraryName);
    }
    auto result = it->second.generate(*this);
    //if (traceSerializer) {
    //    result->traceSerializer = kernelsSerializer->newStructure(kernelName);
    //    result->runsSerializer = result->traceSerializer->newStructure("runs");
    //}
    return result;
}

std::shared_ptr<GridComputeKernelSpecialization>
OpenCLComputeContext::
specializeKernel(const GridComputeKernelTemplate & tmplate)
{
    return std::make_shared<OpenCLComputeKernel>(this, tmplate);
}


// OpenCLComputeKernel

// Parses an OpenCL kernel argument info structure, and turns it into a ComputeKernel type
ComputeKernelType
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
    auto parseType = [] (const char * type)
    {
        return MLDB::parseType("", type);
    };

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
    else type = parseType(clTypeName.c_str());  // Must be a user defined type

    // Add back the dimensions
    for (size_t i = 0;  i < arrayDim;  ++i) {
        type.dims.push_back({false, nullptr});
    }

    type.access = isConst ? ACC_READ : ACC_READ_WRITE;

    return type;
}

#if 0
void
OpenCLComputeKernel::
setComputeFunction(OpenCLProgram programIn,
                   std::string kernelName)
{
    this->clProgram = std::move(programIn);
    this->kernelName = kernelName;
    this->clKernel = clProgram.createKernel(kernelName);
    this->clKernelInfo = this->clKernel.getInfo();

    //using namespace std;
    //cerr << jsonEncode(clKernelInfo) << endl;

    correspondingArgumentNumbers.resize(clKernelInfo.numArgs, -1);

    for (auto & arg: clKernelInfo.args) {
        //cerr << "doing arg " << jsonEncodeStr(arg) << endl;
        auto type = getKernelType(arg);
        //cerr << "type = " << type.print() << endl;
        std::string argName = arg.name;
        auto it = paramIndex.find(argName);
        if (it == paramIndex.end()) {
            continue;  // Parameter is probably referenced in constraints...
            if (this->setters.size() > 0)
                continue;  // should be done in the setter...
            if (arg.addressQualifier == OpenCLArgAddressQualifier::LOCAL) {
                throw MLDB::Exception("Local parameter in kernel with no setters defined; "
                                        "implement a setter to avoid launch failure");
            }
            throw MLDB::Exception("Kernel parameter " + std::to_string(arg.argNum)
                                    + " (" + argName + ") to OpenCL kernel " + kernelName
                                    + " has no counterpart in formal parameter list");
        }
        correspondingArgumentNumbers.at(arg.argNum) = it->second;
        auto & param = params.at(it->second);
        std::string reason;
        if (!param.type.isCompatibleWith(type, &reason)) {
            throw MLDB::Exception("Kernel parameter " + std::to_string(arg.argNum)
                                    + " (" + argName + ") to OpenCL kernel " + kernelName
                                    + ": declared parameter type " + type.print()
                                    + " is not compatible with kernel type " + param.type.print()
                                    + ": " + reason);
        }
    }
}
#endif

#if 0
BoundComputeKernel
OpenCLComputeKernel::
bindImpl(ComputeQueue & queue,
         std::vector<ComputeKernelArgument> argumentsIn, ComputeKernelConstraintSolution knowns) const
{
    try {
        auto op = scopedOperation(OperationType::OPENCL_COMPUTE, "OpenCLComputeKernel bindImpl " + kernelName);

        ExcAssert(this->context);
        auto & upcastContext = dynamic_cast<OpenCLComputeContext &>(*this->context);
        auto kernel = this->clProgram.createKernel(this->kernelName);

        auto bindInfo = std::make_shared<OpenCLBindInfo>();
        bindInfo->clKernel = kernel;
        bindInfo->owner = this;
        bindInfo->clContext = &upcastContext;

        if (traceSerializer) {
            int callNumber = numCalls++;
            bindInfo->traceSerializer = runsSerializer->newStructure(callNumber);

            if (callNumber == 0) {
                auto key = format("%016x", (uint64_t)this->clProgram.operator cl_program());

                traceSerializer->newObject("program", key);
                traceSerializer->newObject("kernel", this->kernelName);

                // Store the program so that we can replay it later
                std::unique_lock guard(tracedProgramMutex);
                if (tracedPrograms.insert(key).second) {
                    auto programInfo = this->clProgram.getProgramInfo();
                    auto buildInfo = this->clProgram.getProgramBuildInfo(this->clKernel.getContext().getDevices().at(0));
                    auto entry = programsSerializer->newStructure(key);
                    entry->newStream("source") << programInfo.source;
                    entry->newObject("build", buildInfo);
                }
            }
        }

        BoundComputeKernel result;
        result.arguments = std::move(argumentsIn);
        result.owner = this;
        result.bindInfo = bindInfo;

        // Copy constraints over
        result.constraints = this->constraints;

        for (auto & arg: result.arguments) {
            if (arg.handler) {
                // Was set by the caller
                result.knowns.setValue(arg.name, arg.handler->toJson());
            }
        }
        for (auto & [name, value]: tuneables) {
            result.knowns.setValue(name, value);
        }

        Json::Value argInfo;
        for (size_t i = 0;  i < this->clKernelInfo.args.size();  ++i) {
            std::string opName = "bind arg " + std::to_string(i) + " " + this->clKernelInfo.args[i].name;
            auto tr = scopedOperation(OperationType::OPENCL_COMPUTE, opName);
            int argNum = correspondingArgumentNumbers.at(i);
            //cerr << "binding OpenCL parameter " << i << " from argument " << paramNum << endl;
            if (argNum == -1) {
                // Will be done via setter...
                continue;
            }

            auto & paramType = params[argNum].type;

            // Handle an argument that wasn't passed (ie, an implicit argument)
            auto handleImplicit = [&] ()
            {
                if (this->clKernelInfo.args[i].addressQualifier == OpenCLArgAddressQualifier::LOCAL) {
                    ExcAssertEqual(paramType.dims.size(), 1);
                    ExcAssert(paramType.dims[0].bound);
                    auto len = result.knowns.evaluate(*paramType.dims[0].bound).asUInt();
                    size_t nbytes = len * paramType.baseType->width;
                    traceOpenCLOperation("binding local array handle with " + std::to_string(nbytes) + " bytes");
                    kernel.bindArg(i, LocalArray<std::byte>(nbytes));
                    Json::Value known;
                    known["elAlign"] = paramType.baseType->align;
                    known["elWidth"] = paramType.baseType->width;
                    known["elType"] = paramType.baseType->typeName;
                    known["length"] = len;
                    known["type"] = OpenCLComputeKernel::getKernelType(this->clKernelInfo.args[i]).print();
                    known["name"] = "<<<Local array>>>";
                    result.knowns.setValue(this->clKernelInfo.args[i].name, std::move(known));
                }
                else if (this->clKernelInfo.args[i].addressQualifier == OpenCLArgAddressQualifier::PRIVATE) {
                    auto type = OpenCLComputeKernel::getKernelType(this->clKernelInfo.args[i]);
                    auto val = result.knowns.getValue(this->clKernelInfo.args[i].name);
                    traceOpenCLOperation("binding known value " + this->clKernelInfo.args[i].name + " = " + val.toStringNoNewLine() + " as " + type.print());
                    Any any(val, type.baseType.get());
                    auto bytes = any.asBytes();
                    kernel.bindArg(i, bytes.data(), bytes.size_bytes());
                }
            };

            if (argNum >= result.arguments.size()) {
                handleImplicit();
            }
            else {
                //cerr << "argNum = " << argNum << endl;
                //cerr << "result.arguments.size() = " << result.arguments.size() << endl;
                const ComputeKernelArgument & arg = result.arguments.at(argNum);
                if (traceSerializer) {
                    Json::Value thisArgInfo;
                    if (arg.handler)
                        thisArgInfo["value"] = arg.handler->toJson();
                    thisArgInfo["spec"] = paramType.print();
                    static auto vdd = getValueDescriptionDescription(true /* detailed */);
                    thisArgInfo["type"] = vdd->printJsonStructured(paramType.baseType);
                    thisArgInfo["aliases"] = jsonEncode(getValueDescriptionAliases(*paramType.baseType->type));
                    argInfo[this->clKernelInfo.args[i].name] = thisArgInfo;
                }

                static std::atomic<int> disamb = 0;
                std::string opName = "bind " + this->clKernelInfo.args[i].name + std::to_string(++disamb);
                if (!arg.handler) {
                    handleImplicit();
                }
                else if (arg.handler->canGetPrimitive()) {
                    auto bytes = arg.handler->getPrimitive(opName, queue);
                    traceOpenCLOperation("binding handle with " + std::to_string(bytes.size()) + " bytes");
                    kernel.bindArg(i, bytes.data(), bytes.size());
                }
                else if (arg.handler->canGetHandle()) {
                    auto handle = arg.handler->getHandle(opName, queue);
                    traceOpenCLOperation("binding handle with " + std::to_string(handle.lengthInBytes()) + " bytes");
                    MemoryRegionAccess access = this->params.at(argNum).type.access;

                    if (traceSerializer && (access & ACC_READ)) {
                        auto [region, version] = upcastContext
                            .getFrozenHostMemoryRegion(opName + " trace", *handle.handle, 0 /* offset */, -1,
                                                    true /* ignore hazards */);
                        //bindInfo->traceSerializer->addRegion(region, this->clKernelInfo.args[i].name);
                        traceVersion(handle.handle->name, version, region);
                    }

                    auto [pin, mem, offset] = upcastContext.getMemoryRegion(opName, *handle.handle, access);
                    bindInfo->argumentPins.emplace_back(std::move(pin));
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
            result.addConstraint(dims[i].range, "==", "grid[" + std::to_string(i) + "]",
                                "Constraint implied by variables named in dimension " + std::to_string(0));
        }

        // Look through for constraints from parameters
        for (auto & p: this->params) {
            if (!p.type.dims.empty() && p.type.dims[0].bound) {
                result.addConstraint(p.type.dims[0].bound, p.type.dims[0].tight ? "==" : "<=", p.name + ".length",
                                    "Constraint implied by array bounds of parameter " + p.name + ": "
                                    + p.type.print());
            }
        }

        // figure out the values of the new constraints
        result.knowns = solve(result.knowns, result.constraints, result.preConstraints);

        if (bindInfo->traceSerializer) {
            bindInfo->traceSerializer->newObject("args", argInfo);
            bindInfo->traceSerializer->newObject("knowns", result.knowns);
            bindInfo->traceSerializer->newObject("constraints", result.constraints);
            bindInfo->traceSerializer->newObject("tuneables", tuneables);
        }

    #if 0
        cerr << "got " << result.constraints.size() << " constraints" << endl;
        for (auto & c: result.constraints) {
            cerr << "  " << c.print()
                << (c.satisfied(result.knowns) ? " [SATSIFIED]" : " [UNSATISFIED]") << endl;
        }
    #endif

        return result;
    } MLDB_CATCH_ALL {
        rethrowException(500, "Binding OpenCL kernel " + this->kernelName);
    }
}
#endif


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
        if (clPlatforms.empty() || !OPENCL_ENABLED) {
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
        ExcAssertEqual(devices.size(), 1);
        return std::make_shared<OpenCLComputeContext>(convertDevice(devices[0]), devices[0]);
    }
};


// OpenCLComputeKernel

OpenCLComputeKernel::
OpenCLComputeKernel(OpenCLComputeContext * owner, const GridComputeKernelTemplate & tmplate)
    : GridComputeKernelSpecialization(owner, tmplate)
{
    this->clContext = owner;
    this->clFunction = dynamic_cast<const OpenCLComputeFunction *>(this->gridFunction.get());
    ExcAssert(this->clFunction);
}


// OpenCLComputeFunction

OpenCLComputeFunction::
OpenCLComputeFunction(OpenCLComputeContext & context, OpenCLProgram clProgram, const std::string & kernelName)
    : clProgram(std::move(clProgram)), kernelName(kernelName)
{
}

GridComputeFunctionArgumentDisposition getDisposition(OpenCLArgAddressQualifier argType)
{
    switch (argType) {
    case OpenCLArgAddressQualifier::GLOBAL:   return GridComputeFunctionArgumentDisposition::BUFFER;
    case OpenCLArgAddressQualifier::CONSTANT: return GridComputeFunctionArgumentDisposition::BUFFER;
    case OpenCLArgAddressQualifier::LOCAL:    return GridComputeFunctionArgumentDisposition::THREADGROUP;
    case OpenCLArgAddressQualifier::PRIVATE : return GridComputeFunctionArgumentDisposition::LITERAL;
    default:
        MLDB_THROW_UNIMPLEMENTED("OpenCL unknown argType", "argType", argType);
    }
}

OpenCLKernel
OpenCLComputeFunction::
generateKernel() const
{
    // TODO: locking?
    return const_cast<OpenCLProgram &>(clProgram).createKernel(kernelName);
}

std::vector<GridComputeFunctionArgument>
OpenCLComputeFunction::
getArgumentInfo() const
{
    auto argInfo = generateKernel().getInfo();

    auto & arguments = argInfo.args;

    std::vector<GridComputeFunctionArgument> result;

    for (size_t i = 0;  i < arguments.size();  ++i) {
        const auto & arg = arguments[i];
        std::string argName = arg.name;
        GridComputeFunctionArgumentDisposition disposition = getDisposition(arg.addressQualifier);
        auto type = OpenCLComputeKernel::getKernelType(arg);

        GridComputeFunctionArgument entry;
        entry.name = argName;
        entry.disposition = disposition;
        entry.type = type;
        entry.computeFunctionArgIndex = arg.argNum;

        result.emplace_back(std::move(entry));
    }

    return result;
}


// OpenCLComputeFunctionLibrary

OpenCLComputeFunctionLibrary::
OpenCLComputeFunctionLibrary(OpenCLComputeContext & context, OpenCLProgram clProgram)
    : context(context), clProgram(std::move(clProgram))
{
}

std::shared_ptr<GridComputeFunction>
OpenCLComputeFunctionLibrary::
getFunction(const std::string & functionName)
{
    return std::make_shared<OpenCLComputeFunction>(context, clProgram, functionName);
}

std::string
OpenCLComputeFunctionLibrary::
getId() const
{
    MLDB_THROW_UNIMPLEMENTED();
}

Json::Value
OpenCLComputeFunctionLibrary::
getMetadata() const
{
    MLDB_THROW_UNIMPLEMENTED();
}

std::shared_ptr<OpenCLComputeFunctionLibrary>
OpenCLComputeFunctionLibrary::
compileFromSourceFile(OpenCLComputeContext & context, const std::string & fileName)
{
    filter_istream stream(fileName);
    Utf8String source = /*"#line 1 \"" + fileName + "\"\n" +*/ stream.readAll();

    return compileFromSource(context, source, fileName);
}

std::shared_ptr<OpenCLComputeFunctionLibrary>
OpenCLComputeFunctionLibrary::
compileFromSource(OpenCLComputeContext & context, const Utf8String & source, const std::string & fileNameToAppearInErrorMessages)
{
    OpenCLProgram program = context.clContext.createProgram(source);
    //string options = "-cl-kernel-arg-info -cl-fp32-correctly-rounded-divide-sqrt -DWBITS=32";
    string options = "-cl-kernel-arg-info";

    // Build for all devices
    auto buildInfo = program.build({context.clDevice}, options);
                
    //cerr << jsonEncode(buildInfo[0]) << endl;

    return std::make_shared<OpenCLComputeFunctionLibrary>(context, std::move(program));
}

void registerOpenCLLibrary(const std::string & libraryName,
                          std::function<std::shared_ptr<OpenCLComputeFunctionLibrary>(OpenCLComputeContext &)> generator)
{
    std::unique_lock guard{libraryRegistryMutex};
    libraryRegistry[libraryName].generate = generator;
}

namespace {

static struct Init {
    Init()
    {
        ComputeRuntime::registerRuntime(ComputeRuntimeId::OPENCL, "opencl",
                                        [] () { return new OpenCLComputeRuntime(); });

        auto compileLibrary = [] (OpenCLComputeContext & context) -> std::shared_ptr<OpenCLComputeFunctionLibrary>
        {
            std::string fileName = "mldb/builtin/opencl/base_kernels.cl";
            return OpenCLComputeFunctionLibrary::compileFromSourceFile(context, fileName);
        };

        registerOpenCLLibrary("base_kernels", compileLibrary);
    }

} init;

} // file scope
} // namespace MLDB
