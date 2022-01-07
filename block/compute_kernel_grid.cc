/** compute_kernel_grid.cc                                                -*- C++ -*-
    Jeremy Barnes, 27 March 2016
    This file is part of MLDB. Copyright 2016 mldb.ai inc. All rights reserved.

    Compute kernel runtime for CPU devices.
*/

#include "compute_kernel_grid.h"
#include "mldb/types/basic_value_descriptions.h"
#include "mldb/types/meta_value_description.h"
#include "mldb/types/structure_description.h"
#include "mldb/types/set_description.h"
#include "mldb/types/generic_array_description.h"
#include "mldb/types/generic_atom_description.h"
#include "mldb/vfs/filter_streams.h"
#include "mldb/utils/environment.h"
#include "mldb/arch/ansi.h"
#include "mldb/block/zip_serializer.h"
#include "mldb/utils/command_expression_impl.h"
#include "mldb/arch/spinlock.h"
#include <compare>

using namespace std;

namespace MLDB {

namespace {

#if 0
std::mutex kernelRegistryMutex;
struct KernelRegistryEntry {
    std::function<std::shared_ptr<GridComputeKernel>(GridComputeContext & context)> generate;
};

std::map<std::string, KernelRegistryEntry> kernelRegistry;
#endif

EnvOption<int> GRID_TRACE_API_CALLS("GRID_COMPUTE_TRACE_API_CALLS", 0);
#if 0
EnvOption<std::string, true> GRID_KERNEL_TRACE_FILE("GRID_KERNEL_TRACE_FILE", "");
EnvOption<std::string, true> GRID_CAPTURE_FILE("GRID_CAPTURE_FILE", "");
EnvOption<bool, false> GRID_ENABLED("GRID_ENABLED", true);

std::shared_ptr<ZipStructuredSerializer> traceSerializer;
std::shared_ptr<StructuredSerializer> regionsSerializer;
std::shared_ptr<StructuredSerializer> kernelsSerializer;
std::shared_ptr<StructuredSerializer> programsSerializer;

using TracedRegionKey = std::tuple<std::string, int>;

struct TracedRegionEntry {
    size_t length = 0;
};

std::mutex tracedRegionMutex;
std::map<std::string, std::shared_ptr<StructuredSerializer>> regionSerializers;
std::map<TracedRegionKey, TracedRegionEntry> tracedRegions;

std::mutex tracedProgramMutex;
std::set<std::string> tracedPrograms;

bool versionIsTraced(const std::string & name, int version)
{
    std::unique_lock guard(tracedRegionMutex);
    return tracedRegions.count({name, version});
}

void traceVersion(std::string name, int version, const FrozenMemoryRegion & region)
{
    if (name == "") {
        name = format("%016llx", (unsigned long long)region.data());
    }
    ExcAssert(name != "");
    ExcAssertGreaterEqual(version, 0);

    if (!traceSerializer)
        return;

    std::unique_lock guard(tracedRegionMutex);
    auto it = tracedRegions.find({name, version});
    if (it != tracedRegions.end()) {
        ExcAssertEqual(it->second.length, region.length());
        return;
    }

    if (!regionSerializers.count(name)) {
        regionSerializers[name] = regionsSerializer->newStructure(name);
    }
    auto thisRegionSerializer = regionSerializers[name];
    ExcAssert(thisRegionSerializer);
    thisRegionSerializer->addRegion(region, version);

    tracedRegions[{name, version}] = {region.length()};
}

struct InitTrace {
    InitTrace()
    {
        if (GRID_KERNEL_TRACE_FILE.specified()) {
            traceSerializer = std::make_shared<ZipStructuredSerializer>(GRID_KERNEL_TRACE_FILE.get());
            regionsSerializer = traceSerializer->newStructure("regions");
            kernelsSerializer = traceSerializer->newStructure("kernels");
            programsSerializer = traceSerializer->newStructure("programs");
        }
    }

    ~InitTrace()
    {
        if (traceSerializer) {
            regionsSerializer->commit();
            kernelsSerializer->commit();
            traceSerializer->commit();
            programsSerializer->commit();
        }
    }
} initTrace;

#endif

__thread int opCount = 0;
Timer startTimer;
double lastTime = 0;

static std::string printTime(double elapsed)
{
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
    return timerStr;
}

enum class OperationType {
    GRID_COMPUTE = 1,
    USER = 2
};

enum class OperationScope {
    ENTER = 1,
    EXIT = 2,
    EXIT_EXC = 3,
    EVENT = 4
};

template<typename... Args>
void traceOperation(OperationScope opScope, OperationType opType, const std::string & opName, Args&&... args)
{
    if (GRID_TRACE_API_CALLS.get()) {
        using namespace MLDB::ansi;
        int tid = std::hash<std::thread::id>()(std::this_thread::get_id());
        double elapsed = startTimer.elapsed_wall();

        std::string opTypeName;
        std::string opColor;

        switch (opType) {
        case OperationType::GRID_COMPUTE:
            opTypeName = "GRID:";
            opColor = ansi::ansi_str_blue();
            break;
        case OperationType::USER:
            opTypeName = "USER:";
            opColor = ansi::ansi_str_bright_blue();
            break;
        }

        std::string opPrefix;

        switch (opScope) {
        case OperationScope::ENTER:
            opPrefix = "BEGIN ";
            break;

        case OperationScope::EVENT:
            opPrefix = "";
            break;

        case OperationScope::EXIT:
            opPrefix = "END ";
            break;

        case OperationScope::EXIT_EXC:
            opPrefix = "EXCEPTION ";
            opColor = ansi::ansi_str_bright_red();
            break;
        };

        if (true || (opScope == OperationScope::EXIT && (elapsed - lastTime) > 0.001)) {
            std::string header = format("%10.6f%9lld t%8x %-8s", elapsed, (long long)((elapsed-lastTime)*1000000000), tid, opTypeName.c_str());
            std::string indent(4 * opCount, ' ');
            std::string toDump = opColor + header + indent + opPrefix + opName
                               + ansi_str_reset() + "\n";
            cerr << toDump << flush;
        }

        lastTime = startTimer.elapsed_wall();
    }
}

template<typename... Args>
void traceOperation(OperationType opType, const std::string & opName, Args&&... args)
{
    traceOperation(OperationScope::EVENT, opType, opName, std::forward<Args>(args)...);
}

template<typename... Args>
void traceGridOperation(const std::string & opName, Args&&... args)
{
    traceOperation(OperationScope::EVENT, OperationType::GRID_COMPUTE, opName, std::forward<Args>(args)...);
}

struct ScopedOperation {
    ScopedOperation(const ScopedOperation &) = delete;
    auto operator = (const ScopedOperation &) = delete;

    template<typename... Args>
    ScopedOperation(OperationType opType,
                    const std::string & opName, Args&&... args)
        : opType(opType), opName(opName)
    {
        traceOperation(OperationScope::ENTER, opType, opName, std::forward<Args>(args)...);
        ++opCount;
    }

    ~ScopedOperation()
    {
        if (!opName.empty()) {
            --opCount;

            if (std::uncaught_exceptions()) {
                traceOperation(OperationScope::EXIT_EXC, opType, opName);
                return;
            }

            double elapsed = timer.elapsed_wall();
            traceOperation(OperationScope::EXIT, opType, opName + " [" + ansi::ansi_str_underline() + printTime(elapsed) + "]");
        }
    }

    OperationType opType;
    std::string opName;
    Timer timer;
};

template<typename... Args>
ScopedOperation MLDB_WARN_UNUSED_RESULT scopedOperation(OperationType opType, const std::string & opName, Args&&... args) 
{
    return ScopedOperation(opType, opName, std::forward<Args>(args)...);
}

} // file scope

// GridMemoryRegionHandleInfo

std::tuple<FrozenMemoryRegion, int /* version */>
GridMemoryRegionHandleInfo::
getReadOnlyHostAccessSync(const GridComputeContext & context,
                            const std::string & opName,
                            size_t offset,
                            ssize_t length,
                            bool ignoreHazards)
{
    if (!ignoreHazards && false) {
        unique_lock guard(mutex);

        if (numWriters != 0) {
            throw MLDB::Exception("Operation '" + opName + "' attempted to read region '" + name + "' with active writer(s) "
                                    + jsonEncodeStr(currentWriters) + " and active reader(s) "
                                    + jsonEncodeStr(currentReaders));
        }

        if (!currentReaders.insert(opName).second) {
            throw MLDB::Exception("Operation '" + opName + " ' is already reading region '" + name + "'");
        }
        ++numReaders;
    }

    if (length == -1) {
        length = lengthInBytes - offset;
    }

    MLDB_THROW_UNIMPLEMENTED();

#if 0
    std::shared_ptr<const void> region
        = context.clQueue->clQueue.enqueueMapBufferBlocking(buffer, CL_MAP_READ,
                                                            offset /* offset */, length);

    auto done = [this, opName, ignoreHazards, region] (const void * mem)
    {
        if (!ignoreHazards) {
            unique_lock guard(mutex);
            --numReaders;
            currentReaders.erase(opName);
        }
    };

    auto pin =  std::shared_ptr<void>(nullptr, done);
    auto data = (const char *)region.get();

    return { { std::move(pin), data, lengthInBytes - offset }, this->version };
#endif
}

std::tuple<std::shared_ptr<const void>, std::any>
GridMemoryRegionHandleInfo::
getGridAccess(const std::string & opName, MemoryRegionAccess access)
{
    unique_lock guard(mutex);

    if (access == ACC_NONE) {
        throw MLDB::Exception("Asked for no access to region");
    }

    if (access == ACC_READ) {
        if (numWriters != 0 && false) {
            throw MLDB::Exception("Operation '" + opName + "' attempted to read region '" + name + "' with active writer(s) "
                                    + jsonEncodeStr(currentWriters) + " and active reader(s) "
                                    + jsonEncodeStr(currentReaders));
        }
        if (!currentReaders.insert(opName).second) {
            throw MLDB::Exception("Operation '" + opName + " ' is already reading region '" + name + "'");
        }
        ++numReaders;

        auto done = [this, opName] (const void * mem)
        {
            unique_lock guard(mutex);
            --numReaders;
            currentReaders.erase(opName);
        };

        auto pin =  std::shared_ptr<void>(nullptr, done);
        //cerr << "returning r/o version " << version << " of " << name << " for " << opName << endl;
        return { std::move(pin), this->buffer };
    }
    else {  // read only or read write
        if (currentReaders.count(opName)) {
            throw MLDB::Exception("Operation '" + opName + " ' is already reading region '" + name + "'");
        }
        if (currentWriters.count(opName)) {
            throw MLDB::Exception("Operation '" + opName + " ' is already writing region '" + name + "'");
        }
        if ((numWriters != 0 || numReaders != 0) && false) {
            throw MLDB::Exception("Operation '" + opName + "' attempted to write region '"
                                    + name + "' with active writer(s) "
                                    + jsonEncodeStr(currentWriters) + " and/or active reader(s) "
                                    + jsonEncodeStr(currentReaders));
        }
        ++version;
        ++numWriters;
        currentWriters.insert(opName);
        if (access == ACC_READ_WRITE) {
            ++numReaders;
            currentReaders.insert(opName);
        }

        auto done = [this, access, opName] (const void * mem)
        {
            unique_lock guard(mutex);
            ++version;
            --numWriters;
            currentWriters.erase(opName);
            if (access == ACC_READ_WRITE) {
                --numReaders;
                currentReaders.erase(opName);
            }
        };

        //cerr << "returning r/w version " << version << " of " << name << " for " << opName << endl;

        //this->buffer.SetPurgeableState(mtlpp::PurgeableState::NonVolatile);

        auto pin =  std::shared_ptr<void>(nullptr, done);
        return { std::move(pin), this->buffer };
    }
}

// GridComputeProfilingInfo

GridComputeProfilingInfo::
GridComputeProfilingInfo()
{
}


// GridComputeEvent

GridComputeEvent::
GridComputeEvent(const std::string & label, bool isResolvedIn, GridComputeQueue * owner)
    : label_(label), owner(owner), future(promise.get_future().share())
{
    ExcAssert(owner);
    if (isResolvedIn)
        resolve();
    ExcAssertEqual(this->isResolved, isResolvedIn);
}

std::shared_ptr<ComputeProfilingInfo>
GridComputeEvent::
getProfilingInfo() const
{
    MLDB_THROW_UNIMPLEMENTED();
}

void
GridComputeEvent::
resolve()
{
    if (isResolved)
        return;

    std::unique_lock guard(mutex);

    isResolved = true;
    promise.set_value();

    std::exception_ptr exc;
    for (auto & callback: callbacks) {
        try {
            callback();
        } catch (...) {
            exc = std::current_exception();
        }
    }
    if (exc)
        std::rethrow_exception(exc);
}

std::shared_ptr<ComputeEvent>
GridComputeEvent::
thenImpl(std::function<void ()> fn, const std::string & label)
{
    std::unique_lock guard(mutex);
    if (isResolved) {
        // If already satisfied, we simply run the callback
        fn();
        return owner->makeAlreadyResolvedEvent(label);
    }

#if 1
    callbacks.push_back(fn);
    return this->shared_from_this();
#else
    // Otherwise, create a user event for the post-then part
    auto nextEvent = owner->makeUnresolvedEvent(label);
    nextEvent->label_ = label + " (GridComputeEvent::thenImpl)";

    auto cb = [fn=std::move(fn), nextEvent] ()
    {
        fn();
        nextEvent->resolve();
    };

    callbacks.push_back(cb);

    return nextEvent;
#endif
}


// GridComputeQueue

GridComputeQueue::
GridComputeQueue(GridComputeContext * owner, GridComputeQueue * parent, const std::string & label,
                 GridDispatchType dispatchType)
    : ComputeQueue(owner, parent), gridOwner(owner), dispatchType(dispatchType)
{
}

GridComputeQueue::~GridComputeQueue()
{
}

#if 0
template<typename CommandEncoder>
void
GridComputeQueue::
beginEncodingImpl(const std::string & opName, CommandEncoder & encoder, bool force)
{
    ExcAssert(encoder);
    encoder.SetLabel(opName.c_str());
    if (force || this->dispatchType == mtlpp::DispatchType::Serial) {
        cerr << "awaiting " << activeCommands.size() << " fences before " << opName << endl;
        for (auto fence: activeCommands)
            encoder.WaitForFence(fence);
        activeCommands.clear();
    }
}

template<typename CommandEncoder>
void
GridComputeQueue::
endEncodingImpl(const std::string & opName, CommandEncoder & encoder, bool force)
{
    auto fence = mtlOwner->mtlDevice.NewFence();
    ExcAssert(fence);
    encoder.UpdateFence(fence);
    encoder.EndEncoding();
    activeCommands.emplace_back(std::move(fence));
}

void
GridComputeQueue::
beginEncoding(const std::string & opName, mtlpp::ComputeCommandEncoder & encoder, bool force)
{
    beginEncodingImpl(opName, encoder, force);
}

void
GridComputeQueue::
beginEncoding(const std::string & opName, mtlpp::BlitCommandEncoder & encoder, bool force)
{
    beginEncodingImpl(opName, encoder, force);
}

void
GridComputeQueue::
endEncoding(const std::string & opName, mtlpp::ComputeCommandEncoder & encoder, bool force)
{
    endEncodingImpl(opName, encoder, force);
}

void
GridComputeQueue::
endEncoding(const std::string & opName, mtlpp::BlitCommandEncoder & encoder, bool force)
{
    endEncodingImpl(opName, encoder, force);
}
#endif

void
GridComputeQueue::
enqueue(const std::string & opName,
        const BoundComputeKernel & bound,
        const std::vector<uint32_t> & grid)
{
    try {
        auto tr = scopedOperation(OperationType::GRID_COMPUTE, "launch kernel " + bound.owner->kernelName + " as " + opName);

        ExcAssert(bound.bindInfo);
        
        const GridBindInfo * bindInfo
            = dynamic_cast<const GridBindInfo *>(bound.bindInfo.get());
        ExcAssert(bindInfo);

        const GridComputeKernel * kernel = bindInfo->owner;

        auto knowns = bound.knowns;

        for (size_t i = 0;  i < grid.size();  ++i) {
            auto & dim = kernel->dims[i];
            knowns.setValue(dim.range, grid[i]);
        }

        std::vector<size_t> mtlGrid, mtlBlock;
        
        //if (kernel->allowGridExpansionFlag)
        //    ExcAssertLessEqual(grid.size(), mtlBlock.size());
        //else
        //    ExcAssertEqual(grid.size(), mtlBlock.size());

        for (size_t i = 0;  i < mtlBlock.size();  ++i) {
            // Pad out the grid so we cover the whole lot.  The kernel will need to be
            // sure to no-op if it's out of bounds.
            auto b = mtlBlock[i];
            auto range = i < grid.size() ? grid[i] : b;
            auto rem = range % b;
            if (rem > 0) {
                if (kernel->allowGridPaddingFlag) {
                    range += (b - rem);
                    //cerr << "padding out dimension " << i << " from " << grid[i]
                    //    << " to " << range << " due to block size of " << b << endl;
                }
                else {
                    throw MLDB::Exception("Grid kernel '" + kernel->kernelName + "' won't launch "
                                            "due to grid dimension " + std::to_string(i)
                                            + " (" + std::to_string(range) + ") not being a "
                                            + "multple of the block size (" + std::to_string(b)
                                            + ").  Consider using allowGridPadding() or modifying "
                                            + "grid calculations");
                }
            }
            mtlGrid.push_back(range);
        }

        if (mtlGrid.empty()) {
            mtlGrid.push_back(1);
        }
        if (mtlBlock.empty()) {
            mtlBlock.push_back(1);
        }

        knowns.setValue("grid", grid);
        knowns.setValue("mtlGrid", mtlGrid);
        knowns.setValue("mtlBlock", mtlBlock);

        std::function<Json::Value (const std::vector<Json::Value> &)>
        readArrayElement = [&] (const std::vector<Json::Value> & args) -> Json::Value
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
            mtlGrid = jsonDecode<decltype(mtlGrid)>(knowns.evaluate(*kernel->gridExpression));
            knowns.setValue("mtlGrid", mtlGrid);
        }

        if (kernel->blockExpression) {
            mtlBlock = jsonDecode<decltype(mtlBlock)>(knowns.evaluate(*kernel->blockExpression));
            knowns.setValue("mtlBlock", mtlBlock);
        }

        //cerr << "launching kernel " << kernel->kernelName << " with grid " << mtlGrid << " and block " << mtlBlock << endl;
        //cerr << "this->block = " << jsonEncodeStr(this->block) << endl;

        if (bindInfo->traceSerializer) {
            bindInfo->traceSerializer->newObject("grid", mtlGrid);
            bindInfo->traceSerializer->newObject("block", mtlBlock);
            bindInfo->traceSerializer->newObject("knowns", knowns);
            bindInfo->traceSerializer->commit();
        }

        auto bindContext = this->newBindContext();

        constexpr bool solveAfter = false;

        for (auto & action: kernel->bindActions) {
            action.apply(*kernel->gridContext, bound.arguments, knowns, solveAfter, *bindContext);
        }

        if (solveAfter) {
            knowns = solve(knowns, bound.constraints, bound.preConstraints);
        }
        //knowns = fullySolve(knowns, bound.constraints, bound.preConstraints);

        this->launch(*bindContext, mtlGrid, mtlBlock);
    } MLDB_CATCH_ALL {
        rethrowException(400, "Error launching Grid kernel " + bound.owner->kernelName);
    }
}

void
GridComputeQueue::
enqueueFillArrayImpl(const std::string & opName,
                     MemoryRegionHandle region, MemoryRegionInitialization init,
                     size_t startOffsetInBytes, ssize_t lengthInBytes,
                     std::span<const std::byte> block)
{
    auto op = scopedOperation(OperationType::GRID_COMPUTE, "enqueueFillArrayImpl " + opName);

    if (startOffsetInBytes > region.lengthInBytes()) {
        throw MLDB::Exception("region is too long");
    }
    if (lengthInBytes == -1)
        lengthInBytes = region.lengthInBytes() - startOffsetInBytes;
    
    if (startOffsetInBytes + lengthInBytes > region.lengthInBytes()) {
        throw MLDB::Exception("overflowing memory region");
    }

    switch (init) {
    case MemoryRegionInitialization::INIT_NONE:
        return;
    case MemoryRegionInitialization::INIT_ZERO_FILLED:
        this->zeroFillArrayConcrete(opName, region, startOffsetInBytes, lengthInBytes);
        return;
    case MemoryRegionInitialization::INIT_BLOCK_FILLED:
        this->blockFillArrayConcrete(opName, region, startOffsetInBytes, lengthInBytes, block);
        return;
    default:
        MLDB_THROW_UNIMPLEMENTED("unknown memory region initialization %d", init);
    }
}

void
GridComputeQueue::
enqueueCopyFromHostImpl(const std::string & opName,
                        MemoryRegionHandle toRegion,
                        FrozenMemoryRegion fromRegion,
                        size_t deviceStartOffsetInBytes)
{
    auto op = scopedOperation(OperationType::GRID_COMPUTE, "GridComputeQueue enqueueCopyFromHostImpl " + opName);
    GridMemoryRegionHandleInfo * upcastHandle = dynamic_cast<GridMemoryRegionHandleInfo *>(toRegion.handle.get());
    if (!upcastHandle) {
        throw MLDB::Exception("Wrong GridComputeContext handle: got " + demangle(typeid(toRegion.handle)));
    }
    if (upcastHandle->backingHostMem) {
        throw MLDB::Exception("cannot copy from host into a read-only host backed array");
    }

    ExcAssertLessEqual(deviceStartOffsetInBytes + fromRegion.length(), toRegion.lengthInBytes());

    this->enqueueCopyFromHostConcrete(opName, toRegion, fromRegion, deviceStartOffsetInBytes);
}

void
GridComputeQueue::
copyFromHostSyncImpl(const std::string & opName,
                            MemoryRegionHandle toRegion,
                            FrozenMemoryRegion fromRegion,
                            size_t deviceStartOffsetInBytes)
{
    auto op = scopedOperation(OperationType::GRID_COMPUTE, "GridComputeQueue copyFromHostSyncImpl " + opName);
    enqueueCopyFromHostImpl(opName, toRegion, fromRegion, deviceStartOffsetInBytes);
    finish();
}

FrozenMemoryRegion
GridComputeQueue::
enqueueTransferToHostImpl(const std::string & opName,
                          MemoryRegionHandle handle)
{
    auto op = scopedOperation(OperationType::GRID_COMPUTE, "GridComputeQueue enqueueTransferToHostSyncImpl " + opName);
    return enqueueTransferToHostConcrete(opName, handle);
}

FrozenMemoryRegion
GridComputeQueue::
transferToHostSyncImpl(const std::string & opName,
                       MemoryRegionHandle handle)
{
    auto op = scopedOperation(OperationType::GRID_COMPUTE, "GridComputeQueue transferToHostSyncImpl " + opName);

    GridMemoryRegionHandleInfo * upcastHandle = dynamic_cast<GridMemoryRegionHandleInfo *>(handle.handle.get());
    if (!upcastHandle) {
        throw MLDB::Exception("Wrong GridComputeContext handle: got " + demangle(typeid(handle)));
    }
    if (upcastHandle->backingHostMem) {
        // It's a read-only, host-first mapping... so nothing to do
        return { handle.handle, (const char *)upcastHandle->backingHostMem, (size_t)upcastHandle->lengthInBytes };
    }

    auto [pin, buffer, offset] = GridComputeContext::getMemoryRegion(opName, *handle.handle, ACC_READ);

    auto blitEncoder = commandBuffer.BlitCommandEncoder();
    beginEncoding(opName, blitEncoder);

    const void * contents;
    auto length = buffer.GetLength();
    ExcAssertLessEqual(handle.handle->lengthInBytes, length);

    switch (buffer.GetStorageMode()) {
    case mtlpp::StorageMode::Managed:
        blitEncoder.Synchronize(buffer);
        // fall through
    case mtlpp::StorageMode::Shared:
        contents = buffer.GetContents();
        break;
    case mtlpp::StorageMode::Private: {
        // It's a private region; we need to create a new region to hold the result, and then
        // synchronize
        mtlpp::ResourceOptions options = mtlpp::ResourceOptions::StorageModeManaged;
        auto tmpBuffer = mtlOwner->mtlDevice.NewBuffer(length == 0 ? 4 : length, options);
        tmpBuffer.SetLabel((opName + " transferToHostSync private copy").c_str());
        blitEncoder.Copy(buffer, offset, tmpBuffer, 0 /* offset */, length - offset);
        blitEncoder.Synchronize(tmpBuffer);
        contents = tmpBuffer.GetContents();

        // We pin the temp buffer instead
        auto freeTmpBuffer = [tmpBuffer] (auto arg) {};
        pin = std::shared_ptr<const void>(nullptr, freeTmpBuffer);
        break;
    }
    default:
        throw MLDB::Exception("Cannot manage MemoryLess storage mode");
    }
    endEncoding(opName, blitEncoder);

    auto myCommandBuffer = commandBuffer;

    finish();

    //ExcAssert(commandBuffer);
    //commandBuffer.Commit();
    //commandBuffer.WaitUntilCompleted();

    auto status = myCommandBuffer.GetStatus();
    if (status != mtlpp::CommandBufferStatus::Completed) {
        auto error = commandBuffer.GetError();
        throw MLDB::Exception(std::string(error.GetDomain().GetCStr()) + ": " + error.GetLocalizedDescription().GetCStr());
    }

    //auto storageMode = buffer.GetStorageMode();
    //cerr << "storage mode is " << (int)storageMode << endl;


    //cerr << "contents " << contents << endl;
    //cerr << "length " << length << endl;

    //cerr << "region length for region " << handle.handle->name
    //     << " with lengthInBytes " << handle.handle->lengthInBytes
    //     << " is " << length << " and contents " << contents << endl;

    FrozenMemoryRegion result(std::move(pin), ((const char *)contents) + offset, handle.handle->lengthInBytes);
    return result;
}

MemoryRegionHandle
GridComputeQueue::
enqueueManagePinnedHostRegionImpl(const std::string & opName, std::span<const std::byte> region, size_t align,
                                  const std::type_info & type, bool isConst)
{
    auto op = scopedOperation(OperationType::GRID_COMPUTE, "GridComputeContext managePinnedHostRegionImpl " + opName);

    return managePinnedHostRegionSyncImpl(opName, region, align, type, isConst);
}

MemoryRegionHandle
GridComputeQueue::
managePinnedHostRegionSyncImpl(const std::string & opName,
                               std::span<const std::byte> region, size_t align,
                               const std::type_info & type, bool isConst)
{
    auto op = scopedOperation(OperationType::GRID_COMPUTE, "GridComputeContext managePinnedHostRegionSyncImpl " + opName);

    cerr << "managing pinned host region " << opName << " of length " << region.size() << endl;

    mtlpp::ResourceOptions options
         = mtlpp::ResourceOptions::StorageModeShared;


    traceGridOperation("region size " + std::to_string(region.size()));

    // This overload calls newBufferWithBytesNoCopy
    // Note that we require a page-aligned address and size, and a single VM region
    // See https://developer.apple.com/documentation/grid/mtldevice/1433382-makebuffer
    // Thus, for now we don't try; instead we copy and blit
    mtlpp::Buffer buffer;
    if (false) {
        auto deallocator = [=] (auto ptr, auto len)
        {
            cerr << ansi::bright_red << "DEALLOCATING PINNED REGION " << opName << ansi::reset
                << " of length " << len << endl;
        };

        buffer = mtlOwner->mtlDevice.NewBuffer((void *)region.data(), region.size(), options, deallocator);
    }
    else {
        if (region.empty()) {
            buffer = mtlOwner->mtlDevice.NewBuffer(4, options);
        }
        else {
            buffer = mtlOwner->mtlDevice.NewBuffer((const void *)region.data(), region.size(), options);
        }
    }
    buffer.SetLabel(opName.c_str());

    auto handle = std::make_shared<GridMemoryRegionHandleInfo>();
    handle->buffer = std::move(buffer);
    handle->offset = 0;
    handle->type = &type;
    handle->isConst = isConst;
    handle->lengthInBytes = region.size();
    handle->version = 0;
    handle->name = opName;
    handle->backingHostMem = region.data();
    MemoryRegionHandle result{std::move(handle)};
    return result;
}

void
GridComputeQueue::
enqueueCopyBetweenDeviceRegionsImpl(const std::string & opName,
                                    MemoryRegionHandle from, MemoryRegionHandle to,
                                    size_t fromOffset, size_t toOffset,
                                    size_t length)
{
    auto op = scopedOperation(OperationType::GRID_COMPUTE, "GridComputeQueue enqueueCopyBetweenDeviceRegionsImpl " + opName);

    GridMemoryRegionHandleInfo * upcastFromHandle = dynamic_cast<GridMemoryRegionHandleInfo *>(from.handle.get());
    if (!upcastFromHandle) {
        throw MLDB::Exception("Wrong GridComputeContext from handle: got " + demangle(typeid(from.handle)));
    }

    auto [fromPin, fromBuffer, fromBaseOffset] = GridComputeContext::getMemoryRegion(opName, *from.handle, ACC_READ);

    ExcAssertLessEqual(fromBaseOffset + fromOffset + length, from.lengthInBytes());

    GridMemoryRegionHandleInfo * upcastToHandle = dynamic_cast<GridMemoryRegionHandleInfo *>(to.handle.get());
    if (!upcastToHandle) {
        throw MLDB::Exception("Wrong GridComputeContext to handle: got " + demangle(typeid(to.handle)));
    }

    auto [toPin, toBuffer, toBaseOffset] = GridComputeContext::getMemoryRegion(opName, *to.handle, ACC_WRITE);

    ExcAssertLessEqual(toBaseOffset + toOffset + length, to.lengthInBytes());

    auto blitEncoder = commandBuffer.BlitCommandEncoder();
    beginEncoding(opName, blitEncoder);
    blitEncoder.Copy(fromBuffer, fromBaseOffset + fromOffset, toBuffer, toBaseOffset + toOffset, length);
    endEncoding(opName, blitEncoder);
}

void
GridComputeQueue::
copyBetweenDeviceRegionsSyncImpl(const std::string & opName,
                                 MemoryRegionHandle from, MemoryRegionHandle to,
                                 size_t fromOffset, size_t toOffset,
                                 size_t length)
{
    enqueueCopyBetweenDeviceRegionsImpl(opName, from, to, fromOffset, toOffset, length);
    finish();
}

void
GridComputeQueue::
finish()
{
    auto op = scopedOperation(OperationType::GRID_COMPUTE, "GridComputeQueue finish");
    flush()->await();
}


// GridComputeMarker

GridComputeMarker::
GridComputeMarker(const std::string & scopeName)
{
    this->scope = std::make_shared<ScopedOperation>(OperationType::USER, scopeName);
}

GridComputeMarker::
~GridComputeMarker()
{
}

std::shared_ptr<ComputeMarker>
GridComputeMarker::
enterScope(const std::string & scopeName)
{
    return std::make_shared<GridComputeMarker>(scopeName);
}


// GridComputeContext

GridComputeContext::
GridComputeContext(ComputeDevice device)
    : device(device),
      queue(std::make_shared<GridComputeQueue>(this, nullptr /* parent */, "(queue for context)", GridDispatchType::SERIAL))
{
}

ComputeDevice
GridComputeContext::
getDevice() const
{
    return device;
}

std::shared_ptr<ComputeMarker>
GridComputeContext::
getScopedMarker(const std::string & scopeName)
{
    return std::make_shared<GridComputeMarker>(scopeName);
}

void
GridComputeContext::
recordMarkerEvent(const std::string & event)
{
    traceOperation(OperationType::USER, event);
}

#if 0
std::tuple<std::shared_ptr<const void>, mtlpp::Buffer, size_t>
GridComputeContext::
getMemoryRegion(const std::string & opName, MemoryRegionHandleInfo & handle, MemoryRegionAccess access)
{
    GridMemoryRegionHandleInfo * upcastHandle = dynamic_cast<GridMemoryRegionHandleInfo *>(&handle);
    if (!upcastHandle) {
        throw MLDB::Exception("TODO: get memory region from block handled from elsewhere: got " + demangle(typeid(handle)));
    }
    auto [pin, mem] = upcastHandle->getGridAccess(opName, access);

    return { std::move(pin), mem, upcastHandle->offset };
}

std::tuple<FrozenMemoryRegion, int /* version */>
GridComputeContext::
getFrozenHostMemoryRegion(const std::string & opName, MemoryRegionHandleInfo & handle,
                          size_t offset, ssize_t length,
                          bool ignoreHazards) const
{
    GridMemoryRegionHandleInfo * upcastHandle = dynamic_cast<GridMemoryRegionHandleInfo *>(&handle);
    if (!upcastHandle) {
        throw MLDB::Exception("TODO: get memory region from block handled from elsewhere: got " + demangle(typeid(handle)));
    }
    return upcastHandle->getReadOnlyHostAccessSync(*this, opName, offset, length, ignoreHazards);
}
#endif

#if 0
static MemoryRegionHandle
doGridAllocate(mtlpp::Heap & mtlHeap,
                mtlpp::Device & mtlDevice,
                 const std::string & regionName,
                 size_t length, size_t align,
                 const std::type_info & type,
                 bool isConst)
{
    cerr << "allocating " << length << " bytes for " << regionName << endl;
    // TODO: align...
    //mtlpp::ResourceOptions options = mtlpp::ResourceOptions::StorageModeManaged;
    mtlpp::ResourceOptions options = mtlpp::ResourceOptions::StorageModePrivate;
    auto buffer = mtlDevice.NewBuffer(length == 0 ? 4 : length, options);
    //auto buffer = mtlHeap.NewBuffer(length == 0 ? 4 : length, options);
    if (!buffer) {
        throw MLDB::Exception("Error allocating buffer from heap");
    }
    buffer.SetLabel(regionName.c_str());

    auto handle = std::make_shared<GridMemoryRegionHandleInfo>();
    handle->buffer = std::move(buffer);
    handle->offset = 0;
    handle->type = &type;
    handle->isConst = isConst;
    handle->lengthInBytes = length;
    handle->name = regionName;
    handle->version = 0;

    MemoryRegionHandle result{std::move(handle)};
    return result;
}
#endif

MemoryRegionHandle
GridComputeContext::
allocateSyncImpl(const std::string & regionName,
                 size_t length, size_t align,
                 const std::type_info & type, bool isConst)
{
    auto op = scopedOperation(OperationType::GRID_COMPUTE, "GridComputeContext allocateSyncImpl " + regionName);
    auto result = doGridAllocate(this->heap, this->mtlDevice, regionName, length, align, type, isConst);
    return result;
}

#if 0
static MemoryRegionHandle
doGridTransferToDevice(mtlpp::Device & mtlDevice,
                         const std::string & opName, FrozenMemoryRegion region,
                         const std::type_info & type, bool isConst)
{
    Timer timer;

    mtlpp::ResourceOptions options = mtlpp::ResourceOptions::StorageModeManaged;
    
    mtlpp::Buffer buffer;
    if (region.length() == 0)
        buffer = mtlDevice.NewBuffer(4, options);
    else {
        buffer = mtlDevice.NewBuffer(region.data(), region.length(), options);
    }
    buffer.SetLabel(opName.c_str());

    auto handle = std::make_shared<GridMemoryRegionHandleInfo>();
    handle->buffer = std::move(buffer);
    handle->offset = 0;
    handle->type = &type;
    handle->isConst = isConst;
    handle->lengthInBytes = region.length();
    handle->name = opName;
    handle->version = 0;
    MemoryRegionHandle result{std::move(handle)};

    using namespace std;
    cerr << "transferring " << opName << " " << region.memusage() / 1000000.0 << " Mbytes of type "
        << demangle(type.name()) << " isConst " << isConst << " to device in "
            << timer.elapsed_wall() << " at "
            << region.memusage() / 1000000.0 / timer.elapsed_wall() << "MB/sec" << endl;

    return result;
}

MemoryRegionHandle
GridComputeContext::
transferToDeviceImpl(const std::string & opName, FrozenMemoryRegion region,
                     const std::type_info & type, bool isConst)
{
    auto op = scopedOperation(OperationType::GRID_COMPUTE, "GridComputeContext transferToDeviceImpl " + opName);
    return doGridTransferToDevice(mtlDevice, opName, region, type, isConst);
}

MemoryRegionHandle
GridComputeContext::
transferToDeviceSyncImpl(const std::string & opName,
                         FrozenMemoryRegion region,
                         const std::type_info & type, bool isConst)
{
    MLDB_THROW_UNIMPLEMENTED();
#if 0
    auto op = scopedOperation(OperationType::GRID_COMPUTE, "GridComputeContext transferToDeviceSyncImpl " + opName);
    auto result = doGridTransferToDevice(clContext, opName, region, type, isConst);
    return result;
#endif
}


FrozenMemoryRegion
GridComputeContext::
transferToHostImpl(const std::string & opName, MemoryRegionHandle handle)
{
    // TODO: async
    return transferToHostSyncImpl(opName, handle);
}

FrozenMemoryRegion
GridComputeContext::
transferToHostSyncImpl(const std::string & opName,
                       MemoryRegionHandle handle)
{
    auto op = scopedOperation(OperationType::GRID_COMPUTE, "GridComputeContext transferToHostSyncImpl " + opName);
    return queue->transferToHostSyncImpl(opName, std::move(handle));
}

MutableMemoryRegion
GridComputeContext::
transferToHostMutableImpl(const std::string & opName, MemoryRegionHandle handle)
{
    MLDB_THROW_UNIMPLEMENTED();

#if 0
    auto op = scopedOperation(OperationType::GRID_COMPUTE, "GridComputeContext transferToHostMutableImpl " + opName);
    ExcAssert(handle.handle);

    auto [pin, mem, offset] = getMemoryRegion(opName, *handle.handle, ACC_READ_WRITE);
    GridEvent clEvent;
    std::shared_ptr<void> memPtr;
    std::tie(memPtr, clEvent)
        = clQueue->clQueue.enqueueMapBuffer(mem, CL_MAP_READ | CL_MAP_WRITE,
                                    offset, handle.lengthInBytes());

    auto event = std::make_shared<GridComputeEvent>(std::move(clEvent));
    auto promise = std::shared_ptr<std::promise<std::any>>();

    auto cb = [handle, promise, memPtr, pin=pin] (const GridEvent & event, auto status)
    {
        if (status == GridEventCommandExecutionStatus::ERROR)
            promise->set_exception(std::make_exception_ptr(MLDB::Exception("Grid error mapping host memory")));
        else {
            promise->set_value(MutableMemoryRegion(memPtr, (char *)memPtr.get(), handle.lengthInBytes()));
        }
    };

    clEvent.addCallback(cb);

    return { std::move(promise), std::move(event) };
#endif
}

MutableMemoryRegion
GridComputeContext::
transferToHostMutableSyncImpl(const std::string & opName,
                              MemoryRegionHandle handle)
{
    MLDB_THROW_UNIMPLEMENTED();

#if 0
    auto op = scopedOperation(OperationType::GRID_COMPUTE, "GridComputeContext transferToHostMutableSyncImpl " + opName);

    ExcAssert(handle.handle);

    auto [pin, mem, offset] = getMemoryRegion(opName, *handle.handle, ACC_READ_WRITE);
    auto memPtr = clQueue->clQueue.enqueueMapBufferBlocking(mem, CL_MAP_READ | CL_MAP_WRITE,
                                                            offset, handle.lengthInBytes());
    MutableMemoryRegion result(std::move(memPtr), (char *)memPtr.get(), handle.lengthInBytes());
    return result;
#endif
}
#endif

std::shared_ptr<ComputeKernel>
GridComputeContext::
getKernel(const std::string & kernelName)
{
    auto op = scopedOperation(OperationType::GRID_COMPUTE, "GridComputeContext getKernel " + kernelName);

    std::unique_lock guard(kernelRegistryMutex);
    auto it = kernelRegistry.find(kernelName);
    if (it == kernelRegistry.end()) {
        throw AnnotatedException(400, "Unable to find Grid compute kernel '" + kernelName + "'",
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

#if 0
void
GridComputeContext::
fillDeviceRegionFromHostImpl(const std::string & opName,
                             MemoryRegionHandle deviceHandle,
                             std::shared_ptr<std::span<const std::byte>> pinnedHostRegion,
                             size_t deviceOffset)
{
    MLDB_THROW_UNIMPLEMENTED();
#if 0
    FrozenMemoryRegion region(pinnedHostRegion, (const char *)pinnedHostRegion->data(), pinnedHostRegion->size_bytes());

    return clQueue
        ->enqueueCopyFromHostImpl(opName, deviceHandle, region, deviceOffset, {}).event();
#endif
}                                     

void
GridComputeContext::
fillDeviceRegionFromHostSyncImpl(const std::string & opName,
                                 MemoryRegionHandle deviceHandle,
                                 std::span<const std::byte> hostRegion,
                                 size_t deviceOffset)
{
    FrozenMemoryRegion region(nullptr, (const char *)hostRegion.data(), hostRegion.size_bytes());

    auto [pin, buffer, offset]
        = GridComputeContext::getMemoryRegion(opName, *deviceHandle.handle, ACC_WRITE);

    std::byte * contents = (std::byte *)buffer.GetContents();
    if (contents == nullptr) {
        MLDB_THROW_UNIMPLEMENTED();
    }
    uint32_t length = buffer.GetLength();

    ExcAssertLessEqual(offset + deviceOffset + hostRegion.size(), length);

    memcpy(contents + offset + deviceOffset, hostRegion.data(), hostRegion.size_bytes());

    buffer.DidModify({(uint32_t)deviceOffset, (uint32_t)hostRegion.size_bytes()});

#if 0
    auto commandQueue = mtlDevice.NewCommandQueue();
    ExcAssert(commandQueue);
    auto commandBuffer = commandQueue.CommandBuffer();
    ExcAssert(commandBuffer);
    auto blitEncoder = commandBuffer.BlitCommandEncoder();
    ExcAssert(blitEncoder);

    blitEncoder.
    blitEncoder.Synchronize(buffer);
    blitEncoder.EndEncoding();
    
    commandBuffer.Commit();
    commandBuffer.WaitUntilCompleted();

    auto status = commandBuffer.GetStatus();
    if (status != mtlpp::CommandBufferStatus::Completed) {
        auto error = commandBuffer.GetError();
        throw MLDB::Exception(std::string(error.GetDomain().GetCStr()) + ": " + error.GetLocalizedDescription().GetCStr());
    }

    clQueue
        ->enqueueCopyFromHostImpl(opName, deviceHandle, region, deviceOffset, {})
    .await();
#endif
}
#endif

MemoryRegionHandle
GridComputeContext::
getSliceImpl(const MemoryRegionHandle & handle, const std::string & regionName,
             size_t startOffsetInBytes, size_t lengthInBytes,
             size_t align, const std::type_info & type, bool isConst)
{
    auto op = scopedOperation(OperationType::GRID_COMPUTE, "GridComputeContext getSliceImpl " + regionName);

    auto info = std::dynamic_pointer_cast<const GridMemoryRegionHandleInfo>(std::move(handle.handle));
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

    auto newInfo = std::make_shared<GridMemoryRegionHandleInfo>();

    newInfo->buffer = info->buffer;
    newInfo->offset = info->offset + startOffsetInBytes;
    newInfo->isConst = isConst;
    newInfo->type = &type;
    newInfo->name = regionName;
    newInfo->lengthInBytes = lengthInBytes;
    newInfo->parent = info;
    newInfo->ownerOffset = startOffsetInBytes;
    newInfo->version = info->version;

    return { newInfo };
}


// GridComputeKernel

void
GridComputeKernel::
allowGridPadding()
{
    allowGridPaddingFlag = true;
}

void
GridComputeKernel::
allowGridExpansion()
{
    allowGridExpansionFlag = true;
}

void
GridComputeKernel::
setGridExpression(const std::string & gridExpr, const std::string & blockExpr)
{
    if (!gridExpr.empty())
        this->gridExpression = CommandExpression::parseArgumentExpression(gridExpr);
    if (!blockExpr.empty())
        this->blockExpression = CommandExpression::parseArgumentExpression(blockExpr);
}

void
GridComputeKernel::
addTuneable(const std::string & name, int64_t defaultValue)
{
    this->tuneables.push_back({name, defaultValue});
}

DEFINE_ENUM_DESCRIPTION_INLINE(GridBindFieldActionType)
{
    addValue("SET_FIELD_FROM_PARAM", GridBindFieldActionType::SET_FIELD_FROM_PARAM);
    addValue("SET_FIELD_FROM_KNOWN", GridBindFieldActionType::SET_FIELD_FROM_KNOWN);
}

void
GridBindFieldAction::
apply(void * object,
      const ValueDescription & desc,
      GridComputeContext & context,
      const std::vector<ComputeKernelArgument> & args,
      ComputeKernelConstraintSolution & knowns) const
{
    auto & fieldDesc = desc.getFieldByNumber(fieldNumber);
    std::string opName = "bind: fill field " + fieldDesc.fieldName;
    switch (action) {
    case GridBindFieldActionType::SET_FIELD_FROM_PARAM: opName += " from arg " + std::to_string(argNum);  break;
    case GridBindFieldActionType::SET_FIELD_FROM_KNOWN: opName += " from known " + jsonEncodeStr(expr);  break;
    }

    auto op = scopedOperation(OperationType::GRID_COMPUTE, opName);
    Json::Value value;
    bool done = false;

    switch (action) {
    case GridBindFieldActionType::SET_FIELD_FROM_PARAM: {
        if (argNum == -1 || argNum >= args.size() || !args.at(argNum).handler) {
            // Problem... this parameter wasn't set from an argument.  It must be in known.
            value = knowns.evaluate(*expr);
            break;
        }
        auto & arg = args.at(argNum);
        ExcAssert(arg.handler);
        value = arg.handler->toJson();
        if (arg.handler->type.baseType->kind == ValueKind::ENUM) {
            // we need to convert to an integer
            auto bytes = arg.handler->getPrimitive(opName, context);
            ExcAssertEqual(bytes.size_bytes(), fieldDesc.width);
            fieldDesc.description->copyValue(bytes.data(), fieldDesc.getFieldPtr(object));
            done = true;
        }
        break;
    }
    case GridBindFieldActionType::SET_FIELD_FROM_KNOWN: {
        value = knowns.evaluate(*expr);
        break;
    }
    default:
        throw MLDB::Exception("Can't handle unknown GridBindFieldAction value");
    }

    traceGridOperation("value " + jsonEncodeStr(value));

    // We go through the JSON conversion so that we can handle compatible type coversions,
    // eg uint16_t -> uint32_t.
    if (!done) {
        StructuredJsonParsingContext jsonContext(value);
        fieldDesc.description->parseJson(fieldDesc.getFieldPtr(object), jsonContext);
    }
}

DEFINE_STRUCTURE_DESCRIPTION_INLINE(GridBindFieldAction)
{
    addField("action", &GridBindFieldAction::action, "");
    addField("fieldNumber", &GridBindFieldAction::fieldNumber, "", -1);
    addField("argNum", &GridBindFieldAction::argNum, "", -1);
    addField("expr", &GridBindFieldAction::expr, "");
}

DEFINE_ENUM_DESCRIPTION_INLINE(GridBindActionType)
{
    addValue("SET_BUFFER_FROM_ARG", GridBindActionType::SET_BUFFER_FROM_ARG, "");
    addValue("SET_BUFFER_FROM_STRUCT", GridBindActionType::SET_BUFFER_FROM_STRUCT, "");
    addValue("SET_BUFFER_THREAD_GROUP", GridBindActionType::SET_BUFFER_THREAD_GROUP, "");
}

void
GridBindAction::
applyArg(GridComputeContext & context,
         const std::vector<ComputeKernelArgument> & args,
         ComputeKernelConstraintSolution & knowns,
         bool setKnowns,
         GridBindContext & bindContext) const
{
    const ComputeKernelArgument & arg = args.at(argNum);

    if (!arg.handler) {
        throw MLDB::Exception("argument " + std::to_string(argNum) + " (" + argName
                              + ") was not passed");
    }

    auto j = arg.handler->toJson();
    std::string jStr;
    if (j.isObject()) {
        jStr = j["name"].asString() + ":" + j["version"].toStringNoNewLine();
    }
    else {
        jStr = j.toStringNoNewLine();
    }
    auto op = scopedOperation(OperationType::GRID_COMPUTE, 
                              "GridComputeKernel bind arg " + argName + " type " + arg.handler->type.print()
                              + " from " + jStr 
                              + " at index " + std::to_string(this->arg.GetIndex()));

#if 0
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
#endif

    static std::atomic<int> disamb = 0;
    std::string opName = "bind " + argName + " " + std::to_string(++disamb);

    ExcAssert(arg.handler);  // if there is no handler, we should be in applyStruct

    mtlpp::Buffer memBuffer;
    size_t offset = 0;
    std::shared_ptr<const void> pin;

    if (arg.handler->canGetPrimitive()) {
        auto bytes = arg.handler->getPrimitive(opName, context);
        traceGridOperation("binding primitive with " + std::to_string(bytes.size()) + " bytes and value " + jsonEncodeStr(arg.handler->toJson()));
        commandEncoder.SetBytes(bytes.data(), bytes.size_bytes(), this->arg.GetIndex());
    }
    else if (arg.handler->canGetHandle()) {
        auto handle = arg.handler->getHandle(opName, context);
        traceGridOperation("binding handle with " + std::to_string(handle.lengthInBytes()) + " bytes");
        MemoryRegionAccess access = type.access;

        if (setKnowns) {
            Json::Value known;
            ExcAssert(type.baseType);
            known["elAlign"] = type.baseType->align;
            known["elWidth"] = type.baseType->width;
            known["elType"] = type.baseType->typeName;
            known["length"] = handle.lengthInBytes() / type.baseType->width;
            known["type"] = type.print();
            known["name"] = handle.handle->name;
            known["version"] = handle.handle->version;

            //cerr << "setting known " << argName << " to " << known << endl;

            knowns.setValue(argName, known);
        }

        if (traceSerializer && (access & ACC_READ)) {
            auto [region, version] = context
                .getFrozenHostMemoryRegion(opName + " trace", *handle.handle, 0 /* offset */, -1,
                                            true /* ignore hazards */);
            //bindInfo->traceSerializer->addRegion(region, this->clKernelInfo.args[i].name);
            traceVersion(handle.handle->name, version, region);
        }

        std::tie(pin, memBuffer, offset)
            = context.getMemoryRegion(opName, *handle.handle, access);

        commandEncoder.SetBuffer(memBuffer, offset, this->arg.GetIndex());
    }
    else if (arg.handler->canGetConstRange()) {
        throw MLDB::Exception("param.getConstRange");
    }
    else {
        throw MLDB::Exception("don't know how to handle passing parameter to Grid");
    }
}

void
GridBindAction::
applyStruct(GridComputeContext & context,
            const std::vector<ComputeKernelArgument> & args,
            ComputeKernelConstraintSolution & knowns,
            bool setKnowns,
            GridBindContext & bindContext) const
{
    auto op = scopedOperation(OperationType::GRID_COMPUTE, 
                              "GridComputeKernel bind struct to arg " + argName + " at index " + std::to_string(arg.GetIndex()));

    void * ptr = nullptr;
    if (type.baseType->align < alignof(void *)) {
        ptr = malloc(type.baseType->width);
        if (!ptr)
            throw MLDB::Exception(errno, "malloc");
    }
    else {
        int res = posix_memalign(&ptr, type.baseType->align, type.baseType->width);
        if (res != 0)
            throw MLDB::Exception(res, "posix_memalign");
    }

    try {
        type.baseType->initializeDefault(ptr);
    } MLDB_CATCH_ALL {
        free(ptr);
        throw;
    }

    auto destruct = [desc=type.baseType] (void * ptr)
    {
        try {
            desc->destruct(ptr);
        } MLDB_CATCH_ALL {
            free(ptr);
            throw;
        }
        free(ptr);
    };

    std::shared_ptr<void> result(ptr, std::move(destruct));

    // Fill it in
    for (auto & field: this->fields) {
        field.apply(result.get(), *type.baseType, context, args, knowns);
    }

    commandEncoder.SetBytes(result.get(), type.baseType->width, this->arg.GetIndex());
}

void
GridBindAction::
applyThreadGroup(GridComputeContext & context,
                 const std::vector<ComputeKernelArgument> & args,
                 ComputeKernelConstraintSolution & knowns,
                 bool setKnowns,
                 GridBindContext & bindContext) const
{
    // Calculate the array bound
    auto len = knowns.evaluate(*expr).asUInt();

    // Get the number of bytes
    size_t nbytes = len * type.baseType->width;

    // Must be a multiple of 16 bytes
    while (nbytes % 16 != 0)
        ++nbytes;

    traceGridOperation("binding local array handle with " + std::to_string(nbytes) + " bytes " + " at index " + std::to_string(arg.GetIndex()));

    // Set the knowns for this binding
    if (setKnowns) {
        Json::Value known;
        known["elAlign"] = type.baseType->align;
        known["elWidth"] = type.baseType->width;
        known["elType"] = type.baseType->typeName;
        known["length"] = len;
        known["type"] = type.print();
        known["name"] = "<<<Local array>>>";

        knowns.setValue(this->argName, std::move(known));
    }

    // Set it up in the command encoder
    commandEncoder.SetThreadgroupMemory(nbytes, this->arg.GetIndex());
}

void
GridBindAction::
apply(GridComputeContext & context,
      const std::vector<ComputeKernelArgument> & args,
      ComputeKernelConstraintSolution & knowns,
      bool setKnowns,
      GridBindContext & bindContext) const
{
    switch (action) {
        case GridBindActionType::SET_BUFFER_FROM_ARG:
            applyArg(context, args, knowns, setKnowns, bindContext);
            break;
        case GridBindActionType::SET_BUFFER_FROM_STRUCT:
            applyStruct(context, args, knowns, setKnowns, bindContext);
            break;
        case GridBindActionType::SET_BUFFER_THREAD_GROUP:
            applyThreadGroup(context, args, knowns, setKnowns, bindContext);
            break;
        default:
            throw MLDB::Exception("Unknown grid action");
    }
}

DEFINE_STRUCTURE_DESCRIPTION_INLINE(GridBindAction)
{
    addField("action", &GridBindAction::action, "");
    addField("type", &GridBindAction::type, "");
    addField("argNum", &GridBindAction::argNum, "");
    addField("argName", &GridBindAction::argName, "");
    addField("expr", &GridBindAction::expr, "");
    addField("fields", &GridBindAction::fields, "");
}

void
GridComputeKernel::
setComputeFunction(mtlpp::Library libraryIn,
                   std::string kernelName)
{
    ExcAssert(libraryIn);

    this->mtlLibrary = std::move(libraryIn);
    this->kernelName = kernelName;

    ns::Error error{ns::Handle()};
    mtlpp::FunctionConstantValues constantValues;
    this->mtlFunction = this->mtlLibrary.NewFunction(kernelName.c_str(), constantValues, &error);
    if (error) {
        cerr << "Error making function" << endl;
        cerr << "domain: " << error.GetDomain().GetCStr() << endl;
        cerr << "descrption: " << error.GetLocalizedDescription().GetCStr() << endl;
        if (error.GetLocalizedFailureReason()) {
            cerr << "reason: " << error.GetLocalizedFailureReason().GetCStr() << endl;
        }
    }
    ExcAssert(this->mtlFunction);

    //cerr << this->mtlFunction.GetName().GetCStr() << endl;

    mtlpp::PipelineOption options = (mtlpp::PipelineOption((int)mtlpp::PipelineOption::ArgumentInfo | (int)mtlpp::PipelineOption::BufferTypeInfo));
    this->computePipelineState
        = mtlContext->mtlDevice.NewComputePipelineState(this->mtlFunction, options, reflection, &error);
    if (error) {
        cerr << "Error making pipeline state" << endl;
        cerr << "domain: " << error.GetDomain().GetCStr() << endl;
        cerr << "descrption: " << error.GetLocalizedDescription().GetCStr() << endl;
        if (error.GetLocalizedFailureReason()) {
            cerr << "reason: " << error.GetLocalizedFailureReason().GetCStr() << endl;
        }
    }
    ExcAssert(computePipelineState);
    ExcAssert(reflection);

    auto arguments = reflection.GetArguments();

    correspondingArgumentNumbers.resize(arguments.GetSize());

    std::vector<GridBindAction> actions;

    for (size_t i = 0;  i < arguments.GetSize();  ++i) {
        mtlpp::Argument arg = arguments[i];
        std::string argName = arg.GetName().GetCStr();
        mtlpp::ArgumentType argType = arg.GetType();

        GridBindAction action;
        auto it = paramIndex.find(argName);

        if (argType == mtlpp::ArgumentType::ThreadgroupMemory) {
            if (it == paramIndex.end()) {
                throw MLDB::Exception("Grid ThreadGroup kernel parameter " + argName + " to kernel " + this->kernelName
                                      + " has no corresponding argument to learn its length from");
            }
            int argNum = it->second;
            auto & paramType = params[argNum].type;

            auto width = arg.GetThreadgroupMemoryDataSize();
            auto align = arg.GetThreadgroupMemoryAlignment();

            ExcAssertEqual(paramType.dims.size(), 1);
            ExcAssert(paramType.dims[0].bound);
            ExcAssertEqual(width, paramType.baseType->width);
            ExcAssertEqual(align, paramType.baseType->align);

            action.action = GridBindActionType::SET_BUFFER_THREAD_GROUP;
            action.arg = arg;
            action.type = paramType;
            action.argName = argName;
            action.expr = paramType.dims[0].bound;
        }
        else if (argType == mtlpp::ArgumentType::Buffer) {
            auto type = getKernelType(arg);
            //cerr << "argument " << i << " has name " << argName
            //    << " and index " << arg.GetIndex()
            //    << " and type " << type.print() << endl;

            if (it == paramIndex.end()) {
                if (type.baseType->kind == ValueKind::STRUCTURE && type.dims.empty()) {
                    std::vector<GridBindFieldAction> fieldActions;

                    // Check if we know the unpacked types
                    auto numFields = type.baseType->getFixedFieldCount();
                    std::vector<std::string> fieldsNotFound;

                    for (size_t i = 0;  i < numFields;  ++i) {
                        const auto & field = type.baseType->getFieldByNumber(i);
                        auto it = paramIndex.find(field.fieldName);
                        if (it != paramIndex.end()) {
                            GridBindFieldAction action;
                            action.action = GridBindFieldActionType::SET_FIELD_FROM_PARAM;
                            action.argNum = it->second;
                            action.fieldNumber = i;
                            // Set expr in case the argument isn't passed, in which case it needs to come
                            // from the known values in the context.
                            action.expr = CommandExpression::parseArgumentExpression(field.fieldName);
                            fieldActions.emplace_back(std::move(action));
                            continue;
                        }
                        else {
                            // We're assuming it comes from somewhere...
                            GridBindFieldAction action;
                            action.action = GridBindFieldActionType::SET_FIELD_FROM_KNOWN;
                            action.fieldNumber = i;
                            action.expr = CommandExpression::parseArgumentExpression(field.fieldName);
                            fieldActions.emplace_back(std::move(action));
                            continue;
                        }
                        fieldsNotFound.push_back(field.fieldName);
                    }

                    if (!fieldsNotFound.empty()) {
                        throw MLDB::Exception("Kernel parameter " + std::to_string(i)
                                        + " (" + argName + ") to Grid kernel " + kernelName
                                        + " cannot be assembled because of missing fields " + jsonEncodeStr(fieldsNotFound));
                    }

                    action.action = GridBindActionType::SET_BUFFER_FROM_STRUCT;
                    action.arg = arg;
                    action.type = type;
                    action.argName = argName;
                    action.fields = std::move(fieldActions);
                }
                else if (this->setters.size() > 0) {
                    continue;  // should be done in the setter...
                }
                else if (argType == mtlpp::ArgumentType::ThreadgroupMemory) {
                    throw MLDB::Exception("Thread group parameter in kernel with no setters defined; "
                                            "implement a setter to avoid launch failure");
                }
                else {
                    throw MLDB::Exception("Kernel parameter " + std::to_string(i)
                                            + " (" + argName + ") to Grid kernel " + kernelName
                                            + " has no counterpart in formal parameter list");
                }
            }
            else {
                int argNum = it->second;
                auto & param = params.at(it->second);
                correspondingArgumentNumbers.at(i) = argNum;

                type.dims = param.type.dims;

                action.action = GridBindActionType::SET_BUFFER_FROM_ARG;
                action.arg = arg;
                action.type = type;  // TODO: get information from param type
                action.argNum = it->second;
                action.argName = arg.GetName().GetCStr();

                //cerr << "checking compatibility" << endl;
                //cerr << "param type " << param.type.print() << endl;
                //cerr << "arg type " << type.print() << endl;

                std::string reason;

                if (!param.type.isCompatibleWith(type, &reason)) {
                    throw MLDB::Exception("Kernel parameter " + std::to_string(i)
                                            + " (" + argName + ") to Grid kernel " + kernelName
                                            + ": declared parameter type " + type.print()
                                            + " is not compatible with kernel type " + param.type.print()
                                            + ": " + reason);
                }
            }
        }
        else {
            throw MLDB::Exception("Can't do Grid buffer or texture arguments (yet)");
        }
        //cerr << "got action\n" << jsonEncode(action) << endl;

        actions.emplace_back(std::move(action));
    }

    //cerr << "got actions\n" << jsonEncode(actions) << endl;
    this->bindActions = std::move(actions);
}

BoundComputeKernel
GridComputeKernel::
bindImpl(std::vector<ComputeKernelArgument> argumentsIn, ComputeKernelConstraintSolution knowns) const
{
    auto op = scopedOperation(OperationType::GRID_COMPUTE, "GridComputeKernel bindImpl " + kernelName);

    ExcAssert(this->context);
    std::shared_ptr<GridBindInfo> bindInfo;
    {
        auto op = scopedOperation(OperationType::GRID_COMPUTE, "setup bindInfo");
        bindInfo = this->getGridBindInfo();
        bindInfo->owner = this;
    }

    //cerr << "kernel " << kernelName << " has "
    //     << computePipelineState.GetMaxTotalThreadsPerThreadgroup() << " max threads per thread group, "
    //     << computePipelineState.GetThreadExecutionWidth() << " thread execution width and requires "
    //     << computePipelineState.GetStaticThreadgroupMemoryLength() << " bytes of threadgroup memory"
    //     << endl;

    if (computePipelineState.GetMaxTotalThreadsPerThreadgroup() < 1024) {
        cerr << "kernel " << kernelName << " has maximum execution width of "
             << computePipelineState.GetMaxTotalThreadsPerThreadgroup()
             << " meaning it may be occupancy limited due to register pressure"
             << endl;
    }

    if (traceSerializer) {
        int callNumber = numCalls++;
        bindInfo->traceSerializer = runsSerializer->newStructure(callNumber);

        if (callNumber == 0) {
            auto key = format("%016x", (uint64_t)this->mtlLibrary.GetPtr());

            traceSerializer->newObject("program", key);
            traceSerializer->newObject("kernel", this->kernelName);

#if 0
            // Store the program so that we can replay it later
            std::unique_lock guard(tracedProgramMutex);
            if (tracedPrograms.insert(key).second) {
                auto programInfo = this->clProgram.getProgramInfo();
                auto buildInfo = this->clProgram.getProgramBuildInfo(this->clKernel.getContext().getDevices().at(0));
                auto entry = programsSerializer->newStructure(key);
                entry->newStream("source") << programInfo.source;
                entry->newObject("build", buildInfo);
            }
#endif
        }
    }

    BoundComputeKernel result;
    {
        auto op = scopedOperation(OperationType::GRID_COMPUTE, "construct result");
        result.arguments = std::move(argumentsIn);
        result.owner = this;
        result.bindInfo = bindInfo;

        // Copy constraints over
        result.constraints = this->constraints;
        result.preConstraints = this->preConstraints;
        result.postConstraints = this->postConstraints;
        result.tuneables = this->tuneables;
        result.knowns = std::move(knowns);

        result.setKnownsFromArguments();
    }

    {
        auto op = scopedOperation(OperationType::GRID_COMPUTE, "add constraints");
        // Run the setters to set the other parameters
        //for (auto & s: this->setters) {
        //    s(kernel, upcastContext);
        //}

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
    }

    // figure out the values of the new constraints
    {
        auto op = scopedOperation(OperationType::GRID_COMPUTE, "solve preConstraints");
        result.knowns = solve(result.knowns, result.constraints, result.preConstraints);
    }

    if (bindInfo->traceSerializer) {
        //bindInfo->traceSerializer->newObject("args", argInfo);
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
}


#if 0

void registerGridComputeKernel(const std::string & kernelName,
                                 std::function<std::shared_ptr<GridComputeKernel>(GridComputeContext & context)> generator)
{
    kernelRegistry[kernelName].generate = generator;
}


namespace {

static struct Init {
    Init()
    {
        ComputeRuntime::registerRuntime(ComputeRuntimeId::GRID, "grid",
                                        [] () { return new GridComputeRuntime(); });

        auto getLibrary = [] (GridComputeContext & context) -> mtlpp::Library
        {
            auto compileLibrary = [&] () -> mtlpp::Library
            {
                ns::Error error{ns::Handle()};
                mtlpp::Library library;

                if (false) {
                    std::string fileName = "mldb/builtin/grid/base_kernels.grid";
                    filter_istream stream(fileName);
                    Utf8String source = "#line 1 \"" + fileName + "\"\n" + stream.readAll();

                    mtlpp::CompileOptions compileOptions;
                    library = context.mtlDevice.NewLibrary(source.rawData(), compileOptions, &error);
                }
                else {
                    std::string fileName = "build/arm64/lib/base_kernels_grid.gridlib";
                    library = context.mtlDevice.NewLibrary(fileName.c_str(), &error);
                }

                if (error) {
                    cerr << "Error compiling" << endl;
                    cerr << "domain: " << error.GetDomain().GetCStr() << endl;
                    cerr << "descrption: " << error.GetLocalizedDescription().GetCStr() << endl;
                    if (error.GetLocalizedFailureReason()) {
                        cerr << "reason: " << error.GetLocalizedFailureReason().GetCStr() << endl;
                    }
                }

                ExcAssert(library);

                return library;
            };

            static const std::string cacheKey = "base_kernels";
            mtlpp::Library library = context.getCacheEntry(cacheKey, compileLibrary);
            return library;
        };
    
        auto createBlockFillArrayKernel = [getLibrary] (GridComputeContext& context) -> std::shared_ptr<GridComputeKernel>
        {
            auto library = getLibrary(context);
            auto result = std::make_shared<GridComputeKernel>(&context);
            result->kernelName = "__blockFillArray";
            result->allowGridExpansion();
            result->addParameter("region", "w", "u8[regionLength]");
            result->addParameter("startOffsetInBytes", "r", "u64");
            result->addParameter("lengthInBytes", "r", "u64");
            result->addParameter("blockData", "r", "u8[blockLengthInBytes]");
            result->addParameter("blockLengthInBytes", "r", "u64");
            result->addConstraint("blockLengthInBytes", ">", "0", "Avoid divide by zero");
            result->addConstraint("(lengthInBytes-startOffsetInBytes) % blockLengthInBytes", "==", "0", "Block must divide evenly in region");
            result->addConstraint("numBlocks", "==", "(lengthInBytes-startOffsetInBytes)/blockLengthInBytes");
            result->addTuneable("threadsPerBlock", 256);
            result->addTuneable("blocksPerGrid", 16);
            result->allowGridPadding();
            result->setGridExpression("[min(blocksPerGrid,ceilDiv(numBlocks, threadsPerBlock))]", "[threadsPerBlock]");
            result->setComputeFunction(library, "__blockFillArrayKernel");

            return result;
        };

        registerGridComputeKernel("__blockFillArray", createBlockFillArrayKernel);

        auto createZeroFillArrayKernel = [getLibrary] (GridComputeContext& context) -> std::shared_ptr<GridComputeKernel>
        {
            auto library = getLibrary(context);
            auto result = std::make_shared<GridComputeKernel>(&context);
            result->kernelName = "__zeroFillArray";
            result->allowGridExpansion();
            result->addParameter("region", "w", "u8[regionLength]");
            result->addParameter("startOffsetInBytes", "r", "u64");
            result->addParameter("lengthInBytes", "r", "u64");
            result->addTuneable("threadsPerBlock", 256);
            result->addTuneable("blocksPerGrid", 16);
            result->allowGridPadding();
            result->setGridExpression("[blocksPerGrid]", "[threadsPerBlock]");
            result->setComputeFunction(library, "__zeroFillArrayKernel");

            return result;
        };

        registerGridComputeKernel("__zeroFillArray", createZeroFillArrayKernel);
    }

} init;

} // file scope

#endif

} // namespace MLDB
