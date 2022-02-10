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
#include "mldb/arch/timers.h"
#include <compare>

using namespace std;

namespace MLDB {

DEFINE_ENUM_DESCRIPTION_INLINE(GridComputeFunctionArgumentDisposition)
{
    addValue("BUFFER", GridComputeFunctionArgumentDisposition::BUFFER, "");
    addValue("LITERAL", GridComputeFunctionArgumentDisposition::LITERAL, "");
    addValue("THREADGROUP", GridComputeFunctionArgumentDisposition::THREADGROUP, "");
}

DEFINE_STRUCTURE_DESCRIPTION_INLINE(GridComputeFunctionArgument)
{
    addField("name", &GridComputeFunctionArgument::name, "");
    addField("computeFunctionArgIndex", &GridComputeFunctionArgument::computeFunctionArgIndex, "");
    addField("type", &GridComputeFunctionArgument::type, "");
    addField("disposition", &GridComputeFunctionArgument::disposition, "");
    addField("implInfo", &GridComputeFunctionArgument::implInfo, "");
}

EnvOption<int> GRID_TRACE_API_CALLS("GRID_COMPUTE_TRACE_API_CALLS", 0);

bool gridTraceApiCalls()
{
    return GRID_TRACE_API_CALLS.get();
}

namespace {

std::mutex kernelRegistryMutex;
struct KernelRegistryEntry {
    std::function<std::shared_ptr<GridComputeKernelTemplate>(GridComputeContext & context)> generate;
};

std::map<std::string, KernelRegistryEntry> kernelRegistry;

EnvOption<std::string, true> GRID_KERNEL_TRACE_FILE("GRID_KERNEL_TRACE_FILE", "");

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

} // file scope


void traceOperationImpl(OperationScope opScope, OperationType opType, const std::string & opName,
                        const std::string & renderedArgs)
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
            opColor = ansi::ansi_str_cyan();
            break;
        case OperationType::METAL_COMPUTE:
            opTypeName = "METAL:";
            opColor = ansi::ansi_str_blue();
            break;
        case OperationType::OPENCL_COMPUTE:
            opTypeName = "OPENCL:";
            opColor = ansi::ansi_str_green();
            break;
        case OperationType::CPU_COMPUTE:
            opTypeName = "CPU:";
            opColor = ansi::ansi_str_yellow();
            break;
        case OperationType::CUDA_COMPUTE:
            opTypeName = "CUDA:";
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
                               + ansi_str_reset() + (renderedArgs.empty() ? "" : " ") + renderedArgs + "\n";
            cerr << toDump << flush;
        }

        lastTime = startTimer.elapsed_wall();
    }
}


ScopedOperation::
~ScopedOperation()
{
    if (timer) {
        --opCount;

        if (std::uncaught_exceptions()) {
            traceOperation(OperationScope::EXIT_EXC, opType, opName);
            return;
        }

        double elapsed = timer->elapsed_wall();
        traceOperation(OperationScope::EXIT, opType, opName + " [" + ansi::ansi_str_underline() + printTime(elapsed) + "]");
    }
}

void ScopedOperation::createTimer()
{
    timer = std::make_shared<Timer>();
}

void ScopedOperation::incrementOpCount()
{
    ++opCount;
}


// GridMemoryRegionHandleInfo

std::tuple<FrozenMemoryRegion, int /* version */>
GridMemoryRegionHandleInfo::
getReadOnlyHostAccessSync(const GridComputeContext & context,
                            const std::string & opName,
                            size_t offset,
                            ssize_t length,
                            bool ignoreHazards)
{
    std::shared_ptr<const void> pin;
    if (!ignoreHazards && false) {
        pin = manager->pinAccess(opName, this, ACC_READ);
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

std::shared_ptr<const void>
GridMemoryRegionHandleInfo::
pinAccess(const std::string & opName, MemoryRegionAccess access)
{
    return manager->pinAccess(opName, this, access);
}

std::shared_ptr<const void>
GridMemoryRegionHandleInfo::Manager::
pinAccess(const std::string & opName, MemoryRegionHandleInfo * info, MemoryRegionAccess access)
{
    unique_lock guard(mutex);

    if (access == ACC_NONE) {
        throw MLDB::Exception("Asked for no access to region");
    }

    if (access == ACC_READ) {
        if (numWriters != 0 && false) {
            throw MLDB::Exception("Operation '" + opName + "' attempted to read region '" + info->name + "' with active writer(s) "
                                    + jsonEncodeStr(currentWriters) + " and active reader(s) "
                                    + jsonEncodeStr(currentReaders));
        }
        if (!currentReaders.insert(opName).second) {
            throw MLDB::Exception("Operation '" + opName + " ' is already reading region '" + info->name + "'");
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
        return pin;
    }
    else {  // read only or read write
        if (currentReaders.count(opName)) {
            throw MLDB::Exception("Operation '" + opName + " ' is already reading region '" + info->name + "'");
        }
        if (currentWriters.count(opName)) {
            throw MLDB::Exception("Operation '" + opName + " ' is already writing region '" + info->name + "'");
        }
        if ((numWriters != 0 || numReaders != 0) && false) {
            throw MLDB::Exception("Operation '" + opName + "' attempted to write region '"
                                    + info->name + "' with active writer(s) "
                                    + jsonEncodeStr(currentWriters) + " and/or active reader(s) "
                                    + jsonEncodeStr(currentReaders));
        }
        ++info->version;
        ++numWriters;
        currentWriters.insert(opName);
        if (access == ACC_READ_WRITE) {
            ++numReaders;
            currentReaders.insert(opName);
        }

        auto done = [this, info, access, opName] (const void * mem)
        {
            unique_lock guard(mutex);
            ++info->version;
            --numWriters;
            currentWriters.erase(opName);
            if (access == ACC_READ_WRITE) {
                --numReaders;
                currentReaders.erase(opName);
            }
        };

        //cerr << "returning r/w version " << version << " of " << name << " for " << opName << endl;

        //this->buffer.SetPurgeableState(mtlpp::PurgeableState::NonVolatile);

        auto pin = std::shared_ptr<void>(nullptr, done);
        return pin;
    }
}

// GridComputeProfilingInfo

GridComputeProfilingInfo::
GridComputeProfilingInfo()
{
}


// GridComputeEvent

GridComputeEvent::
GridComputeEvent(const std::string & label, bool isResolvedIn, const GridComputeQueue * owner)
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
    if (parent) {
        weakParent = parent->weak_from_this();
        ExcAssertEqual(parent->numChildren.fetch_add(1), 0);
    }
}

GridComputeQueue::~GridComputeQueue()
{
    auto sharedParent = weakParent.lock();

    if (parent && !sharedParent) {
        cerr << "ERROR: parent queue was destroyed BEFORE its children" << endl;
    }

    if (sharedParent) {
        if (sharedParent->numChildren.fetch_sub(1) == 0) {
            cerr << "ERROR: parent queue didn't know about its children" << endl;
            abort();
        }
    }

    if (numChildren != 0) {
        cerr << "ERROR: parent queue being destroyed with active children" << endl;
        abort();
    }
}

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

        auto bindContext = this->newBindContext(opName, kernel, bindInfo);

        constexpr bool solveAfter = false;

        for (auto & action: kernel->bindActions) {
            action.apply(*this, bound.arguments, knowns, solveAfter, *bindContext);
        }

        if (solveAfter) {
            knowns = solve(knowns, bound.constraints, bound.preConstraints);
        }
        //knowns = fullySolve(knowns, bound.constraints, bound.preConstraints);

        bindContext->launch(opName, *bindContext, mtlGrid, mtlBlock);
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
        this->enqueueZeroFillArrayConcrete(opName, region, startOffsetInBytes, lengthInBytes);
        return;
    case MemoryRegionInitialization::INIT_BLOCK_FILLED:
        this->enqueueBlockFillArrayConcrete(opName, region, startOffsetInBytes, lengthInBytes, block);
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
    finish("copyFromHostSyncImpl");
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

    return transferToHostSyncConcrete(opName, handle);
}

MemoryRegionHandle
GridComputeQueue::
enqueueManagePinnedHostRegionImpl(const std::string & opName, std::span<const std::byte> region, size_t align,
                                  const std::type_info & type, bool isConst)
{
    auto op = scopedOperation(OperationType::GRID_COMPUTE, "GridComputeContext managePinnedHostRegionImpl " + opName);
    return managePinnedHostRegionSyncImpl(opName, region, align, type, isConst);
}

#if 0
MemoryRegionHandle
GridComputeQueue::
managePinnedHostRegionSyncImpl(const std::string & opName,
                               std::span<const std::byte> region, size_t align,
                               const std::type_info & type, bool isConst)
{
    auto op = scopedOperation(OperationType::GRID_COMPUTE, "GridComputeContext managePinnedHostRegionSyncImpl " + opName);

    //cerr << "managing pinned host region " << opName << " of length " << region.size() << endl;

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
#endif

void
GridComputeQueue::
finish(const std::string & opName)
{
    auto op = scopedOperation(OperationType::GRID_COMPUTE, opName + " GridComputeQueue finish");
    flush(opName)->await();
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
    : device(device)
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

    // First, get the template
    // TODO: cache it...

    auto tmplate = it->second.generate(*this);

    // Secondly, specialize the template
    auto result = specializeKernel(*tmplate);

    if (traceSerializer) {
        result->traceSerializer = kernelsSerializer->newStructure(kernelName);
        result->runsSerializer = result->traceSerializer->newStructure("runs");
    }
    
    return result;
}

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

    std::shared_ptr<GridMemoryRegionHandleInfo> newInfo(info->clone());

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

std::shared_ptr<GridBindInfo>
GridComputeKernel::
getGridBindInfo() const
{
    return std::make_shared<GridBindInfo>(this);
}


// GridBindFieldAction

DEFINE_ENUM_DESCRIPTION_INLINE(GridBindFieldActionType)
{
    addValue("SET_FIELD_FROM_PARAM", GridBindFieldActionType::SET_FIELD_FROM_PARAM);
    addValue("SET_FIELD_FROM_KNOWN", GridBindFieldActionType::SET_FIELD_FROM_KNOWN);
}

void
GridBindFieldAction::
apply(void * object,
      const ValueDescription & desc,
      GridComputeQueue & queue,
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
            auto bytes = arg.handler->getPrimitive(opName, queue);
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
        MLDB_THROW_UNIMPLEMENTED("Can't handle unknown GridBindFieldAction value");
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
    addValue("SET_FROM_ARG", GridBindActionType::SET_FROM_ARG, "");
    addValue("SET_FROM_STRUCT", GridBindActionType::SET_FROM_STRUCT, "");
    addValue("SET_FROM_KNOWN", GridBindActionType::SET_FROM_KNOWN, "");
    addValue("SET_THREAD_GROUP", GridBindActionType::SET_THREAD_GROUP, "");
}

void
GridBindAction::
applyArg(GridComputeQueue & queue,
         const std::vector<ComputeKernelArgument> & args,
         ComputeKernelConstraintSolution & knowns,
         bool setKnowns,
         GridBindContext & bindContext) const
{
    const ComputeKernelArgument & arg = args.at(argNum);

    Json::Value j;
    ComputeKernelType type;

    if (arg.handler) {
        j = arg.handler->toJson();
        type = arg.handler->type;
    }
    else {
        if (knowns.hasValue(argName)) {
            j = knowns.getValue(argName);
            type = this->type;
        }
        else {
            throw MLDB::Exception("argument " + std::to_string(argNum) + " (" + argName
                                + ") was not passed");
        }
    }

    std::string jStr;
    if (j.isObject()) {
        jStr = j["name"].asString() + ":" + j["version"].toStringNoNewLine();
    }
    else {
        jStr = j.toStringNoNewLine();
    }
    auto op = scopedOperation(OperationType::GRID_COMPUTE, 
                              "GridComputeKernel bind arg " + argName + " type " + type.print()
                              + " from " + jStr + " at index " + std::to_string(argNum));

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

    if (arg.handler) {
        //cerr << "has handler" << endl;
        if (arg.handler->canGetPrimitive()) {
            auto bytes = arg.handler->getPrimitive(opName, queue);
            traceGridOperation("binding primitive with " + std::to_string(bytes.size()) + " bytes and value " + jsonEncodeStr(arg.handler->toJson()));
            bindContext.setPrimitive(opName, computeFunctionArgIndex, bytes);
        }
        else if (arg.handler->canGetHandle()) {
            auto handle = arg.handler->getHandle(opName, queue);
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
                auto [region, version] = queue.gridOwner
                    ->getFrozenHostMemoryRegion(opName + " trace", *handle.handle, 0 /* offset */, -1,
                                                true /* ignore hazards */);
                //bindInfo->traceSerializer->addRegion(region, this->clKernelInfo.args[i].name);
                traceVersion(handle.handle->name, version, region);
            }

            bindContext.setBuffer(opName, computeFunctionArgIndex, dynamic_pointer_cast<GridMemoryRegionHandleInfo>(handle.handle), access);
        }
        else if (arg.handler->canGetConstRange()) {
            throw MLDB::Exception("param.getConstRange");
        }
        else {
            throw MLDB::Exception("don't know how to handle passing parameter to Grid");
        }
    }
    else {
        //cerr << "has no handler" << endl;
        traceGridOperation("binding primitive with value " + jStr);
        Any typedVal(j, type.baseType.get());
        bindContext.setPrimitive(opName, computeFunctionArgIndex, typedVal.asBytes());
    }
}

void
GridBindAction::
applyKnown(GridComputeQueue & queue,
            const std::vector<ComputeKernelArgument> & args,
            ComputeKernelConstraintSolution & knowns,
            bool setKnowns,
            GridBindContext & bindContext) const
{
    Json::Value j;
    ComputeKernelType type;
    if (knowns.hasValue(argName)) {
        j = knowns.getValue(argName);
        type = this->type;
    }
    else {
        throw MLDB::Exception("argument " + std::to_string(argNum) + " (" + argName
                            + ") was not passed");
    }

    std::string jStr;
    if (j.isObject()) {
        jStr = j["name"].asString() + ":" + j["version"].toStringNoNewLine();
    }
    else {
        jStr = j.toStringNoNewLine();
    }

    std::string opName = "bind " + argName + " type " + type.print() + " from known " + jStr;

    auto op = scopedOperation(OperationType::GRID_COMPUTE, opName);

    Any typedVal(j, type.baseType.get());
    bindContext.setPrimitive(opName, computeFunctionArgIndex, typedVal.asBytes());
}

void
GridBindAction::
applyStruct(GridComputeQueue & queue,
            const std::vector<ComputeKernelArgument> & args,
            ComputeKernelConstraintSolution & knowns,
            bool setKnowns,
            GridBindContext & bindContext) const
{
    std::string opName = "GridComputeKernel bind struct to arg " + argName + " at index " + std::to_string(argNum);
    auto op = scopedOperation(OperationType::GRID_COMPUTE, opName);

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
        field.apply(result.get(), *type.baseType, queue, args, knowns);
    }

    std::span<const std::byte> bytes((const std::byte *)result.get(), type.baseType->width);

    bindContext.setPrimitive(opName, computeFunctionArgIndex, bytes);
}

void
GridBindAction::
applyThreadGroup(GridComputeQueue & queue,
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

    std::string opName = "binding local array " + argName + " with " + std::to_string(nbytes) + " bytes at index " + std::to_string(argNum);
    traceGridOperation(opName);

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

    // Set it up
    bindContext.setThreadGroupMemory(opName, computeFunctionArgIndex, nbytes);
}

void
GridBindAction::
apply(GridComputeQueue & queue,
      const std::vector<ComputeKernelArgument> & args,
      ComputeKernelConstraintSolution & knowns,
      bool setKnowns,
      GridBindContext & bindContext) const
{
    //cerr << "applying " << jsonEncodeStr(*this) << endl;

    switch (action) {
        case GridBindActionType::SET_FROM_ARG:
            applyArg(queue, args, knowns, setKnowns, bindContext);
            break;
        case GridBindActionType::SET_FROM_STRUCT:
            applyStruct(queue, args, knowns, setKnowns, bindContext);
            break;
        case GridBindActionType::SET_FROM_KNOWN:
            applyKnown(queue, args, knowns, setKnowns, bindContext);
            break;
        case GridBindActionType::SET_THREAD_GROUP:
            applyThreadGroup(queue, args, knowns, setKnowns, bindContext);
            break;
        default:
            throw MLDB::Exception("Unknown grid action");
    }
}

DEFINE_STRUCTURE_DESCRIPTION_INLINE(GridBindAction)
{
    addField("action", &GridBindAction::action, "");
    addField("type", &GridBindAction::type, "");
    addField("computeFunctionArgIndex", &GridBindAction::computeFunctionArgIndex, "");
    addField("argNum", &GridBindAction::argNum, "");
    addField("argName", &GridBindAction::argName, "");
    addField("expr", &GridBindAction::expr, "");
    addField("fields", &GridBindAction::fields, "");
}

GridComputeKernelSpecialization::
GridComputeKernelSpecialization(GridComputeContext * owner, const GridComputeKernelTemplate & tmplate)
    : GridComputeKernel(owner)
{
    // Initialize all of the generic fields from the template
    *(GridComputeKernel *)this = (const GridComputeKernel &)tmplate;

    this->kernelName = tmplate.kernelName;
    this->gridLibrary = owner->getLibrary(tmplate.libraryName);
    this->gridFunction = this->gridLibrary->getFunction(tmplate.kernelName);
    auto kernelArgs = this->gridFunction->getArgumentInfo();

    correspondingArgumentNumbers.resize(kernelArgs.size());

    std::vector<GridBindAction> actions;

    for (size_t i = 0;  i < kernelArgs.size();  ++i) {
        auto & arg = kernelArgs[i];
        const std::string & argName = arg.name;
        ComputeKernelType type = arg.type;  // we take a copy as we modify in the function
        auto disposition = arg.disposition;

        GridBindAction action;
        action.computeFunctionArgIndex = arg.computeFunctionArgIndex;

        auto it = paramIndex.find(argName);

        if (disposition == GridComputeFunctionArgumentDisposition::THREADGROUP) {
            if (arg.type.dims.empty() || !arg.type.dims[0].bound) {
                if (it == paramIndex.end()) {
                    throw MLDB::Exception("Grid thread group kernel parameter " + argName + " to kernel " + this->kernelName
                                        + " has no corresponding argument to learn its length from");
                }
                int argNum = it->second;
                auto & paramType = params[argNum].type;

                auto width = type.baseType->width;  //arg.GetThreadgroupMemoryDataSize();
                auto align = type.baseType->align;  //arg.GetThreadgroupMemoryAlignment();

                ExcAssertEqual(paramType.dims.size(), 1);
                ExcAssert(paramType.dims[0].bound);
                ExcAssertEqual(width, paramType.baseType->width);
                ExcAssertEqual(align, paramType.baseType->align);

                action.argNum = argNum;
                action.type = paramType;
                action.expr = paramType.dims[0].bound;
            }
            else {
                action.type = type;
                action.expr = type.dims[0].bound;
            }

            action.action = GridBindActionType::SET_THREAD_GROUP;
            action.arg = arg;
            action.argName = argName;
        }
        else if (disposition == GridComputeFunctionArgumentDisposition::BUFFER) {
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

                    action.action = GridBindActionType::SET_FROM_STRUCT;
                    action.arg = arg;
                    action.type = type;
                    action.argName = argName;
                    action.fields = std::move(fieldActions);
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

                action.action = GridBindActionType::SET_FROM_ARG;
                action.arg = arg;
                action.type = type;  // TODO: get information from param type
                action.argNum = it->second;
                action.argName = argName;

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
        else if (disposition == GridComputeFunctionArgumentDisposition::LITERAL) {
            //cerr << "setting LITERAL parameter " << argName << " of type " << type.print() << endl;
            if (it == paramIndex.end()) {
                // Set it from a known
                ExcAssert(type.dims.empty());

                action.action = GridBindActionType::SET_FROM_KNOWN;
                action.arg = arg;
                action.type = type;  // TODO: get information from param type
                action.argNum = -1;
                action.argName = argName;
                action.expr = CommandExpression::parseArgumentExpression(argName);
            }
            else {
                // Set it from a parameter
                int argNum = it->second;
                auto & param = params.at(it->second);
                correspondingArgumentNumbers.at(i) = argNum;

                type.dims = param.type.dims;

                action.action = GridBindActionType::SET_FROM_ARG;
                action.arg = arg;
                action.type = type;  // TODO: get information from param type
                action.argNum = argNum;
                action.argName = argName;

                std::string reason;

                if (!param.type.isCompatibleWith(type, &reason)) {
                    throw MLDB::Exception("Kernel literal parameter " + std::to_string(i)
                                            + " (" + argName + ") to Grid kernel " + kernelName
                                            + ": declared parameter type " + type.print()
                                            + " is not compatible with kernel type " + param.type.print()
                                            + ": " + reason);
                }
            }
        }
        else {
            MLDB_THROW_UNIMPLEMENTED("Can't do Grid sampler or texture arguments (yet)");
        }
        //cerr << "got action\n" << jsonEncode(action) << endl;

        actions.emplace_back(std::move(action));
    }

    //cerr << "got actions\n" << jsonEncode(actions) << endl;
    this->bindActions = std::move(actions);
}

BoundComputeKernel
GridComputeKernelSpecialization::
bindImpl(ComputeQueue & queue,
         std::vector<ComputeKernelArgument> argumentsIn,
         ComputeKernelConstraintSolution knowns) const
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
#if 0
    if (computePipelineState.GetMaxTotalThreadsPerThreadgroup() < 1024) {
        cerr << "kernel " << kernelName << " has maximum execution width of "
             << computePipelineState.GetMaxTotalThreadsPerThreadgroup()
             << " meaning it may be occupancy limited due to register pressure"
             << endl;
    }
#endif

    if (traceSerializer) {
        int callNumber = numCalls++;
        bindInfo->traceSerializer = runsSerializer->newStructure(callNumber);

        if (callNumber == 0) {
            auto key = this->gridLibrary->getId();
            traceSerializer->newObject("program", key);
            traceSerializer->newObject("kernel", this->kernelName);

            // Store the program metadata so that we can replay it later
            std::unique_lock guard(tracedProgramMutex);
            if (tracedPrograms.insert(key).second) {
                auto entry = programsSerializer->newStructure(key);
                entry->newObject("metadata", this->gridLibrary->getMetadata());
            }
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

void
GridComputeKernelTemplate::
setComputeFunction(std::string libraryName,
                   std::string kernelName)
{
    this->libraryName = libraryName;
    this->kernelName = kernelName;
}

std::shared_ptr<GridBindInfo>
GridComputeKernelTemplate::
getGridBindInfo() const
{
    MLDB_THROW_UNIMPLEMENTED();
}

BoundComputeKernel
GridComputeKernelTemplate::
bindImpl(ComputeQueue & queue,
         std::vector<ComputeKernelArgument> arguments,
         ComputeKernelConstraintSolution knowns) const
{
    MLDB_THROW_UNIMPLEMENTED();
}


void registerGridComputeKernel(const std::string & kernelName,
                               std::function<std::shared_ptr<GridComputeKernelTemplate>(GridComputeContext & context)> generator)
{
    kernelRegistry[kernelName].generate = generator;
}

namespace {

static struct Init {
    Init()
    {
        auto createBlockFillArrayKernel = [] (GridComputeContext& context) -> std::shared_ptr<GridComputeKernelTemplate>
        {
            auto result = std::make_shared<GridComputeKernelTemplate>(&context);
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
            result->setComputeFunction("base_kernels", "__blockFillArrayKernel");

            return result;
        };

        registerGridComputeKernel("__blockFillArray", createBlockFillArrayKernel);

        auto createZeroFillArrayKernel = [] (GridComputeContext& context) -> std::shared_ptr<GridComputeKernelTemplate>
        {
            auto result = std::make_shared<GridComputeKernelTemplate>(&context);
            result->kernelName = "__zeroFillArray";
            result->allowGridExpansion();
            result->addParameter("region", "w", "u8[regionLength]");
            result->addParameter("startOffsetInBytes", "r", "u64");
            result->addParameter("lengthInBytes", "r", "u64");
            result->addTuneable("threadsPerBlock", 256);
            result->addTuneable("blocksPerGrid", 16);
            result->allowGridPadding();
            result->setGridExpression("[blocksPerGrid]", "[threadsPerBlock]");
            result->setComputeFunction("base_kernels", "__zeroFillArrayKernel");

            return result;
        };

        registerGridComputeKernel("__zeroFillArray", createZeroFillArrayKernel);
    }

} init;

} // file scope

} // namespace MLDB
