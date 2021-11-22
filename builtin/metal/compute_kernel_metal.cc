/** compute_kernel_metal.cc                                                -*- C++ -*-
    Jeremy Barnes, 27 March 2016
    This file is part of MLDB. Copyright 2016 mldb.ai inc. All rights reserved.

    Compute kernel runtime for CPU devices.
*/

#include "compute_kernel_metal.h"
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
#include <compare>

using namespace std;

namespace MLDB {

//#define THROW_UNIMPLEMENTED do { throw MLDB::Exception("Unimplemented"); } while (false)

#define THROW_UNIMPLEMENTED do { throw MLDB::Exception(std::string("Unimplemented: ") + __PRETTY_FUNCTION__ + " at " + __FILE__ + ":" + std::to_string(__LINE__)); } while (false)  // + " at " + __FILE__ + ":" + __LINE__)

namespace {

std::mutex kernelRegistryMutex;
struct KernelRegistryEntry {
    std::function<std::shared_ptr<MetalComputeKernel>(MetalComputeContext & context)> generate;
};

std::map<std::string, KernelRegistryEntry> kernelRegistry;

struct MetalBindInfo: public ComputeKernelBindInfo {
    virtual ~MetalBindInfo() = default;

    mtlpp::Function mtlFunction;
    mtlpp::ComputePipelineState mtlPipelineState;
    const MetalComputeKernel * owner = nullptr;
    std::shared_ptr<StructuredSerializer> traceSerializer;

    // Pins that control the lifetime of the arguments and allow the system to know
    // when an argument is no longer needed
    std::vector<std::shared_ptr<const void>> argumentPins;
};

EnvOption<int> METAL_TRACE_API_CALLS("METAL_COMPUTE_TRACE_API_CALLS", 0);
EnvOption<std::string> METAL_KERNEL_TRACE_FILE("METAL_KERNEL_TRACE_FILE", "");

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
        if (METAL_KERNEL_TRACE_FILE.specified()) {
            traceSerializer = std::make_shared<ZipStructuredSerializer>(METAL_KERNEL_TRACE_FILE.get());
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

template<typename... Args>
void traceOperation(const std::string & opName, Args&&... args)
{
    if (METAL_TRACE_API_CALLS.get()) {
        using namespace MLDB::ansi;
        int tid = std::hash<std::thread::id>()(std::this_thread::get_id());
        double elapsed = startTimer.elapsed_wall();

        std::string header = format("%10.6f t%8x %2d  METAL COMPUTE: ", elapsed, tid, opCount);
        std::string indent(4 * opCount, ' ');
        std::string toDump = (string)ansi_str_blue() + header + indent + ansi_str_bold() + opName
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

            if (std::current_exception()) {
                traceOperation(std::string(ansi::ansi_str_red()) + "EXCEPTION " + opName);
                return;
            }

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

struct MetalMemoryRegionHandleInfo: public MemoryRegionHandleInfo {
    mtlpp::Buffer buffer;
    size_t offset = 0;

    // If we're managing host memory, this is where it is.
    const std::byte * backingHostMem = nullptr;

    std::mutex mutex;
    int numReaders = 0;
    int numWriters = 0;
    std::set<std::string> currentWriters;
    std::set<std::string> currentReaders;

    void init(mtlpp::Buffer mem, size_t offset)
    {
        this->buffer = std::move(mem);
        this->offset = offset;
        this->version = 0;
    }

    std::tuple<FrozenMemoryRegion, int /* version */>
    getReadOnlyHostAccessSync(const MetalComputeContext & context,
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

        THROW_UNIMPLEMENTED;

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

    std::tuple<std::shared_ptr<const void>, mtlpp::Buffer>
    getMetalAccess(const std::string & opName, MemoryRegionAccess access)
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

            auto pin =  std::shared_ptr<void>(nullptr, done);
            return { std::move(pin), this->buffer };
        }
    }
};


} // file scope

// MetalComputeProfilingInfo

MetalComputeProfilingInfo::
MetalComputeProfilingInfo()
{
}


// MetalComputeEvent

MetalComputeEvent::
MetalComputeEvent()
{
    this->isResolved = true;
}

std::shared_ptr<ComputeProfilingInfo>
MetalComputeEvent::
getProfilingInfo() const
{
    THROW_UNIMPLEMENTED;
}

void
MetalComputeEvent::
await() const
{
    // For now, we only create satisfied events...
}

std::shared_ptr<ComputeEvent>
MetalComputeEvent::
thenImpl(std::function<void ()> fn)
{
    std::unique_lock guard(mutex);
    if (isResolved) {
        // If already satisfied, we simply run the callback
        fn();
        return makeAlreadyResolvedEvent();
    }

    // Otherwise, create a user event for the post-then part
    auto nextEvent = makeUnresolvedEvent();

    auto cb = [fn=std::move(fn), nextEvent] ()
    {
        fn();
        nextEvent->resolve();
    };

    callbacks.push_back(cb);

    return nextEvent;
}

void
MetalComputeEvent::
resolve()
{
    THROW_UNIMPLEMENTED;
}

std::shared_ptr<MetalComputeEvent>
MetalComputeEvent::
makeAlreadyResolvedEvent()
{
    auto result = std::make_shared<MetalComputeEvent>();
    result->isResolved = true;
    return result;
}

std::shared_ptr<MetalComputeEvent>
MetalComputeEvent::
makeUnresolvedEvent()
{
    auto result = std::make_shared<MetalComputeEvent>();
    result->isResolved = false;
    return result;
}

std::function<void ()>
MetalComputeEvent::
getCompletionHandler() const
{
    THROW_UNIMPLEMENTED;
}


// MetalComputeQueue

MetalComputeQueue::
MetalComputeQueue(MetalComputeContext * owner, mtlpp::CommandQueue queue)
    : ComputeQueue(owner), mtlOwner(owner), mtlQueue(std::move(queue))
{
}

MetalComputeQueue::
MetalComputeQueue(MetalComputeContext * owner)
    : ComputeQueue(owner), mtlOwner(owner)
{
    mtlQueue = owner->mtlDevice.NewCommandQueue();
}

std::shared_ptr<ComputeEvent>
MetalComputeQueue::
launch(const std::string & opName,
       const BoundComputeKernel & bound,
       const std::vector<uint32_t> & grid,
       const std::vector<std::shared_ptr<ComputeEvent>> & prereqs)
{
    try {
        auto tr = scopedOperation("launch kernel " + bound.owner->kernelName + " as " + opName);

        ExcAssert(bound.bindInfo);
        
        const MetalBindInfo * bindInfo
            = dynamic_cast<const MetalBindInfo *>(bound.bindInfo.get());
        ExcAssert(bindInfo);

        const MetalComputeKernel * kernel = bindInfo->owner;

        CommandExpressionContext knowns(bound.knowns);

        for (size_t i = 0;  i < grid.size();  ++i) {
            auto & dim = kernel->dims[i];
            knowns.setValue(dim.range, grid[i]);
        }

        std::vector<size_t> mtlGrid, mtlBlock = kernel->block;
        
        if (kernel->allowGridExpansionFlag)
            ExcAssertLessEqual(grid.size(), mtlBlock.size());
        else
            ExcAssertEqual(grid.size(), mtlBlock.size());

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
                    throw MLDB::Exception("Metal kernel '" + kernel->kernelName + "' won't launch "
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

        if (kernel->modifyGrid)
            kernel->modifyGrid(mtlGrid, mtlBlock);

        knowns.setValue("grid", grid);
        knowns.setValue("mtlGrid", mtlGrid);
        knowns.setValue("mtlBlock", mtlBlock);

        // figure out the values of the new constraints
        bool progress = true;
        std::set<std::string> unknowns;

        while (progress) {
            progress = false;
            for (auto & c: bound.constraints) {
                progress = progress || c.attemptToSatisfy(knowns, unknowns);
            }
        }

        if (kernel->gridExpression) {
            mtlGrid = jsonDecode<decltype(mtlGrid)>(kernel->gridExpression->apply(knowns));
            knowns.setValue("mtlGrid", mtlGrid);
        }

        if (kernel->blockExpression) {
            mtlBlock = jsonDecode<decltype(mtlBlock)>(kernel->blockExpression->apply(knowns));
            knowns.setValue("mtlBlock", mtlBlock);
        }

        //cerr << "launching kernel " << kernel->kernelName << " with grid " << clGrid << " and block " << clBlock << endl;
        //cerr << "this->block = " << jsonEncodeStr(this->block) << endl;

        if (bindInfo->traceSerializer) {
            bindInfo->traceSerializer->newObject("grid", mtlGrid);
            bindInfo->traceSerializer->newObject("block", mtlBlock);
            bindInfo->traceSerializer->newObject("knowns", knowns.values);
            bindInfo->traceSerializer->commit();
        }

        ExcAssert(mtlQueue);
        mtlpp::CommandBuffer commandBuffer = mtlQueue.CommandBuffer();
        ExcAssert(commandBuffer);

        // TODO: use events to avoid synchronous execution
        for (auto & prereq: prereqs) {
            prereq->await();
        }

        for (auto & action: kernel->bindActions) {
            action.apply(*kernel->mtlContext, bound.arguments, knowns, commandBuffer);
        }

        //for (auto & [name, value]: knowns.values) {
        //    cerr << "known: " << name << " = " << value.toStringNoNewLine() << endl;
        //}

        bool anyNotSatisfied = false;
        for (auto & c: bound.constraints) {
            if (c.satisfied(knowns))
                continue;
            cerr << "constraint for kernel " << kernel->kernelName << " op " << opName << " not satisfied: " << c.print() << endl;
            anyNotSatisfied = true;
        }

        if (anyNotSatisfied) {
            //const CommandExpressionVariables * vars = &knowns;
            //while (vars) {
            //    cerr << "known: " << jsonEncode(vars->values) << endl;
            //    vars = vars->outer;
            //}
            ExcAssert(!anyNotSatisfied);
        }

        mtlGrid.resize(3, 1);
        mtlBlock.resize(3, 1);

        mtlpp::Size gridSize(mtlGrid[0], mtlGrid[1], mtlGrid[2]);
        mtlpp::Size blockSize(mtlBlock[0], mtlBlock[1], mtlBlock[2]);

        mtlpp::ComputeCommandEncoder commandEncoder = commandBuffer.ComputeCommandEncoder();
        commandEncoder.SetComputePipelineState(bindInfo->mtlPipelineState);
        commandEncoder.DispatchThreadgroups(gridSize, blockSize);
        commandEncoder.EndEncoding();

        auto timer = std::make_shared<Timer>();
        commandBuffer.Commit();
        commandBuffer.WaitUntilCompleted();

        auto status = commandBuffer.GetStatus();
        if (status != mtlpp::CommandBufferStatus::Completed) {
            auto error = commandBuffer.GetError();
            throw MLDB::Exception(std::string(error.GetDomain().GetCStr()) + ": " + error.GetLocalizedDescription().GetCStr());
        }

        // Ensure it's submitted before we start using the event
    #if 1
        auto wallTime = timer->elapsed_wall();

        {
            std::unique_lock guard(kernelWallTimesMutex);
            kernelWallTimes[bound.owner->kernelName] += wallTime * 1000.0;
            totalKernelTime += wallTime * 1000.0;
            cerr << "kernel " << bound.owner->kernelName << " executed in " << wallTime * 1000.0 << "ms" << endl;
        }
    #endif
        //doCallback(event, 0);

        return std::make_shared<MetalComputeEvent>();
    } MLDB_CATCH_ALL {
        rethrowException(400, "Error launching kernel " + bound.owner->kernelName);
    }
    THROW_UNIMPLEMENTED;
}

ComputePromiseT<MemoryRegionHandle>
MetalComputeQueue::
enqueueFillArrayImpl(const std::string & opName,
                     MemoryRegionHandle region, MemoryRegionInitialization init,
                     size_t startOffsetInBytes, ssize_t lengthInBytes,
                     const std::any & arg,
                     std::vector<std::shared_ptr<ComputeEvent>> prereqs)
{
    // TODO: once we have events, stop doing this...
    for (auto & prereq: prereqs) {
        prereq->await();
    }

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

ComputePromiseT<MemoryRegionHandle>
MetalComputeQueue::
enqueueCopyFromHostImpl(const std::string & opName,
                        MemoryRegionHandle toRegion,
                        FrozenMemoryRegion fromRegion,
                        size_t deviceStartOffsetInBytes,
                        std::vector<std::shared_ptr<ComputeEvent>> prereqs)
{
        THROW_UNIMPLEMENTED;
#if 0
    auto op = scopedOperation("MetalComputeQueue enqueueCopyFromHostImpl " + opName);

    ExcAssert(toRegion.handle);

    auto clPrereqs = toMetalEventList(prereqs);

    auto [pin, mem, offset] = MetalComputeContext::getMemoryRegion(opName, *toRegion.handle, ACC_WRITE);
    auto res = clQueue.enqueueWriteBuffer(mem, offset, fromRegion.length(), fromRegion.data(), clPrereqs);

    return { toRegion, std::make_shared<MetalComputeEvent>(res) };
#endif
}


void
MetalComputeQueue::
flush()
{
        THROW_UNIMPLEMENTED;
#if 0
    auto op = scopedOperation("MetalComputeQueue flush");
    clQueue.flush();
#endif
}

void
MetalComputeQueue::
finish()
{
        THROW_UNIMPLEMENTED;

#if 0
    auto op = scopedOperation("MetalComputeQueue finish");
    clQueue.finish();
#endif
}

std::shared_ptr<ComputeEvent>
MetalComputeQueue::
makeAlreadyResolvedEvent() const
{
    return std::make_shared<MetalComputeEvent>();
}


// MetalComputeContext

MetalComputeContext::
MetalComputeContext(mtlpp::Device device)
    : mtlDevice(device), queue(this)
{
}

std::tuple<std::shared_ptr<const void>, mtlpp::Buffer, size_t>
MetalComputeContext::
getMemoryRegion(const std::string & opName, MemoryRegionHandleInfo & handle, MemoryRegionAccess access)
{
    MetalMemoryRegionHandleInfo * upcastHandle = dynamic_cast<MetalMemoryRegionHandleInfo *>(&handle);
    if (!upcastHandle) {
        throw MLDB::Exception("TODO: get memory region from block handled from elsewhere: got " + demangle(typeid(handle)));
    }
    auto [pin, mem] = upcastHandle->getMetalAccess(opName, access);

    return { std::move(pin), mem, upcastHandle->offset };
}

std::tuple<FrozenMemoryRegion, int /* version */>
MetalComputeContext::
getFrozenHostMemoryRegion(const std::string & opName, MemoryRegionHandleInfo & handle,
                          size_t offset, ssize_t length,
                          bool ignoreHazards) const
{
    MetalMemoryRegionHandleInfo * upcastHandle = dynamic_cast<MetalMemoryRegionHandleInfo *>(&handle);
    if (!upcastHandle) {
        throw MLDB::Exception("TODO: get memory region from block handled from elsewhere: got " + demangle(typeid(handle)));
    }
    return upcastHandle->getReadOnlyHostAccessSync(*this, opName, offset, length, ignoreHazards);
}

static MemoryRegionHandle
doMetalAllocate(mtlpp::Device & mtlDevice,
                 const std::string & regionName,
                 size_t length, size_t align,
                 const std::type_info & type,
                 bool isConst)
{
    // TODO: align...
    mtlpp::ResourceOptions options = mtlpp::ResourceOptions::StorageModeManaged;
    auto buffer = mtlDevice.NewBuffer(length, options);

    auto handle = std::make_shared<MetalMemoryRegionHandleInfo>();
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

ComputePromiseT<MemoryRegionHandle>
MetalComputeContext::
allocateImpl(const std::string & regionName,
             size_t length, size_t align,
             const std::type_info & type,
             bool isConst,
             MemoryRegionInitialization initialization,
             std::any initWith)
{
    auto op = scopedOperation("MetalComputeContext allocateImpl " + regionName);
    auto result = doMetalAllocate(this->mtlDevice, regionName, length, align, type, isConst);
    return queue.enqueueFillArrayImpl(regionName + " initialize", result, initialization,
                                       0 /* startOffsetInBytes */, -1 /*lengthinBytes*/, initWith);
}

MemoryRegionHandle
MetalComputeContext::
allocateSyncImpl(const std::string & regionName,
                 size_t length, size_t align,
                 const std::type_info & type, bool isConst,
                 MemoryRegionInitialization initialization,
                 std::any initWith)
{
    auto op = scopedOperation("MetalComputeContext allocateSyncImpl " + regionName);
    THROW_UNIMPLEMENTED;
#if 0
    auto result = doMetalAllocate(clContext, regionName, length, align, type, isConst);
    if (initialization != MemoryRegionInitialization::INIT_NONE) {
        return result = clQueue->enqueueFillArrayImpl(regionName + " initialize", std::move(result), initialization,
                                        0 /* startOffsetInBytes */, -1 /*lengthinBytes*/, initWith).move();

    }

    return result;
#endif
}

static MemoryRegionHandle
doMetalTransferToDevice(mtlpp::Device & mtlDevice,
                         const std::string & opName, FrozenMemoryRegion region,
                         const std::type_info & type, bool isConst)
{
    //Timer timer;

    mtlpp::ResourceOptions options = mtlpp::ResourceOptions::StorageModeManaged;
    
    mtlpp::Buffer buffer = mtlDevice.NewBuffer(region.data(), region.length(), options);

    auto handle = std::make_shared<MetalMemoryRegionHandleInfo>();
    handle->buffer = std::move(buffer);
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

ComputePromiseT<MemoryRegionHandle>
MetalComputeContext::
transferToDeviceImpl(const std::string & opName, FrozenMemoryRegion region,
                     const std::type_info & type, bool isConst)
{
    auto op = scopedOperation("MetalComputeContext transferToDeviceImpl " + opName);
    auto result = doMetalTransferToDevice(mtlDevice, opName, region, type, isConst);
    return {std::move(result), std::make_shared<MetalComputeEvent>()};
}

MemoryRegionHandle
MetalComputeContext::
transferToDeviceSyncImpl(const std::string & opName,
                         FrozenMemoryRegion region,
                         const std::type_info & type, bool isConst)
{
    THROW_UNIMPLEMENTED;
#if 0
    auto op = scopedOperation("MetalComputeContext transferToDeviceSyncImpl " + opName);
    auto result = doMetalTransferToDevice(clContext, opName, region, type, isConst);
    return result;
#endif
}


ComputePromiseT<FrozenMemoryRegion>
MetalComputeContext::
transferToHostImpl(const std::string & opName, MemoryRegionHandle handle)
{
    // TODO: async
    auto region = transferToHostSyncImpl(opName, handle);
    auto event = MetalComputeEvent::makeAlreadyResolvedEvent();

    return { region, event };
}

FrozenMemoryRegion
MetalComputeContext::
transferToHostSyncImpl(const std::string & opName,
                       MemoryRegionHandle handle)
{
    auto op = scopedOperation("MetalComputeContext transferToHostSyncImpl " + opName);

    MetalMemoryRegionHandleInfo * upcastHandle = dynamic_cast<MetalMemoryRegionHandleInfo *>(handle.handle.get());
    if (!upcastHandle) {
        throw MLDB::Exception("Wrong MetalComputeContext handle: got " + demangle(typeid(handle)));
    }
    if (upcastHandle->backingHostMem) {
        // It's a read-only, host-first mapping... so nothing to do
        return { handle.handle, (const char *)upcastHandle->backingHostMem, (size_t)upcastHandle->lengthInBytes };
    }

    auto [pin, buffer, offset] = getMemoryRegion(opName, *handle.handle, ACC_READ);

    auto commandQueue = mtlDevice.NewCommandQueue();
    ExcAssert(commandQueue);
    auto commandBuffer = commandQueue.CommandBuffer();
    ExcAssert(commandBuffer);
    auto blitEncoder = commandBuffer.BlitCommandEncoder();
    ExcAssert(blitEncoder);

    blitEncoder.Synchronize(buffer);
    blitEncoder.EndEncoding();
    
    commandBuffer.Commit();
    commandBuffer.WaitUntilCompleted();

    auto status = commandBuffer.GetStatus();
    if (status != mtlpp::CommandBufferStatus::Completed) {
        auto error = commandBuffer.GetError();
        throw MLDB::Exception(std::string(error.GetDomain().GetCStr()) + ": " + error.GetLocalizedDescription().GetCStr());
    }

    auto storageMode = buffer.GetStorageMode();
    cerr << "storage mode is " << (int)storageMode << endl;

    auto contents = buffer.GetContents();
    auto length = buffer.GetLength();
    ExcAssertLessEqual(handle.handle->lengthInBytes, length);

    cerr << "region length for region " << handle.handle->name
         << " with lengthInBytes " << handle.handle->lengthInBytes
         << " is " << length << " and contents " << contents << endl;

    FrozenMemoryRegion result(std::move(pin), (const char *)contents, handle.handle->lengthInBytes);
    return result;
}

ComputePromiseT<MutableMemoryRegion>
MetalComputeContext::
transferToHostMutableImpl(const std::string & opName, MemoryRegionHandle handle)
{
    THROW_UNIMPLEMENTED;

#if 0
    auto op = scopedOperation("MetalComputeContext transferToHostMutableImpl " + opName);
    ExcAssert(handle.handle);

    auto [pin, mem, offset] = getMemoryRegion(opName, *handle.handle, ACC_READ_WRITE);
    MetalEvent clEvent;
    std::shared_ptr<void> memPtr;
    std::tie(memPtr, clEvent)
        = clQueue->clQueue.enqueueMapBuffer(mem, CL_MAP_READ | CL_MAP_WRITE,
                                    offset, handle.lengthInBytes());

    auto event = std::make_shared<MetalComputeEvent>(std::move(clEvent));
    auto promise = std::shared_ptr<std::promise<std::any>>();

    auto cb = [handle, promise, memPtr, pin=pin] (const MetalEvent & event, auto status)
    {
        if (status == MetalEventCommandExecutionStatus::ERROR)
            promise->set_exception(std::make_exception_ptr(MLDB::Exception("Metal error mapping host memory")));
        else {
            promise->set_value(MutableMemoryRegion(memPtr, (char *)memPtr.get(), handle.lengthInBytes()));
        }
    };

    clEvent.addCallback(cb);

    return { std::move(promise), std::move(event) };
#endif
}

MutableMemoryRegion
MetalComputeContext::
transferToHostMutableSyncImpl(const std::string & opName,
                              MemoryRegionHandle handle)
{
    THROW_UNIMPLEMENTED;

#if 0
    auto op = scopedOperation("MetalComputeContext transferToHostMutableSyncImpl " + opName);

    ExcAssert(handle.handle);

    auto [pin, mem, offset] = getMemoryRegion(opName, *handle.handle, ACC_READ_WRITE);
    auto memPtr = clQueue->clQueue.enqueueMapBufferBlocking(mem, CL_MAP_READ | CL_MAP_WRITE,
                                                            offset, handle.lengthInBytes());
    MutableMemoryRegion result(std::move(memPtr), (char *)memPtr.get(), handle.lengthInBytes());
    return result;
#endif
}

std::shared_ptr<ComputeKernel>
MetalComputeContext::
getKernel(const std::string & kernelName)
{
    auto op = scopedOperation("MetalComputeContext getKernel " + kernelName);

    std::unique_lock guard(kernelRegistryMutex);
    auto it = kernelRegistry.find(kernelName);
    if (it == kernelRegistry.end()) {
        throw AnnotatedException(400, "Unable to find Metal compute kernel '" + kernelName + "'",
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

ComputePromiseT<MemoryRegionHandle>
MetalComputeContext::
managePinnedHostRegionImpl(const std::string & opName, std::span<const std::byte> region, size_t align,
                           const std::type_info & type, bool isConst)
{
    auto op = scopedOperation("MetalComputeContext managePinnedHostRegionImpl " + opName);

    auto result = managePinnedHostRegionSyncImpl(opName, region, align, type, isConst);
    return ComputePromiseT<MemoryRegionHandle>(std::move(result), MetalComputeEvent::makeAlreadyResolvedEvent());
}

MemoryRegionHandle
MetalComputeContext::
managePinnedHostRegionSyncImpl(const std::string & opName,
                               std::span<const std::byte> region, size_t align,
                               const std::type_info & type, bool isConst)
{
    auto op = scopedOperation("MetalComputeContext managePinnedHostRegionSyncImpl " + opName);

    mtlpp::ResourceOptions options
         = mtlpp::ResourceOptions::StorageModeShared;

    auto deallocator = [=] (auto ptr, auto len)
    {
        cerr << ansi::bright_red << "DEALLOCATING PINNED REGION " << opName << ansi::reset
             << " of length " << len << endl;
    };

    traceOperation("region size " + std::to_string(region.size()));

    // This overload calls newBufferWithBytesNoCopy
    mtlpp::Buffer buffer = mtlDevice.NewBuffer((void *)region.data(), region.size(), options, deallocator);

    auto handle = std::make_shared<MetalMemoryRegionHandleInfo>();
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

std::shared_ptr<ComputeEvent>
MetalComputeContext::
fillDeviceRegionFromHostImpl(const std::string & opName,
                             MemoryRegionHandle deviceHandle,
                             std::shared_ptr<std::span<const std::byte>> pinnedHostRegion,
                             size_t deviceOffset)
{
    THROW_UNIMPLEMENTED;
#if 0
    FrozenMemoryRegion region(pinnedHostRegion, (const char *)pinnedHostRegion->data(), pinnedHostRegion->size_bytes());

    return clQueue
        ->enqueueCopyFromHostImpl(opName, deviceHandle, region, deviceOffset, {}).event();
#endif
}                                     

void
MetalComputeContext::
fillDeviceRegionFromHostSyncImpl(const std::string & opName,
                                 MemoryRegionHandle deviceHandle,
                                 std::span<const std::byte> hostRegion,
                                 size_t deviceOffset)
{
    THROW_UNIMPLEMENTED;

#if 0
    FrozenMemoryRegion region(nullptr, (const char *)hostRegion.data(), hostRegion.size_bytes());

    clQueue
        ->enqueueCopyFromHostImpl(opName, deviceHandle, region, deviceOffset, {})
    .await();
#endif
}

std::shared_ptr<ComputeEvent>
MetalComputeContext::
copyBetweenDeviceRegionsImpl(const std::string & opName,
                                MemoryRegionHandle from, MemoryRegionHandle to,
                                size_t fromOffset, size_t toOffset,
                                size_t length)
{
    THROW_UNIMPLEMENTED;

#if 0
    auto [fromPin, fromMem, fromBaseOffset] = getMemoryRegion(opName, *from.handle, ACC_READ);
    auto [toPin, toMem, toBaseOffset] = getMemoryRegion(opName, *to.handle, ACC_WRITE);

    auto event = clQueue->clQueue.enqueueCopyBuffer(fromMem, toMem, fromBaseOffset + fromOffset, toBaseOffset + toOffset, length);
    return std::make_shared<MetalComputeEvent>(std::move(event));
#endif
}

void
MetalComputeContext::
copyBetweenDeviceRegionsSyncImpl(const std::string & opName,
                                    MemoryRegionHandle from, MemoryRegionHandle to,
                                    size_t fromOffset, size_t toOffset,
                                    size_t length)
{
    copyBetweenDeviceRegionsImpl(opName, from, to, fromOffset, toOffset, length)->await();
}

std::shared_ptr<ComputeQueue>
MetalComputeContext::
getQueue()
{
    return std::make_shared<MetalComputeQueue>(this);
}

MemoryRegionHandle
MetalComputeContext::
getSliceImpl(const MemoryRegionHandle & handle, const std::string & regionName,
             size_t startOffsetInBytes, size_t lengthInBytes,
             size_t align, const std::type_info & type, bool isConst)
{
    auto op = scopedOperation("MetalComputeContext getSliceImpl " + regionName);

    auto info = std::dynamic_pointer_cast<const MetalMemoryRegionHandleInfo>(std::move(handle.handle));
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

    auto newInfo = std::make_shared<MetalMemoryRegionHandleInfo>();

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


// MetalComputeKernel

template<typename T>
ComputeKernelType
makeBasicType(std::initializer_list<int> vals = {1})
{
    ComputeKernelType result;
    result.simd.insert(result.simd.begin(), vals.begin(), vals.end());
    result.baseType = getDefaultDescriptionSharedT<T>();
    return result;
}

static ComputeKernelType
getKernelTypeFromDataType(mtlpp::DataType dataType)
{
    switch (dataType) {
        case mtlpp::DataType::None: 
        case mtlpp::DataType::Struct: 
        case mtlpp::DataType::Array:
            throw MLDB::Exception("getKernelTypeFromDataType: not a basic type"); 
        case mtlpp::DataType::Float:        return makeBasicType<float>();
        case mtlpp::DataType::Float2:       return makeBasicType<float>({2});
        case mtlpp::DataType::Float3:       return makeBasicType<float>({3});
        case mtlpp::DataType::Float4:       return makeBasicType<float>({4});
        case mtlpp::DataType::Float2x2:     return makeBasicType<float>({2,2});
        case mtlpp::DataType::Float2x3:     return makeBasicType<float>({2,3});
        case mtlpp::DataType::Float2x4:     return makeBasicType<float>({2,4});
        case mtlpp::DataType::Float3x2:     return makeBasicType<float>({3,2});
        case mtlpp::DataType::Float3x3:     return makeBasicType<float>({3,3});
        case mtlpp::DataType::Float3x4:     return makeBasicType<float>({3,4});
        case mtlpp::DataType::Float4x2:     return makeBasicType<float>({4,2});
        case mtlpp::DataType::Float4x3:     return makeBasicType<float>({4,3});
        case mtlpp::DataType::Float4x4:     return makeBasicType<float>({4,4});
        case mtlpp::DataType::Half:         return makeBasicType<half>();
        case mtlpp::DataType::Half2:        return makeBasicType<half>({2});
        case mtlpp::DataType::Half3:        return makeBasicType<half>({3});
        case mtlpp::DataType::Half4:        return makeBasicType<half>({4});
        case mtlpp::DataType::Half2x2:      return makeBasicType<half>({2,2});
        case mtlpp::DataType::Half2x3:      return makeBasicType<half>({2,3});
        case mtlpp::DataType::Half2x4:      return makeBasicType<half>({2,4});
        case mtlpp::DataType::Half3x2:      return makeBasicType<half>({3,2});
        case mtlpp::DataType::Half3x3:      return makeBasicType<half>({3,3});
        case mtlpp::DataType::Half3x4:      return makeBasicType<half>({3,4});
        case mtlpp::DataType::Half4x2:      return makeBasicType<half>({4,2});
        case mtlpp::DataType::Half4x3:      return makeBasicType<half>({4,3});
        case mtlpp::DataType::Half4x4:      return makeBasicType<half>({4,4});
        case mtlpp::DataType::Int:          return makeBasicType<int32_t>();
        case mtlpp::DataType::Int2:         return makeBasicType<int32_t>({2});
        case mtlpp::DataType::Int3:         return makeBasicType<int32_t>({3});
        case mtlpp::DataType::Int4:         return makeBasicType<int32_t>({4});
        case mtlpp::DataType::UInt:         return makeBasicType<uint32_t>();
        case mtlpp::DataType::UInt2:        return makeBasicType<uint32_t>({2});
        case mtlpp::DataType::UInt3:        return makeBasicType<uint32_t>({3});
        case mtlpp::DataType::UInt4:        return makeBasicType<uint32_t>({4});
        case mtlpp::DataType::Short:        return makeBasicType<int16_t>();
        case mtlpp::DataType::Short2:       return makeBasicType<int16_t>({2});
        case mtlpp::DataType::Short3:       return makeBasicType<int16_t>({3});
        case mtlpp::DataType::Short4:       return makeBasicType<int16_t>({4});
        case mtlpp::DataType::UShort:       return makeBasicType<uint16_t>();
        case mtlpp::DataType::UShort2:      return makeBasicType<uint16_t>({2});
        case mtlpp::DataType::UShort3:      return makeBasicType<uint16_t>({3});
        case mtlpp::DataType::UShort4:      return makeBasicType<uint16_t>({4});
        case mtlpp::DataType::Char:         return makeBasicType<int8_t>();
        case mtlpp::DataType::Char2:        return makeBasicType<int8_t>({2});
        case mtlpp::DataType::Char3:        return makeBasicType<int8_t>({3});
        case mtlpp::DataType::Char4:        return makeBasicType<int8_t>({4});
        case mtlpp::DataType::UChar:        return makeBasicType<uint8_t>();
        case mtlpp::DataType::UChar2:       return makeBasicType<uint8_t>({2});
        case mtlpp::DataType::UChar3:       return makeBasicType<uint8_t>({3});
        case mtlpp::DataType::UChar4:       return makeBasicType<uint8_t>({4});
        case mtlpp::DataType::Bool:         return makeBasicType<bool>();
        case mtlpp::DataType::Bool2:        return makeBasicType<bool>({2});
        case mtlpp::DataType::Bool3:        return makeBasicType<bool>({3});
        case mtlpp::DataType::Bool4:        return makeBasicType<bool>({4});
        case mtlpp::DataType::Long:         return makeBasicType<int64_t>();
        case mtlpp::DataType::Long2:        return makeBasicType<int64_t>({2});
        case mtlpp::DataType::Long3:        return makeBasicType<int64_t>({3});
        case mtlpp::DataType::Long4:        return makeBasicType<int64_t>({4});
        case mtlpp::DataType::ULong:        return makeBasicType<uint64_t>();
        case mtlpp::DataType::ULong2:       return makeBasicType<uint64_t>({2});
        case mtlpp::DataType::ULong3:       return makeBasicType<uint64_t>({3});
        case mtlpp::DataType::ULong4:       return makeBasicType<uint64_t>({4});
        default:
            throw MLDB::Exception("getKernelTypeFromDataType: unknown type"); 
    }
}

static MemoryRegionAccess convertAccess(mtlpp::ArgumentAccess access)
{
    switch (access) {
        case mtlpp::ArgumentAccess::ReadOnly: return ACC_READ;
        case mtlpp::ArgumentAccess::ReadWrite: return ACC_READ_WRITE;
        case mtlpp::ArgumentAccess::WriteOnly: return ACC_WRITE;
        default:
            throw MLDB::Exception("Converting unknown MtlArgumentAccess value");
    }
}

ComputeKernelType
getKernelTypeFromArrayType(const mtlpp::ArrayType & arrayType, const std::string & name, int align = -1, int width = -1);

ComputeKernelType
getKernelTypeFromStructType(const mtlpp::StructType & structType, const std::string & name,
                            int align = -1, int width = -1)
{
    auto desc = std::make_shared<GenericStructureDescription>(false, name);
    if (align != -1)
        desc->align = align;
    if (width != -1)
        desc->width = width;

    auto members = structType.GetMembers();
    for (size_t i = 0;  i < members.GetSize();  ++i) {
        auto member = members[i];
        std::string memberName = member.GetName().GetCStr();
        //cerr << member.GetName().GetCStr() << " at offset " << member.GetOffset() << " with data type "
        //        << (int)member.GetDataType() << endl;

        ComputeKernelType kernelType;

        switch (member.GetDataType()) {
        case mtlpp::DataType::None:
            throw MLDB::Exception("Can't handle void (MtlDataType::None) types");
        case mtlpp::DataType::Struct:
            kernelType = getKernelTypeFromStructType(member.GetStructType(), name + "__m__" + memberName);
            break;
        case mtlpp::DataType::Array:
            kernelType = getKernelTypeFromArrayType(member.GetArrayType(), name + "__m__" + memberName);
            break;
        default:
            kernelType = getKernelTypeFromDataType(member.GetDataType());
        }
        //cerr << "  data type " << (int)member.GetDataType() << " " << kernelType.print() << endl;
        //cerr << "  array type " << member.GetArrayType()

        desc->addFieldDesc(memberName, member.GetOffset(), "", kernelType.baseType);
    }
    
    ComputeKernelType type;
    type.baseType = desc;
    return type;
}

ComputeKernelType
getKernelTypeFromArrayType(const mtlpp::ArrayType & arrayType, const std::string & name, int align, int width)
{
    uint32_t length = arrayType.GetArrayLength();
    uint32_t stride = arrayType.GetStride();

    auto dataType = arrayType.GetElementType();
    ComputeKernelType elementType;

    switch (dataType) {
    case mtlpp::DataType::None:
        throw MLDB::Exception("Can't handle void (MtlDataType::None) types");
    case mtlpp::DataType::Struct:
        elementType = getKernelTypeFromStructType(arrayType.GetElementStructType(), name + "__ul__", -1 /* align */, stride /* width */);
        break;
    case mtlpp::DataType::Array:
        elementType = getKernelTypeFromArrayType(arrayType.GetElementArrayType(), name + "__ul__", -1 /* align */, stride /* width */);
        break;
    default:
        elementType = getKernelTypeFromDataType(dataType);
    }

    if (width == -1) {
        width = length * stride;
    }
    else {
        ExcAssertGreater(width, 0);
        ExcAssertEqual(elementType.baseType->width, stride);
        ExcAssertEqual(elementType.baseType->width * length, width);
    }

    if (align == -1) {
        align = elementType.baseType->align;
    }
    else {
        ExcAssertGreater(align, 0);
        // ... TODO more conditions here...
    }

    auto desc = std::make_shared<GenericFixedLengthArrayDescription>(width, align, name, elementType.baseType, length);

    ComputeKernelType result;
    result.baseType = desc;
    return result;
}

// Parses an Metal kernel argument info structure, and turns it into a ComputeKernel type
ComputeKernelType
MetalComputeKernel::
getKernelType(const mtlpp::Argument & arg)
{
    ComputeKernelType type;

    mtlpp::ArgumentType argType = arg.GetType();
    mtlpp::DataType dataType;

    static std::atomic<int> idx(0);
    std::string name = "__metal_arg_" + string(arg.GetName().GetCStr()) + std::to_string(idx++);

    if (argType == mtlpp::ArgumentType::Buffer) {
        cerr << "buffer argument " << arg.GetName().GetCStr() << endl;

        dataType = arg.GetBufferDataType();
        auto structType = arg.GetBufferStructType();
        auto align = arg.GetBufferAlignment();
        auto width = arg.GetBufferDataSize();
        auto pointer = arg.GetBufferPointerType();

        cerr << "align " << align << " width " << width << " pointer " << pointer << endl;

        if (pointer) {
            cerr << "  pointer: acc " << (int)pointer.GetAccess() << " data " << (int)pointer.GetElementType()
                 << " array " << pointer.GetElementArrayType() << " struct " << pointer.GetElementStructType()
                 << " datasize " << pointer.GetDataSize() << endl;
        }

        switch (dataType) {
        case mtlpp::DataType::None:
            throw MLDB::Exception("Can't handle void (MtlDataType::None) types");
        case mtlpp::DataType::Struct:
            type = getKernelTypeFromStructType(arg.GetBufferStructType(), name, align, width);
            break;
        case mtlpp::DataType::Array:
            throw MLDB::Exception("Can't handle arrays as Metal arguments");
            break;
        default:
            type = getKernelTypeFromDataType(dataType);
            type.dims.emplace_back();  // if it's a basic type, it must be an array
        }
    }
    else if (argType == mtlpp::ArgumentType::ThreadgroupMemory) {
        // We know very little about this type, just its alignment and data size
        auto align = arg.GetThreadgroupMemoryAlignment();
        auto width = arg.GetThreadgroupMemoryDataSize();
        type.baseType.reset(new GenericAtomDescription(width, align, name));
    }
    else {
        throw MLDB::Exception("Not implemented: non-buffer and non-thread group argument types");
    }

    type.access = convertAccess(arg.GetAccess());
    ExcAssert(type.baseType);
    return type;
}

void
MetalComputeKernel::
setParameters(SetParameters setter)
{
    setters.emplace_back(std::move(setter));
}

void
MetalComputeKernel::
allowGridPadding()
{
    allowGridPaddingFlag = true;
}

void
MetalComputeKernel::
allowGridExpansion()
{
    allowGridExpansionFlag = true;
}

void
MetalComputeKernel::
setGridExpression(const std::string & gridExpr, const std::string & blockExpr)
{
    if (!gridExpr.empty())
        this->gridExpression = CommandExpression::parseArgumentExpression(gridExpr);
    if (!blockExpr.empty())
        this->blockExpression = CommandExpression::parseArgumentExpression(blockExpr);
}

void
MetalComputeKernel::
addTuneable(const std::string & name, int64_t defaultValue)
{
    this->tuneables.push_back({name, defaultValue});
}

DEFINE_ENUM_DESCRIPTION_INLINE(MetalBindFieldActionType)
{
    addValue("SET_FIELD_FROM_PARAM", MetalBindFieldActionType::SET_FIELD_FROM_PARAM);
    addValue("SET_FIELD_FROM_KNOWN", MetalBindFieldActionType::SET_FIELD_FROM_KNOWN);
}

void
MetalBindFieldAction::
apply(void * object,
      const ValueDescription & desc,
      MetalComputeContext & context,
      const std::vector<ComputeKernelArgument> & args,
      CommandExpressionVariables & knowns) const
{
    auto & fieldDesc = desc.getFieldByNumber(fieldNumber);
    std::string opName = "bind: fill field " + fieldDesc.fieldName;
    switch (action) {
    case MetalBindFieldActionType::SET_FIELD_FROM_PARAM: opName += " from arg " + std::to_string(argNum);  break;
    case MetalBindFieldActionType::SET_FIELD_FROM_KNOWN: opName += " from known " + jsonEncodeStr(expr);  break;
    }

    auto op = scopedOperation(opName);
    Json::Value value;
    bool done = false;

    switch (action) {
    case MetalBindFieldActionType::SET_FIELD_FROM_PARAM: {
        if (argNum == -1 || argNum >= args.size()) {
            // Problem... this parameter wasn't set from an argument.  It must be in known.
            CommandExpressionContext exprContext(&knowns);
            value = expr->apply(exprContext);
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
    case MetalBindFieldActionType::SET_FIELD_FROM_KNOWN: {
        CommandExpressionContext exprContext(&knowns);
        value = expr->apply(exprContext);
        break;
    }
    default:
        throw MLDB::Exception("Can't handle unknown MetalBindFieldAction value");
    }

    traceOperation("value " + jsonEncodeStr(value));

    // We go through the JSON conversion so that we can handle compatible type coversions,
    // eg uint16_t -> uint32_t.
    if (!done) {
        StructuredJsonParsingContext jsonContext(value);
        fieldDesc.description->parseJson(fieldDesc.getFieldPtr(object), jsonContext);
    }
}

DEFINE_STRUCTURE_DESCRIPTION_INLINE(MetalBindFieldAction)
{
    addField("action", &MetalBindFieldAction::action, "");
    addField("fieldNumber", &MetalBindFieldAction::fieldNumber, "", -1);
    addField("argNum", &MetalBindFieldAction::argNum, "", -1);
    addField("expr", &MetalBindFieldAction::expr, "");
}

DEFINE_ENUM_DESCRIPTION_INLINE(MetalBindActionType)
{
    addValue("SET_BUFFER_FROM_ARG", MetalBindActionType::SET_BUFFER_FROM_ARG, "");
    addValue("SET_BUFFER_FROM_STRUCT", MetalBindActionType::SET_BUFFER_FROM_STRUCT, "");
    addValue("SET_BUFFER_THREAD_GROUP", MetalBindActionType::SET_BUFFER_THREAD_GROUP, "");
}

void
MetalBindAction::
applyArg(MetalComputeContext & context,
         const std::vector<ComputeKernelArgument> & args,
         CommandExpressionVariables & knowns,
         mtlpp::CommandBuffer & commandBuffer) const
{
    const ComputeKernelArgument & arg = args.at(argNum);

    auto op = scopedOperation("MetalComputeKernel bind arg " + argName + " type " + arg.handler->type.print() + " from " + arg.handler->toJson().toStringNoNewLine());

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
        traceOperation("binding primitive with " + std::to_string(bytes.size()) + " bytes and value " + jsonEncodeStr(arg.handler->toJson()));
        memBuffer = context.mtlDevice.NewBuffer(bytes.data(), bytes.size_bytes(), mtlpp::ResourceOptions::StorageModePrivate);
    }
    else if (arg.handler->canGetHandle()) {
        auto handle = arg.handler->getHandle(opName, context);
        traceOperation("binding handle with " + std::to_string(handle.lengthInBytes()) + " bytes");
        MemoryRegionAccess access = type.access;

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

        knowns.values[argName] = known;

        if (traceSerializer && (access & ACC_READ)) {
            auto [region, version] = context
                .getFrozenHostMemoryRegion(opName + " trace", *handle.handle, 0 /* offset */, -1,
                                            true /* ignore hazards */);
            //bindInfo->traceSerializer->addRegion(region, this->clKernelInfo.args[i].name);
            traceVersion(handle.handle->name, version, region);
        }

        std::tie(pin, memBuffer, offset)
            = context.getMemoryRegion(opName, *handle.handle, access);
    }
    else if (arg.handler->canGetConstRange()) {
        throw MLDB::Exception("param.getConstRange");
    }
    else {
        throw MLDB::Exception("don't know how to handle passing parameter to Metal");
    }

    mtlpp::ComputeCommandEncoder commandEncoder = commandBuffer.ComputeCommandEncoder();
    commandEncoder.SetBuffer(memBuffer, offset, this->arg.GetIndex());
    commandEncoder.EndEncoding();
}

void
MetalBindAction::
applyStruct(MetalComputeContext & context,
            const std::vector<ComputeKernelArgument> & args,
            CommandExpressionVariables & knowns,
            mtlpp::CommandBuffer & commandBuffer) const
{
    auto op = scopedOperation("MetalComputeKernel bind struct to arg " + argName);

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

    auto memBuffer = context.mtlDevice.NewBuffer(result.get(), type.baseType->width,
                                                 mtlpp::ResourceOptions::StorageModePrivate);
    mtlpp::ComputeCommandEncoder commandEncoder = commandBuffer.ComputeCommandEncoder();
    commandEncoder.SetBuffer(memBuffer, 0 /*offset*/, this->arg.GetIndex());
    commandEncoder.EndEncoding();
}

void
MetalBindAction::
applyThreadGroup(MetalComputeContext & context,
                 const std::vector<ComputeKernelArgument> & args,
                 CommandExpressionVariables & knowns,
                 mtlpp::CommandBuffer & commandBuffer) const
{
    // Calculate the array bound
    CommandExpressionContext exprContext(&knowns);
    auto len = expr->apply(exprContext).asUInt();

    // Get the number of bytes
    size_t nbytes = len * type.baseType->width;

    // Must be a multiple of 16 bytes
    while (nbytes % 16 != 0)
        ++nbytes;

    traceOperation("binding local array handle with " + std::to_string(nbytes) + " bytes");

    // Set the knowns for this binding
    Json::Value known;
    known["elAlign"] = type.baseType->align;
    known["elWidth"] = type.baseType->width;
    known["elType"] = type.baseType->typeName;
    known["length"] = len;
    known["type"] = type.print();
    known["name"] = "<<<Local array>>>";

    knowns.setValue(this->argName, std::move(known));

    // Set it up in the command encoder
    mtlpp::ComputeCommandEncoder commandEncoder = commandBuffer.ComputeCommandEncoder();
    commandEncoder.SetThreadgroupMemory(nbytes, this->arg.GetIndex());
    commandEncoder.EndEncoding();
}

void
MetalBindAction::
apply(MetalComputeContext & context,
      const std::vector<ComputeKernelArgument> & args,
      CommandExpressionVariables & knowns,
      mtlpp::CommandBuffer & buffer) const
{
    switch (action) {
        case MetalBindActionType::SET_BUFFER_FROM_ARG:
            applyArg(context, args, knowns, buffer);
            break;
        case MetalBindActionType::SET_BUFFER_FROM_STRUCT:
            applyStruct(context, args, knowns, buffer);
            break;
        case MetalBindActionType::SET_BUFFER_THREAD_GROUP:
            applyThreadGroup(context, args, knowns, buffer);
            break;
        default:
            throw MLDB::Exception("Unknown metal action");
    }
}

DEFINE_STRUCTURE_DESCRIPTION_INLINE(MetalBindAction)
{
    addField("action", &MetalBindAction::action, "");
    addField("type", &MetalBindAction::type, "");
    addField("argNum", &MetalBindAction::argNum, "");
    addField("argName", &MetalBindAction::argName, "");
    addField("expr", &MetalBindAction::expr, "");
    addField("fields", &MetalBindAction::fields, "");
}

void
MetalComputeKernel::
setComputeFunction(mtlpp::Library libraryIn,
                   std::string kernelName,
                   std::vector<size_t> block)
{
    ExcAssert(libraryIn);

    this->block = std::move(block);
    this->mtlLibrary = std::move(libraryIn);
    this->kernelName = kernelName;

    this->mtlFunction = this->mtlLibrary.NewFunction(kernelName.c_str());
    ExcAssert(this->mtlFunction);

    //cerr << this->mtlFunction.GetName().GetCStr() << endl;

    ns::Error error{ns::Handle()};
    mtlpp::PipelineOption options = (mtlpp::PipelineOption((int)mtlpp::PipelineOption::ArgumentInfo | (int)mtlpp::PipelineOption::BufferTypeInfo));
    mtlpp::ComputePipelineState computePipelineState
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

    std::vector<MetalBindAction> actions;

    for (size_t i = 0;  i < arguments.GetSize();  ++i) {
        mtlpp::Argument arg = arguments[i];
        std::string argName = arg.GetName().GetCStr();
        mtlpp::ArgumentType argType = arg.GetType();

        MetalBindAction action;
        auto it = paramIndex.find(argName);

        if (argType == mtlpp::ArgumentType::ThreadgroupMemory) {
            if (it == paramIndex.end()) {
                throw MLDB::Exception("Metal ThreadGroup kernel parameter " + argName + " to kernel " + this->kernelName
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

            action.action = MetalBindActionType::SET_BUFFER_THREAD_GROUP;
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
                    std::vector<MetalBindFieldAction> fieldActions;

                    // Check if we know the unpacked types
                    auto numFields = type.baseType->getFixedFieldCount();
                    std::vector<std::string> fieldsNotFound;

                    for (size_t i = 0;  i < numFields;  ++i) {
                        const auto & field = type.baseType->getFieldByNumber(i);
                        auto it = paramIndex.find(field.fieldName);
                        if (it != paramIndex.end()) {
                            MetalBindFieldAction action;
                            action.action = MetalBindFieldActionType::SET_FIELD_FROM_PARAM;
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
                            MetalBindFieldAction action;
                            action.action = MetalBindFieldActionType::SET_FIELD_FROM_KNOWN;
                            action.fieldNumber = i;
                            action.expr = CommandExpression::parseArgumentExpression(field.fieldName);
                            fieldActions.emplace_back(std::move(action));
                            continue;
                        }
                        fieldsNotFound.push_back(field.fieldName);
                    }

                    if (!fieldsNotFound.empty()) {
                        throw MLDB::Exception("Kernel parameter " + std::to_string(i)
                                        + " (" + argName + ") to Metal kernel " + kernelName
                                        + " cannot be assembled because of missing fields " + jsonEncodeStr(fieldsNotFound));
                    }

                    action.action = MetalBindActionType::SET_BUFFER_FROM_STRUCT;
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
                                            + " (" + argName + ") to Metal kernel " + kernelName
                                            + " has no counterpart in formal parameter list");
                }
            }
            else {
                int argNum = it->second;
                auto & param = params.at(it->second);
                correspondingArgumentNumbers.at(i) = argNum;

                type.dims = param.type.dims;

                action.action = MetalBindActionType::SET_BUFFER_FROM_ARG;
                action.arg = arg;
                action.type = type;  // TODO: get information from param type
                action.argNum = it->second;
                action.argName = arg.GetName().GetCStr();

                cerr << "checking compatibility" << endl;
                cerr << "param type " << param.type.print() << endl;
                cerr << "arg type " << type.print() << endl;

                std::string reason;

                if (!param.type.isCompatibleWith(type, &reason)) {
                    throw MLDB::Exception("Kernel parameter " + std::to_string(i)
                                            + " (" + argName + ") to Metal kernel " + kernelName
                                            + ": declared parameter type " + type.print()
                                            + " is not compatible with kernel type " + param.type.print()
                                            + ": " + reason);
                }
            }
        }
        else {
            throw MLDB::Exception("Can't do Metal buffer or texture arguments (yet)");
        }
        //cerr << "got action\n" << jsonEncode(action) << endl;

        actions.emplace_back(std::move(action));
    }

    //cerr << "got actions\n" << jsonEncode(actions) << endl;
    this->bindActions = std::move(actions);
}

BoundComputeKernel
MetalComputeKernel::
bindImpl(std::vector<ComputeKernelArgument> argumentsIn) const
{
    auto op = scopedOperation("MetalComputeKernel bindImpl " + kernelName);

    ExcAssert(this->context);

    ns::Error error{ns::Handle()};
    mtlpp::ComputePipelineReflection reflection;
    mtlpp::ComputePipelineState computePipelineState
        = mtlContext->mtlDevice.NewComputePipelineState(this->mtlFunction, mtlpp::PipelineOption::None, reflection, &error);
    if (error) {
        cerr << "Error making pipeline state" << endl;
        cerr << "domain: " << error.GetDomain().GetCStr() << endl;
        cerr << "descrption: " << error.GetLocalizedDescription().GetCStr() << endl;
        if (error.GetLocalizedFailureReason()) {
            cerr << "reason: " << error.GetLocalizedFailureReason().GetCStr() << endl;
        }
    }
    ExcAssert(computePipelineState);

    auto bindInfo = std::make_shared<MetalBindInfo>();
    bindInfo->owner = this;
    bindInfo->mtlPipelineState = computePipelineState;

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
            result.addConstraint(p.type.dims[0].bound, "==", p.name + ".length",
                                 "Constraint implied by array bounds of parameter " + p.name + ": "
                                 + p.type.print());
        }
    }

    // figure out the values of the new constraints

    bool progress = true;

    while (progress) {
        progress = false;
        for (auto & c: result.constraints) {
            progress = progress || c.attemptToSatisfy(result.knowns, result.unknowns);
        }
    }

    if (bindInfo->traceSerializer) {
        //bindInfo->traceSerializer->newObject("args", argInfo);
        bindInfo->traceSerializer->newObject("knowns", result.knowns.values);
        bindInfo->traceSerializer->newObject("unknowns", result.unknowns);
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


// MetalComputeRuntime

EnvOption<int> METAL_DEFAULT_DEVICE("METAL_DEFAULT_DEVICE", -1);

struct MetalComputeRuntime: public ComputeRuntime {

    std::vector<mtlpp::Device> mtlDevices;
    std::vector<ComputeDevice> devices;

    MetalComputeRuntime()
    {
        auto queriedDevices = mtlpp::Device::CopyAllDevices();
        for (size_t i = 0;  i < queriedDevices.GetSize();  ++i) {
            auto device = queriedDevices[i];
            //cerr << "metal device " << i << " is " << device.GetName().GetCStr() << endl;
            mtlDevices.push_back(queriedDevices[i]);
            devices.push_back({ComputeRuntimeId::METAL, 0 /* runtime instance */, (uint8_t)i, 0, 0});
        }
    }

    mtlpp::Device convertDevice(ComputeDevice device) const
    {
        if (device.runtime != ComputeRuntimeId::METAL) {
            throw MLDB::Exception("Attempt to pass non-Metal device " + device.info() + " to Metal");
        }
        return mtlDevices.at(device.deviceInstance);
    }

    virtual ~MetalComputeRuntime()
    {
    }

    virtual ComputeRuntimeId getId() const
    {
        return ComputeRuntimeId::METAL;
    }

    virtual std::string printRestOfDevice(ComputeDevice device) const
    {
        return std::to_string(device.deviceInstance);
    }

    virtual std::string printHumanReadableDeviceInfo(ComputeDevice device) const
    {
        if (device.runtimeInstance > 0 || device.deviceInstance >= mtlDevices.size()) {
            return "<<INVALID METAL PLATFORM OR DEVICE INDEX>>";
        }
        std::string result = mtlDevices[device.deviceInstance].GetName().GetCStr();
        return result;
    }

    virtual ComputeDevice getDefaultDevice() const
    {
        if (mtlDevices.empty())
            return ComputeDevice::none();

        if (METAL_DEFAULT_DEVICE.specified()) {
            ExcAssertLess(METAL_DEFAULT_DEVICE.get(), mtlDevices.size());
            return {ComputeRuntimeId::METAL, 0 /* platform */,
                    (uint16_t)METAL_DEFAULT_DEVICE.get(), 0, 0};
        }

        // TODO: deal with the possibility of the default not being at index zero...
        return {ComputeRuntimeId::METAL, 0 /* platform */, 0 /* device */,0, 0};
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
        if (devices.size() != 1) {
            throw MLDB::Exception("Metal Compute Kernel driver only can accept a single device");
        }
        return std::make_shared<MetalComputeContext>(convertDevice(devices[0]));
    }

};

void registerMetalComputeKernel(const std::string & kernelName,
                                 std::function<std::shared_ptr<MetalComputeKernel>(MetalComputeContext & context)> generator)
{
    kernelRegistry[kernelName].generate = generator;
}

namespace {

static struct Init {
    Init()
    {
        ComputeRuntime::registerRuntime(ComputeRuntimeId::METAL, "metal",
                                        [] () { return new MetalComputeRuntime(); });

        auto getLibrary = [] (MetalComputeContext & context) -> mtlpp::Library
        {
            auto compileLibrary = [&] () -> mtlpp::Library
            {
                std::string fileName = "mldb/builtin/metal/base_kernels.metal";
                filter_istream stream(fileName);
                Utf8String source = "#line 1 \"" + fileName + "\"\n" + stream.readAll();

                ns::Error error{ns::Handle()};
                mtlpp::CompileOptions compileOptions;
                //compileOptions.setPreprocessorMacro("WBITS","32");
                mtlpp::Library library = context.mtlDevice.NewLibrary(source.rawData(), compileOptions, &error);

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
    
        auto createBlockFillArrayKernel = [getLibrary] (MetalComputeContext& context) -> std::shared_ptr<MetalComputeKernel>
        {
            auto library = getLibrary(context);
            auto result = std::make_shared<MetalComputeKernel>(&context);
            result->kernelName = "__blockFillArray";
            result->allowGridExpansion();
            result->addParameter("region", "w", "u8[regionLength]");
            result->addParameter("startOffsetInBytes", "r", "u64");
            result->addParameter("lengthInBytes", "r", "u64");
            result->addParameter("blockData", "r", "u8[blockLengthInBytes]");
            result->addParameter("blockLengthInBytes", "r", "u64");
            result->setComputeFunction(library, "__blockFillArrayKernel", { 256 });

            return result;
        };

        registerMetalComputeKernel("__blockFillArray", createBlockFillArrayKernel);

        auto createZeroFillArrayKernel = [getLibrary] (MetalComputeContext& context) -> std::shared_ptr<MetalComputeKernel>
        {
            auto library = getLibrary(context);
            auto result = std::make_shared<MetalComputeKernel>(&context);
            result->kernelName = "__zeroFillArray";
            result->allowGridExpansion();
            result->addParameter("region", "w", "u8[regionLength]");
            result->addParameter("startOffsetInBytes", "r", "u64");
            result->addParameter("lengthInBytes", "r", "u64");
            result->setComputeFunction(library, "__zeroFillArrayKernel", { 256 });

            return result;
        };

        registerMetalComputeKernel("__zeroFillArray", createZeroFillArrayKernel);
    }

} init;

} // file scope

} // namespace MLDB
