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

struct MetalMemoryRegionHandleInfo: public MemoryRegionHandleInfo {
    mtlpp::Buffer buffer;
    size_t offset = 0;

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

#if 0
MetalEventList toMetalEventList(const std::vector<std::shared_ptr<ComputeEvent>> & prereqs)
{
    MetalEventList clPrereqs;
    for (auto & ev: prereqs) {
        if (!ev)
            continue;
        auto clPrereq = std::dynamic_pointer_cast<const MetalComputeEvent>(ev);
        ExcAssert(clPrereq);
        if (!clPrereq->ev)
            continue;  // already satisfied
        clPrereqs.events.emplace_back(clPrereq->ev);
    }

    return clPrereqs;
}
#endif

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
    THROW_UNIMPLEMENTED;
#if 0    
    if (!ev) {
        traceOperation("await(): already satisfied");
        return;  // null event; already satisfied
    }

    auto tr = scopedOperation("await()");
    return ev.waitUntilFinished();
#endif
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
    THROW_UNIMPLEMENTED;

#if 0
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

        // TODO: constraints

        std::vector<size_t> clGrid, clBlock = kernel->block;
        
        if (kernel->allowGridExpansionFlag)
            ExcAssertLessEqual(grid.size(), clBlock.size());
        else
            ExcAssertEqual(grid.size(), clBlock.size());

        for (size_t i = 0;  i < clBlock.size();  ++i) {
            // Pad out the grid so we cover the whole lot.  The kernel will need to be
            // sure to no-op if it's out of bounds.
            auto b = clBlock[i];
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
            clGrid.push_back(range);
        }

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

        // figure out the values of the new constraints

#if 0
        bool progress = true;
        std::set<std::string> unknowns;

        while (progress) {
            progress = false;
            for (auto & c: bound.constraints) {
                progress = progress || c.attemptToSatisfy(knowns, unknowns);
            }
        }

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
#endif

        if (kernel->gridExpression) {
            clGrid = jsonDecode<decltype(clGrid)>(kernel->gridExpression->apply(knowns));
            knowns.setValue("clGrid", clGrid);
        }

        if (kernel->blockExpression) {
            clBlock = jsonDecode<decltype(clBlock)>(kernel->blockExpression->apply(knowns));
            knowns.setValue("clBlock", clBlock);
        }

        //cerr << "launching kernel " << kernel->kernelName << " with grid " << clGrid << " and block " << clBlock << endl;
        //cerr << "this->block = " << jsonEncodeStr(this->block) << endl;

        if (bindInfo->traceSerializer) {
            bindInfo->traceSerializer->newObject("grid", clGrid);
            bindInfo->traceSerializer->newObject("block", clBlock);
            bindInfo->traceSerializer->newObject("knowns", knowns.values);
            bindInfo->traceSerializer->commit();
        }

        auto timer = std::make_shared<Timer>();

        auto clPrereqs = toMetalEventList(prereqs);

        auto event = clQueue.launch(bindInfo->clKernel, clGrid, clBlock, clPrereqs);

        // Ensure it's submitted before we start using the event
        clQueue.flush();

    #if 0
        std::string kernelName = kernel->kernelName;

        auto execTimes = std::make_shared<std::map<MetalEventCommandExecutionStatus, double>>();

        auto doCallback = [this, kernelName, opName, execTimes, timer]
                (const MetalEvent & event, MetalEventCommandExecutionStatus status)
        {
            traceOperation("completion callback " + opName + " with status " + jsonEncodeStr(status));
            auto wallTime = timer->elapsed_wall();

            // TODO: lock?
            //execTimes->emplace(status, timer->elapsed_wall());

            //std::string msg = format("kernel %s status %s wallTime %.2fms\n",
            //                         kernelName.c_str(), jsonEncodeStr(status).c_str(), wallTime * 1000.0);
            //cerr << msg;

            if (status != MetalEventCommandExecutionStatus::COMPLETE)
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

        //event.addCallback(doCallback, MetalEventCommandExecutionStatus::QUEUED);
        //event.addCallback(doCallback, MetalEventCommandExecutionStatus::RUNNING);
        //event.addCallback(doCallback, MetalEventCommandExecutionStatus::SUBMITTED);
        event.addCallback(doCallback, MetalEventCommandExecutionStatus::COMPLETE);
        //event.addCallback(doCallback, MetalEventCommandExecutionStatus::ERROR);
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

        return std::make_shared<MetalComputeEvent>(std::move(event));
    } MLDB_CATCH_ALL {
        rethrowException(400, "Error launching kernel " + bound.owner->kernelName);
    }
#endif
}

ComputePromiseT<MemoryRegionHandle>
MetalComputeQueue::
enqueueFillArrayImpl(const std::string & opName,
                     MemoryRegionHandle region, MemoryRegionInitialization init,
                     size_t startOffsetInBytes, ssize_t lengthInBytes,
                     const std::any & arg,
                     std::vector<std::shared_ptr<ComputeEvent>> prereqs)
{
    THROW_UNIMPLEMENTED;

#if 0
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
#endif
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
    : mtlDevice(device)
{
}

#if 0
std::tuple<std::shared_ptr<const void>, cl_mem, size_t>
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
#endif

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

#if 0
static MemoryRegionHandle
doMetalAllocate(MetalContext & clContext,
                 const std::string & regionName,
                 size_t length, size_t align,
                 const std::type_info & type,
                 bool isConst)
{
    THROW_UNIMPLEMENTED;

#if 0
    // TODO: align...
    MetalMemObject mem = clContext.createBuffer(CL_MEM_READ_WRITE, length);

    auto handle = std::make_shared<MetalMemoryRegionHandleInfo>();
    handle->buffer = std::move(mem);
    handle->offset = 0;
    handle->type = &type;
    handle->isConst = isConst;
    handle->lengthInBytes = length;
    handle->name = regionName;
    handle->version = 0;

    MemoryRegionHandle result{std::move(handle)};
    return result;
#endif
}
#endif

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
    THROW_UNIMPLEMENTED;
#if 0
    auto result = doMetalAllocate(clContext, regionName, length, align, type, isConst);
    return clQueue->enqueueFillArrayImpl(regionName + " initialize", result, initialization,
                                       0 /* startOffsetInBytes */, -1 /*lengthinBytes*/, initWith);
#endif
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
    THROW_UNIMPLEMENTED;
#if 0
    auto op = scopedOperation("MetalComputeContext transferToHostImpl " + opName);

    ExcAssert(handle.handle);

    auto [pin, mem, offset] = getMemoryRegion(opName, *handle.handle, ACC_READ);
    //MetalEvent clEvent;
    //std::shared_ptr<void> memPtr;
    auto res = clQueue->clQueue.enqueueMapBuffer(mem, CL_MAP_READ,
                                    offset /* offset */, handle.lengthInBytes());

    //cerr << "transferToHostImpl: opName " << opName << " bytes " << handle.lengthInBytes() << endl;

    auto & memPtr = std::get<0>(res);
    auto & clEvent = std::get<1>(res);

    //cerr << "clEvent is " << clEvent.event.operator cl_event() << endl;

    //cerr << jsonEncode(clEvent.getInfo()) << endl;

    auto event = std::make_shared<MetalComputeEvent>(clEvent);
    auto promise = std::make_shared<std::promise<std::any>>();
    auto data = (char *)memPtr.get();

    auto cb = [handle, promise, data, memPtr, pin=pin] (const MetalEvent & event, auto status)
    {
        //cerr << "transferToHostImpl callback" << endl;
        if (status == MetalEventCommandExecutionStatus::ERROR)
            promise->set_exception(std::make_exception_ptr(MLDB::Exception("Metal error mapping host memory")));
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
#endif
}

FrozenMemoryRegion
MetalComputeContext::
transferToHostSyncImpl(const std::string & opName,
                       MemoryRegionHandle handle)
{
    THROW_UNIMPLEMENTED;
#if 0
    auto op = scopedOperation("MetalComputeContext transferToHostSyncImpl " + opName);

    ExcAssert(handle.handle);

    auto [pin, mem, offset] = getMemoryRegion(opName, *handle.handle, ACC_READ);
    auto memPtr = clQueue->clQueue.enqueueMapBufferBlocking(mem, CL_MAP_READ,
                                                            offset, handle.lengthInBytes());
    const char * data = (const char *)memPtr.get();
    FrozenMemoryRegion result(std::move(memPtr), data, handle.lengthInBytes());
    return result;
#endif
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
         = mtlpp::ResourceOptions::StorageModeManaged;

    auto deallocator = [] (auto ptr, auto len)
    {
    };

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

    THROW_UNIMPLEMENTED;

#if 0
    newInfo->buffer = MetalMemObject(info->buffer, false /* already retained */);
    newInfo->offset = info->offset + startOffsetInBytes;
    newInfo->isConst = isConst;
    newInfo->type = &type;
    newInfo->name = regionName;
    newInfo->lengthInBytes = lengthInBytes;
    newInfo->parent = info;
    newInfo->ownerOffset = startOffsetInBytes;
    newInfo->version = info->version;

    return { newInfo };
#endif

#if 0
    cl_buffer_region region = { startOffsetInBytes, lengthInBytes };
    cl_int error = CL_NONE;
    MetalMemObject mem(clCreateSubBuffer(info->mem, isConst ? CL_MEM_READ_ONLY : CL_MEM_READ_WRITE,
                        CL_BUFFER_CREATE_TYPE_REGION, &region, &error),
                        true /* already retained */);
    checkMetalError(error, "clCreateSubBuffer");

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

// Parses an Metal kernel argument info structure, and turns it into a ComputeKernel type
ComputeKernelType
MetalComputeKernel::
getKernelType(const mtlpp::Argument & arg)
{
    ComputeKernelType type;

    mtlpp::ArgumentType argType = arg.GetType();
    mtlpp::DataType dataType;

    if (argType == mtlpp::ArgumentType::Buffer) {
        cerr << "buffer argument " << arg.GetName().GetCStr() << endl;

        dataType = arg.GetBufferDataType();
        auto structType = arg.GetBufferStructType();
        auto align = arg.GetBufferAlignment();
        auto width = arg.GetBufferDataSize();

        cerr << "align " << align << " width " << width << endl;

        if (structType) {

            auto desc = std::make_shared<GenericStructureDescription>(false, "__metalKernelArg" + string(arg.GetName().GetCStr()));
            desc->align = align;
            desc->width = width;

            cerr << "structure type" << endl;
            auto members = structType.GetMembers();
            for (size_t i = 0;  i < members.GetSize();  ++i) {
                auto member = members[i];
                cerr << member.GetName().GetCStr() << " at offset " << member.GetOffset() << endl;
                auto kernelType = getKernelTypeFromDataType(member.GetDataType());
                cerr << "  data type " << (int)member.GetDataType() << " " << kernelType.print() << endl;
                //cerr << "  array type " << member.GetArrayType()

                desc->addFieldDesc(member.GetName().GetCStr(), member.GetOffset(), "", kernelType.baseType);
            }
            
            type.baseType = desc;
        }
        else {
            cerr << "dataType = " << (int)dataType << endl;
            type = getKernelTypeFromDataType(dataType);
            type.dims.emplace_back();  // if it's a basic type, it must be an array
        }
    }
    else if (argType == mtlpp::ArgumentType::ThreadgroupMemory) {
        throw MLDB::Exception("Not implemented: thread group argument types");

    }
    else {
        throw MLDB::Exception("Not implemented: non-buffer and non-thread group argument types");
    }

    mtlpp::ArgumentAccess access = arg.GetAccess();
    switch (access) {
        case mtlpp::ArgumentAccess::ReadOnly: type.access = ACC_READ;  break;
        case mtlpp::ArgumentAccess::ReadWrite: type.access = ACC_READ_WRITE;  break;
        case mtlpp::ArgumentAccess::WriteOnly: type.access = ACC_WRITE;  break;
        default:
            type.access = ACC_NONE;
    }

    ExcAssert(type.baseType);
    return type;

#if 0
    bool isConst = info.typeQualifier.test(MetalArgTypeQualifier::CONST);
    int arrayDim = 0;
    std::string clTypeName = info.typeName;
    while (!clTypeName.empty() && clTypeName.back() == '*') {
        ++arrayDim;
        clTypeName.pop_back();
    }

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
        type.dims.push_back({nullptr});
    }

    type.access = isConst ? ACC_READ : ACC_READ_WRITE;

    return type;
#endif
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
    mtlpp::ComputePipelineReflection reflection;
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

    for (size_t i = 0;  i < arguments.GetSize();  ++i) {
        mtlpp::Argument arg = arguments[i];
        cerr << "argument " << i << " has name " << arg.GetName().GetCStr() << " and index " << arg.GetIndex() << endl;

        auto type = getKernelType(arg);
        cerr << "type = " << type.print() << endl;
        std::string argName = arg.GetName().GetCStr();

        auto it = paramIndex.find(argName);
        if (it == paramIndex.end()) {
            if (type.baseType->kind == ValueKind::STRUCTURE && type.dims.empty()) {
                // Check if we know the unpacked types
                auto numFields = type.baseType->getFixedFieldCount();
                std::vector<std::string> fieldsNotFound;
                for (size_t i = 0;  i < numFields;  ++i) {
                    const auto & field = type.baseType->getFieldByNumber(i);
                    if (paramIndex.count(field.fieldName))
                        continue;
                    fieldsNotFound.push_back(field.fieldName);
                }

                if (fieldsNotFound.empty()) {
                    // Can be assembled from known parameters
                    continue;
                }
                else if (fieldsNotFound.size() < numFields) {
                    throw MLDB::Exception("Kernel parameter " + std::to_string(i)
                                    + " (" + argName + ") to Metal kernel " + kernelName
                                    + " cannot be assembled because of missing fields " + jsonEncodeStr(fieldsNotFound));
                }
            }
            if (this->setters.size() > 0)
                continue;  // should be done in the setter...
            mtlpp::ArgumentType type = arg.GetType();
            if (type == mtlpp::ArgumentType::ThreadgroupMemory) {
                throw MLDB::Exception("Thread group parameter in kernel with no setters defined; "
                                        "implement a setter to avoid launch failure");
            }
            throw MLDB::Exception("Kernel parameter " + std::to_string(i)
                                    + " (" + argName + ") to Metal kernel " + kernelName
                                    + " has no counterpart in formal parameter list");
        }
        correspondingArgumentNumbers.at(i) = it->second;
        auto & param = params.at(it->second);
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

BoundComputeKernel
MetalComputeKernel::
bindImpl(std::vector<ComputeKernelArgument> argumentsIn) const
{
    auto op = scopedOperation("MetalComputeKernel bindImpl " + kernelName);

    ExcAssert(this->context);
    auto & upcastContext = dynamic_cast<MetalComputeContext &>(*this->context);

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

#if 0
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
#endif

    mtlpp::CommandBuffer commandBuffer = upcastContext.mtlQueue.CommandBuffer();
    ExcAssert(commandBuffer);

    mtlpp::ComputeCommandEncoder commandEncoder = commandBuffer.ComputeCommandEncoder();

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

    THROW_UNIMPLEMENTED;

#if 0
    Json::Value argInfo;
    for (size_t i = 0;  i < this->clKernelInfo.args.size();  ++i) {
        std::string opName = "bind arg " + std::to_string(i) + " " + this->clKernelInfo.args[i].name;
        auto tr = scopedOperation(opName);
        int argNum = correspondingArgumentNumbers.at(i);
        //cerr << "binding Metal parameter " << i << " from argument " << paramNum << endl;
        if (argNum == -1) {
            // Will be done via setter...
            continue;
        }

        auto & paramType = params[argNum].type;

        // Handle an argument that wasn't passed (ie, an implicit argument)
        auto handleImplicit = [&] ()
        {
            THROW_UNIMPLEMENTED;
#if 0
            if (this->clKernelInfo.args[i].addressQualifier == MetalArgAddressQualifier::LOCAL) {
                ExcAssertEqual(paramType.dims.size(), 1);
                ExcAssert(paramType.dims[0].bound);
                auto len = paramType.dims[0].bound->apply(result.knowns).asUInt();
                size_t nbytes = len * paramType.baseType->width;
                traceOperation("binding local array handle with " + std::to_string(nbytes) + " bytes");
                kernel.bindArg(i, LocalArray<std::byte>(nbytes));
                Json::Value known;
                known["elAlign"] = paramType.baseType->align;
                known["elWidth"] = paramType.baseType->width;
                known["elType"] = paramType.baseType->typeName;
                known["length"] = len;
                known["type"] = MetalComputeKernel::getKernelType(this->clKernelInfo.args[i]).print();
                known["name"] = "<<<Local array>>>";
                result.knowns.setValue(this->clKernelInfo.args[i].name, std::move(known));
            }
            else if (this->clKernelInfo.args[i].addressQualifier == MetalArgAddressQualifier::PRIVATE) {
                auto type = MetalComputeKernel::getKernelType(this->clKernelInfo.args[i]);
                auto val = result.knowns.getValue(this->clKernelInfo.args[i].name);
                traceOperation("binding known value " + this->clKernelInfo.args[i].name + " = " + val.toStringNoNewLine() + " as " + type.print());
                std::shared_ptr<void> mem(type.baseType->constructDefault(), [=] (void * p) { type.baseType->destroy(p); });
                StructuredJsonParsingContext context(val);
                type.baseType->parseJson(mem.get(), context);
                kernel.bindArg(i, mem.get(), type.baseType->width);
            }
#endif
        };

        if (argNum >= result.arguments.size()) {
            handleImplicit();
        }
        else {
            //cerr << "argNum = " << argNum << endl;
            //cerr << "result.arguments.size() = " << result.arguments.size() << endl;
            const ComputeKernelArgument & arg = result.arguments.at(argNum);

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
            std::string opName = "bind " + this->clKernelInfo.args[i].name + std::to_string(++disamb);
            if (!arg.handler) {
                handleImplicit();
            }
            else if (arg.handler->canGetPrimitive()) {
                auto bytes = arg.handler->getPrimitive(opName, upcastContext);
                traceOperation("binding handle with " + std::to_string(bytes.size()) + " bytes");
                kernel.bindArg(i, bytes.data(), bytes.size());
            }
            else if (arg.handler->canGetHandle()) {
                auto handle = arg.handler->getHandle(opName, upcastContext);
                traceOperation("binding handle with " + std::to_string(handle.lengthInBytes()) + " bytes");
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
                throw MLDB::Exception("don't know how to handle passing parameter to Metal");
            }
        }
    }

    // Run the setters to set the other parameters
    for (auto & s: this->setters) {
        s(kernel, upcastContext);
    }
#endif

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

#if 0
    if (bindInfo->traceSerializer) {
        bindInfo->traceSerializer->newObject("args", argInfo);
        bindInfo->traceSerializer->newObject("knowns", result.knowns.values);
        bindInfo->traceSerializer->newObject("unknowns", result.unknowns);
        bindInfo->traceSerializer->newObject("constraints", result.constraints);
        bindInfo->traceSerializer->newObject("tuneables", tuneables);
    }
#endif

#if 0
    cerr << "got " << result.constraints.size() << " constraints" << endl;
    for (auto & c: result.constraints) {
        cerr << "  " << c.print()
             << (c.satisfied(result.knowns) ? " [SATSIFIED]" : " [UNSATISFIED]") << endl;
    }
#endif

#if 0
    commandEncoder.SetBuffer(inBuffer, 0, 0);
    commandEncoder.SetBuffer(outBuffer, 0, 1);
    commandEncoder.SetComputePipelineState(computePipelineState);
    commandEncoder.DispatchThreadgroups(
        mtlpp::Size(1, 1, 1),
        mtlpp::Size(dataCount, 1, 1));
    commandEncoder.EndEncoding();

    mtlpp::BlitCommandEncoder blitCommandEncoder = commandBuffer.BlitCommandEncoder();
    blitCommandEncoder.Synchronize(outBuffer);
    blitCommandEncoder.EndEncoding();

    commandBuffer.Commit();
    commandBuffer.WaitUntilCompleted();
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

#if 0
        auto getProgram = [] (MetalComputeContext & context) -> MetalProgram
        {

            auto compileProgram = [&] () -> MetalProgram
            {
                std::string fileName = "mldb/builtin/metal/base_kernels.cl";
                filter_istream stream(fileName);
                Utf8String source = "#line 1 \"" + fileName + "\"\n" + stream.readAll();

                MetalProgram program = context.clContext.createProgram(source);
                string options = "-cl-kernel-arg-info";

                // Build for all devices
                auto buildInfo = program.build(context.clDevices, options);
                
                cerr << jsonEncode(buildInfo[0]) << endl;
                return program;
            };

            static const std::string cacheKey = "__base_kernels";
            MetalProgram program = context.getCacheEntry(cacheKey, compileProgram);
            return program;
        };
    
        auto createBlockFillArrayKernel = [getProgram] (MetalComputeContext& context) -> std::shared_ptr<MetalComputeKernel>
        {
            auto program = getProgram(context);
            auto result = std::make_shared<MetalComputeKernel>();
            result->kernelName = "__blockFillArray";
            result->allowGridExpansion();
            result->addParameter("region", "w", "u8[regionLength]");
            result->addParameter("startOffsetInBytes", "r", "u64");
            result->addParameter("lengthInBytes", "r", "u64");
            result->addParameter("blockData", "r", "u8[blockLengthInBytes]");
            result->addParameter("blockLengthInBytes", "r", "u64");
            result->setComputeFunction(program, "__blockFillArrayKernel", { 256 });

            return result;
        };

        registerMetalComputeKernel("__blockFillArray", createBlockFillArrayKernel);

        auto createZeroFillArrayKernel = [getProgram] (MetalComputeContext& context) -> std::shared_ptr<MetalComputeKernel>
        {
            auto program = getProgram(context);
            auto result = std::make_shared<MetalComputeKernel>();
            result->kernelName = "__zeroFillArray";
            result->allowGridExpansion();
            result->addParameter("region", "w", "u8[regionLength]");
            result->addParameter("startOffsetInBytes", "r", "u64");
            result->addParameter("lengthInBytes", "r", "u64");
            result->setComputeFunction(program, "__zeroFillArrayKernel", { 256 });

            return result;
        };

        registerMetalComputeKernel("__zeroFillArray", createZeroFillArrayKernel);
#endif
    }

} init;

} // file scope

} // namespace MLDB
