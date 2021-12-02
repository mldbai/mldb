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

std::mutex kernelRegistryMutex;
struct KernelRegistryEntry {
    std::function<std::shared_ptr<OpenCLComputeKernel>(OpenCLComputeContext & context)> generate;
};

std::map<std::string, KernelRegistryEntry> kernelRegistry;

struct OpenCLBindInfo: public ComputeKernelBindInfo {
    virtual ~OpenCLBindInfo() = default;

    OpenCLKernel clKernel;
    const OpenCLComputeKernel * owner = nullptr;
    std::shared_ptr<StructuredSerializer> traceSerializer;

    // Pins that control the lifetime of the arguments and allow the system to know
    // when an argument is no longer needed
    std::vector<std::shared_ptr<const void>> argumentPins;
};

EnvOption<int> OPENCL_TRACE_API_CALLS("OPENCL_COMPUTE_TRACE_API_CALLS", 0);
EnvOption<std::string> OPENCL_KERNEL_TRACE_FILE("OPENCL_KERNEL_TRACE_FILE", "");

std::shared_ptr<ZipStructuredSerializer> traceSerializer;
std::shared_ptr<StructuredSerializer> regionsSerializer;
std::shared_ptr<StructuredSerializer> kernelsSerializer;
std::shared_ptr<StructuredSerializer> programsSerializer;

using TracedRegionKey = std::tuple<std::string, int>;
//struct TracedRegionKey {
//    std::string name;
//    int version = -1;
//    auto operator < (const TracedRegionKey & other) const = default;
//};

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
        if (OPENCL_KERNEL_TRACE_FILE.specified()) {
            traceSerializer = std::make_shared<ZipStructuredSerializer>(OPENCL_KERNEL_TRACE_FILE.get());
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

struct OpenCLMemoryRegionHandleInfo: public MemoryRegionHandleInfo {
    OpenCLMemObject memBase;
    size_t offset = 0;

    std::mutex mutex;
    int numReaders = 0;
    int numWriters = 0;
    std::set<std::string> currentWriters;
    std::set<std::string> currentReaders;

    void init(OpenCLMemObject mem, size_t offset)
    {
        this->memBase = std::move(mem);
        this->offset = offset;
        this->version = 0;
    }

    std::tuple<FrozenMemoryRegion, int /* version */>
    getReadOnlyHostAccessSync(const OpenCLComputeContext & context,
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

        std::shared_ptr<const void> region
            = context.clQueue->clQueue.enqueueMapBufferBlocking(memBase, CL_MAP_READ,
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
    }

    std::tuple<std::shared_ptr<const void>, cl_mem>
    getOpenCLAccess(const std::string & opName, MemoryRegionAccess access)
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
            return { std::move(pin), this->memBase };
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
            return { std::move(pin), this->memBase };
        }
    }
};

OpenCLEventList toOpenCLEventList(const std::vector<std::shared_ptr<ComputeEvent>> & prereqs)
{
    OpenCLEventList clPrereqs;
    for (auto & ev: prereqs) {
        if (!ev)
            continue;
        auto clPrereq = std::dynamic_pointer_cast<const OpenCLComputeEvent>(ev);
        ExcAssert(clPrereq);
        if (!clPrereq->ev)
            continue;  // already satisfied (dummy event)
        clPrereqs.events.emplace_back(clPrereq->ev);
    }

    return clPrereqs;
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
thenImpl(std::function<void ()> fn, const std::string & label)
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
    try {
        auto tr = scopedOperation("launch kernel " + bound.owner->kernelName + " as " + opName);

        ExcAssert(bound.bindInfo);
        
        const OpenCLBindInfo * bindInfo
            = dynamic_cast<const OpenCLBindInfo *>(bound.bindInfo.get());
        ExcAssert(bindInfo);

        const OpenCLComputeKernel * kernel = bindInfo->owner;

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

        auto clPrereqs = toOpenCLEventList(prereqs);

        auto event = clQueue.launch(bindInfo->clKernel, clGrid, clBlock, clPrereqs);

        // Ensure it's submitted before we start using the event
        clQueue.flush();

    #if 0
        std::string kernelName = kernel->kernelName;

        auto execTimes = std::make_shared<std::map<OpenCLEventCommandExecutionStatus, double>>();

        auto doCallback = [this, kernelName, opName, execTimes, timer]
                (const OpenCLEvent & event, OpenCLEventCommandExecutionStatus status)
        {
            traceOperation("completion callback " + opName + " with status " + jsonEncodeStr(status));
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

        return std::make_shared<OpenCLComputeEvent>(std::move(event));
    } MLDB_CATCH_ALL {
        rethrowException(400, "Error launching OpenCL kernel " + bound.owner->kernelName);
    }
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

ComputePromiseT<MemoryRegionHandle>
OpenCLComputeQueue::
enqueueCopyFromHostImpl(const std::string & opName,
                        MemoryRegionHandle toRegion,
                        FrozenMemoryRegion fromRegion,
                        size_t deviceStartOffsetInBytes,
                        std::vector<std::shared_ptr<ComputeEvent>> prereqs)
{
    auto op = scopedOperation("OpenCLComputeQueue enqueueCopyFromHostImpl " + opName);

    ExcAssert(toRegion.handle);

    auto clPrereqs = toOpenCLEventList(prereqs);

    auto [pin, mem, offset] = OpenCLComputeContext::getMemoryRegion(opName, *toRegion.handle, ACC_WRITE);
    auto res = clQueue.enqueueWriteBuffer(mem, offset, fromRegion.length(), fromRegion.data(), clPrereqs);

    return { toRegion, std::make_shared<OpenCLComputeEvent>(res) };
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
makeAlreadyResolvedEvent(const std::string & label) const
{
    return std::make_shared<OpenCLComputeEvent>();
}


// OpenCLComputeContext

OpenCLComputeContext::
OpenCLComputeContext(std::vector<OpenCLDevice> clDevices, std::vector<ComputeDevice> devices)
    : clContext(clDevices),
        clDevices(std::move(clDevices)), devices(std::move(devices))
{
    clQueue = std::make_shared<OpenCLComputeQueue>(this);
}

ComputeDevice
OpenCLComputeContext::
getDevice() const
{
    return devices.at(0);
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

    auto [pin, mem, offset] = getMemoryRegion(opName, *handle.handle, ACC_READ);
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

    auto cb = [handle, promise, data, memPtr, pin=pin] (const OpenCLEvent & event, auto status)
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

    auto [pin, mem, offset] = getMemoryRegion(opName, *handle.handle, ACC_READ);
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

    auto [pin, mem, offset] = getMemoryRegion(opName, *handle.handle, ACC_READ_WRITE);
    OpenCLEvent clEvent;
    std::shared_ptr<void> memPtr;
    std::tie(memPtr, clEvent)
        = clQueue->clQueue.enqueueMapBuffer(mem, CL_MAP_READ | CL_MAP_WRITE,
                                    offset, handle.lengthInBytes());

    auto event = std::make_shared<OpenCLComputeEvent>(std::move(clEvent));
    auto promise = std::shared_ptr<std::promise<std::any>>();

    auto cb = [handle, promise, memPtr, pin=pin] (const OpenCLEvent & event, auto status)
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

    auto [pin, mem, offset] = getMemoryRegion(opName, *handle.handle, ACC_READ_WRITE);
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
    if (traceSerializer) {
        result->traceSerializer = kernelsSerializer->newStructure(kernelName);
        result->runsSerializer = result->traceSerializer->newStructure("runs");
    }
    return result;
}

ComputePromiseT<MemoryRegionHandle>
OpenCLComputeContext::
managePinnedHostRegionImpl(const std::string & opName, std::span<const std::byte> region, size_t align,
                           const std::type_info & type, bool isConst)
{
    auto op = scopedOperation("OpenCLComputeContext managePinnedHostRegionImpl " + opName);

    auto result = managePinnedHostRegionSyncImpl(opName, region, align, type, isConst);
    return ComputePromiseT<MemoryRegionHandle>(std::move(result), clQueue->makeAlreadyResolvedEvent(opName));
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
OpenCLComputeContext::
fillDeviceRegionFromHostImpl(const std::string & opName,
                             MemoryRegionHandle deviceHandle,
                             std::shared_ptr<std::span<const std::byte>> pinnedHostRegion,
                             size_t deviceOffset)
{
    FrozenMemoryRegion region(pinnedHostRegion, (const char *)pinnedHostRegion->data(), pinnedHostRegion->size_bytes());

    return clQueue
        ->enqueueCopyFromHostImpl(opName, deviceHandle, region, deviceOffset, {}).event();
}                                     

void
OpenCLComputeContext::
fillDeviceRegionFromHostSyncImpl(const std::string & opName,
                                 MemoryRegionHandle deviceHandle,
                                 std::span<const std::byte> hostRegion,
                                 size_t deviceOffset)
{
    FrozenMemoryRegion region(nullptr, (const char *)hostRegion.data(), hostRegion.size_bytes());

    clQueue
        ->enqueueCopyFromHostImpl(opName, deviceHandle, region, deviceOffset, {})
    .await();
}

std::shared_ptr<ComputeEvent>
OpenCLComputeContext::
copyBetweenDeviceRegionsImpl(const std::string & opName,
                                MemoryRegionHandle from, MemoryRegionHandle to,
                                size_t fromOffset, size_t toOffset,
                                size_t length)
{
    auto [fromPin, fromMem, fromBaseOffset] = getMemoryRegion(opName, *from.handle, ACC_READ);
    auto [toPin, toMem, toBaseOffset] = getMemoryRegion(opName, *to.handle, ACC_WRITE);

    auto event = clQueue->clQueue.enqueueCopyBuffer(fromMem, toMem, fromBaseOffset + fromOffset, toBaseOffset + toOffset, length);
    return std::make_shared<OpenCLComputeEvent>(std::move(event));
}

void
OpenCLComputeContext::
copyBetweenDeviceRegionsSyncImpl(const std::string & opName,
                                    MemoryRegionHandle from, MemoryRegionHandle to,
                                    size_t fromOffset, size_t toOffset,
                                    size_t length)
{
    copyBetweenDeviceRegionsImpl(opName, from, to, fromOffset, toOffset, length)->await();
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

    auto info = std::dynamic_pointer_cast<const OpenCLMemoryRegionHandleInfo>(std::move(handle.handle));
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

    auto newInfo = std::make_shared<OpenCLMemoryRegionHandleInfo>();

    newInfo->memBase = OpenCLMemObject(info->memBase, false /* already retained */);
    newInfo->offset = info->offset + startOffsetInBytes;
    newInfo->isConst = isConst;
    newInfo->type = &type;
    newInfo->name = regionName;
    newInfo->lengthInBytes = lengthInBytes;
    newInfo->parent = info;
    newInfo->ownerOffset = startOffsetInBytes;
    newInfo->version = info->version;

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
        type.dims.push_back({nullptr});
    }

    type.access = isConst ? ACC_READ : ACC_READ_WRITE;

    return type;
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
setGridExpression(const std::string & gridExpr, const std::string & blockExpr)
{
    if (!gridExpr.empty())
        this->gridExpression = CommandExpression::parseArgumentExpression(gridExpr);
    if (!blockExpr.empty())
        this->blockExpression = CommandExpression::parseArgumentExpression(blockExpr);
}

void
OpenCLComputeKernel::
addTuneable(const std::string & name, int64_t defaultValue)
{
    this->tuneables.push_back({name, defaultValue});
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
        //cerr << "doing arg " << jsonEncodeStr(arg) << endl;
        auto type = getKernelType(arg);
        //cerr << "type = " << type.print() << endl;
        std::string argName = arg.name;
        auto it = paramIndex.find(argName);
        if (it == paramIndex.end()) {
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
        auto tr = scopedOperation(opName);
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
                auto len = paramType.dims[0].bound->apply(result.knowns).asUInt();
                size_t nbytes = len * paramType.baseType->width;
                traceOperation("binding local array handle with " + std::to_string(nbytes) + " bytes");
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
                traceOperation("binding known value " + this->clKernelInfo.args[i].name + " = " + val.toStringNoNewLine() + " as " + type.print());
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
        bindInfo->traceSerializer->newObject("args", argInfo);
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
        return std::make_shared<OpenCLComputeContext>(clDevices, vector<ComputeDevice>{devices.begin(), devices.end()});
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
            result->setComputeFunction(program, "__zeroFillArrayKernel", { 256 });

            return result;
        };

        registerOpenCLComputeKernel("__zeroFillArray", createZeroFillArrayKernel);
    }

} init;

} // file scope
} // namespace MLDB
