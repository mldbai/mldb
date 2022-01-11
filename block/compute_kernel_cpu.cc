/** compute_kernel_cpu.cc                                                -*- C++ -*-
    Jeremy Barnes, 27 March 2016
    This file is part of MLDB. Copyright 2016 mldb.ai inc. All rights reserved.

    Compute kernel runtime for CPU devices.
*/

#include "compute_kernel_cpu.h"
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
#include "mldb/arch/exception.h"
#include <compare>

using namespace std;

namespace MLDB {

namespace {

std::mutex libraryRegistryMutex;
struct LibraryRegistryEntry {
    std::function<std::shared_ptr<CPUComputeFunctionLibrary>(CPUComputeContext & context)> generate;
};

std::map<std::string, LibraryRegistryEntry> libraryRegistry;

EnvOption<bool, false> CPU_ENABLED("CPU_ENABLED", true);

struct CPUMemoryRegionHandleInfo: public GridMemoryRegionHandleInfo {
    std::byte * buffer = nullptr;
    const std::byte * constBuffer = nullptr;

    virtual CPUMemoryRegionHandleInfo * clone() const
    {
        return new CPUMemoryRegionHandleInfo(*this);
    }

    void init(std::byte * mem, size_t offset)
    {
        this->constBuffer = this->buffer = mem;
        this->offset = offset;
        this->version = 0;
    }

    void init(const std::byte * mem, size_t offset)
    {
        this->constBuffer = mem;
        this->buffer = nullptr;
        this->offset = offset;
        this->version = 0;
    }
};

} // file scope


// CPUComputeEvent

CPUComputeEvent::
CPUComputeEvent(const std::string & label, bool isResolvedIn, const CPUComputeQueue * queue)
    : GridComputeEvent(label, isResolvedIn, queue)
{
}

void
CPUComputeEvent::
await() const
{
    MLDB_THROW_UNIMPLEMENTED();
}

std::shared_ptr<CPUComputeEvent>
CPUComputeEvent::
makeAlreadyResolvedEvent(const std::string & label, const CPUComputeQueue * queue)
{
    auto result = std::make_shared<CPUComputeEvent>(label, true /* already resolved */, queue);
    return result;
}

std::shared_ptr<CPUComputeEvent>
CPUComputeEvent::
makeUnresolvedEvent(const std::string & label, const CPUComputeQueue * queue)
{
    auto result = std::make_shared<CPUComputeEvent>(label, false /* already resolved */, queue);
    return result;
}

// CPUComputeQueue

CPUComputeQueue::
CPUComputeQueue(CPUComputeContext * owner, CPUComputeQueue * parent,
                const std::string & label,
                GridDispatchType dispatchType)
    : GridComputeQueue(owner, parent, label, dispatchType), cpuOwner(owner)
{
    MLDB_THROW_UNIMPLEMENTED();
}

CPUComputeQueue::~CPUComputeQueue()
{
}

std::shared_ptr<ComputeQueue>
CPUComputeQueue::
parallel(const std::string & opName)
{
    return std::make_shared<CPUComputeQueue>(this->cpuOwner, this, opName, GridDispatchType::PARALLEL);
}

std::shared_ptr<ComputeQueue>
CPUComputeQueue::
serial(const std::string & opName)
{
    return std::make_shared<CPUComputeQueue>(this->cpuOwner, this, opName, GridDispatchType::SERIAL);
}

void
CPUComputeQueue::
enqueueZeroFillArrayConcrete(const std::string & opName,
                             MemoryRegionHandle region,
                             size_t startOffsetInBytes, ssize_t lengthInBytes)
{
    auto [pin, buffer, offset] = CPUComputeContext::getMemoryRegion(opName, *region.handle, ACC_WRITE);
    MLDB_THROW_UNIMPLEMENTED();
}                                
                                
void
CPUComputeQueue::
enqueueBlockFillArrayConcrete(const std::string & opName,
                              MemoryRegionHandle region,
                              size_t startOffsetInBytes, ssize_t lengthInBytes,
                              std::span<const std::byte> block)
{
    ComputeQueue::enqueueFillArrayImpl(opName, region, MemoryRegionInitialization::INIT_BLOCK_FILLED, startOffsetInBytes, lengthInBytes, block);
}                                
                                
void
CPUComputeQueue::
enqueueCopyFromHostConcrete(const std::string & opName,
                            MemoryRegionHandle toRegion,
                            FrozenMemoryRegion fromRegion,
                            size_t deviceStartOffsetInBytes)
{
    auto [pin, buffer, offset] = CPUComputeContext::getMemoryRegion(opName, *toRegion.handle, ACC_WRITE);
    MLDB_THROW_UNIMPLEMENTED();
}                            
                            

FrozenMemoryRegion
CPUComputeQueue::
enqueueTransferToHostConcrete(const std::string & opName, MemoryRegionHandle handle)
{
    MLDB_THROW_UNIMPLEMENTED();
}


FrozenMemoryRegion
CPUComputeQueue::
transferToHostSyncConcrete(const std::string & opName, MemoryRegionHandle handle)
{
    MLDB_THROW_UNIMPLEMENTED();
}

struct CPUBindContext: public GridBindContext {
    CPUBindContext(CPUComputeQueue * queue,
                   const std::string & opName,
                   const GridComputeKernel * kernel,
                   const GridBindInfo * bindInfo)
        : kernel(dynamic_cast<const CPUComputeKernel *>(kernel)),
          queue(queue), opName(opName)
    {
        ExcAssert(this->kernel);
        MLDB_THROW_UNIMPLEMENTED();
    }

    virtual ~CPUBindContext()
    {
    }

    const CPUComputeKernel * kernel = nullptr;
    CPUComputeQueue * queue = nullptr;
    std::string opName;
    
    virtual void
    setPrimitive(const std::string & opName, int argNum, std::span<const std::byte> bytes) override
    {
        traceCPUOperation("setPrimitive " + opName, "argNum %d with %zd bytes", argNum, bytes.size_bytes());
        MLDB_THROW_UNIMPLEMENTED();
    }

    virtual void
    setBuffer(const std::string & opName, int argNum,
              std::shared_ptr<GridMemoryRegionHandleInfo> handle,
              MemoryRegionAccess access) override
    {
        traceCPUOperation("setBuffer " + opName, "argNum %d handle %s:%d with %zd bytes",
                            argNum, handle->name.c_str(), handle->version, handle->lengthInBytes);
        MLDB_THROW_UNIMPLEMENTED();
    }

    virtual void
    setThreadGroupMemory(const std::string & opName, int argNum, size_t nBytes) override
    {
        traceCPUOperation("setPrimitive " + opName, "argNum %d with %zd bytes", argNum, nBytes);
        MLDB_THROW_UNIMPLEMENTED();
    }

    virtual void launch(const std::string & opName,
                        GridBindContext & context, std::vector<size_t> grid,
                        std::vector<size_t> block) override
    {
        CPUBindContext & bindContext = dynamic_cast<CPUBindContext &>(context);
        auto * kernel = bindContext.kernel;

        try {
            auto op = scopedOperation(OperationType::CPU_COMPUTE, "launch kernel " + opName);
            grid.resize(3, 1);
            block.resize(3, 1);

            auto invocations = grid[0] * grid[1] * grid[2] * block[0] * block[1] * block[2];
            double memoryRequiredMb = invocations * 2048 / 1024.0 / 1024.0;

            traceCPUOperation("grid size " + jsonEncodeStr(grid));
            traceCPUOperation("block size " + jsonEncodeStr(block));
            traceCPUOperation(std::to_string(invocations) + " invocations requiring " + std::to_string(memoryRequiredMb)
                                + "MB of wired scratchpad memory");

            MLDB_THROW_UNIMPLEMENTED();
        } MLDB_CATCH_ALL {
            rethrowException(400, "Error launching CPU kernel " + kernel->kernelName);
        }
    }
};

std::shared_ptr<GridBindContext>
CPUComputeQueue::
newBindContext(const std::string & opName,
               const GridComputeKernel * kernel,
               const GridBindInfo * bindInfo)
{
    ExcAssert(bindInfo);
    return std::make_shared<CPUBindContext>(this, opName, kernel, bindInfo);
}

FrozenMemoryRegion
CPUComputeQueue::
enqueueTransferToHostImpl(const std::string & opName,
                          MemoryRegionHandle handle)
{
    auto op = scopedOperation(OperationType::CPU_COMPUTE, "CPUComputeQueue enqueueTransferToHostSyncImpl " + opName);
    MLDB_THROW_UNIMPLEMENTED();
}

FrozenMemoryRegion
CPUComputeQueue::
transferToHostSyncImpl(const std::string & opName,
                       MemoryRegionHandle handle)
{
    auto op = scopedOperation(OperationType::CPU_COMPUTE, "CPUComputeQueue transferToHostSyncImpl " + opName);
    MLDB_THROW_UNIMPLEMENTED();
}

MutableMemoryRegion
CPUComputeQueue::
enqueueTransferToHostMutableImpl(const std::string & opName, MemoryRegionHandle handle)
{
    MLDB_THROW_UNIMPLEMENTED();
}

MutableMemoryRegion
CPUComputeQueue::
transferToHostMutableSyncImpl(const std::string & opName,
                              MemoryRegionHandle handle)
{
    MLDB_THROW_UNIMPLEMENTED();
}

MemoryRegionHandle
CPUComputeQueue::
enqueueManagePinnedHostRegionImpl(const std::string & opName, std::span<const std::byte> region, size_t align,
                                  const std::type_info & type, bool isConst)
{
    auto op = scopedOperation(OperationType::CPU_COMPUTE, "CPUComputeContext managePinnedHostRegionImpl " + opName);
    return managePinnedHostRegionSyncImpl(opName, region, align, type, isConst);
}

MemoryRegionHandle
CPUComputeQueue::
managePinnedHostRegionSyncImpl(const std::string & opName,
                               std::span<const std::byte> region, size_t align,
                               const std::type_info & type, bool isConst)
{
    auto op = scopedOperation(OperationType::CPU_COMPUTE, "CPUComputeContext managePinnedHostRegionSyncImpl " + opName);
    MLDB_THROW_UNIMPLEMENTED();
}

void
CPUComputeQueue::
enqueueCopyBetweenDeviceRegionsImpl(const std::string & opName,
                                    MemoryRegionHandle from, MemoryRegionHandle to,
                                    size_t fromOffset, size_t toOffset,
                                    size_t length)
{
    MLDB_THROW_UNIMPLEMENTED();
}

void
CPUComputeQueue::
copyBetweenDeviceRegionsSyncImpl(const std::string & opName,
                                 MemoryRegionHandle from, MemoryRegionHandle to,
                                 size_t fromOffset, size_t toOffset,
                                 size_t length)
{
    enqueueCopyBetweenDeviceRegionsImpl(opName, from, to, fromOffset, toOffset, length);
    finish();
}

std::shared_ptr<ComputeEvent>
CPUComputeQueue::
flush()
{
    auto op = scopedOperation(OperationType::CPU_COMPUTE, "CPUComputeQueue flush");
    MLDB_THROW_UNIMPLEMENTED();
}

void
CPUComputeQueue::
enqueueBarrier(const std::string & label)
{
    auto op = scopedOperation(OperationType::CPU_COMPUTE, "CPUComputeQueue enqueueBarrier " + label);
    MLDB_THROW_UNIMPLEMENTED();
}

void
CPUComputeQueue::
finish()
{
    auto op = scopedOperation(OperationType::CPU_COMPUTE, "CPUComputeQueue finish");
    flush()->await();
}

std::shared_ptr<ComputeEvent>
CPUComputeQueue::
makeAlreadyResolvedEvent(const std::string & label) const
{
    return CPUComputeEvent::makeAlreadyResolvedEvent(label, this);
}


// CPUComputeContext

CPUComputeContext::
CPUComputeContext()
    : GridComputeContext(ComputeDevice::host())
{
}

std::tuple<std::shared_ptr<const void>, const std::byte *, size_t>
CPUComputeContext::
getMemoryRegion(const std::string & opName, MemoryRegionHandleInfo & handle, MemoryRegionAccess access)
{
    CPUMemoryRegionHandleInfo * upcastHandle = dynamic_cast<CPUMemoryRegionHandleInfo *>(&handle);
    if (!upcastHandle) {
        throw MLDB::Exception("TODO: get memory region from block handled from elsewhere: got " + demangle(typeid(handle)));
    }
    auto pin = upcastHandle->pinAccess(opName, access);

    return { std::move(pin), upcastHandle->buffer, upcastHandle->offset };
}

std::tuple<FrozenMemoryRegion, int /* version */>
CPUComputeContext::
getFrozenHostMemoryRegion(const std::string & opName, MemoryRegionHandleInfo & handle,
                          size_t offset, ssize_t length,
                          bool ignoreHazards) const
{
    CPUMemoryRegionHandleInfo * upcastHandle = dynamic_cast<CPUMemoryRegionHandleInfo *>(&handle);
    if (!upcastHandle) {
        throw MLDB::Exception("TODO: get memory region from block handled from elsewhere: got " + demangle(typeid(handle)));
    }
    return upcastHandle->getReadOnlyHostAccessSync(*this, opName, offset, length, ignoreHazards);
}

static MemoryRegionHandle
doCPUAllocate(const std::string & regionName,
              size_t length, size_t align,
              const std::type_info & type,
              bool isConst)
{
    cerr << "allocating " << length << " bytes for " << regionName << endl;
    std::byte * buffer = nullptr;

    MLDB_THROW_UNIMPLEMENTED();

    auto handle = std::make_shared<CPUMemoryRegionHandleInfo>();
    handle->init(buffer, 0 /* offset */);
    handle->type = &type;
    handle->isConst = isConst;
    handle->lengthInBytes = length;
    handle->name = regionName;
    handle->version = 0;

    MemoryRegionHandle result{std::move(handle)};
    return result;
}

MemoryRegionHandle
CPUComputeContext::
allocateSyncImpl(const std::string & regionName,
                 size_t length, size_t align,
                 const std::type_info & type, bool isConst)
{
    auto op = scopedOperation(OperationType::CPU_COMPUTE, "CPUComputeContext allocateSyncImpl " + regionName);
    auto result = doCPUAllocate(regionName, length, align, type, isConst);
    return result;
}

static MemoryRegionHandle
doCPUTransferToDevice(const std::string & opName, FrozenMemoryRegion region,
                      const std::type_info & type, bool isConst)
{
    std::byte * buffer = nullptr;
    MLDB_THROW_UNIMPLEMENTED();

    auto handle = std::make_shared<CPUMemoryRegionHandleInfo>();
    handle->init(buffer, 0);
    handle->type = &type;
    handle->isConst = isConst;
    handle->lengthInBytes = region.length();
    handle->name = opName;
    handle->version = 0;
    MemoryRegionHandle result{std::move(handle)};

    return result;
}

std::shared_ptr<ComputeQueue>
CPUComputeContext::
getQueue(const std::string & queueName)
{
    return std::make_shared<CPUComputeQueue>(this, nullptr /* parent */, queueName, GridDispatchType::SERIAL);
}

std::shared_ptr<GridComputeFunctionLibrary>
CPUComputeContext::
getLibrary(const std::string & libraryName)
{
    auto op = scopedOperation(OperationType::CPU_COMPUTE, "CPUComputeContext getLibrary " + libraryName);

    std::unique_lock guard(libraryRegistryMutex);
    auto it = libraryRegistry.find(libraryName);
    if (it == libraryRegistry.end()) {
        throw AnnotatedException(400, "Unable to find CPU compute library '" + libraryName + "'",
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
CPUComputeContext::
specializeKernel(const GridComputeKernelTemplate & tmplate)
{
    return std::make_shared<CPUComputeKernel>(this, tmplate);
}


// CPUComputeKernel

CPUComputeKernel::
CPUComputeKernel(CPUComputeContext * owner, const GridComputeKernelTemplate & tmplate)
    : GridComputeKernelSpecialization(owner, tmplate)
{
    this->cpuContext = owner;
    this->cpuFunction = dynamic_cast<const CPUComputeFunction *>(this->gridFunction.get());
    ExcAssert(this->cpuFunction);
}


// CPUComputeFunction

CPUComputeFunction::
CPUComputeFunction(CPUComputeContext & context)
{
}

std::vector<GridComputeFunctionArgument>
CPUComputeFunction::
getArgumentInfo() const
{
    std::vector<GridComputeFunctionArgument> result;

    MLDB_THROW_UNIMPLEMENTED();

#if 0
    for (size_t i = 0;  i < arguments.GetSize();  ++i) {
        mtlpp::Argument arg = arguments[i];
        std::string argName = arg.GetName().GetCStr();
        GridComputeFunctionArgumentDisposition disposition = getDisposition(arg.GetType());
        auto type = CPUComputeKernel::getKernelType(arg);

        GridComputeFunctionArgument entry;
        entry.name = argName;
        entry.disposition = disposition;
        entry.type = type;
        entry.computeFunctionArgIndex = arg.GetIndex();

        result.emplace_back(std::move(entry));
    }
#endif

    return result;
}


// CPUComputeFunctionLibrary

CPUComputeFunctionLibrary::
CPUComputeFunctionLibrary(CPUComputeContext & context)
    : context(context)
{
}

std::shared_ptr<GridComputeFunction>
CPUComputeFunctionLibrary::
getFunction(const std::string & functionName)
{
    MLDB_THROW_UNIMPLEMENTED();
}

std::string
CPUComputeFunctionLibrary::
getId() const
{
    MLDB_THROW_UNIMPLEMENTED();
}

Json::Value
CPUComputeFunctionLibrary::
getMetadata() const
{
    MLDB_THROW_UNIMPLEMENTED();
}

#if 0
std::shared_ptr<CPUComputeFunctionLibrary>
CPUComputeFunctionLibrary::
compileFromSourceFile(CPUComputeContext & context, const std::string & fileName)
{
    filter_istream stream(fileName);
    Utf8String source = /*"#line 1 \"" + fileName + "\"\n" +*/ stream.readAll();

    return compileFromSource(context, source, fileName);
}
#endif

#if 0
std::shared_ptr<CPUComputeFunctionLibrary>
CPUComputeFunctionLibrary::
compileFromSource(CPUComputeContext & context, const Utf8String & source, const std::string & fileNameToAppearInErrorMessages)
{
    ns::Error error{ns::Handle()};

    mtlpp::CompileOptions compileOptions;
    mtlpp::Library library = context.mtlDevice.NewLibrary(source.rawData(), compileOptions, &error);

    checkCPUError(error, "Error compiling CPU library from source", "fileName", fileNameToAppearInErrorMessages);

    return std::make_shared<CPUComputeFunctionLibrary>(context, std::move(library));
}
#endif

#if 0
std::shared_ptr<CPUComputeFunctionLibrary>
CPUComputeFunctionLibrary::
loadMtllib(CPUComputeContext & context, const std::string & libraryFilename)
{
    ns::Error error{ns::Handle()};

    mtlpp::Library library = context.mtlDevice.NewLibrary(libraryFilename.c_str(), &error);

    checkCPUError(error, "Error loading CPU library", "fileName", libraryFilename);

    return std::make_shared<CPUComputeFunctionLibrary>(context, std::move(library));
}
#endif

// CPUComputeRuntime

EnvOption<int> CPU_DEFAULT_DEVICE("CPU_DEFAULT_DEVICE", -1);

struct CPUComputeRuntime: public ComputeRuntime {

    CPUComputeRuntime()
    {
    }

    virtual ~CPUComputeRuntime()
    {
    }

    virtual ComputeRuntimeId getId() const
    {
        return ComputeRuntimeId::CPU;
    }

    virtual std::string printRestOfDevice(ComputeDevice device) const
    {
        return "";
    }

    virtual std::string printHumanReadableDeviceInfo(ComputeDevice device) const
    {
        return "CPU (local CPU used to emulate a grid-based compute device)";
    }

    virtual ComputeDevice getDefaultDevice() const
    {
        return ComputeDevice::host();
    }

    // Enumerate the devices available for this runtime
    virtual std::vector<ComputeDevice> enumerateDevices() const
    {
        return { ComputeDevice::host() };
    }

    // Get a compute context for this runtime
    virtual std::shared_ptr<ComputeContext>
    getContext(std::span<const ComputeDevice> devices) const
    {
        if (devices.size() != 1) {
            throw MLDB::Exception("CPU Compute Kernel driver only can accept a single device");
        }
        ExcAssertEqual(devices[0], ComputeDevice::host());
        return std::make_shared<CPUComputeContext>();
    }

};

void registerCPULibrary(const std::string & libraryName,
                          std::function<std::shared_ptr<CPUComputeFunctionLibrary>(CPUComputeContext &)> generator)
{
    std::unique_lock guard{libraryRegistryMutex};
    libraryRegistry[libraryName].generate = generator;
}

namespace {

static struct Init {
    Init()
    {
        ComputeRuntime::registerRuntime(ComputeRuntimeId::CPU, "cpu",
                                        [] () { return new CPUComputeRuntime(); });

        //registerCPULibrary("base_kernels", compileLibrary);
    }

} init;


} // file scope
} // namespace MLDB
