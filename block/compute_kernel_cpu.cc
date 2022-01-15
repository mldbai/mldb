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
std::map<std::string, std::shared_ptr<CPUComputeFunctionLibrary>> libraryRegistry;

EnvOption<bool, false> CPU_ENABLED("CPU_ENABLED", true);

struct CPUMemoryRegionHandleInfo: public GridMemoryRegionHandleInfo {
    std::byte * buffer = nullptr;
    const std::byte * constBuffer = nullptr;
    std::shared_ptr<const void> handle;  // underlying handle we want to keep around

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
        this->buffer = nullptr;
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
    // No-op for now as we don't have anything asynchronous happening
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
    std::memset((std::byte *)buffer + offset + startOffsetInBytes, 0, lengthInBytes);
}                                
                                
void
CPUComputeQueue::
enqueueBlockFillArrayConcrete(const std::string & opName,
                              MemoryRegionHandle region,
                              size_t startOffsetInBytes, ssize_t lengthInBytes,
                              std::span<const std::byte> block)
{
    auto [pin, buffer, offset] = CPUComputeContext::getMemoryRegion(opName, *region.handle, ACC_WRITE);
    for (size_t i = 0;  i < lengthInBytes;  i += block.size_bytes()) {
        memcpy((std::byte *)buffer + offset + startOffsetInBytes + i, block.data(), block.size());
    }
}                                
                                
void
CPUComputeQueue::
enqueueCopyFromHostConcrete(const std::string & opName,
                            MemoryRegionHandle toRegion,
                            FrozenMemoryRegion fromRegion,
                            size_t deviceStartOffsetInBytes)
{
    auto [pin, buffer, offset] = CPUComputeContext::getMemoryRegion(opName, *toRegion.handle, ACC_WRITE);
    memcpy((std::byte *)buffer + offset + deviceStartOffsetInBytes, fromRegion.data(), fromRegion.length());
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
          queue(queue),
          opName(opName)
    {
        ExcAssert(this->kernel);
        argTuple = this->kernel->cpuFunction->createArgTuple();
        isBound.resize(this->kernel->cpuFunction->argumentInfo.size(), false);
    }

    virtual ~CPUBindContext()
    {
    }

    HostComputeKernel hostKernel;
    const CPUComputeKernel * kernel = nullptr;
    CPUComputeQueue * queue = nullptr;
    std::string opName;
    std::any argTuple;
    std::vector<bool> isBound;
    size_t numBound = 0;
    std::map<std::string, size_t> threadGroupMemoryOffsets;
    size_t threadGroupMemoryRequired = 0;

    void setIsBound(int argNum)
    {
        bool current = isBound.at(argNum);
        if (current) {
            throw MLDB::Exception("Attempt to double-bind argument number " + std::to_string(argNum));
        }
        isBound[argNum] = true;
        ++numBound;
    }

    virtual void
    setPrimitive(const std::string & opName, int argNum, std::span<const std::byte> bytes) override
    {
        traceCPUOperation("setPrimitive " + opName, "argNum %d with %zd bytes", argNum, bytes.size_bytes());
        setIsBound(argNum);
        const TupleSetter & argSetter = std::any_cast<const TupleSetter &>(kernel->cpuFunction->argumentInfo.at(argNum).marshal);
        argSetter(*queue, opName, argTuple, bytes);
    }

    virtual void
    setBuffer(const std::string & opName, int argNum,
              std::shared_ptr<GridMemoryRegionHandleInfo> handle,
              MemoryRegionAccess access) override
    {
        traceCPUOperation("setBuffer " + opName, "argNum %d handle %s:%d with %zd bytes",
                            argNum, handle->name.c_str(), handle->version, handle->lengthInBytes);
        setIsBound(argNum);
        const TupleSetter & argSetter = std::any_cast<const TupleSetter &>(kernel->cpuFunction->argumentInfo.at(argNum).marshal);
        argSetter(*queue, opName, argTuple, {handle, access});
    }

    virtual void
    setThreadGroupMemory(const std::string & opName, int argNum, size_t nBytes) override
    {
        traceCPUOperation("setThreadGroupMemory " + opName, "argNum %d with %zd bytes", argNum, nBytes);
        setIsBound(argNum);
        auto & arg = kernel->cpuFunction->argumentInfo.at(argNum);

        threadGroupMemoryOffsets[arg.name] = threadGroupMemoryRequired;
        threadGroupMemoryRequired += nBytes;
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

            traceCPUOperation("grid size " + jsonEncodeStr(grid));
            traceCPUOperation("block size " + jsonEncodeStr(block));
            traceCPUOperation(std::to_string(invocations) + " invocations");

            if (numBound != isBound.size()) {
                throw MLDB::Exception("not all arguments bound");
            }

            this->kernel->cpuFunction->
                launch(*this->queue, opName, this->argTuple, grid, block,
                       this->threadGroupMemoryRequired, this->threadGroupMemoryOffsets);
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

    ExcAssert(handle.handle);
    auto info = std::dynamic_pointer_cast<const CPUMemoryRegionHandleInfo>(std::move(handle.handle));
    ExcAssert(info);
    //ExcAssert(info->constBuffer);

    if (!info->constBuffer) {
        cerr << "Null buffer: " << info->name << endl;
    }

    FrozenMemoryRegion raw(info, (char *)info->constBuffer, info->lengthInBytes);
    return raw;
}

FrozenMemoryRegion
CPUComputeQueue::
transferToHostSyncImpl(const std::string & opName,
                       MemoryRegionHandle handle)
{
    auto op = scopedOperation(OperationType::CPU_COMPUTE, "CPUComputeQueue transferToHostSyncImpl " + opName);

    return enqueueTransferToHostImpl(opName, std::move(handle));
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
    MemoryRegionHandle handle;
    auto info = std::make_shared<CPUMemoryRegionHandleInfo>();
    //ExcAssert(isConst);
    ExcAssert(region.data() || region.size() == 0);
    if (isConst) {
        info->init(region.data(), 0 /* offset */);
    }
    else {
        info->init((std::byte *)region.data(), 0 /* offset */);
    }

    info->lengthInBytes = region.size_bytes();
    info->type = &type;
    info->isConst = isConst;
    info->name = opName;
    handle.handle = info;

    return handle;
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
    // No-op for now as it's all synchronous
    return makeAlreadyResolvedEvent("flush");
}

void
CPUComputeQueue::
enqueueBarrier(const std::string & label)
{
    auto op = scopedOperation(OperationType::CPU_COMPUTE, "CPUComputeQueue enqueueBarrier " + label);
    // No-op for now as it's all synchronous
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
    : GridComputeContext(ComputeDevice::host()), backingStore(std::make_shared<MemorySerializer>())
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

    switch (access) {
    case ACC_NONE:
        return { std::move(pin), nullptr, 0 };
    case ACC_READ:
        return { std::move(pin), upcastHandle->constBuffer, upcastHandle->offset };
    case ACC_WRITE:  // fall through
    case ACC_READ_WRITE:
        return { std::move(pin), upcastHandle->buffer, upcastHandle->offset };
    default:
        MLDB_THROW_UNIMPLEMENTED("Unknown access value");
    }
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

MemoryRegionHandle
CPUComputeContext::
allocateSyncImpl(const std::string & regionName,
                 size_t length, size_t align,
                 const std::type_info & type, bool isConst)
{
    auto op = scopedOperation(OperationType::CPU_COMPUTE, "CPUComputeContext allocateSyncImpl " + regionName);
    MutableMemoryRegion mem = backingStore->allocateWritable(length, align);

    auto handle = std::make_shared<CPUMemoryRegionHandleInfo>();
    handle->init((std::byte *)mem.data(), 0 /* offset */);
    handle->type = &type;
    handle->isConst = isConst;
    handle->lengthInBytes = length;
    handle->name = regionName;
    handle->version = 0;
    handle->handle = mem.handle();

    ExcAssert(handle->buffer);
    ExcAssert(handle->constBuffer);

    MemoryRegionHandle result{std::move(handle)};
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
    auto result = it->second;
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
CPUComputeFunction()
{
}

std::vector<GridComputeFunctionArgument>
CPUComputeFunction::
getArgumentInfo() const
{
    return argumentInfo;
}


// CPUComputeFunctionLibrary

CPUComputeFunctionLibrary::
CPUComputeFunctionLibrary()
{
}

std::shared_ptr<GridComputeFunction>
CPUComputeFunctionLibrary::
getFunction(const std::string & functionName)
{
    std::unique_lock guard{libraryRegistryMutex};

    auto it = functions.find(functionName);
    if (it == functions.end()) {

        auto it2 = initializers.find(functionName);
        if (it2 == initializers.end()) {
            for (auto & [name, fn]: functions) {
                cerr << "  have " << name << endl;
            }
            throw MLDB::Exception("Couldn't find CPU compute function " + functionName);
        }

        it = functions.emplace(functionName, it2->second()).first;
    }
    return it->second;
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

void registerCpuKernelImpl(const std::string & libraryName, const std::string & functionName,
                           std::function<std::shared_ptr<CPUComputeFunction> ()> generator)
{
    ExcAssert(generator);

    std::unique_lock guard{libraryRegistryMutex};

    if (!libraryRegistry.count(libraryName)) {
        libraryRegistry[libraryName] = std::make_shared<CPUComputeFunctionLibrary>();
    }

    auto library = libraryRegistry[libraryName];
    ExcAssert(library);

    if (library->initializers.count(functionName)) {
        throw MLDB::Exception("Function " + functionName + " already registered in library " + libraryName);
    }

    library->initializers[functionName] = generator;
}

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

void throwDimensionException(unsigned dim, unsigned n)
{
    throw MLDB::Exception("Dimension overflow: passed dimension %d not in range 0-%d", dim, n);
}

void throwOverflow()
{
    throw MLDB::Exception("Sizing calculation overflow");
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
