/** compute_kernel_host.h                                                -*- C++ -*-
    Jeremy Barnes, 27 March 2016
    This file is part of MLDB. Copyright 2016 mldb.ai inc. All rights reserved.

    Compute kernel runtime for CPU devices.
*/

#include "compute_kernel_host.h"
#include "mldb/types/basic_value_descriptions.h"


namespace MLDB {

namespace details {

void copyUsingValueDescription(const ValueDescription * desc,
                               std::span<const std::byte> from, void * to,
                               const std::type_info & toType)
{
    if (toType != *desc->type) {
        throw MLDB::Exception("Attempt to copy different types using ValueDescription: from = "
                              + demangle(*desc->type) + " to = " + demangle(toType));
    }

    desc->copyValue(from.data(), to);
}

const std::type_info & getTypeFromValueDescription(const ValueDescription * desc)
{
    if (!desc)
        return typeid(void);
    return *(desc->type);
}

} // namespace Details

namespace {

std::mutex kernelRegistryMutex;
struct KernelRegistryEntry {
    std::function<std::shared_ptr<ComputeKernel>()> generate;
};

std::map<std::string, KernelRegistryEntry> kernelRegistry;

} // file scope

// HostComputeKernel

namespace {

struct HostComputeKernelBindInfo: public ComputeKernelBindInfo {
    HostComputeKernel::Callable call;
};

struct HostMemoryRegionInfo: public MemoryRegionHandleInfo {
    const void * data = nullptr;
    std::shared_ptr<const void> handle;  // underlying handle we want to keep around

    void init(const MutableMemoryRegion & region)
    {
        this->data = region.data();
        this->lengthInBytes = region.length();
        this->isConst = false;
        this->handle = region.handle();
    }

    void init(const FrozenMemoryRegion & region)
    {
        this->data = region.data();
        this->lengthInBytes = region.length();
        this->isConst = false;
        this->handle = region.handle();
    }

    template<typename T>
    void init(const MutableMemoryRegionT<T> & array)
    {
        this->init(array.raw());
        this->type = &typeid(T);
    }

    template<typename T>
    void init(const FrozenMemoryRegionT<T> & array)
    {
        this->init(array.raw());
        this->type = &typeid(T);
    }
};

} // file scope

BoundComputeKernel
HostComputeKernel::
bindImpl(std::vector<ComputeKernelArgument> arguments, ComputeKernelConstraintSolution knowns) const
{
    auto bindInfo = std::make_shared<HostComputeKernelBindInfo>();
    bindInfo->call = createCallable(*this->context, arguments);

    BoundComputeKernel result;
    result.arguments = std::move(arguments);
    result.owner = this;
    result.bindInfo = std::move(bindInfo);
    result.constraints = constraints;
    result.preConstraints = preConstraints;
    result.postConstraints = postConstraints;
    result.tuneables = this->tuneables;
    result.knowns = std::move(knowns);

    result.setKnownsFromArguments();

    return result;
}

void
HostComputeKernel::
call(const BoundComputeKernel & bound, std::span<ComputeKernelGridRange> grid) const
{
    const HostComputeKernelBindInfo * hostInfo
        = dynamic_cast<const HostComputeKernelBindInfo *>(bound.bindInfo.get());
    ExcAssert(hostInfo);

    hostInfo->call(*context, grid);
}


// HostComputeContext

struct HostComputeContext: public ComputeContext {

    HostComputeContext()
        : backingStore(new MemorySerializer())
    {
    }

    virtual ~HostComputeContext() = default;

    std::shared_ptr<MappedSerializer> backingStore;

    virtual ComputeDevice getDevice() const override { return ComputeDevice::host(); }

    virtual ComputePromiseT<MemoryRegionHandle>
    allocateImpl(const std::string & regionName,
                 size_t length, size_t align,
                 const std::type_info & type, bool isConst,
                 MemoryRegionInitialization initialization,
                 std::any initWith = std::any()) override
    {
        MutableMemoryRegion mem;

        switch (initialization) {
            case INIT_NONE:
                mem = backingStore->allocateWritable(length, align);
                break;
            case INIT_ZERO_FILLED:
                mem = backingStore->allocateZeroFilledWritable(length, align);
                break;
            case INIT_BLOCK_FILLED: {
                mem = backingStore->allocateWritable(length, align);
                auto [init, len] = std::any_cast<std::pair<const void *, size_t>>(initWith);
                ExcAssertGreater(len, 0);
                ExcAssertEqual(length % len, 0);
                for (size_t offset = 0;  offset < length;  offset += len) {
                    std::memcpy(mem.data() + offset, init, len);
                }
                break;
            }
            default:
                throw MLDB::Exception("Unknown initialization in allocateImpl");
        }

        auto result = std::make_shared<HostMemoryRegionInfo>();
        result->init(std::move(mem));
        result->type = &type;
        result->isConst = isConst;
        result->name = regionName;
        return { {std::move(result)}, std::make_shared<HostComputeEvent>() };
    }

    virtual ComputePromiseT<MemoryRegionHandle>
    transferToDeviceImpl(const std::string & opName,
                         FrozenMemoryRegion region,
                         const std::type_info & type, bool isConst) override
    {
        auto handle = std::make_shared<HostMemoryRegionInfo>();
        handle->lengthInBytes = region.length();
        handle->data = region.data();
        handle->type = &type;
        handle->isConst = isConst;
        return { { {handle} }, std::make_shared<HostComputeEvent>() };
    }

    virtual ComputePromiseT<FrozenMemoryRegion>
    transferToHostImpl(const std::string & opName, MemoryRegionHandle handle) override
    {
        ExcAssert(handle.handle);
        auto info = std::dynamic_pointer_cast<const HostMemoryRegionInfo>(std::move(handle.handle));
        ExcAssert(info);

        FrozenMemoryRegion raw(info, (char *)info->data, info->lengthInBytes);
        return { std::move(raw), std::make_shared<HostComputeEvent>() };
    }

    virtual ComputePromiseT<MutableMemoryRegion>
    transferToHostMutableImpl(const std::string & opName, MemoryRegionHandle handle) override
    {
        ExcAssert(handle.handle);
        auto info = std::dynamic_pointer_cast<const HostMemoryRegionInfo>(std::move(handle.handle));
        ExcAssert(info);

        MutableMemoryRegion raw(info, (char *)info->data, info->lengthInBytes );

        return { raw, std::make_shared<HostComputeEvent>() };
    }

    virtual void
    fillDeviceRegionFromHostImpl(const std::string & opName,
                                 MemoryRegionHandle deviceHandle,
                                 std::shared_ptr<std::span<const std::byte>> pinnedHostRegion,
                                 size_t deviceOffset = 0) override
    {
        fillDeviceRegionFromHostSyncImpl(opName, deviceHandle, *pinnedHostRegion, deviceOffset);
    }                                     

    virtual void
    fillDeviceRegionFromHostSyncImpl(const std::string & opName,
                                     MemoryRegionHandle deviceHandle,
                                     std::span<const std::byte> hostRegion,
                                     size_t deviceOffset = 0) override
    {
        ExcAssert(deviceHandle.handle);
        auto info = std::dynamic_pointer_cast<const HostMemoryRegionInfo>(std::move(deviceHandle.handle));
        ExcAssert(info);
        ExcAssert(!info->isConst);
        ExcAssertLessEqual(deviceOffset, info->lengthInBytes);
        ExcAssertLessEqual(deviceOffset + hostRegion.size(), info->lengthInBytes);

        std::memcpy(((std::byte *)info->data) + deviceOffset, hostRegion.data(), hostRegion.size());
    }

    virtual std::shared_ptr<ComputeKernel>
    getKernel(const std::string & kernelName) override
    {
        std::unique_lock guard(kernelRegistryMutex);
        auto it = kernelRegistry.find(kernelName);
        if (it == kernelRegistry.end()) {
            throw AnnotatedException(400, "Unable to find compute kernel '" + kernelName + "'",
                                          "kernelName", kernelName);
        }
        auto result = it->second.generate();
        result->context = this;
        return result;
    }

    virtual std::shared_ptr<ComputeQueue>
    getQueue(const std::string & queueName) override
    {
        return std::make_shared<HostComputeQueue>(this);
    }

    virtual MemoryRegionHandle
    getSliceImpl(const MemoryRegionHandle & handle, const std::string & regionName,
                 size_t startOffsetInBytes, size_t lengthInBytes,
                 size_t align, const std::type_info & type, bool isConst) override
    {
        ExcAssert(handle.handle);
        auto info = std::dynamic_pointer_cast<const HostMemoryRegionInfo>(handle.handle);
        if (!info) {
            auto & target = *handle.handle;
            throw MLDB::Exception("HostComputeContext: our info was " + demangle(typeid(target)));
        }
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

        auto newInfo = std::make_shared<HostMemoryRegionInfo>();
        newInfo->data = (const char *)info->data + startOffsetInBytes;
        newInfo->handle = info->handle;
        newInfo->isConst = isConst;
        newInfo->type = &type;
        newInfo->name = regionName;
        newInfo->lengthInBytes = lengthInBytes;
        newInfo->parent = info;
        newInfo->ownerOffset = startOffsetInBytes;

        return { newInfo };
    }

};


// HostComputeQueue

HostComputeQueue::
HostComputeQueue(HostComputeContext * owner, HostComputeQueue * parent)
    : ComputeQueue(owner, parent), hostOwner(owner)
{
}

std::shared_ptr<ComputeQueue>
HostComputeQueue::
parallel(const std::string & opName)
{
    return std::make_shared<HostComputeQueue>(this->hostOwner, this);
}

std::shared_ptr<ComputeQueue>
HostComputeQueue::
serial(const std::string & opName)
{
    return std::make_shared<HostComputeQueue>(this->hostOwner, this);
}

void
HostComputeQueue::
enqueue(const std::string & opName,
        const BoundComputeKernel & kernel,
        const std::vector<uint32_t> & grid)
{
    ExcAssertEqual(kernel.owner->dims.size(), grid.size());

    auto hostOwner = dynamic_cast<const HostComputeKernel *>(kernel.owner);
    if (!hostOwner)
        throw MLDB::Exception("Attempt to enqueue kernel of type " + demangle(typeid(*kernel.owner))
                              + " on HostComputeQueue (expected type HostComputeKernel)");

    Timer timer;
    std::vector<ComputeKernelGridRange> ranges(grid.begin(), grid.end());
    hostOwner->call(kernel, ranges);
    auto wallTime = timer.elapsed_wall();
    //using namespace std;
    //cerr << "calling " << kernel.owner->kernelName << " took " << timer.elapsed() << endl;
    {
        std::unique_lock guard(kernelWallTimesMutex);
        kernelWallTimes[kernel.owner->kernelName] += wallTime * 1000.0;
        totalKernelTime += wallTime * 1000.0;
    }
}

ComputePromiseT<MemoryRegionHandle>
HostComputeQueue::
enqueueFillArrayImpl(const std::string & opName,
                     MemoryRegionHandle regionIn, MemoryRegionInitialization init,
                     size_t startOffsetInBytes, ssize_t lengthInBytes,
                     const std::any & arg)
{
    return ComputeQueue::enqueueFillArrayImpl(opName, std::move(regionIn), init,
                                              startOffsetInBytes, lengthInBytes, arg);
}

void
HostComputeQueue::
enqueueCopyFromHostImpl(const std::string & opName,
                        MemoryRegionHandle toRegion,
                        FrozenMemoryRegion fromRegion,
                        size_t deviceStartOffsetInBytes)
{
    enqueueCopyFromHostSyncImpl(opName, toRegion, fromRegion, deviceStartOffsetInBytes);
}

void
HostComputeQueue::
enqueueCopyFromHostSyncImpl(const std::string & opName,
                            MemoryRegionHandle toRegion,
                            FrozenMemoryRegion fromRegion,
                            size_t deviceStartOffsetInBytes)
{
    ExcAssertLessEqual(deviceStartOffsetInBytes + fromRegion.length(), toRegion.lengthInBytes());
    ExcAssert(toRegion.handle);
    ExcAssert(!toRegion.handle->isConst);
    auto info = std::dynamic_pointer_cast<const HostMemoryRegionInfo>(std::move(toRegion.handle));
    ExcAssert(info);
    std::memcpy((std::byte *)info->data + deviceStartOffsetInBytes, fromRegion.data(), fromRegion.length());
}

ComputePromiseT<FrozenMemoryRegion>
HostComputeQueue::
enqueueTransferToHostImpl(const std::string & opName,
                          MemoryRegionHandle handle)
{
    return owner->transferToHostImpl(opName, std::move(handle));
}

FrozenMemoryRegion
HostComputeQueue::
transferToHostSyncImpl(const std::string & opName,
                       MemoryRegionHandle handle)
{
    return owner->transferToHostSyncImpl(opName, std::move(handle));
}

ComputePromiseT<MemoryRegionHandle>
HostComputeQueue::
enqueueManagePinnedHostRegionImpl(const std::string & opName,
                                  std::span<const std::byte> region, size_t align,
                                  const std::type_info & type, bool isConst)
{
    return { managePinnedHostRegionSyncImpl(opName, region, align, type, isConst), std::make_shared<HostComputeEvent>() };
}

MemoryRegionHandle
HostComputeQueue::
managePinnedHostRegionSyncImpl(const std::string & opName,
                                std::span<const std::byte> region, size_t align,
                                const std::type_info & type, bool isConst)
{
    auto mem = this->hostOwner->backingStore->allocateWritable(region.size(), align);
    std::copy_n(region.data(), region.size(), (std::byte *)mem.data());
    auto result = std::make_shared<HostMemoryRegionInfo>();
    result->init(std::move(mem));
    result->type = &type;
    result->isConst = isConst;
    result->name = opName;
    return { std::move(result) };
}

void
HostComputeQueue::
enqueueCopyBetweenDeviceRegionsImpl(const std::string & opName,
                                    MemoryRegionHandle from, MemoryRegionHandle to,
                                    size_t fromOffset, size_t toOffset,
                                    size_t length)
{
    copyBetweenDeviceRegionsSyncImpl(opName, from, to, fromOffset, toOffset, length);
}

void
HostComputeQueue::
copyBetweenDeviceRegionsSyncImpl(const std::string & opName,
                                    MemoryRegionHandle from, MemoryRegionHandle to,
                                    size_t fromOffset, size_t toOffset,
                                    size_t length)
{
    ExcAssert(from.handle);
    auto fromInfo = std::dynamic_pointer_cast<const HostMemoryRegionInfo>(std::move(from.handle));
    ExcAssert(fromInfo);
    
    ExcAssert(to.handle);
    auto toInfo = std::dynamic_pointer_cast<const HostMemoryRegionInfo>(std::move(to.handle));
    ExcAssert(toInfo);
    ExcAssert(!toInfo->isConst);
    
    ExcAssertLessEqual(fromOffset, fromInfo->lengthInBytes);
    ExcAssertLessEqual(fromOffset + length, fromInfo->lengthInBytes);
    ExcAssertLessEqual(toOffset, toInfo->lengthInBytes);
    ExcAssertLessEqual(toOffset + length, toInfo->lengthInBytes);
    
    std::memcpy(((std::byte *)toInfo->data) + toOffset, (const std::byte *)fromInfo->data + fromOffset, length);
}

std::shared_ptr<ComputeEvent>
HostComputeQueue::
flush()
{
    return makeAlreadyResolvedEvent("flush");
}

void
HostComputeQueue::
finish()
{
    // no-op
}

std::shared_ptr<ComputeEvent>
HostComputeQueue::
makeAlreadyResolvedEvent(const std::string & label) const
{
    return std::make_shared<HostComputeEvent>();
}


struct HostComputeRuntime: public ComputeRuntime {
    virtual ~HostComputeRuntime()
    {
    }

    virtual ComputeRuntimeId getId() const override
    {
        return ComputeRuntimeId::HOST;
    }

    virtual std::string printRestOfDevice(ComputeDevice device) const override
    {
        return "";
    }

    virtual std::string printHumanReadableDeviceInfo(ComputeDevice device) const override
    {
        return "Host CPU";
    }

    virtual ComputeDevice getDefaultDevice() const override
    {
        return ComputeDevice::host();
    }

    virtual std::vector<ComputeDevice> enumerateDevices() const override
    {
        return { ComputeDevice::host() };
    }

    virtual std::shared_ptr<ComputeContext>
    getContext(std::span<const ComputeDevice> devices) const override
    {
        if (devices.size() != 1 || devices[0] != ComputeDevice::host()) {
            throw MLDB::Exception("HOST compute context can only operate on a single host device");
        }
        return std::make_shared<HostComputeContext>();
    }
};

void registerHostComputeKernel(const std::string & kernelName,
                               std::function<std::shared_ptr<ComputeKernel>()> generator)
{
    kernelRegistry[kernelName].generate = generator;
}

namespace {

void zeroFillArrayKernel(ComputeContext & context,
                         std::span<uint8_t> region,
                         uint64_t startOffsetInBytes,
                         uint64_t lengthInBytes)
{
    region = region.subspan(startOffsetInBytes, lengthInBytes);
    std::memset(region.data(), 0, region.size());
}

void blockFillArrayKernel(ComputeContext & context,
                          std::span<uint8_t> region,
                          uint64_t startOffsetInBytes,
                          uint64_t lengthInBytes,
                          std::span<const uint8_t> block,
                          uint64_t blockLengthInBytes)
{
    region = region.subspan(startOffsetInBytes, lengthInBytes);
    ExcAssertEqual(lengthInBytes % blockLengthInBytes, 0);
    ExcAssertEqual(block.size(), blockLengthInBytes);

    for (size_t i = 0;  i < lengthInBytes;  i += blockLengthInBytes) {
        std::memcpy(region.data() + i, block.data(), block.size());
    }
}

static struct Init {
    Init()
    {
        ComputeRuntime::registerRuntime(ComputeRuntimeId::HOST, "host",
                                        [] () { return new HostComputeRuntime(); });

        auto createBlockFillArrayKernel = [] () -> std::shared_ptr<ComputeKernel>
        {
            auto result = std::make_shared<HostComputeKernel>();
            result->kernelName = "__blockFillArray";
            result->device = ComputeDevice::host();
            result->addParameter("region", "w", "u8[regionLength]");
            result->addParameter("startOffsetInBytes", "r", "u64");
            result->addParameter("lengthInBytes", "r", "u64");
            result->addParameter("blockData", "r", "u8[blockLengthInBytes]");
            result->addParameter("blockLengthInBytes", "r", "u64");
            result->setComputeFunction(blockFillArrayKernel);
            return result;
        };

        registerHostComputeKernel("__blockFillArray", createBlockFillArrayKernel);

        auto createZeroFillArrayKernel = [] () -> std::shared_ptr<ComputeKernel>
        {
            auto result = std::make_shared<HostComputeKernel>();
            result->kernelName = "__zeroFillArray";
            result->device = ComputeDevice::host();
            result->addParameter("region", "w", "u8[regionLength]");
            result->addParameter("startOffsetInBytes", "r", "u64");
            result->addParameter("lengthInBytes", "r", "u64");
            result->setComputeFunction(zeroFillArrayKernel);
            return result;
        };

        registerHostComputeKernel("__zeroFillArray", createZeroFillArrayKernel);
    }

} init;

};



} // namespace MLDB
