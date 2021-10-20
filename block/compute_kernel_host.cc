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

} // file scope

BoundComputeKernel
HostComputeKernel::
bindImpl(std::vector<ComputeKernelArgument> arguments) const
{
    auto bindInfo = std::make_shared<HostComputeKernelBindInfo>();
    bindInfo->call = createCallable(*this->context, arguments);

    BoundComputeKernel result;
    result.arguments = std::move(arguments);
    result.owner = this;
    result.bindInfo = std::move(bindInfo);

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


// HostComputeQueue

std::shared_ptr<ComputeEvent>
HostComputeQueue::
launch(const std::string & opName,
       const BoundComputeKernel & kernel,
       const std::vector<uint32_t> & grid,
       const std::vector<std::shared_ptr<ComputeEvent>> & prereqs)
{
    ExcAssertEqual(kernel.owner->dims.size(), grid.size());

    // For now... async
    if (!prereqs.empty()) {
        for (auto & e: prereqs) {
            ExcAssert(e);
            e->await();
        }
    }

    auto hostOwner = dynamic_cast<const HostComputeKernel *>(kernel.owner);
    if (!hostOwner)
        throw MLDB::Exception("Attempt to enqueue kernel of type " + demangle(typeid(*kernel.owner))
                              + " on HostComputeQueu (expected type HostComputeKernel)");

    Timer timer;
    std::vector<ComputeKernelGridRange> ranges(grid.begin(), grid.end());
    hostOwner->call(kernel, ranges);
    auto wallTime = timer.elapsed_wall();
    using namespace std;
    cerr << "calling " << kernel.owner->kernelName << " took " << timer.elapsed() << endl;
    {
        std::unique_lock guard(kernelWallTimesMutex);
        kernelWallTimes[kernel.owner->kernelName] += wallTime * 1000.0;
        totalKernelTime += wallTime * 1000.0;
    }

    return std::make_shared<HostComputeEvent>();
}

ComputePromiseT<MemoryRegionHandle>
HostComputeQueue::
enqueueFillArrayImpl(const std::string & opName,
                     MemoryRegionHandle regionIn, MemoryRegionInitialization init,
                     size_t startOffsetInBytes, ssize_t lengthInBytes,
                     const std::any & arg,
                     std::vector<std::shared_ptr<ComputeEvent>> prereqs)
{
    return ComputeQueue::enqueueFillArrayImpl(opName, std::move(regionIn), init,
                                              startOffsetInBytes, lengthInBytes, arg, std::move(prereqs));
}

void
HostComputeQueue::
flush()
{
    // no-op
}

void
HostComputeQueue::
finish()
{
    // no-op
}

std::shared_ptr<ComputeEvent>
HostComputeQueue::
makeAlreadyResolvedEvent() const
{
    return std::make_shared<HostComputeEvent>();
}

// HostComputeContext

struct HostComputeContext: public ComputeContext {

    HostComputeContext()
        : backingStore(new MemorySerializer())
    {
    }

    virtual ~HostComputeContext() = default;

    std::shared_ptr<MappedSerializer> backingStore;

    struct MemoryRegionInfo: public MemoryRegionHandleInfo {
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

    virtual ComputePromiseT<MemoryRegionHandle>
    allocateImpl(const std::string & regionName,
                 size_t length, size_t align,
                 const std::type_info & type, bool isConst,
                 MemoryRegionInitialization initialization,
                 std::any initWith = std::any())
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
            case INIT_KERNEL: {
                throw MLDB::Exception("Kernel initialization not implemented yet");
            }
            default:
                throw MLDB::Exception("Unknown initialization in allocateImpl");
        }

        auto result = std::make_shared<MemoryRegionInfo>();
        result->init(std::move(mem));
        result->type = &type;
        result->isConst = isConst;
        result->name = regionName;
        return { {std::move(result)}, std::make_shared<HostComputeEvent>() };
    }

    virtual ComputePromiseT<MemoryRegionHandle>
    transferToDeviceImpl(const std::string & opName,
                         FrozenMemoryRegion region,
                         const std::type_info & type, bool isConst)
    {
        auto handle = std::make_shared<MemoryRegionInfo>();
        handle->lengthInBytes = region.length();
        handle->data = region.data();
        handle->type = &type;
        handle->isConst = isConst;
        return { { {handle} }, std::make_shared<HostComputeEvent>() };
    }

    virtual ComputePromiseT<FrozenMemoryRegion>
    transferToHostImpl(const std::string & opName, MemoryRegionHandle handle)
    {
        ExcAssert(handle.handle);
        auto info = std::static_pointer_cast<const MemoryRegionInfo>(std::move(handle.handle));
        ExcAssert(info);

        FrozenMemoryRegion raw(info, (char *)info->data, info->lengthInBytes);
        return { std::move(raw), std::make_shared<HostComputeEvent>() };
    }

    virtual ComputePromiseT<MutableMemoryRegion>
    transferToHostMutableImpl(const std::string & opName, MemoryRegionHandle handle)
    {
        auto info = std::static_pointer_cast<const MemoryRegionInfo>(std::move(handle.handle));
        ExcAssert(info);

        MutableMemoryRegion raw(info, (char *)info->data, info->lengthInBytes );

        return { raw, std::make_shared<HostComputeEvent>() };
    }

    virtual std::shared_ptr<ComputeKernel>
    getKernel(const std::string & kernelName)
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

    virtual ComputePromiseT<MemoryRegionHandle>
    managePinnedHostRegion(const std::string & regionName,
                           std::span<const std::byte> region, size_t align,
                           const std::type_info & type, bool isConst)
    {
        auto mem = backingStore->allocateWritable(region.size(), align);
        std::copy_n(region.data(), region.size(), (std::byte *)mem.data());
        auto result = std::make_shared<MemoryRegionInfo>();
        result->init(std::move(mem));
        result->type = &type;
        result->isConst = isConst;
        result->name = regionName;
        return { { std::move(result) }, std::make_shared<HostComputeEvent>() };
    }

    virtual std::shared_ptr<ComputeQueue>
    getQueue()
    {
        return std::make_shared<HostComputeQueue>(this);
    }

    virtual MemoryRegionHandle
    getSliceImpl(const MemoryRegionHandle & handle, const std::string & regionName,
                 size_t startOffsetInBytes, size_t lengthInBytes,
                 size_t align, const std::type_info & type, bool isConst) override
    {
        ExcAssert(handle.handle);
        auto info = std::dynamic_pointer_cast<const MemoryRegionInfo>(handle.handle);
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

        auto newInfo = std::make_shared<MemoryRegionInfo>();
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
                         std::span<char> region,
                         uint64_t startOffsetInBytes,
                         uint64_t lengthInBytes)
{
    region = region.subspan(startOffsetInBytes, lengthInBytes);
    std::memset(region.data(), 0, region.size());
}

void blockFillArrayKernel(ComputeContext & context,
                          std::span<char> region,
                          uint64_t startOffsetInBytes,
                          uint64_t lengthInBytes,
                          std::span<const char> block,
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
