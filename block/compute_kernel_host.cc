/** compute_kernel_host.h                                                -*- C++ -*-
    Jeremy Barnes, 27 March 2016
    This file is part of MLDB. Copyright 2016 mldb.ai inc. All rights reserved.

    Compute kernel runtime for CPU devices.
*/

#include "compute_kernel_host.h"
#include "mldb/types/basic_value_descriptions.h"


namespace MLDB {

namespace {

std::mutex kernelRegistryMutex;
struct KernelRegistryEntry {
    std::function<std::shared_ptr<ComputeKernel>()> generate;
};

std::map<std::string, KernelRegistryEntry> kernelRegistry;

} // file scope

struct HostComputeEvent: public ComputeEvent {
    virtual ~HostComputeEvent() = default;
    virtual ComputeProfilingInfo getProfilingInfo() const
    {
        return ComputeProfilingInfo();
    }

    virtual void await() const
    {
    }
};


struct HostComputeContext: public ComputeContext {

    HostComputeContext()
        : backingStore(new MemorySerializer())
    {
    }

    virtual ~HostComputeContext() = default;

    std::shared_ptr<MappedSerializer> backingStore;

    struct MemoryRegionInfo: public MemoryRegionHandleInfo {
        const void * data = nullptr;
        size_t lengthInBytes = 0;
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

    virtual MemoryRegionHandle
    allocateImpl(size_t length, size_t align,
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
        return { std::move(result) };
    }

    virtual std::tuple<MemoryRegionHandle, std::shared_ptr<ComputeEvent>>
    transferToDeviceImpl(FrozenMemoryRegion region,
                         const std::type_info & type, bool isConst)
    {
        auto handle = std::make_shared<MemoryRegionInfo>();
        handle->lengthInBytes = region.length();
        handle->data = region.data();
        handle->type = &type;
        handle->isConst = isConst;
        return { { {handle} }, std::make_shared<HostComputeEvent>() };
    }

    virtual std::tuple<FrozenMemoryRegion, std::shared_ptr<ComputeEvent>>
    transferToHostImpl(MemoryRegionHandle handle)
    {
        auto info = std::static_pointer_cast<const MemoryRegionInfo>(std::move(handle.handle));
        if (!info)
            return { {}, {} };

        FrozenMemoryRegion raw(info, (char *)info->data, info->lengthInBytes);
        return { raw, std::make_shared<HostComputeEvent>() };
    }

    virtual std::tuple<MutableMemoryRegion, std::shared_ptr<ComputeEvent>>
    transferToHostMutableImpl(MemoryRegionHandle handle)
    {
        auto info = std::static_pointer_cast<const MemoryRegionInfo>(std::move(handle.handle));
        if (!info)
            return { {}, {} };

        MutableMemoryRegion raw(info, (char *)info->data, info->lengthInBytes, backingStore.get());

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

    virtual MemoryRegionHandle
    managePinnedHostRegion(std::span<const std::byte> region, size_t align,
                           const std::type_info & type, bool isConst)
    {
        auto mem = backingStore->allocateWritable(region.size(), align);
        std::copy_n(region.data(), region.size(), (std::byte *)mem.data());
        auto result = std::make_shared<MemoryRegionInfo>();
        result->init(std::move(mem));
        result->type = &type;
        result->isConst = isConst;
        return { { std::move(result) } };
    }

    virtual std::shared_ptr<ComputeQueue>
    getQueue()
    {
        return std::make_shared<ComputeQueue>(this);
    }

    // Return the MappedSerializer that owns the memory allocated on the host for this
    // device.  It's needed for the generic MemoryRegion functions to know how to manipulate
    // memory handles.  In practice it probably means that each runtime needs to define a
    // MappedSerializer derivitive.
    virtual MappedSerializer * getSerializer()
    {
        return backingStore.get();
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

static struct Init {
    Init()
    {
        ComputeRuntime::registerRuntime(ComputeRuntimeId::HOST, "host",
                                        [] () { return new HostComputeRuntime(); });
    }

} init;

};



} // namespace MLDB
