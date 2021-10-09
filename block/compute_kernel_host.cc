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
                 MemoryRegionInitialization initialization,
                 std::any initWith = std::any())
    {
        auto mem = backingStore->allocateWritable(length, align);
        auto result = std::make_shared<MemoryRegionInfo>();
        result->init(std::move(mem));
        result->isConst = false;
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
    managePinnedHostRegion(std::span<const std::byte> region, size_t align)
    {
        auto mem = backingStore->allocateWritable(region.size(), align);
        std::copy_n(region.data(), region.size(), (std::byte *)mem.data());
        auto result = std::make_shared<MemoryRegionInfo>();
        result->init(std::move(mem));
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

#if 0
    template<typename T>
    auto transferToDeviceImmutable(const FrozenMemoryRegionT<T> & obj, const char * what)
        -> std::tuple<MemoryArrayHandleT<const T>, std::shared_ptr<ComputeEvent>, size_t>
    {
        auto handle = std::make_shared<MemoryRegionInfo>();
        handle->lengthInBytes = obj.raw().length();
        handle->data = obj.raw().data();
        handle->type = &typeid(std::remove_const_t<T>);
        handle->isConst = true;
        return { { {handle} }, std::make_shared<ComputeEvent>(), obj.memusage() };
    }

    template<typename T>
    auto transferToHost(MemoryArrayHandleT<T> array)
        -> std::tuple<FrozenMemoryRegionT<std::remove_const_t<T>>, std::shared_ptr<ComputeEvent>>
    {
        auto handle = std::static_pointer_cast<const MemoryRegionInfo>(std::move(array.handle));
        if (!handle)
            return { {}, {} };

        if (*handle->type != typeid(std::remove_const_t<T>)) {
            throw MLDB::Exception("Attempt to cast to wrong type: from " + demangle(handle->type->name())
                                  + " to " + type_name<T>());
        }

        FrozenMemoryRegion raw(handle, (char *)handle->data, handle->lengthInBytes);
        return { raw, std::make_shared<ComputeEvent>() };
    }

    template<typename T>
    auto transferToHostMutable(MemoryArrayHandleT<T> array)
        -> std::tuple<MutableMemoryRegionT<std::remove_const_t<T>>, std::shared_ptr<ComputeEvent>>
    {
        static_assert(!std::is_const_v<T>, "mutable transfer requires non-const type");
        auto handle = std::static_pointer_cast<const MemoryRegionInfo>(std::move(array.handle));
        if (!handle)
            return { {}, {} };

        if (*handle->type != typeid(T)) {
            throw MLDB::Exception("Attempt to cast to wrong type: from " + demangle(handle->type->name())
                                  + " to " + type_name<T>());
        }

        MutableMemoryRegion raw(handle, (char *)handle->data, handle->lengthInBytes, backingStore.get());
        return { raw, std::make_shared<ComputeEvent>() };
    }

    template<typename T>
    auto transferToHostSync(MemoryArrayHandleT<T> array) -> FrozenMemoryRegionT<std::remove_const_t<T>>
    {
        auto [result, ignore1] = transferToHost(std::move(array));
        return result;
    }

    template<typename T>
    auto transferToHostMutableSync(MemoryArrayHandleT<T> array) -> MutableMemoryRegionT<T>
    {
        static_assert(!std::is_const_v<T>, "mutable transfer requires non-const type");
        auto [result, ignore1] = transferToHostMutable(std::move(array));
        return result;
    }

    /** Transfers the array to the CPU so that it can be written from the CPU... but
     *  promises that the initial contents will never be read, which enables us to
     *  not copy any data to the CPU. */
    template<typename T>
    auto transferToHostUninitialized(MemoryArrayHandleT<T> array) -> MutableMemoryRegionT<T>
    {
        auto handle = std::static_pointer_cast<const MemoryRegionInfo>(std::move(array.handle));
        if (!handle)
            return {};

        if (*handle->type != typeid(std::remove_const_t<T>)) {
            throw MLDB::Exception("Attempt to cast to wrong type: from " + demangle(handle->type->name())
                                  + " to " + type_name<T>());
        }
        if (handle->isConst) {
            throw MLDB::Exception("Attempt to map const memory as mutable");
        }

        MutableMemoryRegion raw(handle, (char *)handle->data, handle->lengthInBytes, backingStore.get());
        return raw;
    }

    template<typename T>
    auto allocArray(size_t size) -> MemoryArrayHandleT<T>
    {
    }

    template<typename T>
    auto allocZeroInitializedArray(size_t size) -> MemoryArrayHandleT<T>
    {
        auto mem = backingStore->allocateZeroFilledWritableT<T>(size);
        auto result = std::make_shared<MemoryRegionInfo>();
        result->init(std::move(mem));
        return { { std::move(result) } };
    }

    template<typename T>
    auto manageMemoryRegion(const std::vector<T> & obj) -> MemoryArrayHandleT<T>
    {
        return manageMemoryRegion(static_cast<std::span<const T>>(obj));
    }

    template<typename T, size_t N>
    auto manageMemoryRegion(const std::span<const T, N> & obj) -> MemoryArrayHandleT<T>
    {
        auto mem = backingStore->allocateWritableT<T>(obj.size());
        std::copy_n(obj.data(), obj.size(), mem.data());
        auto result = std::make_shared<MemoryRegionInfo>();
        result->init(std::move(mem));
        return { { std::move(result) } };
    }

    template<typename T>
    auto enqueueFillArray(const MemoryArrayHandleT<T> & t, const T & fillWith, size_t offset = 0,
                          ssize_t length = -1) -> std::shared_ptr<ComputeEvent>
    {
        throw MLDB::Exception("enqueueFillArray");
    }
#endif
};

struct HostComputeRuntime: public ComputeRuntime {
    virtual ~HostComputeRuntime()
    {
    }

    virtual ComputeRuntimeId getId() const
    {
        return HOST;
    }

    // Enumerate the devices available for this runtime
    virtual std::vector<ComputeDevice> enumerateDevices() const
    {
        return { ComputeDevice::host() };
    }

    // Get a compute context for this runtime
    virtual std::shared_ptr<ComputeContext> getContext() const
    {
        return std::make_shared<HostComputeContext>();
    }
};


#if 0
std::shared_ptr<ComputeKernel>
CpuComputeContext::
getKernel(const std::string & kernelName)
{
}
#endif

void registerHostComputeKernel(const std::string & kernelName,
                               std::function<std::shared_ptr<ComputeKernel>()> generator)
{
    kernelRegistry[kernelName].generate = generator;
}

namespace {

static struct Init {
    Init()
    {
        ComputeRuntime::registerRuntime(HOST, "host", [] () { return new HostComputeRuntime(); });
    }

} init;

};



} // namespace MLDB
