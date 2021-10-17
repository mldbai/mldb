/** compute_kernel_multi.h                                                -*- C++ -*-
    Jeremy Barnes, 27 March 2016
    This file is part of MLDB. Copyright 2016 mldb.ai inc. All rights reserved.

    Compute kernel runtime for CPU devices.
*/

#include "compute_kernel_multi.h"
#include "mldb/types/basic_value_descriptions.h"


namespace MLDB {

struct MultiComputeContext;

DEFINE_ENUM_DESCRIPTION_INLINE(ComputeMultiMode)
{
    addValue("COMPARE", ComputeMultiMode::COMPARE, "Run kernel on multiple devices, comparing the output");
}

namespace {

struct MultiMemoryRegionInfo: public MemoryRegionHandleInfo {
    virtual ~MultiMemoryRegionInfo() = default;

    // Handles from each of the contexts
    std::vector<MemoryRegionHandle> handles;
};

/// Type handler for multi kernels, that unpacks memory handles into the underlying one for
/// the particular runtime.
struct MultiAbstractArgumentHandler: public AbstractArgumentHandler {

    MultiAbstractArgumentHandler(std::shared_ptr<const AbstractArgumentHandler> underlyingIn,
                                 MultiComputeContext & multiContext,
                                 uint32_t index)
        : underlying(std::move(underlyingIn)), index(index), multiContext(multiContext)
    {
        ExcAssert(underlying);
        this->type = underlying->type;
        this->isConst = underlying->isConst;
    }

    // Pass through to this for most methods
    std::shared_ptr<const AbstractArgumentHandler> underlying;

    // Which index are we in the list of contexts?  Used to know which underlying
    // contexts and handles to pass through.
    uint32_t index = 0;

    // The MultiComputeContext to which everything belongs
    MultiComputeContext & multiContext;

    // Return the underlying context for this index.  This involves upcasting to the
    // which isn't possible (the underlying contexts have no idea they are part of a
    // bigger context).  So instead we get the MultiContext passed in, and we just
    // ensure that the context we're trying to fix up is the same one it's meant to
    // be.
    MultiComputeContext & fixupContext(ComputeContext & context) const
    {
        // Verify that it matches...
        ExcAssertEqual(multiContext.contexts.at(index).get(), &context);

        return multiContext;

        //MultiComputeContext & multiContext = dynamic_cast<MultiComputeContext &>(context);
        //ExcAssertLess(index, multiContext.contexts.size());
        //return *multiContext.contexts[index];
    }

    virtual bool canGetPrimitive() const override
    {
        return underlying->canGetPrimitive();
    }

    virtual std::span<const std::byte>
    getPrimitive(ComputeContext & context) const override
    {
        return underlying->getPrimitive(fixupContext(context));
    }

    virtual bool canGetRange() const override
    {
        return underlying->canGetRange();
    }

    virtual std::tuple<void *, size_t, std::shared_ptr<const void>>
    getRange(ComputeContext & context) const override
    {
        return underlying->getRange(fixupContext(context));
    }

    virtual bool canGetConstRange() const override
    {
        return underlying->canGetConstRange();
    }

    virtual std::tuple<const void *, size_t, std::shared_ptr<const void>>
    getConstRange(ComputeContext & context) const override
    {
        MemoryRegionHandle handle = getHandle(context);
        auto [region, pin] = multiContext.contexts[index]->transferToHostImpl(handle);
        return { region.data(), region.length(), pin };
    }

    virtual bool canGetHandle() const override
    {
        return underlying->canGetHandle();
    }

    virtual MemoryRegionHandle
    getHandle(ComputeContext & context) const override
    {
        MemoryRegionHandle handle = underlying->getHandle(context);
        if (!handle.handle)
            return MemoryRegionHandle();
        auto info = std::dynamic_pointer_cast<const MultiMemoryRegionInfo>(std::move(handle.handle));
        ExcAssert(!!info);
        ExcAssertLess(this->index, info->handles.size());
        return { info->handles[this->index] };
    }

    virtual std::string info() const override
    {
        return AbstractArgumentHandler::info() + " (multiple contexts)";
    }
};

} // file scope

// MultiComputeKernel

namespace {

struct MultiBindInfo: public ComputeKernelBindInfo {
    virtual ~MultiBindInfo() = default;
    std::vector<BoundComputeKernel> boundKernels;
};

} // file scope

BoundComputeKernel
MultiComputeKernel::
bindImpl(std::vector<ComputeKernelArgument> arguments) const
{
    std::vector<BoundComputeKernel> boundKernels;

    for (size_t i = 0;  i < this->kernels.size();  ++i) {

        // We need to modify the params to match this callable...
        std::vector<ComputeKernelArgument> ourArguments;
        ourArguments.reserve(arguments.size());

        // Convert a bound parameter to one which will work for this particular sub-context
        auto convertArgument = [&] (ComputeKernelArgument p) -> ComputeKernelArgument
        {
            auto oldHandler = std::move(p.handler);
            p.handler = std::make_shared<MultiAbstractArgumentHandler>(std::move(oldHandler), *multiContext, i);
            return p;
        };

        // Create our parameter list
        for (size_t j = 0;  j < params.size();  ++j) {
            auto & a = arguments[j];
            ourArguments.emplace_back(convertArgument(a));
        }

        boundKernels.emplace_back(this->kernels[i]->bindImpl(arguments));
    }

    // Create our info structure to carry around the bound arguments
    auto bindInfo = std::make_shared<MultiBindInfo>();
    bindInfo->boundKernels = std::move(boundKernels);

    // And finally assemble the result
    BoundComputeKernel result;
    result.owner = this;
    result.arguments = std::move(arguments);
    result.bindInfo = std::move(bindInfo);
    return result;

#if 0
    auto call = [this,
                 multiContext = this->multiContext,
                 callables = std::move(callables),
                 callableParams = std::move(callableParams)]
                (ComputeContext & context, std::span<ComputeKernelGridRange> idx,
                 std::span<const std::shared_ptr<ComputeEvent>> prereqs)
    {
        using namespace std;
        ExcAssertEqual(&context, multiContext);

        auto compareParameters = [&] (bool pre)
        {
            // Now, for each writable parameter, compare the results...
            for (size_t i = 0;  i < this->params.size();  ++i) {
                if (this->params[i].type.dims.size() == 0 || (pre && this->params[i].access == "w"))
                    continue;

                bool printedBanner = false;
                auto printBanner = [&] ()
                {
                    if (printedBanner)
                        return;
                    using namespace std;
                    cerr << "comparing contents of parameter " << this->params[i].name << " access "
                        << this->params[i].access << endl;
                    printedBanner = true;
                };

                auto reference = callableParams[0][i];
                //auto & h = *reference.handler; 
                //cerr << "  reference.handler = " << reference.handler << " " << demangle(typeid(h)) << endl;
                auto [referenceData, referenceLength, referencePin]
                    = reference.handler->getConstRange(*this->multiContext->contexts[0]);

                for (size_t j = 1;  j < this->kernels.size();  ++j) {
                    auto kernelGenerated = callableParams[j][i];
                    //auto & h = *kernelGenerated.handler; 
                    //cerr << "  kernelGenerated.handler = " << kernelGenerated.handler << demangle(typeid(h)) << endl;
                    auto [kernelGeneratedData, kernelGeneratedLength, kernelGeneratedPin]
                        = kernelGenerated.handler->getConstRange(*this->multiContext->contexts[j]);
                    

                    if (referenceLength == 0) {
                        if (printedBanner)
                            cerr << "  null data; continuing" << endl;
                        continue;
                    }

                    ExcAssertEqual(referenceLength, kernelGeneratedLength);

                    if (referenceData == kernelGeneratedData) {
                        if (printedBanner)
                            cerr << "  kernel " << j << " has same data as kernel 0; continuing" << endl;
                        continue;
                    }

                    ExcAssertNotEqual(referenceData, kernelGeneratedData);

                    if (memcmp(referenceData, kernelGeneratedData, referenceLength) == 0) {
                        if (printedBanner)
                            cerr << "  kernel " << j << " is bit-identical; continuing" << endl;
                        continue;
                    }

                    const ValueDescription * desc = this->params[i].type.baseType.get();

                    size_t n = referenceLength / desc->size;
                    const char * p1 = (const char *)referenceData;
                    const char * p2 = (const char *)kernelGeneratedData;

                    size_t numDifferences = 0;

                    for (size_t k = 0;  k < n;  ++k, p1 += desc->size, p2 += desc->size) {
                        if (memcmp(p1, p2, desc->size) == 0)
                            continue;
                        std::string v1, v2;
                        StringJsonPrintingContext c1(v1), c2(v2);
                        desc->printJson(p1, c1);
                        desc->printJson(p2, c2);

                        if (v1 != v2) {
                            printBanner();
                            ++numDifferences;
                            if (numDifferences == 6) {
                                cerr << "..." << endl;
                            }
                            else if (numDifferences <= 5) {
                                cerr << "difference on element " << k << endl;
                                cerr << "  v1 = " << v1 << endl;
                                cerr << "  v2 = " << v2 << endl;
                            }
                        }
                    }
                    if (numDifferences > 0) {
                        cerr << "  " << numDifferences << " of " << n << " were different" << endl;
                    }
                }
            }
        };

        cerr << "--------------- beginning kernel " << this->kernelName << " pre-validation" << endl;
        compareParameters(true /* pre */);

        cerr << endl << "--------------- running kernel " << this->kernelName << endl;

        std::vector<std::shared_ptr<ComputeEvent>> events;

        for (size_t i = 0;  i < callables.size();  ++i) {
            // Map the prerequisites back to their underlying type for the call
            std::vector<std::shared_ptr<ComputeEvent>> prereqsi;
            for (auto & evPtr: prereqs) {
                ExcAssert(evPtr);
                auto & cast = dynamic_cast<const MultiComputeEvent &>(*evPtr);
                prereqsi.push_back(cast.events.at(i));
            }

            auto ev = callables[i](*multiContext->contexts[i], idx, prereqsi);
            events.emplace_back(std::move(ev));
        }

        cerr << endl << "--------------- finished kernel " << this->kernelName << endl;
        compareParameters(false /* post */);
        cerr << "--------------- finished kernel " << this->kernelName << " validation" << endl;

        return std::make_shared<MultiComputeEvent>(std::move(events));
    };

    return call;
#endif
}


// MultiComputeEvent

MultiComputeEvent::
MultiComputeEvent(std::vector<std::shared_ptr<ComputeEvent>> events)
    :events(std::move(events))
{
}

std::shared_ptr<ComputeProfilingInfo>
MultiComputeEvent::
getProfilingInfo() const
{
    return std::make_shared<ComputeProfilingInfo>();
}

void
MultiComputeEvent::
await() const
{
    for (auto & e: events) {
        e->await();
    }
}


// MultiComputeQueue

MultiComputeQueue::
MultiComputeQueue(MultiComputeContext * owner,
                  std::vector<std::shared_ptr<ComputeQueue>> queues)
    : ComputeQueue(owner),
      multiOwner(owner),
      queues(std::move(queues))
{
}

std::shared_ptr<ComputeEvent>
MultiComputeQueue::
launch(const BoundComputeKernel & kernel,
       const std::vector<uint32_t> & grid,
       const std::vector<std::shared_ptr<ComputeEvent>> & prereqs)
{
    const MultiBindInfo * multiInfo = dynamic_cast<const MultiBindInfo *>(kernel.bindInfo.get());
    ExcAssert(multiInfo);

    std::vector<std::shared_ptr<ComputeEvent>> events;

    for (size_t i = 0;  i < queues.size();  ++i) {
        // Unpack the prerequisites to get the event for this device
        std::vector<std::shared_ptr<ComputeEvent>> ourPrereqs;
        for (auto & e: prereqs) {
            ExcAssert(e);
            const MultiComputeEvent * multiEvent = dynamic_cast<const MultiComputeEvent *>(e.get());
            ExcAssert(multiEvent);
            ourPrereqs.emplace_back(multiEvent->events.at(i));
        }

        // Launch on the child queue
        auto ev = queues[i]->launch(multiInfo->boundKernels[i], grid, ourPrereqs);
        events.emplace_back(std::move(ev));
    }

    return std::make_shared<MultiComputeEvent>(std::move(events));
}

void
MultiComputeQueue::
flush()
{
    for (auto & q: queues) {
        q->flush();
    }
}

std::shared_ptr<ComputeEvent>
MultiComputeQueue::
enqueueFillArrayImpl(MemoryRegionHandle region, MemoryRegionInitialization init,
                     size_t startOffsetInBytes, ssize_t lengthInBytes,
                     const std::any & arg)
{
    // We need to do this on each of the contexts
    // We only need to transfer the first...
    if (!region.handle) {
        ExcAssertEqual(lengthInBytes, 0);
        return nullptr;
    }

    auto info = std::dynamic_pointer_cast<const MultiMemoryRegionInfo>(std::move(region.handle));
    if (!info) {
        auto & got = *region.handle;
        throw MLDB::Exception("Multi transferToHostImpl: wrong info type: got " + demangle(typeid(got))
                                + " expected " + type_name<MultiMemoryRegionInfo>());
    }

    std::vector<std::shared_ptr<ComputeEvent>> events;
    ExcAssertEqual(info->handles.size(), queues.size());
    events.reserve(queues.size());

    for (size_t i = 0;  i < queues.size();  ++i) {
        auto event = queues[i]->enqueueFillArrayImpl(info->handles[i], init, startOffsetInBytes, lengthInBytes, arg);
        if (event) {
            events.emplace_back(std::move(event));
        }
    }

    return std::make_shared<MultiComputeEvent>(std::move(events));
}


// MultiComputeContext

MultiComputeContext::
MultiComputeContext()
{
    for (ComputeRuntimeId runtimeId: ComputeRuntime::enumerateRegisteredRuntimes()) {
        if (runtimeId == ComputeRuntimeId::MULTI)
            continue; // Don't call ourselves recursively...
        auto runtime = ComputeRuntime::getRuntimeForId(runtimeId);
        auto device = runtime->getDefaultDevice();
        contexts.emplace_back(runtime->getContext(std::vector{device}));
    }
    ExcAssertGreaterEqual(contexts.size(), 1);
}

MemoryRegionHandle
MultiComputeContext::
allocateImpl(size_t length, size_t align,
                const std::type_info & type, bool isConst,
                MemoryRegionInitialization initialization,
                std::any initWith)
{
    // Allocate with all of the runtimes.
    std::vector<MemoryRegionHandle> handles;
    for (auto & c: contexts) {
        handles.emplace_back(c->allocateImpl(length, align, type, isConst, initialization, initWith));
        using namespace std;
        cerr << "allocating " << length << " bytes of " << demangle(type) << " for context " << c << endl;
    }

    auto result = std::make_shared<MultiMemoryRegionInfo>();
    result->handles = std::move(handles);
    result->type = &type;
    result->isConst = isConst;
    result->lengthInBytes = length;
    return { std::move(result) };
}

std::tuple<MemoryRegionHandle, std::shared_ptr<ComputeEvent>>
MultiComputeContext::
transferToDeviceImpl(FrozenMemoryRegion region,
                        const std::type_info & type, bool isConst)
{
    std::vector<MemoryRegionHandle> handles;
    std::vector<std::shared_ptr<ComputeEvent>> events;

    for (auto & c: contexts) {
        auto [handle, event] = c->transferToDeviceImpl(region, type, isConst);
        handles.emplace_back(std::move(handle));
        events.emplace_back(std::move(event));
    }

    auto handle = std::make_shared<MultiMemoryRegionInfo>();
    handle->handles = std::move(handles);
    handle->type = &type;
    handle->isConst = isConst;
    handle->lengthInBytes = region.length();
    return { { {handle} }, std::make_shared<MultiComputeEvent>(std::move(events)) };
}

std::tuple<FrozenMemoryRegion, std::shared_ptr<ComputeEvent>>
MultiComputeContext::
transferToHostImpl(MemoryRegionHandle handle)
{
    // We only need to transfer the first...
    if (!handle.handle)
        return { {}, {} };
    auto info = std::dynamic_pointer_cast<const MultiMemoryRegionInfo>(std::move(handle.handle));
    if (!info) {
        auto & got = *handle.handle;
        throw MLDB::Exception("Multi transferToHostImpl: wrong info type: got " + demangle(typeid(got))
                                + " expected " + type_name<MultiMemoryRegionInfo>());
    }

    return contexts.at(0)->transferToHostImpl(info->handles.at(0));
}

std::tuple<MutableMemoryRegion, std::shared_ptr<ComputeEvent>>
MultiComputeContext::
transferToHostMutableImpl(MemoryRegionHandle handle)
{
    // We only need to transfer the first...
    if (!handle.handle)
        return { {}, {} };
    auto info = std::dynamic_pointer_cast<const MultiMemoryRegionInfo>(std::move(handle.handle));
    if (!info)
        throw MLDB::Exception("Multi transferToHostMutableImpl: wrong info type");

    return contexts.at(0)->transferToHostMutableImpl(info->handles.at(0));
}

std::shared_ptr<ComputeKernel>
MultiComputeContext::
getKernel(const std::string & kernelName)
{
    std::vector<std::shared_ptr<ComputeKernel>> kernels;
    for (auto & c: contexts) {
        kernels.emplace_back(c->getKernel(kernelName));
    }

    auto result = std::make_shared<MultiComputeKernel>(this, kernels);
    return result;
}

MemoryRegionHandle
MultiComputeContext::
managePinnedHostRegion(std::span<const std::byte> region, size_t align,
                        const std::type_info & type, bool isConst)
{
    std::vector<MemoryRegionHandle> handles;
    for (auto & c: contexts) {
        handles.emplace_back(c->managePinnedHostRegion(region, align, type, isConst));
    }

    auto result = std::make_shared<MultiMemoryRegionInfo>();
    result->handles = std::move(handles);
    result->type = &type;
    result->isConst = isConst;
    result->lengthInBytes = region.size();
    return { { std::move(result) } };
}

std::shared_ptr<ComputeQueue>
MultiComputeContext::
getQueue()
{
    std::vector<std::shared_ptr<ComputeQueue>> queues;
    for (auto & c: contexts)
        queues.emplace_back(c->getQueue());

    return std::make_shared<MultiComputeQueue>(this, std::move(queues));
}

// Return the MappedSerializer that owns the memory allocated on the host for this
// device.  It's needed for the generic MemoryRegion functions to know how to manipulate
// memory handles.  In practice it probably means that each runtime needs to define a
// MappedSerializer derivitive.
MappedSerializer *
MultiComputeContext::
getSerializer()
{
    // TODO: all messed up...
    return contexts.at(0)->getSerializer();
}


// MultiComputeRuntime

struct MultiComputeRuntime: public ComputeRuntime {
    virtual ~MultiComputeRuntime()
    {
    }

    virtual ComputeRuntimeId getId() const override
    {
        return ComputeRuntimeId::MULTI;
    }

    virtual std::string printRestOfDevice(ComputeDevice device) const override
    {
        return "";
    }

    virtual std::string printHumanReadableDeviceInfo(ComputeDevice device) const override
    {
        return "Multiple Devices";
    }

    virtual ComputeDevice getDefaultDevice() const override
    {
        ComputeDevice result;
        result.runtime = ComputeRuntimeId::MULTI;
        result.runtimeInstance = (uint8_t)ComputeMultiMode::COMPARE;
        return result;
    }

    virtual std::vector<ComputeDevice> enumerateDevices() const override
    {
        return { getDefaultDevice() };
    }

    virtual std::shared_ptr<ComputeContext>
    getContext(std::span<const ComputeDevice> devices) const override
    {
        if (devices.size() != 1 || devices[0].runtime != ComputeRuntimeId::MULTI) {
            throw MLDB::Exception("MULTI compute context can only operate on a single multi device");
        }
        return std::make_shared<MultiComputeContext>(/*ComputeMultiMode(device.runtimeInstance*/);
    }
};

MultiComputeKernel::
MultiComputeKernel(MultiComputeContext * context, std::vector<std::shared_ptr<ComputeKernel>> kernelsIn)
    : kernels(std::move(kernelsIn))
{
    ExcAssertGreater(kernels.size(), 0);
    for (auto & k : kernels)
        ExcAssert(k);
    this->context = context;
    this->multiContext = context;
    this->kernelName = kernels[0]->kernelName;
    this->dims = kernels[0]->dims;
    this->params = kernels[0]->params;
    this->paramIndex = kernels[0]->paramIndex;
}

namespace {

static struct Init {
    Init()
    {
        ComputeRuntime::registerRuntime(ComputeRuntimeId::MULTI, "multi",
                                        [] () { return new MultiComputeRuntime(); });
    }
} init;

};



} // namespace MLDB
