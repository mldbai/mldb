/** compute_kernel_multi.h                                                -*- C++ -*-
    Jeremy Barnes, 27 March 2016
    This file is part of MLDB. Copyright 2016 mldb.ai inc. All rights reserved.

    Compute kernel runtime for CPU devices.
*/

#include "compute_kernel_multi.h"
#include "mldb/types/basic_value_descriptions.h"


using namespace std;


namespace MLDB {

struct MultiComputeContext;

DEFINE_ENUM_DESCRIPTION_INLINE(ComputeMultiMode)
{
    addValue("COMPARE", ComputeMultiMode::COMPARE, "Run kernel on multiple devices, comparing the output");
}

namespace {

struct MultiMemoryRegionInfo: public MemoryRegionHandleInfo {
    MultiMemoryRegionInfo(std::vector<MemoryRegionHandle> handles)
        : handles(std::move(handles))
    {
    }

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
    getPrimitive(const std::string & opName, ComputeContext & context) const override
    {
        return underlying->getPrimitive(opName, fixupContext(context));
    }

    virtual bool canGetRange() const override
    {
        return underlying->canGetRange();
    }

    virtual std::tuple<void *, size_t, std::shared_ptr<const void>>
    getRange(const std::string & opName, ComputeContext & context) const override
    {
        return underlying->getRange(opName, fixupContext(context));
    }

    virtual bool canGetConstRange() const override
    {
        return underlying->canGetConstRange();
    }

    virtual std::tuple<const void *, size_t, std::shared_ptr<const void>>
    getConstRange(const std::string & opName, ComputeContext & context) const override
    {
        MemoryRegionHandle handle = getHandle(opName, context);
        auto region = multiContext.contexts[index]->transferToHostImpl(opName + "transferTohost", handle).get();
        return { region.data(), region.length(), region.handle() };
    }

    virtual bool canGetHandle() const override
    {
        return underlying->canGetHandle();
    }

    virtual MemoryRegionHandle
    getHandle(const std::string & opName, ComputeContext & context) const override
    {
        MemoryRegionHandle handle = underlying->getHandle(opName, context);
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

        boundKernels.emplace_back(this->kernels[i]->bindImpl(ourArguments));
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

void
MultiComputeKernel::
compareParameters(bool pre, const BoundComputeKernel & boundKernel) const
{
    const MultiBindInfo & bindInfo = dynamic_cast<const MultiBindInfo &>(*boundKernel.bindInfo);

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

        auto reference = bindInfo.boundKernels.at(0).arguments.at(i);
        //auto & h = *reference.handler; 
        //cerr << "  reference.handler = " << reference.handler << " " << demangle(typeid(h)) << endl;
        auto [referenceData, referenceLength, referencePin]
            = reference.handler->getConstRange("compareParameters", *this->multiContext->contexts[0]);

        for (size_t j = 1;  j < this->kernels.size();  ++j) {
            auto kernelGenerated = bindInfo.boundKernels.at(j).arguments.at(i);
            //auto & h = *kernelGenerated.handler; 
            //cerr << "  kernelGenerated.handler = " << kernelGenerated.handler << demangle(typeid(h)) << endl;
            auto [kernelGeneratedData, kernelGeneratedLength, kernelGeneratedPin]
                = kernelGenerated.handler->getConstRange("compareParameters", *this->multiContext->contexts[j]);
            

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

std::shared_ptr<ComputeEvent>
MultiComputeEvent::
thenImpl(std::function<void ()> fn)
{
    std::vector<std::shared_ptr<ComputeEvent>> newEvents;

    // Count how many of the underlying ones have triggered
    auto counter = std::make_shared<std::atomic<size_t>>(0);

    for (size_t i = 0;  i < events.size();  ++i) {
        auto newThen = [n=events.size(), counter, fn] ()
        {
            if (counter->fetch_add(1) == n - 1) {
                // last one has triggered, call fn
                fn();
            }
        };
        newEvents.emplace_back(events[i]->thenImpl(newThen));
    }

    return std::make_shared<MultiComputeEvent>(std::move(newEvents));
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

namespace {

// Unpack multiple prerequisites into a list for each underlying runtime
std::vector<std::vector<std::shared_ptr<ComputeEvent>>>
unpackPrereqs(size_t n, const std::vector<std::shared_ptr<ComputeEvent>> & prereqs)
{
    std::vector<std::vector<std::shared_ptr<ComputeEvent>>> result(n);

    for (auto & e: prereqs) {
        const MultiComputeEvent * multiEvent = dynamic_cast<const MultiComputeEvent *>(e.get());
        ExcAssert(multiEvent);
        for (size_t i = 0;  i < n;  ++i) {
            result[i].emplace_back(multiEvent->events.at(i));
            // Unpack the prerequisites to get the event for this device
        }        
    }

    return result;
}

std::shared_ptr<const MultiMemoryRegionInfo> getMultiInfo(const MemoryRegionHandle & handle)
{
    if (!handle.handle) {
        throw MLDB::Exception("Null handle passed to Multi region function");
    }

    auto info = std::dynamic_pointer_cast<const MultiMemoryRegionInfo>(std::move(handle.handle));

    if (!info) {
        auto & got = *handle.handle;
        throw MLDB::Exception("Wrong info type: got " + demangle(typeid(got))
                                + " expected " + type_name<MultiMemoryRegionInfo>());
    }

    return info;
}

// Create a promise that returns the value of the function applied to the vector of
// returned values from the promises
template<typename T, typename Fn, typename Return = std::invoke_result_t<Fn, std::vector<T>>>
ComputePromiseT<Return>
thenReduce(std::vector<ComputePromiseT<T>> promises, Fn && fn)
{
    struct Accum {
        Accum(size_t n, Fn fn)
            : vals(n), fn(std::forward<Fn>(fn))
        {
        }

        std::mutex mutex;
        std::vector<T> vals;
        size_t count = 0;
        Fn fn;
        std::promise<std::any> promise;
        std::vector<ComputePromise> promisesOut;
        std::exception_ptr exc;

        void except(std::exception_ptr exc)
        {
            std::unique_lock guard(mutex);
            if (!this->exc)
                this->exc = std::move(exc);
            fire();
        }

        void set(size_t i, T val)
        {
            std::unique_lock guard(mutex);
            vals[i] = std::move(val);
            fire();
        }

        void fire()
        {
            if (++count != vals.size())
                return;

            if (exc)
                promise.set_exception(std::move(exc));

            try {
                auto result = fn(std::move(vals));
                promise.set_value(std::move(result));
            } MLDB_CATCH_ALL {
                promise.set_exception(std::current_exception());
            }
        }
    };

    auto accum = std::make_shared<Accum>(promises.size(), std::forward<Fn>(fn));

    std::vector<std::shared_ptr<ComputeEvent>> eventsOut;

    for (size_t i = 0;  i < promises.size();  ++i) {
        auto setValue = [accum, i] (T val)
        {
            accum->set(i, std::move(val));
        };

        // TODO: exceptions
        auto newPromise = promises[i].then(std::move(setValue));

        eventsOut.emplace_back(newPromise.event());
        accum->promisesOut.emplace_back(std::move(newPromise));
    }

    std::shared_ptr<std::promise<std::any>> promise(accum, &accum->promise);
    auto event = std::make_shared<MultiComputeEvent>(std::move(eventsOut));

    ComputePromiseT<Return> result(std::move(promise), std::move(event));

    return result;
}

ComputePromiseT<MemoryRegionHandle>
reduceHandles(const std::string & regionName,
              std::vector<ComputePromiseT<MemoryRegionHandle>> promises,
              size_t length, const std::type_info & type, bool isConst)
{
    auto getResult = [type=&type, isConst, length, regionName] (const std::vector<MemoryRegionHandle> & handles) -> MemoryRegionHandle
    {
        auto result = std::make_shared<MultiMemoryRegionInfo>(std::move(handles));
        result->type = type;
        result->isConst = isConst;
        result->lengthInBytes = length;
        result->name = regionName;
        return { std::move(result) };
    };

    return thenReduce(std::move(promises), getResult);
}

} // file scope

#if 0
std::shared_ptr<ComputeEvent>
MultiComputeQueue::
launch(const std::string & opName,
       const BoundComputeKernel & kernel,
       const std::vector<uint32_t> & grid,
       const std::vector<std::shared_ptr<ComputeEvent>> & prereqs)
{
    const MultiBindInfo * multiInfo = dynamic_cast<const MultiBindInfo *>(kernel.bindInfo.get());
    ExcAssert(multiInfo);

    std::vector<std::shared_ptr<ComputeEvent>> events;
    auto unpackedPrereqs = unpackPrereqs(queues.size(), prereqs);

    for (size_t i = 0;  i < queues.size();  ++i) {
        // Launch on the child queue
        auto ev = queues[i]->launch(opName, multiInfo->boundKernels[i], grid, unpackedPrereqs[i]);
        events.emplace_back(std::move(ev));
    }

    return std::make_shared<MultiComputeEvent>(std::move(events));
}
#endif

std::shared_ptr<ComputeEvent>
MultiComputeQueue::
launch(const std::string & opName,
       const BoundComputeKernel & kernel,
       const std::vector<uint32_t> & grid,
       const std::vector<std::shared_ptr<ComputeEvent>> & prereqs)
{
    const MultiBindInfo * multiInfo = dynamic_cast<const MultiBindInfo *>(kernel.bindInfo.get());
    ExcAssert(multiInfo);

    const MultiComputeKernel * multiKernel = dynamic_cast<const MultiComputeKernel *>(kernel.owner);
    ExcAssert(multiKernel);

    std::vector<std::shared_ptr<ComputeEvent>> events;
    auto unpackedPrereqs = unpackPrereqs(queues.size(), prereqs);

    constexpr bool compareMode = true;

    if (compareMode)
        multiKernel->compareParameters(true /* pre */, kernel);

    for (size_t i = 0;  i < queues.size();  ++i) {
        // Launch on the child queue
        auto ev = queues[i]->launch(opName, multiInfo->boundKernels[i], grid, unpackedPrereqs[i]);
        events.emplace_back(std::move(ev));
    }

    if (compareMode)
        multiKernel->compareParameters(false /* pre */, kernel);

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

void
MultiComputeQueue::
finish()
{
    for (auto & q: queues) {
        q->finish();
    }
}

ComputePromiseT<MemoryRegionHandle>
MultiComputeQueue::
enqueueFillArrayImpl(const std::string & opName,
                     MemoryRegionHandle region, MemoryRegionInitialization init,
                     size_t startOffsetInBytes, ssize_t lengthInBytes,
                     const std::any & arg,
                     std::vector<std::shared_ptr<ComputeEvent>> prereqs)
{
    auto info = getMultiInfo(region);

    std::vector<ComputePromiseT<MemoryRegionHandle>> promises;
    ExcAssertEqual(info->handles.size(), queues.size());
    promises.reserve(queues.size());

    auto unpackedPrereqs = unpackPrereqs(queues.size(), prereqs);

    for (size_t i = 0;  i < queues.size();  ++i) {
        promises.emplace_back(queues[i]->enqueueFillArrayImpl(opName, info->handles[i], init, startOffsetInBytes, lengthInBytes, arg, unpackedPrereqs[i]));
    }

    auto returnResult = [region=std::move(region)] (auto unused) { return region; };
    return thenReduce(std::move(promises), std::move(returnResult));
}

std::shared_ptr<ComputeEvent>
MultiComputeQueue::
makeAlreadyResolvedEvent() const
{
    std::vector<std::shared_ptr<ComputeEvent>> events;
    for (auto & q: queues) {
        events.emplace_back(q->makeAlreadyResolvedEvent());
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

ComputePromiseT<MemoryRegionHandle>
MultiComputeContext::
allocateImpl(const std::string & regionName,
             size_t length, size_t align,
             const std::type_info & type, bool isConst,
             MemoryRegionInitialization initialization,
             std::any initWith)
{
    // Allocate with all of the runtimes.
    std::vector<ComputePromiseT<MemoryRegionHandle>> promises;
    for (auto & c: contexts) {
        promises.emplace_back(c->allocateImpl(regionName, length, align, type, isConst, initialization, initWith));
    }
    return reduceHandles(regionName, std::move(promises), length, type, isConst);
}

ComputePromiseT<MemoryRegionHandle>
MultiComputeContext::
transferToDeviceImpl(const std::string & opName,
                     FrozenMemoryRegion region,
                     const std::type_info & type, bool isConst)
{
    // Allocate with all of the runtimes.
    std::vector<ComputePromiseT<MemoryRegionHandle>> promises;

    for (auto & c: contexts) {
        promises.emplace_back(c->transferToDeviceImpl(opName, region, type, isConst));
    }
    return reduceHandles(opName, std::move(promises), region.length(), type, isConst);
}

ComputePromiseT<FrozenMemoryRegion>
MultiComputeContext::
transferToHostImpl(const std::string & opName, MemoryRegionHandle handle)
{
    auto info = getMultiInfo(handle);
    return contexts.at(0)->transferToHostImpl(opName, info->handles.at(0));
}

ComputePromiseT<MutableMemoryRegion>
MultiComputeContext::
transferToHostMutableImpl(const std::string & opName, MemoryRegionHandle handle)
{
    auto info = getMultiInfo(handle);
    return contexts.at(0)->transferToHostMutableImpl(opName, info->handles.at(0));
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

ComputePromiseT<MemoryRegionHandle>
MultiComputeContext::
managePinnedHostRegion(const std::string & regionName,
                       std::span<const std::byte> region, size_t align,
                       const std::type_info & type, bool isConst)
{
    std::vector<ComputePromiseT<MemoryRegionHandle>> promises;
    for (auto & c: contexts) {
        promises.emplace_back(c->managePinnedHostRegion(regionName, region, align, type, isConst));
    }
    return reduceHandles(regionName, std::move(promises), region.size(), type, isConst);
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

MemoryRegionHandle
MultiComputeContext::
getSliceImpl(const MemoryRegionHandle & handle, const std::string & regionName,
             size_t startOffsetInBytes, size_t lengthInBytes,
             size_t align, const std::type_info & type, bool isConst)
{
    auto info = std::dynamic_pointer_cast<const MultiMemoryRegionInfo>(std::move(handle.handle));
    ExcAssert(info);

    std::vector<MemoryRegionHandle> newHandles;
    for (size_t i = 0;  i < contexts.size();  ++i) {
        newHandles.emplace_back(contexts[i]->getSliceImpl(info->handles[i], regionName,
                                                           startOffsetInBytes, lengthInBytes,
                                                           align, type, isConst));
    }

    auto newInfo = std::make_shared<MultiMemoryRegionInfo>(std::move(newHandles));
    newInfo->isConst = isConst;
    newInfo->type = &type;
    newInfo->name = regionName;
    newInfo->lengthInBytes = lengthInBytes;
    newInfo->parent = info;
    newInfo->ownerOffset = startOffsetInBytes;

    return { newInfo };
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
