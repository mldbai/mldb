/** compute_kernel_cpu.h                                                -*- C++ -*-
    Jeremy Barnes, 27 March 2016
    This file is part of MLDB. Copyright 2016 mldb.ai inc. All rights reserved.

    Compute kernel runtime for CPU devices.
*/

#pragma once

#include "mldb/block/compute_kernel.h"
#include "mldb/block/compute_kernel_grid.h"
#include "mldb/block/compute_kernel_host.h"
#include "mldb/types/value_description.h"
#include "mldb/types/vector_description.h"
#include "mldb/types/tuple_description.h"
#include "mldb/types/span_description.h"
#include "mldb/utils/possibly_dynamic_buffer.h"

namespace MLDB {

template<typename... Args>
void traceCPUOperation(const std::string & opName, Args&&... args)
{
    traceOperation(OperationScope::EVENT, OperationType::CPU_COMPUTE, opName, std::forward<Args>(args)...);
}

struct CPUComputeContext;
struct CPUComputeQueue;

using CPUComputeProfilingInfo = GridComputeProfilingInfo;

// enable_shared_from_this is to ensure that we can pin lifetimes of events until the
// completion handlers have finished.
struct CPUComputeEvent: public GridComputeEvent {
    CPUComputeEvent(const std::string & label, bool resolved, const CPUComputeQueue * owner);  // may or may not be already resolved

    virtual ~CPUComputeEvent() = default;

    virtual void await() const override;

    static std::shared_ptr<CPUComputeEvent>
    makeAlreadyResolvedEvent(const std::string & label, const CPUComputeQueue * owner);
    static std::shared_ptr<CPUComputeEvent>
    makeUnresolvedEvent(const std::string & label, const CPUComputeQueue * owner);
};


// CPUComputeQueue

struct CPUComputeQueue: public GridComputeQueue, std::enable_shared_from_this<CPUComputeQueue> {
    CPUComputeQueue(CPUComputeContext * owner, CPUComputeQueue * parent,
                    const std::string & label,
                    GridDispatchType dispatchType);
    virtual ~CPUComputeQueue();

    CPUComputeContext * cpuOwner = nullptr;

    virtual std::shared_ptr<ComputeQueue> parallel(const std::string & opName) override;
    virtual std::shared_ptr<ComputeQueue> serial(const std::string & opName) override;

    virtual FrozenMemoryRegion
    enqueueTransferToHostImpl(const std::string & opName,
                              MemoryRegionHandle handle) override;

    virtual FrozenMemoryRegion
    transferToHostSyncImpl(const std::string & opName,
                           MemoryRegionHandle handle) override;

    virtual MutableMemoryRegion
    enqueueTransferToHostMutableImpl(const std::string & opName,
                                     MemoryRegionHandle handle) override;

    virtual MutableMemoryRegion
    transferToHostMutableSyncImpl(const std::string & opName,
                                  MemoryRegionHandle handle) override;

    virtual MemoryRegionHandle
    enqueueManagePinnedHostRegionImpl(const std::string & opName,
                                      std::span<const std::byte> region, size_t align,
                                      const std::type_info & type, bool isConst) override;

    virtual MemoryRegionHandle
    managePinnedHostRegionSyncImpl(const std::string & opName,
                                   std::span<const std::byte> region, size_t align,
                                   const std::type_info & type, bool isConst) override;

    virtual void
    enqueueCopyBetweenDeviceRegionsImpl(const std::string & opName,
                                        MemoryRegionHandle from, MemoryRegionHandle to,
                                        size_t fromOffset, size_t toOffset,
                                        size_t length) override;

    virtual void
    copyBetweenDeviceRegionsSyncImpl(const std::string & opName,
                                     MemoryRegionHandle from, MemoryRegionHandle to,
                                     size_t fromOffset, size_t toOffset,
                                     size_t length) override;

    virtual std::shared_ptr<ComputeEvent> makeAlreadyResolvedEvent(const std::string & label) const override;

    virtual void enqueueBarrier(const std::string & label) override;
    virtual std::shared_ptr<ComputeEvent> flush() override;
    virtual void finish() override;

protected:
    virtual void
    enqueueZeroFillArrayConcrete(const std::string & opName,
                                 MemoryRegionHandle region,
                                 size_t startOffsetInBytes, ssize_t lengthInBytes) override;
    virtual void
    enqueueBlockFillArrayConcrete(const std::string & opName,
                                  MemoryRegionHandle region,
                                  size_t startOffsetInBytes, ssize_t lengthInBytes,
                                  std::span<const std::byte> block) override;
    virtual void
    enqueueCopyFromHostConcrete(const std::string & opName,
                                MemoryRegionHandle toRegion,
                                FrozenMemoryRegion fromRegion,
                                size_t deviceStartOffsetInBytes) override;

    virtual FrozenMemoryRegion
    enqueueTransferToHostConcrete(const std::string & opName, MemoryRegionHandle handle) override;

    virtual FrozenMemoryRegion
    transferToHostSyncConcrete(const std::string & opName, MemoryRegionHandle handle) override;

    // Subclasses override to create a new bind context
    virtual std::shared_ptr<GridBindContext>
    newBindContext(const std::string & opName,
                   const GridComputeKernel * kernel, const GridBindInfo * bindInfo) override;
};


// CPUComputeContext

struct CPUComputeContext: public GridComputeContext {

    CPUComputeContext();

    virtual ~CPUComputeContext() = default;

    std::shared_ptr<MappedSerializer> backingStore;

    virtual MemoryRegionHandle
    allocateSyncImpl(const std::string & regionName,
                     size_t length, size_t align,
                     const std::type_info & type, bool isConst) override;

    // pin, region, length in bytes
    static std::tuple<std::shared_ptr<const void>, const std::byte *, size_t>
    getMemoryRegion(const std::string & opName, MemoryRegionHandleInfo & handle,
                    MemoryRegionAccess access);

    std::tuple<FrozenMemoryRegion, int /* version */>
    getFrozenHostMemoryRegion(const std::string & opName,
                              MemoryRegionHandleInfo & handle,
                              size_t offset, ssize_t length,
                              bool ignoreHazards) const;

    virtual std::shared_ptr<GridComputeFunctionLibrary>
    getLibrary(const std::string & name) override;

    virtual std::shared_ptr<ComputeQueue>
    getQueue(const std::string & queueName) override;

protected:
    virtual std::shared_ptr<GridComputeKernelSpecialization>
    specializeKernel(const GridComputeKernelTemplate & tmplate) override;
};


// CPUComputeFunction

struct CPUComputeFunction: public GridComputeFunction {
    CPUComputeFunction();

    virtual ~CPUComputeFunction() = default;

    virtual std::vector<GridComputeFunctionArgument> getArgumentInfo() const override;

    std::vector<GridComputeFunctionArgument> argumentInfo;
    HostComputeKernel kernel;
    std::function<std::any ()> createArgTuple;
    std::function<void (GridComputeQueue &, std::string, const std::any &, std::vector<size_t>, std::vector<size_t>, size_t, const std::map<std::string, size_t> &)> launch;
};


// CPUComputeFunctionLibrary

struct CPUComputeFunctionLibrary: public GridComputeFunctionLibrary {
    CPUComputeFunctionLibrary();

    virtual ~CPUComputeFunctionLibrary() = default;

    virtual std::shared_ptr<GridComputeFunction>
    getFunction(const std::string & functionName) override;

    virtual std::string getId() const override;

    virtual Json::Value getMetadata() const override;

    // Return a version compiled from source read from the given filename
    static std::shared_ptr<CPUComputeFunctionLibrary>
    compileFromSourceFile(CPUComputeContext & context, const std::string & fileName);

    // Return a version compiled from source given in the sourceCode string
    static std::shared_ptr<CPUComputeFunctionLibrary>
    compileFromSource(CPUComputeContext & context, const Utf8String & sourceCode, const std::string & fileNameToAppearInErrorMessages);

    // NOTE: protected by global CPU library mutex
    std::map<std::string, std::function<std::shared_ptr<CPUComputeFunction> ()> > initializers;
    std::map<std::string, std::shared_ptr<CPUComputeFunction>> functions;
};


// CPUComputeKernel

struct CPUComputeKernel: public GridComputeKernelSpecialization {

    CPUComputeKernel(CPUComputeContext * owner, const GridComputeKernelTemplate & tmplate);

    CPUComputeContext * cpuContext = nullptr;
    const CPUComputeFunction * cpuFunction = nullptr;
};

enum class CPUComputeKernelArgKind {
    NONE,
    BYTES,
    HANDLE,
    THREADGROUP
};

// Encodes the argument in a discriminated union
struct CPUComputeKernelArgValue {
    CPUComputeKernelArgValue(std::span<const std::byte> bytes)
        : kind(CPUComputeKernelArgKind::BYTES), bytes(bytes)
    {
    }

    CPUComputeKernelArgValue(std::shared_ptr<GridMemoryRegionHandleInfo> handle, MemoryRegionAccess access)
        : kind(CPUComputeKernelArgKind::HANDLE), handle(handle), access(access)
    {
    }

    CPUComputeKernelArgValue(size_t numThreadGroupBytes)
        : kind(CPUComputeKernelArgKind::THREADGROUP), numThreadGroupBytes(numThreadGroupBytes)
    {
    }

    CPUComputeKernelArgKind kind = CPUComputeKernelArgKind::NONE;
    std::span<const std::byte> bytes;
    std::shared_ptr<GridMemoryRegionHandleInfo> handle;
    MemoryRegionAccess access;
    ssize_t numThreadGroupBytes = -1;
};

template<int GlobalOrLocal, int IdOrSize, int Dim, typename AsType>
struct GridQuery {
    operator AsType() const
    {
        return val;
    }

    AsType val = AsType();
};

template<typename T>
struct LocalArray {
};

using Pin = std::shared_ptr<const void>;
using ArgSetter = std::function<Pin (ComputeQueue & queue, const std::string & opName, void * arg, const CPUComputeKernelArgValue & value)>;
using TupleSetter = std::function<Pin (ComputeQueue & queue, const std::string & opName, std::any & tupleAny, const CPUComputeKernelArgValue & value)>;

struct CPUGridKernelParameterInfo {
    const char * name;
    const char * kind;
    const char * type;
    const char * dims;
};

template<typename T>
std::tuple<ComputeKernelType, ArgSetter, GridComputeFunctionArgumentDisposition>
handleCpuKernelCallArgument(std::span<T> *)
{
    ComputeKernelType result(details::getBestValueDescriptionT<T>(), "rw");
    result.dims.emplace_back();

    auto setArg = [] (ComputeQueue & queue, const std::string & opName, void * argPtr,
                      const CPUComputeKernelArgValue & value)
    {
        if (value.kind != CPUComputeKernelArgKind::HANDLE) {
            MLDB_THROW_UNIMPLEMENTED("attempt to pass non-range memory region to arg that needs a span");            
        }

        auto & arg = *reinterpret_cast<std::span<T> *>(argPtr);

        auto [pin, ptr, offset] = CPUComputeContext::getMemoryRegion(opName, *value.handle, ACC_READ_WRITE);
        arg = { (T *)(ptr + offset), value.handle->lengthInBytes / sizeof(T) };
        return std::move(pin);
    };

    return { result, std::move(setArg), GridComputeFunctionArgumentDisposition::BUFFER };
}

template<typename T>
std::tuple<ComputeKernelType, ArgSetter, GridComputeFunctionArgumentDisposition>
handleCpuKernelCallArgument(std::span<const T> *)
{
    ComputeKernelType result(details::getBestValueDescriptionT<T>(), "r");
    result.dims.emplace_back();

    auto setArg = [] (ComputeQueue & queue, const std::string & opName, void * argPtr,
                      const CPUComputeKernelArgValue & value)
    {
        if (value.kind != CPUComputeKernelArgKind::HANDLE) {
            MLDB_THROW_UNIMPLEMENTED("attempt to pass non-range memory region to arg that needs a span");            
        }

        auto & arg = *reinterpret_cast<std::span<const T> *>(argPtr);

        auto [pin, ptr, offset] = CPUComputeContext::getMemoryRegion(opName, *value.handle, ACC_READ);
        arg = { reinterpret_cast<const T *>(ptr + offset), value.handle->lengthInBytes / sizeof(T) };
        return std::move(pin);
    };

    return { result, std::move(setArg), GridComputeFunctionArgumentDisposition::BUFFER };
}

template<typename T>
std::tuple<ComputeKernelType, ArgSetter, GridComputeFunctionArgumentDisposition>
handleCpuKernelCallArgument(T *)
{
    ComputeKernelType result(details::getBestValueDescriptionT<std::remove_const_t<T>>(),
                             "r");

    auto setArg = [] (ComputeQueue & queue, const std::string & opName, void * argPtr,
                      const CPUComputeKernelArgValue & value)
    {
        if (value.kind != CPUComputeKernelArgKind::BYTES) {
            MLDB_THROW_UNIMPLEMENTED("attempt to pass non-literal to arg that needs a byte range");            
        }

        auto & arg = *reinterpret_cast<T *>(argPtr);
        static const auto desc = details::getBestValueDescriptionT<std::remove_const_t<T>>();
        details::copyUsingValueDescription(desc.get(), value.bytes, &arg, typeid(T));
        return nullptr;
    };

    return { result, setArg, GridComputeFunctionArgumentDisposition::LITERAL };    
}

template<int GlobalOrLocal, int IdOrSize, int Dim, typename T>
std::tuple<ComputeKernelType, ArgSetter, GridComputeFunctionArgumentDisposition>
handleCpuKernelCallArgument(GridQuery<GlobalOrLocal, IdOrSize, Dim, T> *)
{
    ComputeKernelType result(details::getBestValueDescriptionT<std::remove_const_t<T>>(),
                             "r");

    return { result, nullptr /* no arg setter */, GridComputeFunctionArgumentDisposition::LITERAL };    
}

template<typename T>
std::tuple<ComputeKernelType, ArgSetter, GridComputeFunctionArgumentDisposition>
handleCpuKernelCallArgument(LocalArray<T> *)
{
    ComputeKernelType result(details::getBestValueDescriptionT<std::remove_const_t<T>>(),
                             "rw");
    return { result, nullptr /* no arg setter */, GridComputeFunctionArgumentDisposition::THREADGROUP };    
}

template<size_t N, typename Tuple>
std::vector<GridComputeFunctionArgument>
getArgumentInfos(const std::string & functionName, const CPUGridKernelParameterInfo parameterInfo[])
{
    return {};
}

template<typename T, size_t N, typename Tuple>
GridComputeFunctionArgument
getArgumentInfo(const std::string & parameterName, int argNumber)
{
    auto [outputType, setArg, disposition] = handleCpuKernelCallArgument((T*)0);

    TupleSetter setEntry = [setArg=std::move(setArg), parameterName]
        (ComputeQueue & queue, const std::string & opName,
         std::any & tupleAny, const CPUComputeKernelArgValue & value)
    {
        Tuple & tuple = std::any_cast<Tuple &>(tupleAny);
        return setArg(queue, opName, &std::get<N>(tuple), value);
    };

    GridComputeFunctionArgument arg;
    arg.name = parameterName;
    arg.disposition = disposition;
    arg.type = std::move(outputType);
    arg.marshal = setEntry;
    arg.computeFunctionArgIndex = argNumber;

    return arg;
}

template<size_t N, typename Tuple, typename First, typename... Rest>
std::vector<GridComputeFunctionArgument>
getArgumentInfos(const std::string & functionName, const CPUGridKernelParameterInfo parameterInfo[N + sizeof...(Rest) + 1])
{
    auto rest = getArgumentInfos<N + 1, Tuple, Rest...>(functionName, parameterInfo);
    
    GridComputeFunctionArgument arg = getArgumentInfo<First, N, Tuple>(parameterInfo[N].name, N);
    rest.push_back(std::move(arg));

    return rest;
}

void registerCpuKernelImpl(const std::string & libraryName, const std::string & functionName,
                           std::function<std::shared_ptr<CPUComputeFunction> ()> generator);

void throwDimensionException(unsigned dim, unsigned n) MLDB_NORETURN;
void throwOverflow() MLDB_NORETURN;

struct CheckedSize {
    CheckedSize(size_t sz = std::numeric_limits<size_t>::max())
        : sz(sz)
    {
    }

    size_t sz;

    template<typename T> operator T () const { ExcAssertEqual((T)sz, sz);  return sz; }

    template<typename T1, typename T2, typename T3>
    static CheckedSize madd(T1 x, T2 y, T3 z)
    {
        size_t res;
        if (MLDB_UNLIKELY(__builtin_mul_overflow(x, y, &res))
            || MLDB_UNLIKELY(__builtin_add_overflow(res, z, &res)))
            throwOverflow();
        return { res };
    }

    template<typename T1, typename T2>
    static CheckedSize mul(T1 x, T2 y)
    {
        size_t res;
        if (MLDB_UNLIKELY(__builtin_mul_overflow(x, y, &res)))
            throwOverflow();
        return { res };
    }

    template<typename T1, typename T2>
    static CheckedSize add(T1 x, T2 y)
    {
        size_t res;
        if (MLDB_UNLIKELY(__builtin_add_overflow(x, y, &res)))
            throwOverflow();
        return { res };
    }
};

inline std::ostream & operator << (std::ostream & stream, const CheckedSize & sz)
{
    return stream << sz.sz;
}

struct GridBounds {
    GridBounds(std::array<size_t, 3> bounds = { 0, 0, 0 })
        : bounds(bounds)
    {
    }

    GridBounds(const std::vector<size_t> & bounds)
    {
        ExcAssertEqual(bounds.size(), 3);
        this->bounds = { bounds[0], bounds[1], bounds[2] };
    }

    struct Iterator;
    Iterator begin() const;
    Iterator end() const;

    std::array<size_t, 3> bounds;

    size_t getSize(unsigned n) const
    {
        if (MLDB_UNLIKELY(n > 2)) {
            throwDimensionException(n, 3);
        }
        
        return bounds[n];
    }

    size_t getProd(unsigned n) const
    {
        if (MLDB_UNLIKELY(n > 3)) {
            throwDimensionException(n, 3);
        }

        CheckedSize result = 1;
        for (unsigned i = 0;  i < n;  ++i) {
            result = CheckedSize::mul(bounds[i], result.sz);
        }

        return result;
    }

    size_t getLinearSize() const { return getProd(3); }
};

inline std::ostream & operator << (std::ostream & stream, const GridBounds & bounds)
{
    return stream << "[" << bounds.getSize(0)
                  << "," << bounds.getSize(1)
                  << "," << bounds.getSize(2)
                  << "]";
}

struct GridIndex {
    GridIndex(size_t index, const GridBounds * bounds)
        : index(index), bounds(bounds)
    {
        if (MLDB_UNLIKELY(index >= bounds->getProd(3)))
            throwOverflow();
    }

    size_t index;  // linear index
    const GridBounds * bounds;

    CheckedSize getIndex(unsigned n) const
    {
        return index % bounds->getProd(n + 1) / bounds->getProd(n);
    }

    CheckedSize getSize(unsigned n) const
    {
        return bounds->getSize(n);
    }

    CheckedSize getLinearIndex() const
    {
        return index;
    }

    CheckedSize getLinearSize() const
    {
        return bounds->getProd(3);
    }

    GridIndex linearOffset(size_t ofs) const
    {
        return GridIndex(CheckedSize::add(index, ofs), bounds);
    }
};

inline std::ostream & operator << (std::ostream & stream, const GridIndex & index)
{
    return stream << "[" << index.getIndex(0) << "/" << index.getSize(0)
                  << "," << index.getIndex(1) << "/" << index.getSize(1)
                  << "," << index.getIndex(2) << "/" << index.getSize(2)
                  << "]";
}

struct GridBounds::Iterator {
    using iterator_category = std::forward_iterator_tag;
    using value_type = GridIndex;
    using difference_type = ssize_t;
    using pointer = const uint32_t*;
    using reference = const uint32_t&;

    auto operator <=> (const Iterator & other) const = default;

    Iterator operator++()
    {
        ++current;
        return *this;
    }

    value_type operator * () const
    {
        return { current, bounds };
    }

    size_t current = 0;
    const GridBounds * bounds;
};

GridBounds::Iterator GridBounds::begin() const { return { 0, this }; }
GridBounds::Iterator GridBounds::end() const { return { getProd(3), this }; }


struct ThreadExecutionState {

    ThreadExecutionState(GridIndex localIndex,
                         const GridIndex * globalIndex)
        : localIndex(localIndex), globalIndex(globalIndex)
    {
        using namespace std;
        cerr << "    running thread group with index " << localIndex << endl;
    }

    CheckedSize localId(unsigned n)
    {
        return localIndex.getIndex(n);
    }

    CheckedSize localSize(unsigned n)
    {
        return localIndex.getSize(n);
    }

    CheckedSize globalId(unsigned n)
    {
        return CheckedSize::madd(globalIndex->getIndex(n).sz, localIndex.getSize(n).sz, localIndex.getIndex(n).sz);
    }

    CheckedSize globalSize(int n)
    {
        return CheckedSize::mul(globalIndex->getSize(n).sz, localIndex.getSize(n).sz);
    }

    GridIndex localIndex;
    const GridIndex * globalIndex;
};


struct SimdGroupExecutionState {

    SimdGroupExecutionState(GridIndex firstThreadIndex,
                            size_t threadsPerSimdGroup,
                            const GridIndex * globalIndex)
        : firstThreadIndex(firstThreadIndex),
          threadsPerSimdGroup(threadsPerSimdGroup),
          globalIndex(globalIndex)
    {
        using namespace std;
        cerr << "  running SIMD group with index " << firstThreadIndex << endl;
    }

    std::vector<ThreadExecutionState> threads() const
    {
        std::vector<ThreadExecutionState> result;
        for (size_t i = 0;  i < threadsPerSimdGroup;  ++i) {
            GridIndex threadIndex = firstThreadIndex.linearOffset(i);
            ThreadExecutionState state(threadIndex, globalIndex);
            result.emplace_back(state);
        }
        return result;
    }

    GridIndex firstThreadIndex;
    size_t threadsPerSimdGroup;
    const GridIndex * globalIndex;
};

struct ThreadGroupExecutionState {
    ThreadGroupExecutionState(GridIndex globalIndex, const GridBounds * localBounds,
                              std::byte * localMem,
                              std::map<std::string, size_t> localMemOffsets)
        : globalIndex(globalIndex), localBounds(localBounds), localMem(localMem),
          localMemOffsets(std::move(localMemOffsets))
    {
        using namespace std;
        cerr << "running thread group with global index " << globalIndex << endl;
    }

    template<typename T>
    T * getLocal(const char * name) const
    {
        auto it = localMemOffsets.find(name);
        if (it == localMemOffsets.end()) {
            throw MLDB::Exception("Couldn't find local memory for %s in %zd values", name, localMemOffsets.size());
        }
        std::byte * p = localMem + it->second;
        size_t s = (size_t)p;
        ExcAssertEqual(s % alignof(T), 0);
        return (T *)p;
    }

    std::vector<SimdGroupExecutionState> simdGroups() const
    {
        size_t workGroupSize = localBounds->getLinearSize();
        size_t threadsPerSimdGroup = std::min<size_t>(32, workGroupSize);
        ExcAssertEqual(workGroupSize % threadsPerSimdGroup, 0);
        std::vector<SimdGroupExecutionState> result;
        for (size_t i = 0;  i < localBounds->getProd(3);  i += threadsPerSimdGroup) {
            GridIndex firstThreadIndex(i, localBounds);
            SimdGroupExecutionState state(firstThreadIndex, threadsPerSimdGroup, &globalIndex);
            result.push_back(state);
        }
        return result;
    }

    GridIndex globalIndex;
    const GridBounds * localBounds;
    std::byte * localMem;
    std::map<std::string, size_t> localMemOffsets;
};

template<typename... Args>
void registerCpuKernel(const std::string & libraryName, const std::string & functionName,
                       void (*fn) (const ThreadGroupExecutionState &, Args...),
                       const CPUGridKernelParameterInfo parameterInfo[sizeof...(Args)])
{
    auto initialize = [=] () -> std::shared_ptr<CPUComputeFunction>
    {
        auto argInfos = getArgumentInfos<0, std::tuple<Args...>, Args...>(functionName, parameterInfo);
        std::reverse(argInfos.begin(), argInfos.end());

        //HostComputeKernel kernel;
        //for (size_t i = 0;  i < argInfos.size();  ++i) {
        //    kernel.addParameter(argInfos[i].name, argInfos[i].type);
        //}
        //kernel.setGridComputeFunction(fn);

        //using namespace std;
        //cerr << jsonEncode(argInfos) << endl;

        auto createArgTuple = [] () -> std::any
        {
            return std::tuple<Args...>();
        };

        auto launch = [fn] (GridComputeQueue & queue, const std::string & opName, const std::any & argsAny,
                            const std::vector<size_t> & grid, const std::vector<size_t> & block,
                            size_t localMemBytesRequired, const std::map<std::string, size_t> & localMemOffsets)
        {
            const auto & args = std::any_cast<const std::tuple<Args...> &>(argsAny);
            GridBounds gridBounds(grid);
            GridBounds blockBounds(block);

            uint64_t localMem[(localMemBytesRequired + 7) / 8];

            for (GridIndex globalIndex: gridBounds) {
                ThreadGroupExecutionState state(globalIndex, &blockBounds, (std::byte *)&localMem, localMemOffsets);
                HostComputeKernel::apply(fn, args, state);
            }
        };

        auto function = std::make_shared<CPUComputeFunction>();
        function->argumentInfo = std::move(argInfos);
        //function->kernel = std::move(kernel);

        function->createArgTuple = std::move(createArgTuple);
        function->launch = std::move(launch);

        return function;
    };

    registerCpuKernelImpl(libraryName, functionName, std::move(initialize));
}

}  // namespace MLDB