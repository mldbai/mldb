/** compute_kernel_host.h                                                -*- C++ -*-
    Jeremy Barnes, 27 March 2016
    This file is part of MLDB. Copyright 2016 mldb.ai inc. All rights reserved.

    Compute kernel runtime for CPU devices.
*/

#pragma once

#include "compute_kernel.h"
#include "mldb/types/enum_description.h"
#include "mldb/types/generic_atom_description.h"

namespace MLDB {

struct HostComputeContext;

struct ComputeKernelGridRange {
    ComputeKernelGridRange() = default;

    ComputeKernelGridRange(uint32_t range)
        : first_(0), last_(range), range_(range)
    {
    }

    struct Iterator {
        using iterator_category = std::forward_iterator_tag;
        using value_type = uint32_t;
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
            return current;
        }

        uint32_t current = 0;
    };

    uint32_t first_ = 0;  // Where this part of the grid starts; first <= last <= range
    uint32_t last_ = 0;   // Where this part of the grid finishes;
    uint32_t range_ = 0;  // Overall grid range (goes from 0 to range)

    uint32_t range() const { return range_; };

    Iterator begin() { return { first_ }; }
    Iterator end() { return { last_ }; }
};


// HostComputeEvent
// We do everything synchronously (for now), so nothing much really going on here

struct HostComputeEvent: public ComputeEvent {
    virtual ~HostComputeEvent() = default;

    virtual std::shared_ptr<ComputeProfilingInfo> getProfilingInfo() const override
    {
        return std::make_shared<ComputeProfilingInfo>();
    }

    virtual void await() const override
    {
    }

    virtual std::shared_ptr<ComputeEvent> thenImpl(std::function<void ()> fn, const std::string & label) override
    {
        fn();
        return std::make_shared<HostComputeEvent>();
    }
};

namespace details {

using Pin = std::shared_ptr<const void>;

template<typename T>
auto getBestValueDescription(T *, std::enable_if_t<MLDB::has_default_description_v<T>> * = 0)
{
    return getDefaultDescriptionSharedT<T>();
}

std::shared_ptr<const ValueDescription> getValueDescriptionForType(const std::type_info & type);

template<typename T>
std::shared_ptr<const ValueDescription>
getBestValueDescription(T *, std::enable_if_t<!MLDB::has_default_description_v<T>> * = 0)
{
    auto result = getValueDescriptionForType(typeid(T));

    if (result) {
        return result;
    }

    if constexpr (std::is_enum_v<T>) {
        return std::make_shared<GenericEnumDescription>(getDefaultDescriptionSharedT<std::underlying_type_t<T>>(), type_name<T>(), &typeid(T));
    }
    else {
        return std::make_shared<GenericAtomDescription>(sizeof(T), alignof(T), type_name<T>(), &typeid(T));
    }

    throw MLDB::Exception("Couldn't find any kind of value description for type " + type_name<T>());
}

template<typename T>
auto getBestValueDescriptionT()
{
    return getBestValueDescription((T*)0);
}

template<typename T>
std::tuple<ComputeKernelType, std::function<Pin(const std::string & opName, MemoryArrayHandleT<T> & out, ComputeKernelArgument & in, ComputeQueue & queue)>>
marshalParameterForCpuKernelCall(MemoryArrayHandleT<T> *)
{
    ComputeKernelType result(getBestValueDescriptionT<T>(), "rw");
    result.dims.emplace_back();

    auto convertParam = [] (const std::string & opName, MemoryArrayHandleT<T> & out, ComputeKernelArgument & in, ComputeQueue & queue) -> Pin
    {
        if (in.handler->canGetHandle()) {
            auto handle = in.handler->getHandle(opName + " marshal", queue);
            out = {std::move(handle)};
            return nullptr;
        }
        throw MLDB::Exception("attempt to pass non-handle memory region to arg that needs a handle (not implemented)");
    };

    return { result, convertParam };
}

template<typename T>
std::tuple<ComputeKernelType, std::function<Pin(const std::string & opName, MemoryArrayHandleT<const T> & out, ComputeKernelArgument & in, ComputeQueue & queue)>>
marshalParameterForCpuKernelCall(MemoryArrayHandleT<const T> *)
{
    ComputeKernelType result(getBestValueDescriptionT<T>(), "r");
    result.dims.emplace_back();

    auto convertParam = [] (const std::string & opName, MemoryArrayHandleT<const T> & out, ComputeKernelArgument & in, ComputeQueue & queue) -> Pin
    {
        if (in.handler->canGetHandle()) {
            auto handle = in.handler->getHandle(opName + " marshal", queue);
            out = MemoryArrayHandleT<const T>(std::move(handle.handle));
            return nullptr;
        }
        throw MLDB::Exception("attempt to pass non-handle memory region to arg that needs a handle (not implemented)");
    };

    return { result, convertParam };
}

template<typename T>
std::tuple<ComputeKernelType, std::function<Pin(const std::string & opName, MutableMemoryRegionT<T> & out, ComputeKernelArgument & in, ComputeQueue & queue)>>
marshalParameterForCpuKernelCall(MutableMemoryRegionT<T> *)
{
    ComputeKernelType result(getBestValueDescriptionT<T>(), "rw");
    result.dims.emplace_back();

    auto convertParam = [] (const std::string & opName, MutableMemoryRegionT<T> & out, ComputeKernelArgument & in, ComputeQueue & queue) -> Pin
    {
        if (in.handler->canGetRange()) {
            auto [data, length, handle] = in.handler->getRange(opName + " marshal", queue);
            MutableMemoryRegion raw{ handle, (char *)data, length };
            out = std::move(raw);
            return nullptr;
        }
        throw MLDB::Exception("attempt to pass non-mutable range memory region to arg that needs a mutable range");
    };

    return { result, convertParam };
}

template<typename T>
std::tuple<ComputeKernelType, std::function<Pin(const std::string & opName, FrozenMemoryRegionT<T> & out, ComputeKernelArgument & in, ComputeQueue & queue)>>
marshalParameterForCpuKernelCall(FrozenMemoryRegionT<T> *)
{
    ComputeKernelType result(getBestValueDescriptionT<T>(), "r");
    result.dims.emplace_back();

    auto convertParam = [] (const std::string & opName, FrozenMemoryRegionT<T> & out, ComputeKernelArgument & in, ComputeQueue & queue) -> Pin
    {
        if (in.handler->canGetConstRange()) {
            auto [data, length, handle] = in.handler->getConstRange(opName + " marshal", queue);
            FrozenMemoryRegion raw{ handle, (const char *)data, length };
            out = std::move(raw);
            return nullptr;
        }
        throw MLDB::Exception("attempt to pass non-handle memory region to arg that needs a handle (not implemented)");
    };

    return { result, convertParam };
}

template<typename T>
std::tuple<ComputeKernelType, std::function<Pin (const std::string & opName, T * & out, ComputeKernelArgument & in, ComputeQueue & queue)>>
marshalParameterForCpuKernelCall(T **);

template<typename T>
std::tuple<ComputeKernelType, std::function<Pin (const std::string & opName, const T * & out, ComputeKernelArgument & in, ComputeQueue & queue)>>
marshalParameterForCpuKernelCall(const T **);

template<typename T>
std::tuple<ComputeKernelType, std::function<Pin (const std::string & opName, std::span<T> & out, ComputeKernelArgument & in, ComputeQueue & queue)>>
marshalParameterForCpuKernelCall(std::span<T> *)
{
    ComputeKernelType result(getBestValueDescriptionT<T>(), "rw");
    result.dims.emplace_back();

    auto convertParam = [] (const std::string & opName, std::span<T> & out, ComputeKernelArgument & in, ComputeQueue & queue) -> Pin
    {
        if (in.handler->canGetRange()) {
            auto [ptr, size, pin] = in.handler->getRange(opName + " marshal", queue);
            out = { reinterpret_cast<T *>(ptr), size / sizeof(T) };
            return std::move(pin);
        }
        throw MLDB::Exception("attempt to pass non-range memory region to arg that needs a span (not implemented)");
    };

    return { result, convertParam };   
}

template<typename T>
std::tuple<ComputeKernelType, std::function<Pin (const std::string & opName, std::span<const T> & out, ComputeKernelArgument & in, ComputeQueue & queue)>>
marshalParameterForCpuKernelCall(std::span<const T> *)
{
    ComputeKernelType result(getBestValueDescriptionT<T>(), "r");
    result.dims.emplace_back();

    auto convertParam = [] (const std::string & opName, std::span<const T> & out, ComputeKernelArgument & in, ComputeQueue & queue) -> Pin
    {
        if (in.handler->canGetConstRange()) {
            auto [ptr, size, pin] = in.handler->getConstRange(opName, queue);
            out = { reinterpret_cast<const T *>(ptr), size / sizeof(T) };
            return std::move(pin);
        }
        throw MLDB::Exception("attempt to pass non-range memory region to arg that needs a span (not implemented)");
    };

    return { result, convertParam };
}

// Implemented in .cc file to avoid including value_description.h
// Copies from to to via the value description
void copyUsingValueDescription(const ValueDescription * desc,
                               std::span<const std::byte> from, void * to,
                               const std::type_info & toType);

const std::type_info & getTypeFromValueDescription(const ValueDescription * desc);

template<typename T>
std::tuple<ComputeKernelType, std::function<Pin(const std::string & opName, T & out, ComputeKernelArgument & in, ComputeQueue & queue)>>
marshalParameterForCpuKernelCall(T *)
{
    ComputeKernelType result(getBestValueDescriptionT<std::remove_const_t<T>>(),
                             "r");

    auto convertParam = [] (const std::string & opName, T & out, ComputeKernelArgument & in, ComputeQueue & queue) -> Pin
    {
        ExcAssert(in.handler->canGetPrimitive());
        std::span<const std::byte> mem = in.handler->getPrimitive(opName, queue);
        copyUsingValueDescription(in.handler->type.baseType.get(), mem, &out, typeid(T));
        return nullptr;
    };

    return { result, convertParam };    
}

template<typename T>
std::tuple<ComputeKernelType, std::function<Pin (const std::string & opName, T & out, ComputeKernelArgument & in, ComputeQueue & queue)>>
marshalParameterForCpuKernelCall()
{
    return marshalParameterForCpuKernelCall((T *)nullptr);
}

} // namespace details

struct HostComputeKernel: public ComputeKernel {
    // This is called for each passed parameter, with T representing the type of the parameter
    // which was passed and arg its value.  The formal specification of the parameter is in
    // params[n].
    template<typename T>
    void extractParam(T & arg, ComputeKernelArgument param, size_t n, ComputeQueue & queue,
                      std::vector<details::Pin> & pins) const
    {
        //const ComputeKernel::ParameterInfo & formalArgument = params[n];
        //const ComputeKernelType & formalType = formalArgument.type;
        //const ComputeKernelType & inputType = param.abstractType;

        auto [outputType, marshal] = details::marshalParameterForCpuKernelCall<T>();

#if 0
        // Verify dimensionality
        int formalDims = formalType.dims();
        int inputDims = inputType.dims();
        int outputDims = outputType.dims();

        if (formalDims != inputDims || formalDims != outputDims) {
            throw AnnotatedException("attempt to pass parameter with incompatible number of dimensions");
        }

        bool isConst = params[n].isConst;
        bool argIsConst

#endif
        const std::type_info & requiredType
            = details::getTypeFromValueDescription(param.handler->type.baseType.get());

        std::string reason;
        if (!outputType.isCompatibleWith(param.handler->type, &reason)) {
            throw MLDB::Exception("Attempting to convert parameter from passed type " + type_name<T>()
                             + " to required type " + param.handler->type.print() + " passing parameter "
                             + std::to_string(n) + " ('" + params[n].name + "')  of kernel " + kernelName
                             + ": " + reason);
        }

        try {
            //using namespace std;
            //cerr << "setting parameter " << n << " named " << this->params[n].name << " from " << param.handler->info() << endl;
            //using namespace std;
            //cerr << "converting parameter " << n << " with formal type " << params[n].type.print()
            //     << " from type " << param.handler->type.print() << " to type " << type_name<T>(arg)
            //     << endl;
            auto pin = marshal("kernel " + this->kernelName + " bind param " + std::to_string(n) + " " + this->params[n].name,
                               arg, param, queue);
            if (pin) {
                pins.emplace_back(std::move(pin));
            }
        } MLDB_CATCH_ALL {
            rethrowException(500, "Attempting to convert parameter from passed type " + type_name<T>()
                             + " to required type " + demangle(requiredType.name()) + " passing parameter "
                             + std::to_string(n) + " ('" + params[n].name + "')  of kernel " + kernelName
                             + " with abstract type " + params[n].type.print(),
                             "abstractType", params[n].type);
        }
    }

    template<size_t N, typename... Args>
    void extractParams(std::tuple<Args...> & args, std::vector<ComputeKernelArgument> & params,
                       ComputeQueue & queue, std::vector<details::Pin> & pins) const
    {
        if constexpr (N < sizeof...(Args)) {
            this->extractParam(std::get<N>(args), params.at(N), N, queue, pins);
            this->extractParams<N + 1>(args, params, queue, pins);
        }
        else {
            // validate number of parameters
            if (N < params.size()) {
                throw AnnotatedException(500, "Error in calling compute function '" + kernelName + "': not enough parameters");
            }
            else if (N > params.size()) {
                throw AnnotatedException(500, "Error in calling compute function '" + kernelName + "': too many parameters");
            }
            // Make sure all formal parameters are set
        }
    }

    template<typename Fn, typename... InitialArgs, typename Tuple, std::size_t... I>
    static MLDB_ALWAYS_INLINE void apply_impl(Fn && fn, const Tuple & tupleArgs, std::integer_sequence<size_t, I...>,
                          InitialArgs&&... initialArgs)
    {
        fn(std::forward<InitialArgs>(initialArgs)..., std::get<I>(tupleArgs)...);
    }

    template<typename Fn, typename... InitialArgs, typename... TupleArgs>
    static MLDB_ALWAYS_INLINE void apply(Fn && fn, const std::tuple<TupleArgs...> & tupleArgs, InitialArgs&&... initialArgs)
    {
        return apply_impl(std::forward<Fn>(fn), tupleArgs, std::make_index_sequence<sizeof...(TupleArgs)>{},
                          std::forward<InitialArgs>(initialArgs)...);
    }

    void checkComputeFunctionArity(size_t numExtraComputeFunctionArgs) const
    {
        if (numExtraComputeFunctionArgs != params.size()) {

            throw AnnotatedException(500, "Error setting compute function for '" + kernelName
                                     + "': compute function needs " + std::to_string(numExtraComputeFunctionArgs)
                                     + " but there are " + std::to_string(params.size()) + " parameters listed");
        }
    }

    // Perform the abstract bind() operation, returning a BoundComputeKernel
    virtual BoundComputeKernel bindImpl(ComputeQueue & queue,
                                        std::vector<ComputeKernelArgument> arguments,
                                        ComputeKernelConstraintSolution knowns) const override;

    virtual void call(ComputeQueue & queue, const BoundComputeKernel & bound, std::span<ComputeKernelGridRange> grid) const;

    using Launch = std::function<void (ComputeQueue & queue, std::span<ComputeKernelGridRange> idx)>;

    using Bind = std::function<Launch (ComputeQueue & queue, std::vector<ComputeKernelArgument> & params)>;
    Bind bind;

    template<typename Fn>
    void setBind(Fn && bind)
    {
        this->bind = bind;
    }

    template<typename... Args>
    void setGridComputeFunction(void (*fn) (Args...))
    {
        checkComputeFunctionArity(sizeof...(Args));

        auto result = [this, fn] (ComputeQueue & queue, std::vector<ComputeKernelArgument> & params) -> Launch
        {
            ExcAssertEqual(params.size(), sizeof...(Args));
            std::tuple<Args...> args;
            std::vector<details::Pin> pins;
            this->extractParams<0>(args, params, queue, pins);
            return [fn, args, pins = std::move(pins)]
                (ComputeQueue & queue,
                 std::span<ComputeKernelGridRange> grid)
            {
                ExcAssertEqual(grid.size(), 0);
                HostComputeKernel::apply(fn, args);
            };
        };

        setBind(std::move(result));
    }

    template<typename... Args>
    void setComputeFunction(void (*fn) (ComputeContext & context, Args...))
    {
        checkComputeFunctionArity(sizeof...(Args));

        auto result = [this, fn] (ComputeQueue & queue, std::vector<ComputeKernelArgument> & params) -> Launch
        {
            ExcAssertEqual(params.size(), sizeof...(Args));
            std::tuple<Args...> args;
            std::vector<details::Pin> pins;
            this->extractParams<0>(args, params, queue, pins);
            return [fn, args, pins = std::move(pins)]
                (ComputeQueue & queue,
                 std::span<ComputeKernelGridRange> grid)
            {
                ExcAssertEqual(grid.size(), 0);
                HostComputeKernel::apply(fn, args, *queue.owner);
            };
        };

        setBind(result);
    }

    template<typename... Args>
    void set1DComputeFunction(void (*fn) (ComputeContext & context, uint32_t i1, uint32_t r1, Args...))
    {
        checkComputeFunctionArity(sizeof...(Args));
        
        auto result = [this, fn] (ComputeQueue & queue, std::vector<ComputeKernelArgument> & params) -> Launch
        {
            ExcAssertEqual(params.size(), sizeof...(Args));
            std::tuple<Args...> args;
            std::vector<details::Pin> pins;
            this->extractParams<0>(args, params, queue, pins);
            return [fn, args, pins = std::move(pins)]
                (ComputeQueue & queue,
                 std::span<ComputeKernelGridRange> grid)
            {
                ExcAssertEqual(grid.size(), 1);
                for (uint32_t idx: grid[0]) {
                    HostComputeKernel::apply(fn, args, *queue.owner, idx, grid[0].range());
                }
            };
        };

        setBind(result);
    }

    template<typename... Args>
    void set1DComputeFunction(void (*fn) (ComputeContext & context, ComputeKernelGridRange & r1, Args...))
    {
        checkComputeFunctionArity(sizeof...(Args));
        
        auto result = [this, fn] (ComputeQueue & queue, std::vector<ComputeKernelArgument> & params) -> Launch
        {
            ExcAssertEqual(params.size(), sizeof...(Args));
            std::tuple<Args...> args;
            std::vector<details::Pin> pins;
            this->extractParams<0>(args, params, queue, pins);
            return [fn, args, pins = std::move(pins)]
                (ComputeQueue & queue,
                 std::span<ComputeKernelGridRange> grid)
            {
                ExcAssertEqual(grid.size(), 1);
                HostComputeKernel::apply(fn, args, *queue.owner, grid[0]);
            };
        };

        setBind(result);
    }

    template<typename... Args>
    void set2DComputeFunction(void (*fn) (ComputeContext & context, uint32_t i1, uint32_t r1, uint32_t i2, uint32_t r2, Args...))
    {
        auto result = [this, fn] (ComputeQueue & queue, std::vector<ComputeKernelArgument> & params) -> Launch
        {
            ExcAssertEqual(params.size(), sizeof...(Args));
            std::tuple<Args...> args;
            std::vector<details::Pin> pins;
            this->extractParams<0>(args, params, queue, pins);
            return [fn, args, pins = std::move(pins)]
                (ComputeQueue & queue,
                 std::span<ComputeKernelGridRange> grid)
            {
                ExcAssertEqual(grid.size(), 2);
                for (uint32_t i0: grid[0]) {
                    for (uint32_t i1: grid[1]) {
                        HostComputeKernel::apply(fn, args, *queue.owner, i0, grid[0].range(), i1, grid[1].range());
                    }
                }
            };
        };

        setBind(result);
    }

    template<typename... Args>
    void set2DComputeFunction(void (*fn) (ComputeContext & context, uint32_t i1, uint32_t r1, ComputeKernelGridRange & r2, Args...))
    {
        auto result = [this, fn] (ComputeQueue & queue, std::vector<ComputeKernelArgument> & params) -> Launch
        {
            ExcAssertEqual(params.size(), sizeof...(Args));
            std::tuple<Args...> args;
            std::vector<details::Pin> pins;
            this->extractParams<0>(args, params, queue, pins);
            return [fn, args, pins = std::move(pins)] 
                (ComputeQueue & queue,
                 std::span<ComputeKernelGridRange> grid)
            {
                ExcAssertEqual(grid.size(), 2);
                for (uint32_t i0: grid[0]) {
                    HostComputeKernel::apply(fn, args, *queue.owner, i0, grid[0].range(), grid[1]);
                }
            };
        };

        setBind(result);
    }

    template<typename... Args>
    void set2DComputeFunction(void (*fn) (ComputeContext & context, ComputeKernelGridRange & r1, uint32_t i2, uint32_t r2, Args...))
    {
        auto result = [this, fn] (ComputeQueue & queue, std::vector<ComputeKernelArgument> & params) -> Launch
        {
            ExcAssertEqual(params.size(), sizeof...(Args));
            std::tuple<Args...> args;
            std::vector<details::Pin> pins;
            this->extractParams<0>(args, params, queue, pins);
            return [fn, args, pins = std::move(pins)]
                (ComputeQueue & queue,
                 std::span<ComputeKernelGridRange> grid)
            {
                ExcAssertEqual(grid.size(), 2);
                for (uint32_t i1: grid[1]) {
                    HostComputeKernel::apply(fn, args, *queue.owner, grid[0], i1, grid[1].range());
                }
            };
        };

        setBind(result);
    }

    template<typename... Args>
    void set3DComputeFunction(void (*fn) (ComputeContext & context, uint32_t i1, uint32_t r1, uint32_t i2, uint32_t r2, ComputeKernelGridRange & r3, Args...))
    {
        auto result = [this, fn] (ComputeQueue & queue,std::vector<ComputeKernelArgument> & params) -> Launch
        {
            ExcAssertEqual(params.size(), sizeof...(Args));
            std::tuple<Args...> args;
            std::vector<details::Pin> pins;
            this->extractParams<0>(args, params, queue, pins);
            return [fn, args, pins = std::move(pins)]
                (ComputeQueue & queue,
                 std::span<ComputeKernelGridRange> grid)
            {
                ExcAssertEqual(grid.size(), 3);
                for (uint32_t i0: grid[0]) {
                    for (uint32_t i1: grid[1]) {
                        HostComputeKernel::apply(fn, args, *queue.owner,
                                                 i0, grid[0].range(),
                                                 i1, grid[1].range(),
                                                grid[2]);
                    }
                }
            };
        };

        setBind(result);
    }

    template<typename... Args>
    void set3DComputeFunction(void (*fn) (ComputeContext & context, ComputeKernelGridRange & r1, uint32_t i2, uint32_t r2, uint32_t i3, uint32_t r3, Args...))
    {
        auto result = [this, fn] (ComputeQueue & queue, std::vector<ComputeKernelArgument> & params) -> Launch
        {
            ExcAssertEqual(params.size(), sizeof...(Args));
            std::tuple<Args...> args;
            std::vector<details::Pin> pins;
            this->extractParams<0>(args, params, queue, pins);
            return [fn, args, pins = std::move(pins)]
                (ComputeQueue & queue,
                 std::span<ComputeKernelGridRange> grid)
            {
               ExcAssertEqual(grid.size(), 3);
                for (uint32_t i1: grid[1]) {
                    for (uint32_t i2: grid[2]) {
                        HostComputeKernel::apply(fn, args, *queue.owner,
                                                 grid[0],
                                                 i1, grid[1].range(),
                                                 i2, grid[2].range());
                    }
                }
            };
        };

        setBind(result);
    }

};


// HostComputeQueue

struct HostComputeQueue: public ComputeQueue {
    HostComputeQueue(HostComputeContext * owner, HostComputeQueue * parent = nullptr);

    HostComputeContext * hostOwner = nullptr;

    virtual ~HostComputeQueue() = default;

    virtual std::shared_ptr<ComputeQueue> parallel(const std::string & opName) override;
    virtual std::shared_ptr<ComputeQueue> serial(const std::string & opName) override;

    virtual void
    enqueue(const std::string & opName,
            const BoundComputeKernel & kernel,
            const std::vector<uint32_t> & GenericEnumDescription) override;

    virtual void
    enqueueFillArrayImpl(const std::string & opName,
                         MemoryRegionHandle region, MemoryRegionInitialization init,
                         size_t startOffsetInBytes, ssize_t lengthInBytes,
                         std::span<const std::byte> block) override;

    virtual void
    enqueueCopyFromHostImpl(const std::string & opName,
                            MemoryRegionHandle toRegion,
                            FrozenMemoryRegion fromRegion,
                            size_t deviceStartOffsetInBytes) override;

    virtual void
    copyFromHostSyncImpl(const std::string & opName,
                                MemoryRegionHandle toRegion,
                                FrozenMemoryRegion fromRegion,
                                size_t deviceStartOffsetInBytes) override;

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

    virtual std::shared_ptr<ComputeEvent> flush() override;

    virtual void enqueueBarrier(const std::string & label) override;
    virtual void finish() override;
    virtual std::shared_ptr<ComputeEvent>
    makeAlreadyResolvedEvent(const std::string & label) const;
};

void registerHostComputeKernel(const std::string & kernelName,
                           std::function<std::shared_ptr<ComputeKernel>()> generator);

}  // namespace MLDB