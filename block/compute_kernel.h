/** compute_kernel.h                                                -*- C++ -*-
    Jeremy Barnes, 27 March 2016
    This file is part of MLDB. Copyright 2016 mldb.ai inc. All rights reserved.

    Basic primitives around memory regions.  Once frozen, these are the
    representation that covers CPU memory, device memory and remote
    memory and implements the primitives that allow data to be made
    available and brought to the compute resources required.
*/

#pragma once

#include "memory_region.h"
#include <string>
#include "mldb/types/annotated_exception.h"
#include "mldb/arch/demangle.h"
//#include "mldb/types/structure_description.h"
#include <any>
#include <iostream>

namespace MLDB {

struct ComputeDevice {
    static const ComputeDevice CPU;
};

struct ComputeKernelType {
    std::string str;
    std::string print() const { return str; }
};

//DECLARE_STRUCTURE_DESCRIPTION(ComputeKernelType);

std::shared_ptr<const ComputeKernelType>
parseType(const std::string & type);

struct ComputeContext;
struct BoundComputeKernel;

struct ComputeKernel {
    std::string kernelName;
    ComputeDevice device;

    struct ParameterInfo {
        std::string name;
        std::string access;
        std::shared_ptr<const ComputeKernelType> type;
    };

    std::vector<ParameterInfo> params;
    std::map<std::string, int> paramIndex;

    struct DimensionInfo {
        std::string name;
        std::string range;
    };

    std::vector<DimensionInfo> dims;

    using Callable = std::function<void (ComputeContext & context, std::span<const uint32_t> idx, std::span<const uint32_t> rng)>;

    void addParameter(const std::string & parameterName, const std::string & access, const std::string & typeStr)
    {
        if (!paramIndex.emplace(parameterName, params.size()).second) {
            throw AnnotatedException(500, "Duplicate kernel parameter name: '" + parameterName + "'");
        }
        params.push_back({parameterName, access, parseType(typeStr)});
    }

    void addDimension(const std::string & dimensionName, const std::string & range)
    {
        dims.push_back({dimensionName, range});
    }

    template<typename... NamesAndArgs>
    BoundComputeKernel bind(NamesAndArgs&&... namesAndArgs);

    // Filled in to map to the concrete representation
    std::function<Callable (std::vector<std::any> & params)> createCallable;

    template<typename T>
    void extractParam(T & arg, std::any param, size_t n) const
    {
        const std::type_info & requiredType = param.type();
        try {
            auto cast = std::any_cast<T>(std::move(param));
            arg = std::move(cast);
        } MLDB_CATCH_ALL {
            rethrowException(500, "Attempting to convert parameter from passed type " + type_name<T>()
                             + " to required type " + demangle(requiredType.name()) + " passing parameter "
                             + std::to_string(n) + " ('" + params[n].name + "')  of kernel " + kernelName
                             + " with abstract type " + params[n].type->print());
        }
    }

    template<size_t N, typename... Args>
    void extractParams(std::tuple<Args...> & args, std::vector<std::any> & params) const
    {
        if constexpr (N < sizeof...(Args)) {
            this->extractParam(std::get<N>(args), params.at(N), N);
            this->extractParams<N + 1>(args, params);
        }
    }

    template<typename Fn, typename... InitialArgs, typename Tuple, std::size_t... I>
    static void apply_impl(Fn && fn, const Tuple & tupleArgs, std::integer_sequence<size_t, I...>,
                          InitialArgs&&... initialArgs)
    {
        fn(std::forward<InitialArgs>(initialArgs)..., std::get<I>(tupleArgs)...);
    }

    template<typename Fn, typename... InitialArgs, typename... TupleArgs>
    static void apply(Fn && fn, const std::tuple<TupleArgs...> & tupleArgs, InitialArgs&&... initialArgs)
    {
        return apply_impl(std::forward<Fn>(fn), tupleArgs, std::make_index_sequence<sizeof...(TupleArgs)>{},
                          std::forward<InitialArgs>(initialArgs)...);
    }

    template<typename... Args>
    void setComputeFunction(void (*fn) (ComputeContext & context, Args...))
    {
        auto result = [this, fn] (std::vector<std::any> & params) -> Callable
        {
            ExcAssertEqual(params.size(), sizeof...(Args));
            std::tuple<Args...> args;
            this->extractParams<0>(args, params);
            return [fn, args] (ComputeContext & context, std::span<const uint32_t> idx, std::span<const uint32_t> rng)
            {
                ExcAssert(idx.empty());
                ExcAssert(rng.empty());
                ComputeKernel::apply(fn, args, context);
            };
        };

        createCallable = result;
    }

    template<typename... Args>
    void set1DComputeFunction(void (*fn) (ComputeContext & context, uint32_t i1, uint32_t r1, Args...))
    {
        auto result = [this, fn] (std::vector<std::any> & params) -> Callable
        {
            ExcAssertEqual(params.size(), sizeof...(Args));
            std::tuple<Args...> args;
            this->extractParams<0>(args, params);
            return [fn, args] (ComputeContext & context, std::span<const uint32_t> idx, std::span<const uint32_t> rng)
            {
                ComputeKernel::apply(fn, args, context, idx[0], rng[0]);
            };
        };

        createCallable = result;
    }
};

struct BoundComputeKernel {
    const ComputeKernel * owner = nullptr;
    std::vector<std::any> arguments;
    ComputeKernel::Callable call;

    void operator () (ComputeContext & context,
                      std::span<const uint32_t> idx, std::span<const uint32_t> rng) const
    {
        try {
            this->call(context, idx, rng);
            using namespace std;
            cerr << "completed calling " << owner->kernelName << endl;
        } MLDB_CATCH_ALL {
            rethrowException(500, "Error launching kernel " + owner->kernelName);
        }
    }
};

namespace details {
template<typename Arg>
void bindOne(const ComputeKernel * owner, std::vector<std::any> & arguments, const std::string & argName, Arg&& arg)
{
    ExcAssert(owner);

    auto argIndexIt = owner->paramIndex.find(argName);
    if (argIndexIt == owner->paramIndex.end())
        throw MLDB::Exception("Couldn't bind arg: argument " + argName
                                + " is not an argument of kernel " + owner->kernelName);

    size_t argIndex = argIndexIt->second;
    using namespace std;
    cerr << "binding " << argName << " at index " << argIndex << " to value of type " << type_name<Arg>() << endl;

    if (arguments.at(argIndex).has_value())
        throw MLDB::Exception("Attempt to double bind argument " + argName
                                + " of kernel " + owner->kernelName);

    arguments[argIndex] = std::move(arg);
}

inline void bind(const ComputeKernel * owner, std::vector<std::any> & arguments) // end of recursion
{
    for (size_t i = 0;  i < arguments.size();  ++i) {
        if (!arguments[i].has_value()) {
            throw MLDB::Exception("kernel " + owner->kernelName + " didn't set argument "
                                    + owner->params.at(i).name);
        }
    }
}

template<typename Arg, typename... Rest>
void bind(const ComputeKernel * owner, std::vector<std::any> & arguments, const std::string & argName, Arg&& arg, Rest&&... rest)
{
    details::bindOne(owner, arguments, argName, std::forward<Arg>(arg));
    details::bind(owner, arguments, std::forward<Rest>(rest)...);
}

} // namespace details

template<typename... NamesAndArgs>
BoundComputeKernel ComputeKernel::bind(NamesAndArgs&&... namesAndArgs)
{
    // These are bound to the values in NamesAndArgs
    std::vector<std::any> arguments(this->params.size());
    details::bind(this, arguments, std::forward<NamesAndArgs>(namesAndArgs)...);

    BoundComputeKernel result;
    result.owner = this;
    result.call = this->createCallable(arguments);

    return result;
}


struct ComputeProfilingInfo {

};

struct ComputeEvent {
    ComputeProfilingInfo getProfilingInfo() const;
};

struct ComputeQueue {
    ComputeQueue(ComputeContext * owner)
        : owner(owner)
    {
    }

    ComputeContext * owner = nullptr;

    std::shared_ptr<ComputeEvent> launch(const BoundComputeKernel & kernel,
                                         const std::vector<uint32_t> & grid,
                                         const std::vector<std::shared_ptr<ComputeEvent>> & prereqs = {})
    {
        ExcAssertEqual(kernel.owner->dims.size(), grid.size());
        if (grid.size() == 0) {
            kernel(*owner, {}, {});
        }
        else if (grid.size() == 1) {
            for (uint32_t i = 0;  i < grid[0];  ++i) {
                std::array<uint32_t, 2> dims = {i};
                kernel(*owner, dims, grid);
            }
        }
        else if (grid.size() == 2) {
            for (uint32_t i = 0;  i < grid[0];  ++i) {
                for (uint32_t j = 0;  j < grid[1];  ++j) {
                    std::array<uint32_t, 2> dims = {i,j};
                    kernel(*owner, dims, grid);
                }
            }
        }
        else if (grid.size() == 3) {
            for (uint32_t i = 0;  i < grid[0];  ++i) {
                for (uint32_t j = 0;  j < grid[1];  ++j) {
                    for (uint32_t k = 0;  k < grid[2];  ++k) {
                        std::array<uint32_t, 3> dims = {i,j,k};
                        kernel(*owner, dims, grid);
                    }
                }
            }
        }
        else {
            throw MLDB::Exception("Kernels can be launched from 0 to 3 dimensions");
        }

        return std::shared_ptr<ComputeEvent>();
    }

    template<typename T>
    std::shared_ptr<ComputeEvent>
    enqueueFillArray(const MemoryArrayHandleT<T> & region, const T & val,
                     size_t start = 0, ssize_t length = -1)
    {
        return std::make_shared<ComputeEvent>();
    }

    void flush()
    {
    }
};

struct ComputeContext {

    ComputeContext()
        : backingStore(new MemorySerializer())
    {
    }

    std::shared_ptr<MappedSerializer> backingStore;

    struct MemoryRegionInfo {
        const void * data = nullptr;
        size_t lengthInBytes = 0;
        const std::type_info * type = nullptr;
        bool isConst = true;
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
    auto transferToCpu(MemoryArrayHandleT<T> array)
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
    auto transferToCpuSync(MemoryArrayHandleT<T> array) -> FrozenMemoryRegionT<std::remove_const_t<T>>
    {
        auto [result, ignore1] = transferToCpu(std::move(array));
        return result;
    }

    /** Transfers the array to the CPU so that it can be written from the CPU... but
     *  promises that the initial contents will never be read, which enables us to
     *  not copy any data to the CPU. */
    template<typename T>
    auto transferToCpuUninitialized(MemoryArrayHandleT<T> array) -> MutableMemoryRegionT<T>
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
        auto mem = backingStore->allocateWritableT<T>(size);
        auto result = std::make_shared<MemoryRegionInfo>();
        result->init(std::move(mem));
        return { { std::move(result) } };
    }

    template<typename T>
    auto allocZeroInitializedArray(size_t size) -> MemoryArrayHandleT<T>
    {
        auto mem = backingStore->allocateZeroFilledWritableT<T>(size);
        auto result = std::make_shared<MemoryRegionInfo>();
        result->init(std::move(mem));
        return { { std::move(result) } };
    }

    auto getKernel(const std::string & kernelName, ComputeDevice device) -> std::shared_ptr<ComputeKernel>;

    template<typename T>
    auto manageMemoryRegion(const std::vector<T> & obj) -> MemoryArrayHandleT<T>
    {
        return MemoryArrayHandleT<T>();
    }

    template<typename T, size_t N>
    auto manageMemoryRegion(const std::span<const T, N> & obj) -> MemoryArrayHandleT<T>
    {
        return MemoryArrayHandleT<T>();
    }

    template<typename T>
    auto enqueueFillArray(const MemoryArrayHandleT<T> & t, const T & fillWith, size_t offset = 0,
                          ssize_t length = -1) -> std::shared_ptr<ComputeEvent>
    {
        return nullptr;
    }
};

void registerComputeKernel(const std::string & kernelName,
                           std::function<std::shared_ptr<ComputeKernel>(ComputeDevice device)> generator);

} // namespace MLDB
