/** compute_kernel_host.h                                                -*- C++ -*-
    Jeremy Barnes, 27 March 2016
    This file is part of MLDB. Copyright 2016 mldb.ai inc. All rights reserved.

    Compute kernel runtime for CPU devices.
*/

#pragma once

#include "compute_kernel.h"

namespace MLDB {

namespace details {

using Pin = std::shared_ptr<const void>;

template<typename T>
std::tuple<ComputeKernelType, std::function<Pin(MemoryArrayHandleT<T> & out, ComputeKernelArgument & in, ComputeContext & context)>>
marshalParameterForCpuKernelCall(MemoryArrayHandleT<T> *)
{
    ComputeKernelType result(type_name<T>(),
                             getDefaultDescriptionSharedT<T>());
    result.access = "rw";

    auto convertParam = [] (MemoryArrayHandleT<T> & out, ComputeKernelArgument & in, ComputeContext & context) -> Pin
    {
        if (in.getHandle) {
            auto handle = in.getHandle(in.value, context);
            out = {std::move(handle)};
            return nullptr;
        }
        throw MLDB::Exception("attempt to pass non-handle memory region to arg that needs a handle (not implemented)");
    };

    return { result, convertParam };
}

template<typename T>
std::tuple<ComputeKernelType, std::function<Pin(MemoryArrayHandleT<const T> & out, ComputeKernelArgument & in, ComputeContext & context)>>
marshalParameterForCpuKernelCall(MemoryArrayHandleT<const T> *)
{
    ComputeKernelType result(type_name<T>(),
                             getDefaultDescriptionSharedT<T>());
    result.access = "r";

    auto convertParam = [] (MemoryArrayHandleT<const T> & out, ComputeKernelArgument & in, ComputeContext & context) -> Pin
    {
        if (in.getHandle) {
            auto handle = in.getHandle(in.value, context);
            out = MemoryArrayHandleT<const T>(std::move(handle.handle));
            return nullptr;
        }
        throw MLDB::Exception("attempt to pass non-handle memory region to arg that needs a handle (not implemented)");
    };

    return { result, convertParam };
}

template<typename T>
std::tuple<ComputeKernelType, std::function<Pin (T * & out, ComputeKernelArgument & in, ComputeContext & context)>>
marshalParameterForCpuKernelCall(T **);

template<typename T>
std::tuple<ComputeKernelType, std::function<Pin (const T * & out, ComputeKernelArgument & in, ComputeContext & context)>>
marshalParameterForCpuKernelCall(const T **);

template<typename T>
std::tuple<ComputeKernelType, std::function<Pin (std::span<T> & out, ComputeKernelArgument & in, ComputeContext & context)>>
marshalParameterForCpuKernelCall(std::span<T> *)
{
     ComputeKernelType result(type_name<T>(),
                              getDefaultDescriptionSharedT<T>());
    result.access = "rw";

    auto convertParam = [] (std::span<T> & out, ComputeKernelArgument & in, ComputeContext & context) -> Pin
    {
        if (in.getRange) {
            auto [ptr, size, pin] = in.getRange(in.value, context);
            out = { reinterpret_cast<T *>(ptr), size };
            return std::move(pin);
        }
        throw MLDB::Exception("attempt to pass non-range memory region to arg that needs a span (not implemented)");
    };

    return { result, convertParam };   
}

template<typename T>
std::tuple<ComputeKernelType, std::function<Pin (std::span<const T> & out, ComputeKernelArgument & in, ComputeContext & context)>>
marshalParameterForCpuKernelCall(std::span<const T> *)
{
    ComputeKernelType result(type_name<T>(),
                             getDefaultDescriptionSharedT<T>());
    result.access = "r";

    auto convertParam = [] (std::span<const T> & out, ComputeKernelArgument & in, ComputeContext & context) -> Pin
    {
        if (in.getConstRange) {
            auto [ptr, size, pin] = in.getConstRange(in.value, context);
            out = { reinterpret_cast<const T *>(ptr), size };
            return std::move(pin);
        }
        throw MLDB::Exception("attempt to pass non-range memory region to arg that needs a span (not implemented)");
    };

    return { result, convertParam };
}

template<typename T>
std::tuple<ComputeKernelType, std::function<Pin(T & out, ComputeKernelArgument & in, ComputeContext & context)>>
marshalParameterForCpuKernelCall(T *)
{
    ComputeKernelType result(type_name<T>(),
                             getDefaultDescriptionSharedT<std::remove_const_t<T>>());

    if constexpr (std::is_const_v<T>)
        result.access = "r";
    else
        result.access = "rw";

    auto convertParam = [] (T & out, ComputeKernelArgument & in, ComputeContext & context) -> Pin
    {
        out = std::forward<T>(std::any_cast<T>(in.value));
        return nullptr;
    };

    return { result, convertParam };    
}

template<typename T>
std::tuple<ComputeKernelType, std::function<Pin (T & out, ComputeKernelArgument & in, ComputeContext & context)>>
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
    void extractParam(T & arg, ComputeKernelArgument param, size_t n, ComputeContext & context,
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
        const std::type_info & requiredType = param.type();

        try {
            //using namespace std;
            //cerr << "converting parameter " << n << " with formal type " << params[n].type.print()
            //     << " from type " << demangle(param.type().name()) << " to type " << type_name<T>(arg)
            //     << endl;
            auto pin = marshal(arg, param, context);
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
                       ComputeContext & context, std::vector<details::Pin> & pins) const
    {
        if constexpr (N < sizeof...(Args)) {
            this->extractParam(std::get<N>(args), params.at(N), N, context, pins);
            this->extractParams<N + 1>(args, params, context, pins);
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

    template<typename... Args>
    void setComputeFunction(void (*fn) (ComputeContext & context, Args...))
    {
        checkComputeFunctionArity(sizeof...(Args));

        auto result = [this, fn] (ComputeContext & context, std::vector<ComputeKernelArgument> & params) -> Callable
        {
            ExcAssertEqual(params.size(), sizeof...(Args));
            std::tuple<Args...> args;
            std::vector<details::Pin> pins;
            this->extractParams<0>(args, params, context, pins);
            return [fn, args, pins = std::move(pins)] (ComputeContext & context, std::span<ComputeKernelGridRange> grid)
            {
                ExcAssertEqual(grid.size(), 0);
                HostComputeKernel::apply(fn, args, context);
            };
        };

        createCallable = result;
    }

    template<typename... Args>
    void set1DComputeFunction(void (*fn) (ComputeContext & context, uint32_t i1, uint32_t r1, Args...))
    {
        checkComputeFunctionArity(sizeof...(Args));
        
        auto result = [this, fn] (ComputeContext & context, std::vector<ComputeKernelArgument> & params) -> Callable
        {
            ExcAssertEqual(params.size(), sizeof...(Args));
            std::tuple<Args...> args;
            std::vector<details::Pin> pins;
            this->extractParams<0>(args, params, context, pins);
            return [fn, args, pins = std::move(pins)] (ComputeContext & context, std::span<ComputeKernelGridRange> grid)
            {
                ExcAssertEqual(grid.size(), 1);
                for (uint32_t idx: grid[0]) {
                    HostComputeKernel::apply(fn, args, context, idx, grid[0].range());
                }
            };
        };

        createCallable = result;
    }

    template<typename... Args>
    void set1DComputeFunction(void (*fn) (ComputeContext & context, ComputeKernelGridRange & r1, Args...))
    {
        checkComputeFunctionArity(sizeof...(Args));
        
        auto result = [this, fn] (ComputeContext & context, std::vector<ComputeKernelArgument> & params) -> Callable
        {
            ExcAssertEqual(params.size(), sizeof...(Args));
            std::tuple<Args...> args;
            std::vector<details::Pin> pins;
            this->extractParams<0>(args, params, context, pins);
            return [fn, args, pins = std::move(pins)] (ComputeContext & context, std::span<ComputeKernelGridRange> grid)
            {
                ExcAssertEqual(grid.size(), 1);
                HostComputeKernel::apply(fn, args, context, grid[0]);
            };
        };

        createCallable = result;
    }

    template<typename... Args>
    void set2DComputeFunction(void (*fn) (ComputeContext & context, uint32_t i1, uint32_t r1, uint32_t i2, uint32_t r2, Args...))
    {
        auto result = [this, fn] (ComputeContext & context, std::vector<ComputeKernelArgument> & params) -> Callable
        {
            ExcAssertEqual(params.size(), sizeof...(Args));
            std::tuple<Args...> args;
            std::vector<details::Pin> pins;
            this->extractParams<0>(args, params, context, pins);
            return [fn, args, pins = std::move(pins)] (ComputeContext & context, std::span<ComputeKernelGridRange> grid)
            {
                ExcAssertEqual(grid.size(), 2);
                for (uint32_t i0: grid[0]) {
                    for (uint32_t i1: grid[1]) {
                        HostComputeKernel::apply(fn, args, context, i0, grid[0].range(), i1, grid[1].range());
                    }
                }
            };
        };

        createCallable = result;
    }

    template<typename... Args>
    void set2DComputeFunction(void (*fn) (ComputeContext & context, uint32_t i1, uint32_t r1, ComputeKernelGridRange & r2, Args...))
    {
        auto result = [this, fn] (ComputeContext & context, std::vector<ComputeKernelArgument> & params) -> Callable
        {
            ExcAssertEqual(params.size(), sizeof...(Args));
            std::tuple<Args...> args;
            std::vector<details::Pin> pins;
            this->extractParams<0>(args, params, context, pins);
            return [fn, args, pins = std::move(pins)] (ComputeContext & context, std::span<ComputeKernelGridRange> grid)
            {
                ExcAssertEqual(grid.size(), 2);
                for (uint32_t i0: grid[0]) {
                    HostComputeKernel::apply(fn, args, context, i0, grid[0].range(), grid[1]);
                }
            };
        };

        createCallable = result;
    }

};

void registerHostComputeKernel(const std::string & kernelName,
                           std::function<std::shared_ptr<ComputeKernel>()> generator);

}  // namespace MLDB