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
#include "mldb/utils/command_expression.h"
#include <any>
#include <iostream>

namespace MLDB {

struct ComputeDevice {
    static const ComputeDevice CPU;
};

struct ComputeKernelDimension {
    std::shared_ptr<CommandExpression> bound; // or nullptr if no bounds
};

DECLARE_STRUCTURE_DESCRIPTION(ComputeKernelDimension);

struct ComputeKernelType {
    ComputeKernelType() = default;

    ComputeKernelType(std::string str,
                      std::shared_ptr<const ValueDescription> baseType)
        : str(std::move(str)), baseType(std::move(baseType))
    {
    }

    std::string str;
    std::shared_ptr<const ValueDescription> baseType;
    std::string access;

    std::string print() const
    {
        auto result = str;
        for (auto [bound]: dims) {
            result += "[" + bound->surfaceForm + "]";
        }
        return result;
    }
    
    std::vector<ComputeKernelDimension> dims;  // if empty, scalar, otherwise n-dimensional array
};

DECLARE_STRUCTURE_DESCRIPTION(ComputeKernelType);

ComputeKernelType parseType(const std::string & type);

struct ComputeContext;
struct BoundComputeKernel;

using GetRange = std::function<std::tuple<void *, size_t, std::shared_ptr<const void>>(const std::any & val, ComputeContext & context)>;
using GetConstRange = std::function<std::tuple<const void *, size_t, std::shared_ptr<const void>>(const std::any & val, ComputeContext & context)>;
using GetHandle = std::function<MemoryRegionHandle (const std::any & val, ComputeContext & context)>;

struct ComputeKernelArgument {
    std::any value;
    ComputeKernelType abstractType;
    GetRange getRange;
    GetConstRange getConstRange;
    GetHandle getHandle;

    const std::type_info & type() const { return value.type(); }
    bool has_value() const { return value.has_value(); }
};

namespace details {

// Forward definition as ComputeContext is defined later on
template<typename T>
FrozenMemoryRegionT<std::remove_const_t<T>>
transferToCpuSync(ComputeContext & context, const MemoryArrayHandleT<T> & handle);

template<typename T>
MutableMemoryRegionT<T>
transferToCpuMutableSync(ComputeContext & context, const MemoryArrayHandleT<T> & handle);

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

struct ComputeKernel {
    std::string kernelName;
    ComputeDevice device;
    ComputeContext * context = nullptr;

    struct ParameterInfo {
        std::string name;
        std::string access;
        ComputeKernelType type;
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

    // This function extracts the latest values of the bound parameters and sets them to
    // be marshaled by the calling infrastructure.  It should be called once for each time
    // that the callable is used (in other words, the callable should be called only once).
    std::function<Callable (ComputeContext & context, std::vector<ComputeKernelArgument> & params)> createCallable;

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
            return [fn, args, pins = std::move(pins)] (ComputeContext & context, std::span<const uint32_t> idx, std::span<const uint32_t> rng)
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
        checkComputeFunctionArity(sizeof...(Args));
        
        auto result = [this, fn] (ComputeContext & context, std::vector<ComputeKernelArgument> & params) -> Callable
        {
            ExcAssertEqual(params.size(), sizeof...(Args));
            std::tuple<Args...> args;
            std::vector<details::Pin> pins;
            this->extractParams<0>(args, params, context, pins);
            return [fn, args, pins = std::move(pins)] (ComputeContext & context, std::span<const uint32_t> idx, std::span<const uint32_t> rng)
            {
                ComputeKernel::apply(fn, args, context, idx[0], rng[0]);
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
            return [fn, args, pins = std::move(pins)] (ComputeContext & context, std::span<const uint32_t> idx, std::span<const uint32_t> rng)
            {
                ComputeKernel::apply(fn, args, context, idx[0], rng[0], idx[1], rng[1]);
            };
        };

        createCallable = result;
    }
};

struct BoundComputeKernel {
    const ComputeKernel * owner = nullptr;
    std::vector<ComputeKernelArgument> arguments;
    ComputeKernel::Callable call;

    void operator () (ComputeContext & context,
                      std::span<const uint32_t> idx, std::span<const uint32_t> rng) const
    {
        try {
            this->call(context, idx, rng);
        } MLDB_CATCH_ALL {
            rethrowException(500, "Error launching kernel " + owner->kernelName);
        }
    }
};

namespace details {

template<typename T>
std::tuple<ComputeKernelType, GetRange, GetConstRange, GetHandle>
getAbstractType(MemoryArrayHandleT<T> *)
{
    ComputeKernelType result(type_name<MemoryArrayHandleT<T>>(),
                             getDefaultDescriptionSharedT<T>());
    result.dims.push_back({nullptr});
    result.access = "rw";

    GetRange getRange = [] (const std::any & val, ComputeContext & context) -> std::tuple<void *, size_t, std::shared_ptr<const void>>
    {
        const auto & handle = std::any_cast<const MemoryArrayHandleT<T> &>(val);
        MutableMemoryRegionT<T> region = transferToCpuMutableSync(context, handle);
        return { region.data(), region.length(), region.raw().handle() };
    };

    GetConstRange getConstRange = [] (const std::any & val, ComputeContext & context) -> std::tuple<const void *, size_t, std::shared_ptr<const void>>
    {
        const auto & handle = std::any_cast<const MemoryArrayHandleT<T> &>(val);
        ExcAssert(handle.handle);
        FrozenMemoryRegionT<T> region = transferToCpuSync(context, handle);
        return { region.data(), region.length(), region.raw().handle() };
    };

    GetHandle getHandle = [] (const std::any & val, ComputeContext & context) -> MemoryRegionHandle
    {
        const auto & handle = std::any_cast<const MemoryArrayHandleT<T> &>(val);
        return handle;
    };

    return { result, getRange, getConstRange, getHandle };
}

template<typename T>
std::tuple<ComputeKernelType, GetRange, GetConstRange, GetHandle>
getAbstractType(MemoryArrayHandleT<const T> *)
{
    ComputeKernelType result(type_name<MemoryArrayHandleT<T>>(),
                             getDefaultDescriptionSharedT<T>());
    result.dims.push_back({nullptr});
    result.access = "r";

    GetConstRange getConstRange = [] (const std::any & val, ComputeContext & context) -> std::tuple<const void *, size_t, std::shared_ptr<const void>>
    {
        const auto & handle = std::any_cast<const MemoryArrayHandleT<const T> &>(val);
        FrozenMemoryRegionT<T> region = transferToCpuSync(context, handle);
        return { region.data(), region.length(), region.raw().handle() };
    };

    GetHandle getHandle = [] (const std::any & val, ComputeContext & context) -> MemoryRegionHandle
    {
        const auto & handle = std::any_cast<const MemoryArrayHandleT<const T> &>(val);
        return handle;
    };

    return { result, nullptr /* getRange */, getConstRange, getHandle };
}

template<typename T>
std::tuple<ComputeKernelType, GetRange, GetConstRange, GetHandle>
getAbstractType(T *)
{
    ComputeKernelType result(type_name<T>(),
                             getDefaultDescriptionSharedT<std::remove_const_t<T>>());

    if constexpr (std::is_const_v<T>)
        result.access = "r";
    else
        result.access = "rw";

    return { result, nullptr, nullptr, nullptr };
}

template<typename T>
std::tuple<ComputeKernelType, GetRange, GetConstRange, GetHandle>
getAbstractType()
{
    return getAbstractType((T *)nullptr);
}

template<typename Arg>
void bindOne(const ComputeKernel * owner, std::vector<ComputeKernelArgument> & arguments, const std::string & argName, Arg&& arg)
{
    ExcAssert(owner);

    auto argIndexIt = owner->paramIndex.find(argName);
    if (argIndexIt == owner->paramIndex.end())
        throw MLDB::Exception("Couldn't bind arg: argument " + argName
                                + " is not an argument of kernel " + owner->kernelName);

    size_t argIndex = argIndexIt->second;
    //using namespace std;
    //cerr << "binding " << argName << " at index " << argIndex << " to value of type " << type_name<Arg>() << endl;

    if (arguments.at(argIndex).has_value())
        throw MLDB::Exception("Attempt to double bind argument " + argName
                                + " of kernel " + owner->kernelName);

    auto [type, getRange, getConstRange, getHandle] = getAbstractType<std::remove_reference_t<Arg>>();
    arguments[argIndex] = { std::forward<Arg>(arg), type, getRange, getConstRange, getHandle };
}

inline void bind(const ComputeKernel * owner, std::vector<ComputeKernelArgument> & arguments) // end of recursion
{
    for (size_t i = 0;  i < arguments.size();  ++i) {
        if (!arguments[i].has_value()) {
            throw MLDB::Exception("kernel " + owner->kernelName + " didn't set argument "
                                    + owner->params.at(i).name);
        }
    }
}

template<typename Arg, typename... Rest>
void bind(const ComputeKernel * owner, std::vector<ComputeKernelArgument> & arguments, const std::string & argName, Arg&& arg, Rest&&... rest)
{
    details::bindOne(owner, arguments, argName, std::forward<Arg>(arg));
    details::bind(owner, arguments, std::forward<Rest>(rest)...);
}

} // namespace details

template<typename... NamesAndArgs>
BoundComputeKernel ComputeKernel::bind(NamesAndArgs&&... namesAndArgs)
{
    // These are bound to the values in NamesAndArgs
    std::vector<ComputeKernelArgument> arguments(this->params.size());
    details::bind(this, arguments, std::forward<NamesAndArgs>(namesAndArgs)...);

    BoundComputeKernel result;
    result.owner = this;
    result.call = this->createCallable(*context, arguments);

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
        auto mapped = details::transferToCpuMutableSync(*owner, region);
        if (start > mapped.length()) {
            throw MLDB::Exception("enqueueFillArray: array start index out of bounds");
        }
        if (length == -1)
            length = mapped.length() - start;
        if (start + length > mapped.length()) {
            throw MLDB::Exception("enqueueFillArray: array end index out of bounds");
        }

        std::fill_n(mapped.data() + start, length, val);

        return std::shared_ptr<ComputeEvent>();
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

    struct MemoryRegionInfo: public MemoryRegionHandleInfo {
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
    auto transferToCpuMutable(MemoryArrayHandleT<T> array)
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
    auto transferToCpuSync(MemoryArrayHandleT<T> array) -> FrozenMemoryRegionT<std::remove_const_t<T>>
    {
        auto [result, ignore1] = transferToCpu(std::move(array));
        return result;
    }

    template<typename T>
    auto transferToCpuMutableSync(MemoryArrayHandleT<T> array) -> MutableMemoryRegionT<T>
    {
        static_assert(!std::is_const_v<T>, "mutable transfer requires non-const type");
        auto [result, ignore1] = transferToCpuMutable(std::move(array));
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
};

namespace details {
template<typename T>
FrozenMemoryRegionT<std::remove_const_t<T>>
transferToCpuSync(ComputeContext & context, const MemoryArrayHandleT<T> & handle)
{
    return context.transferToCpuSync(handle);
}

template<typename T>
MutableMemoryRegionT<T>
transferToCpuMutableSync(ComputeContext & context, const MemoryArrayHandleT<T> & handle)
{
    return context.transferToCpuMutableSync(handle);
}

} // namespace details

void registerComputeKernel(const std::string & kernelName,
                           std::function<std::shared_ptr<ComputeKernel>(ComputeDevice device)> generator);

} // namespace MLDB
