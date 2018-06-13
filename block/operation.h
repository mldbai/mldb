/** operation.h                                                    -*- C++ -*-
    Jeremy Barnes, 4 May 2018
    Copyright (c) 2018 Element AI Inc.  All rights reserved.
    This file is part of MLDB. Copyright 2018 mldb.ai inc. All rights reserved.

    Definition of operations for data processing.
*/

#pragma once

#include "memory_region.h"
#include <memory>
#include "mldb/types/string.h"
#include "mldb/types/value_description.h"
#include "mldb/http/http_exception.h"
#include <mutex>
#include <iostream>


namespace MLDB {

struct Scalar;
struct Method;
struct Operation;
struct DeviceOperation;
struct Variable;

void recordOperation(const Operation & op);
void recordVariable(const Variable & var);


/*****************************************************************************/
/* DEVICE TYPE                                                               */
/*****************************************************************************/

enum DeviceType {
    DEVICE_CPU,
    DEVICE_GPU
};


/*****************************************************************************/
/* DEVICE API                                                                */
/*****************************************************************************/

enum DeviceApi {
    API_NATIVE,   ///< Device speaks native code
    API_OPENCL,   ///< Device speaks OpenCL
    API_PTX,      ///< Device speaks PTX (Nvidia)
    API_GCN       ///< Device speaks GCN (AMD)
};


/*****************************************************************************/
/* ABSTRACT OPERATION                                                        */
/*****************************************************************************/

/** An abstract operation is some kind of function that performs some kind
    of computation, transformation, action or side-effect.

    It has four types of parameters:
    - Type parameters, which are bound to abstract types, and eventually to
      a concrete type.  These can include things like sizes.
    - Value parameters, which are bound to abstract values, then a
      concrete value layout/type and eventually to an actual value in memory
    - Device parameters, which are originally bound to abstract devices,
      then to concrete device types, and finally to an actual device;
    - Operation parameters (for higher level operations), which are eventually
      bound to real operations.

    There can be multiple abstract operations with the same name; these all
    represent an operation family which allows multiple choices in how a
    given operation is performed.  These will all have constraints associated
    with them; for example, a low precision accumulate type will lead to a
    loss in accuracy, and so may only be used for when the matrix sizes are
    low.

    Each operation has a complexity associated with it, which broadly measures
    calculation complexity, memory bandwidth complexity to main memory, etc.
    
    Examples:

    - decodeJpeg: Block b -> FrameBufferImage i[int w, int h, fmt="rgba", tp=uint8]
      - Type parameters: b's type, i's type, int w, int h 
      - Value parameters: b's value
      - Device parameters: device on which it runs (CPU only)
      - Operation parameters: none

    - matMult x, y -> z:
               Tensor x{tp: t1, dims=uint[w,i]},
               Tensor y{tp: t2, dims=uint[i,h]},
               Tensor z{tp: t3, dims=uint[w,h]},
               Type multtp = preferred_mult_type(t1, t2),
               Type accumtp = preferred_accum_type(t1, t2),
               Type t3 = accumtp
               
      - Type parameters
         - t1, t2, t3, multtp, accumtp: numeric types, for input
           matrices, output matrix, and for the multiply and accumulate
           operations
         - w, i, h: dimensions of matrices
      - Value parameters: x, y: input tensors (must be matrices due to
        constraints on dims)
      - Device parameters: device on which it runs (GPU or CPU)
      - Operation parameters: none

*/

struct AbstractOperation {
    Utf8String name;

    struct Argument {
        Utf8String name;
        Utf8String type;   // Argument type, either ADT or param
        std::vector<Argument> typeArgs;  // Arguments to the type
    };

    std::vector<Argument> types;
    std::vector<Argument> devices;
    std::vector<Argument> args;
};


/*****************************************************************************/
/* DEVICE                                                                    */
/*****************************************************************************/

struct Device {

    // Return the host device, on which operations are guaranteed to be
    // runnable.
    static Device getHost()
    {
        return Device();
    }
};

struct OperationContext;
struct Function;
struct AbstractDataType;

template<typename T>
std::shared_ptr<const AbstractDataType>
doGetType()
{
    return T::getType();
}
        
template<typename... Args>
std::vector<std::shared_ptr<const AbstractDataType> >
doGetTypes()
{
    return { doGetType<Args>()... };
}

struct Method {
    Method(Utf8String name,
           std::shared_ptr<const AbstractDataType> resultType,
           std::vector<std::shared_ptr<const AbstractDataType> > argTypes)
        : name(std::move(name)),
          resultType(std::move(resultType)),
          argTypes(std::move(argTypes))
    {
    }

    Utf8String name;
    std::shared_ptr<const AbstractDataType> resultType;
    std::vector<std::shared_ptr<const AbstractDataType> > argTypes;
};

template<typename Fn>
struct MethodT;

template<typename Return, typename... Args>
struct MethodT<Return (Args...)>
    : public Method {
    MethodT(Utf8String name)
        : Method(std::move(name),
                 doGetType<Return>(),
                 doGetTypes<Args...>())
    {
    }
    
    Return call(Args&&... args) const
    {
        // TODO: immediate mode
        return Return("result");
    }
};


/*****************************************************************************/
/* ABSTRACT DATA TYPE                                                        */
/*****************************************************************************/

struct AbstractDataType {

    AbstractDataType(Utf8String typeName)
        : typeName(std::move(typeName))
    {
    }
    
    Utf8String typeName;
    
    virtual ~AbstractDataType()
    {
    }
    
    virtual Utf8String getTypeName() const
    {
        return typeName;
    }

    virtual Utf8String getImplementationName() const
    {
        return typeName;
    }
    
    virtual std::shared_ptr<const Method>
    getMethod(const Utf8String & methodName) const
    {
        auto it = methods.find(methodName);
        if (it == methods.end()) {
            throw HttpReturnException(400, "Method '" + methodName
                                      + "' of type '" + typeName
                                      + "' not found");
        }

        return it->second;
    }

    template<typename Fn>
    std::shared_ptr<const MethodT<Fn> >
    getMethodT(const Utf8String & methodName) const
    {
        return doGetMethodT(methodName, (Fn *)0);
    }

    template<typename Return, typename... Args>
    std::shared_ptr<const MethodT<Return (Args...)> >
    doGetMethodT(const Utf8String & methodName,
                 Return (*fn) (Args...)) const
    {
        auto method = getMethod(methodName);
        // For each argument, convert it if necessary
        // ...

        // Convert the result
        // ...

        return std::make_shared<MethodT<Return (Args...)> >(methodName);
    }

    void addMethod(const Utf8String & methodName,
                   std::shared_ptr<const Method> method)
    {
        if (!methods.emplace(methodName, std::move(method)).second)
            throw HttpReturnException(400, "Method already added");
    }

    template<typename Fn>
    void addMethodT(Utf8String name)
    {
        return doAddMethodT(std::move(name), (Fn *)0);
    }

    template<typename Ret, typename... Args>
    void doAddMethodT(Utf8String name, Ret (*) (Args...))
    {
        std::shared_ptr<const AbstractDataType> returnType
            = doGetType<Ret>();
        
        std::vector<std::shared_ptr<const AbstractDataType> > argTypes
            = { doGetType<Args>()... };

        auto method = std::make_shared<Method>
            (name, std::move(returnType), std::move(argTypes));

        addMethod(std::move(name), std::move(method));
    }
    
    static std::shared_ptr<const AbstractDataType>
    getType(const Utf8String & typeName)
    {
        std::unique_lock<std::mutex> guard(typesMutex);
        auto it = types.find(typeName);
        if (it == types.end()) {
            throw HttpReturnException(400, "Couldn't find abstract data type '"
                                      + typeName + "'");
        }
        return it->second;
    }

    static std::shared_ptr<void>
    registerType(const Utf8String & typeName,
                 std::shared_ptr<const AbstractDataType> type)
    {
        std::unique_lock<std::mutex> guard(typesMutex);
        
        auto it_added = types.emplace(typeName, std::move(type));
        if (!it_added.second) {
            throw HttpReturnException(400, "Double added type '" + typeName
                                      + "'");
        }

        auto it = it_added.first;

        auto unregister = [=] (void *)
            {
                std::unique_lock<std::mutex> guard(typesMutex);
                types.erase(it);
            };

        std::shared_ptr<void> result(nullptr, unregister);

        return result;
    }
    
private:
    std::map<Utf8String, std::shared_ptr<const Method> > methods;

    static std::mutex typesMutex;
    static std::map<Utf8String, std::shared_ptr<const AbstractDataType> > types;
};


/*****************************************************************************/
/* VARIABLE                                                                  */
/*****************************************************************************/

struct Variable {
    Variable() = default;

    virtual ~Variable() = default;
    
    Variable(Utf8String name)
        : name(std::move(name))
    {
        recordVariable(*this);
    }

    virtual bool hasValue() const
    {
        return false;
    }
    
    virtual Utf8String getValueString() const
    {
        throw HttpReturnException(400, "Variable has no value set");
    }
    
    Utf8String name;
};


/*****************************************************************************/
/* ABSTRACT DATA TYPE INSTANCE                                               */
/*****************************************************************************/

/** An instance of an abstract data type.  Due to the late binding of concrete
    types, this knows about its abstract type but not its concrete type
    (unless it has an immediate value, in which case the concrete type is
    a maximally generic one).
*/
struct AbstractDataTypeInstance: public Variable {
    AbstractDataTypeInstance(Utf8String name, const Utf8String & typeName)
        : Variable(std::move(name)),
          type(AbstractDataType::getType(typeName))
    {
    }
    
    std::shared_ptr<const AbstractDataType> type;

    template<typename Fn>
    std::shared_ptr<const MethodT<Fn> >
    getMethodT(const Utf8String & name) const
    {
        return doGetMethodT(name, (Fn *)0);
    }
    
    template<typename Return, typename... Args>
    std::shared_ptr<const MethodT<Return (Args...)> >
    doGetMethodT(const Utf8String & name, Return (*) (Args...)) const
    {
        ExcAssert(type);
        return type->getMethodT<Return (Args...)>(name);
    }

    virtual bool hasValue() const
    {
        return false;
    }
    
    virtual Utf8String getValueString() const
    {
        throw HttpReturnException(400, "Variable has no value set");
    }
};


/*****************************************************************************/
/* SCALAR                                                                    */
/*****************************************************************************/

// Describes a scalar type, along with its operations
struct Scalar: public Variable {
    Scalar(Utf8String name)
        : Variable(std::move(name))
    {
    }

    virtual ~Scalar() = default;
    
    template<typename T>
    Scalar(Utf8String name, const T & value,
           std::shared_ptr<const ValueDescriptionT<T> > desc
               = getDefaultDescriptionSharedT<T>())
        : Variable(std::move(name)),
          valueType(std::move(desc))
    {
        this->value.reset(valueType->constructCopy(&value),
                          [=] (void * val) { valueType->destroy(val); });
    }

    Scalar(Utf8String name, const Scalar & value)
        : Variable(std::move(name)),
          valueType(value.valueType),
          value(value.value)
    {
    }
    
    std::shared_ptr<const ValueDescription> valueType;
    std::shared_ptr<const void> value;
    
    virtual bool hasValue() const
    {
        return value && valueType;
    }
    
    virtual Utf8String getValueString() const
    {
        if (!hasValue())
            throw HttpReturnException(400, "Scalar has no value set");
        return valueType->printJsonString(value.get());
    }

    static std::shared_ptr<const AbstractDataType> getType()
    {
        return nullptr;
    }
};

std::ostream & operator << (std::ostream & stream, const Variable & var)
{
    stream << var.name;
    if (var.hasValue()) {
        stream << "=" << var.getValueString();
    }
    return stream;
}

struct Image: public AbstractDataTypeInstance {
    Image(Utf8String name)
        : AbstractDataTypeInstance(std::move(name), "image")
    {
    }

    Scalar width() const
    {
        auto method = getMethodT<Scalar ()>("width");
        return method->call();
    }
    
    Scalar height() const
    {
        auto method = getMethodT<Scalar ()>("height");
        return method->call();
    }

    static std::shared_ptr<const AbstractDataType> getType();
};

struct Row: public AbstractDataTypeInstance {
    Row(Utf8String name)
        : AbstractDataTypeInstance(std::move(name), "row")
    {
    }

    static std::shared_ptr<const AbstractDataType> getType();
};

struct Block: public AbstractDataTypeInstance {
    Block(Utf8String name, Scalar uri)
        : AbstractDataTypeInstance(std::move(name), "block"),
          uri(std::move(uri))
    {
    }

    Scalar uri;

    static std::shared_ptr<const AbstractDataType> getType();
};



/*****************************************************************************/
/* OPERATION                                                                 */
/*****************************************************************************/

/** Abstract operation, which handles all things that can be done,
    including memory operations.
*/

struct Operation {
    Operation(Utf8String name)
        : name(std::move(name))
    {
        recordOperation(*this);
    }

    virtual ~Operation() = default;

    virtual std::shared_ptr<const DeviceOperation>
    compileForDevice(const Device & device) const = 0;
    
    Utf8String name;

    static OperationContext & context();
};


/*****************************************************************************/
/* DEVICE OPERATION                                                          */
/*****************************************************************************/

/** Operation that can concretely run on a device.  It's the equivalent of a
    "kernel" in Tensorflow.

    In order to compile for a device, we:
    1/ Have concrete types attached to each of the inputs and outputs, and
       concrete data layouts
    2/ Choose a concrete implementation of the function for the device.
*/

struct DeviceOperation {
    // ...
};


/*****************************************************************************/
/* OPERATION CONTEXT                                                         */
/*****************************************************************************/

struct OperationContext {

    OperationContext(OperationContext * parent = getCurrentContext())
        : parent(parent)
    {
        contextStack.push_back(this);
    }

    ~OperationContext()
    {
        // TODO: add all operations to parent
        ExcAssert(!contextStack.empty());
        contextStack.pop_back();
    }

    // Set up the immediate mode on the local host.  Operations will be
    // run immediately on the local host only and their results returned.
    // This is useful for debugging as operations are defined; it
    // allows for variables and parameters to have a real, bound value.
    void immediateModeOnHost()
    {
        std::vector<Device> devices{Device::getHost()};
        setImmediateModeDevices(std::move(devices));
    }

    void setImmediateModeDevices(std::vector<Device> devices)
    {
        this->immediateModeDevices = std::move(devices);
    }

    Row returnRow(Utf8String resultName,
                  const std::vector<Variable> & vars)
    {
        return Row(std::move(resultName));
    }

    Variable getResult() const
    {
        return Row("hello");
    }

    void recordOperation(const Operation & op)
    {
        operations.emplace_back(op.name);
    }

    void recordVariable(const Variable & var)
    {
#if 0
        if (!var.hasValue() && !immediateModeDevices.empty()) {
            throw HttpReturnException
                (400, "Variable '" + var.name
                 + "' with no value in immediate mode code");
        }
#endif
        variables.emplace_back(var.name);
    }
    
private:
    friend class Operation;

    OperationContext * parent;

    static OperationContext * getCurrentContext()
    {
        if (contextStack.empty())
            return nullptr;
        return contextStack.back();
    }
    
    static thread_local std::vector<OperationContext *> contextStack;

    std::vector<Device> immediateModeDevices;
    std::vector<Utf8String> operations;
    std::vector<Utf8String> variables;
};

struct MemoryOperation: public Operation {
};

struct ReadBlockOperation: public Operation {
    ReadBlockOperation(Utf8String name)
        : Operation(std::move(name))
    {
    }

    virtual std::shared_ptr<const DeviceOperation>
    compileForDevice(const Device & device) const override
    {
        return nullptr;
    }
    
    Block operator () (Scalar uri) const
    {
        return Block(name, uri);
    }
};

struct DecodeJpegOperation: public Operation {
    DecodeJpegOperation(Utf8String name)
        : Operation(std::move(name))
    {
    }
    
    virtual std::shared_ptr<const DeviceOperation>
    compileForDevice(const Device & device) const override
    {
        return nullptr;
    }

    Image operator () (Block image) const
    {
        return Image("image");
    }
};

struct CallMethodOperation: public Operation {
};


/*****************************************************************************/
/* SCALAR                                                                    */
/*****************************************************************************/

} // namespace MLDB
