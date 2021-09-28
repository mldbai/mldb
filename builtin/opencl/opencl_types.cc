/** opencl_types.h
    Jeremy Barnes, 21 September 2017
    Copyright (c) 2017 Element AI Inc.  All rights reserved.
    This file is part of MLDB. Copyright 2015 mldb.ai inc. All rights reserved.

    OpenCL plugin, to allow execution of OpenCL code and OpenCL functions
    to be defined.
*/

#include "opencl_types.h"
#include "mldb/types/basic_value_descriptions.h"
#include "mldb/types/array_description.h"
#include "mldb/types/set_description.h"
#include "mldb/types/annotated_exception.h"
#include <regex>
#include <iostream>

using namespace std;

namespace MLDB {


/*****************************************************************************/
/* OPENCL EXCEPTION                                                          */
/*****************************************************************************/
// https://stackoverflow.com/questions/24326432/convenient-way-to-show-opencl-error-codes
// algorithmically determined from the API header files

std::string clGetErrorString(int errorCode)
{
    switch (errorCode) {
    case 0: return "CL_SUCCESS";
    case -1: return "CL_DEVICE_NOT_FOUND";
    case -2: return "CL_DEVICE_NOT_AVAILABLE";
    case -3: return "CL_COMPILER_NOT_AVAILABLE";
    case -4: return "CL_MEM_OBJECT_ALLOCATION_FAILURE";
    case -5: return "CL_OUT_OF_RESOURCES";
    case -6: return "CL_OUT_OF_HOST_MEMORY";
    case -7: return "CL_PROFILING_INFO_NOT_AVAILABLE";
    case -8: return "CL_MEM_COPY_OVERLAP";
    case -9: return "CL_IMAGE_FORMAT_MISMATCH";
    case -10: return "CL_IMAGE_FORMAT_NOT_SUPPORTED";
    case -12: return "CL_MAP_FAILURE";
    case -13: return "CL_MISALIGNED_SUB_BUFFER_OFFSET";
    case -14: return "CL_EXEC_STATUS_ERROR_FOR_EVENTS_IN_WAIT_LIST";
    case -15: return "CL_COMPILE_PROGRAM_FAILURE";
    case -16: return "CL_LINKER_NOT_AVAILABLE";
    case -17: return "CL_LINK_PROGRAM_FAILURE";
    case -18: return "CL_DEVICE_PARTITION_FAILED";
    case -19: return "CL_KERNEL_ARG_INFO_NOT_AVAILABLE";
    case -30: return "CL_INVALID_VALUE";
    case -31: return "CL_INVALID_DEVICE_TYPE";
    case -32: return "CL_INVALID_PLATFORM";
    case -33: return "CL_INVALID_DEVICE";
    case -34: return "CL_INVALID_CONTEXT";
    case -35: return "CL_INVALID_QUEUE_PROPERTIES";
    case -36: return "CL_INVALID_COMMAND_QUEUE";
    case -37: return "CL_INVALID_HOST_PTR";
    case -38: return "CL_INVALID_MEM_OBJECT";
    case -39: return "CL_INVALID_IMAGE_FORMAT_DESCRIPTOR";
    case -40: return "CL_INVALID_IMAGE_SIZE";
    case -41: return "CL_INVALID_SAMPLER";
    case -42: return "CL_INVALID_BINARY";
    case -43: return "CL_INVALID_BUILD_OPTIONS";
    case -44: return "CL_INVALID_PROGRAM";
    case -45: return "CL_INVALID_PROGRAM_EXECUTABLE";
    case -46: return "CL_INVALID_KERNEL_NAME";
    case -47: return "CL_INVALID_KERNEL_DEFINITION";
    case -48: return "CL_INVALID_KERNEL";
    case -49: return "CL_INVALID_ARG_INDEX";
    case -50: return "CL_INVALID_ARG_VALUE";
    case -51: return "CL_INVALID_ARG_SIZE";
    case -52: return "CL_INVALID_KERNEL_ARGS";
    case -53: return "CL_INVALID_WORK_DIMENSION";
    case -54: return "CL_INVALID_WORK_GROUP_SIZE";
    case -55: return "CL_INVALID_WORK_ITEM_SIZE";
    case -56: return "CL_INVALID_GLOBAL_OFFSET";
    case -57: return "CL_INVALID_EVENT_WAIT_LIST";
    case -58: return "CL_INVALID_EVENT";
    case -59: return "CL_INVALID_OPERATION";
    case -60: return "CL_INVALID_GL_OBJECT";
    case -61: return "CL_INVALID_BUFFER_SIZE";
    case -62: return "CL_INVALID_MIP_LEVEL";
    case -63: return "CL_INVALID_GLOBAL_WORK_SIZE";
    case -64: return "CL_INVALID_PROPERTY";
    case -65: return "CL_INVALID_IMAGE_DESCRIPTOR";
    case -66: return "CL_INVALID_COMPILER_OPTIONS";
    case -67: return "CL_INVALID_LINKER_OPTIONS";
    case -68: return "CL_INVALID_DEVICE_PARTITION_COUNT";
    case -69: return "CL_INVALID_PIPE_SIZE";
    case -70: return "CL_INVALID_DEVICE_QUEUE";
    case -71: return "CL_INVALID_SPEC_ID";
    case -72: return "CL_MAX_SIZE_RESTRICTION_EXCEEDED";
    case -1002: return "CL_INVALID_D3D10_DEVICE_KHR";
    case -1003: return "CL_INVALID_D3D10_RESOURCE_KHR";
    case -1004: return "CL_D3D10_RESOURCE_ALREADY_ACQUIRED_KHR";
    case -1005: return "CL_D3D10_RESOURCE_NOT_ACQUIRED_KHR";
    case -1006: return "CL_INVALID_D3D11_DEVICE_KHR";
    case -1007: return "CL_INVALID_D3D11_RESOURCE_KHR";
    case -1008: return "CL_D3D11_RESOURCE_ALREADY_ACQUIRED_KHR";
    case -1009: return "CL_D3D11_RESOURCE_NOT_ACQUIRED_KHR";
    case -1010: return "CL_INVALID_DX9_MEDIA_ADAPTER_KHR";
    case -1011: return "CL_INVALID_DX9_MEDIA_SURFACE_KHR";
    case -1012: return "CL_DX9_MEDIA_SURFACE_ALREADY_ACQUIRED_KHR";
    case -1013: return "CL_DX9_MEDIA_SURFACE_NOT_ACQUIRED_KHR";
    case -1093: return "CL_INVALID_EGL_OBJECT_KHR";
    case -1092: return "CL_EGL_RESOURCE_NOT_ACQUIRED_KHR";
    case -1001: return "CL_PLATFORM_NOT_FOUND_KHR";
    case -1057: return "CL_DEVICE_PARTITION_FAILED_EXT";
    case -1058: return "CL_INVALID_PARTITION_COUNT_EXT";
    case -1059: return "CL_INVALID_PARTITION_NAME_EXT";
    case -1094: return "CL_INVALID_ACCELERATOR_INTEL";
    case -1095: return "CL_INVALID_ACCELERATOR_TYPE_INTEL";
    case -1096: return "CL_INVALID_ACCELERATOR_DESCRIPTOR_INTEL";
    case -1097: return "CL_ACCELERATOR_TYPE_NOT_SUPPORTED_INTEL";
    case -1000: return "CL_INVALID_GL_SHAREGROUP_REFERENCE_KHR";
    case -1098: return "CL_INVALID_VA_API_MEDIA_ADAPTER_INTEL";
    case -1099: return "CL_INVALID_VA_API_MEDIA_SURFACE_INTEL";
    case -1100: return "CL_VA_API_MEDIA_SURFACE_ALREADY_ACQUIRED_INTEL";
    case -1101: return "CL_VA_API_MEDIA_SURFACE_NOT_ACQUIRED_INTEL";
    case -9999: return "CL_NVIDIA_KERNEL_EXECUTION_FAULT";
    default: return "CL_UNKNOWN_ERROR " + std::to_string(errorCode);
    }
}

std::string
OpenCLException::
printCode(cl_int returnCode)
{
    auto j = jsonEncode(OpenCLStatus(returnCode));
    if (j.isString())
        return j.asString();
    return clGetErrorString(returnCode);
}

void checkOpenCLError(cl_int returnCode,
                      const char * operation)
{
    if (returnCode == CL_SUCCESS)
        return;
    throw OpenCLException(returnCode, operation);
}

void checkOpenCLError(cl_int returnCode,
                      const std::string & operation)
{
    checkOpenCLError(returnCode, operation.c_str());
}


/*****************************************************************************/
/* OPENCL STATUS                                                             */
/*****************************************************************************/

DEFINE_ENUM_DESCRIPTION_INLINE(OpenCLStatus)
{
#define DO_VALUE(name) addValue(#name, (OpenCLStatus)CL_##name)
    DO_VALUE(SUCCESS);
    DO_VALUE(DEVICE_NOT_FOUND);
    DO_VALUE(DEVICE_NOT_AVAILABLE);
    DO_VALUE(COMPILER_NOT_AVAILABLE);
    DO_VALUE(MEM_OBJECT_ALLOCATION_FAILURE);
    DO_VALUE(OUT_OF_RESOURCES);
    DO_VALUE(OUT_OF_HOST_MEMORY);
    DO_VALUE(PROFILING_INFO_NOT_AVAILABLE);
    DO_VALUE(MEM_COPY_OVERLAP);
    DO_VALUE(IMAGE_FORMAT_MISMATCH);
    DO_VALUE(IMAGE_FORMAT_NOT_SUPPORTED);
    DO_VALUE(BUILD_PROGRAM_FAILURE);
    DO_VALUE(MAP_FAILURE);
    DO_VALUE(MISALIGNED_SUB_BUFFER_OFFSET);
    DO_VALUE(EXEC_STATUS_ERROR_FOR_EVENTS_IN_WAIT_LIST);
    DO_VALUE(COMPILE_PROGRAM_FAILURE);
    DO_VALUE(LINKER_NOT_AVAILABLE);
    DO_VALUE(LINK_PROGRAM_FAILURE);
    DO_VALUE(DEVICE_PARTITION_FAILED);
    DO_VALUE(KERNEL_ARG_INFO_NOT_AVAILABLE);

    DO_VALUE(INVALID_VALUE);
    DO_VALUE(INVALID_DEVICE_TYPE);
    DO_VALUE(INVALID_PLATFORM);
    DO_VALUE(INVALID_DEVICE);
    DO_VALUE(INVALID_CONTEXT);
    DO_VALUE(INVALID_QUEUE_PROPERTIES);
    DO_VALUE(INVALID_COMMAND_QUEUE);
    DO_VALUE(INVALID_HOST_PTR);
    DO_VALUE(INVALID_MEM_OBJECT);
    DO_VALUE(INVALID_IMAGE_FORMAT_DESCRIPTOR);
    DO_VALUE(INVALID_IMAGE_SIZE);
    DO_VALUE(INVALID_SAMPLER);
    DO_VALUE(INVALID_BINARY);
    DO_VALUE(INVALID_BUILD_OPTIONS);
    DO_VALUE(INVALID_PROGRAM);
    DO_VALUE(INVALID_PROGRAM_EXECUTABLE);
    DO_VALUE(INVALID_KERNEL_NAME);
    DO_VALUE(INVALID_KERNEL_DEFINITION);
    DO_VALUE(INVALID_KERNEL);
    DO_VALUE(INVALID_ARG_INDEX);
    DO_VALUE(INVALID_ARG_VALUE);
    DO_VALUE(INVALID_ARG_SIZE);
    DO_VALUE(INVALID_KERNEL_ARGS);
    DO_VALUE(INVALID_WORK_DIMENSION);
    DO_VALUE(INVALID_WORK_GROUP_SIZE);
    DO_VALUE(INVALID_WORK_ITEM_SIZE);
    DO_VALUE(INVALID_GLOBAL_OFFSET);
    DO_VALUE(INVALID_EVENT_WAIT_LIST);
    DO_VALUE(INVALID_EVENT);
    DO_VALUE(INVALID_OPERATION);
    DO_VALUE(INVALID_GL_OBJECT);
    DO_VALUE(INVALID_BUFFER_SIZE);
    DO_VALUE(INVALID_MIP_LEVEL);
    DO_VALUE(INVALID_GLOBAL_WORK_SIZE);
    DO_VALUE(INVALID_PROPERTY);
    DO_VALUE(INVALID_IMAGE_DESCRIPTOR);
    DO_VALUE(INVALID_COMPILER_OPTIONS);
    DO_VALUE(INVALID_LINKER_OPTIONS);
    DO_VALUE(INVALID_DEVICE_PARTITION_COUNT);
    DO_VALUE(INVALID_PIPE_SIZE);
    DO_VALUE(INVALID_DEVICE_QUEUE);
#undef DO_VALUE
};

/*****************************************************************************/

/*****************************************************************************/
/* PROPERTY GETTERS                                                          */
/*****************************************************************************/

struct OpenCLIgnoreExceptions {
};

template<typename... Args>
struct ThrowArgException;

template<>
struct ThrowArgException<> {
    static constexpr bool value = true;
};

template<typename First, typename... Others>
struct ThrowArgException<First, Others...> {
    static constexpr bool value = ThrowArgException<Others...>::value;
};

template<typename... Others>
struct ThrowArgException<OpenCLIgnoreExceptions, Others...> {
    static constexpr bool value = false;
};

void throwInfoException(cl_int code,
                        const ValueDescription & desc,
                        const void * field,
                        const void * base) MLDB_NORETURN;

void throwInfoException(cl_int code,
                        const ValueDescription & desc,
                        const void * field,
                        const void * base)
{
    // Use the value info to find which field caused the error

    const ValueDescription::FieldDescription * fieldDescription
        = desc.getFieldDescription(base, field);
    std::string fieldName;
    if (!fieldDescription) {
        fieldName = "<unknown field>";
    }
    else {
        fieldName = fieldDescription->fieldName;
    }
    throw OpenCLException(code, "clGetXXXInfo "
                          + desc.typeName
                          + "::" + fieldName);
}

template<typename Fn>
cl_int extractArgType(Fn&&fn, std::string & val)
{
    size_t len = 0;
    int res = fn(0, nullptr, &len);
    if (res != CL_SUCCESS)
        return res;
    
    char buf[len];
    res = fn(len, buf, nullptr);
    if (res != CL_SUCCESS)
        return res;

    val = string(buf, buf + len);
    return CL_SUCCESS;
}

template<typename Fn>
cl_int extractArgType(Fn&&fn, std::vector<std::string> & val,
                      const std::regex & splitOn)
{
    std::string unsplit;
    cl_int res = extractArgType(std::forward<Fn>(fn), unsplit);
    if (res != CL_SUCCESS)
        return res;
    
    val = { std::sregex_token_iterator(unsplit.begin(),
                                       unsplit.end(),
                                       splitOn),
            std::sregex_token_iterator() };

    if (val.size() == 1 && val[0] == "") {
        val.clear();
    }

    return CL_SUCCESS;
}

template<typename Fn, typename T>
cl_int extractArgType(Fn&&fn, std::vector<T> & where,
                      typename std::enable_if<std::is_pod<T>::value>::type * = 0)
{
    size_t len = 0;
    int res = fn(0, nullptr, &len);
    if (res != CL_SUCCESS)
        return res;

    std::vector<T> result(len / sizeof(T));
        
    res = fn(len, result.data(), nullptr);

    if (res != CL_SUCCESS)
        return res;

    where = std::move(result);

    return res;
}

template<typename Fn, typename T>
cl_int extractArgType(Fn&&fn, Bitset<T> & where)
{
    return extractArgType(std::forward<Fn>(fn), where.val);
}

template<typename Fn, typename T>
cl_int extractArgType(Fn && fn, T & val,
                      typename std::enable_if<std::is_pod<T>::value>::type * = 0)
{
    return fn(sizeof(val), &val, nullptr);
}

template<typename Fn, typename T>
cl_int extractArgType(Fn && fn, T& val, OpenCLIgnoreExceptions)
{
    return extractArgType(std::forward<Fn>(fn), val);
}

template<typename Fn, typename T, typename Arg>
cl_int extractArgType(Fn && fn, T& val, Arg&& arg, OpenCLIgnoreExceptions)
{
    return extractArgType(std::forward<Fn>(fn), val, std::forward<Arg>(arg));
}

template<typename Fn, typename T, typename Base, typename... Args>
cl_int doArgType(Fn&& fn, T & where, Base * base, Args&&... args)
{
    cl_int res = extractArgType(std::forward<Fn>(fn), where,
                                std::forward<Args>(args)...);
    if (res == CL_SUCCESS)
        return res;

    // Throw the right exception
    static const auto descr = getDefaultDescriptionSharedT<Base>();

    if (ThrowArgException<Args...>::value) {
        throwInfoException(res, *descr, &where, base);
    }

    return res;
}

template<typename Entity, typename T, typename Base, typename... Args>
cl_int clInfoCall(cl_int (*fn) (Entity obj, cl_uint what, size_t, void *, size_t *),
                Entity obj,
                cl_uint what,
                T & where, Base * base,
                Args&&... args)
{
    auto bound = [&] (size_t szin, void * arg, size_t * szout) -> cl_int
        {
            return fn(obj, what, szin, arg, szout);
        };

    return doArgType(bound, where, base, std::forward<Args>(args)...);
}

template<typename Entity, typename Param, typename T, typename Base, typename... Args>
cl_int clInfoCall(cl_int (*fn) (Entity obj, Param param, cl_uint what, size_t, void *, size_t *),
                Entity obj,
                Param param,
                cl_uint what,
                T & where, Base * base,
                Args&&... args)
{
    auto bound = [&] (size_t szin, void * arg, size_t * szout) -> cl_int
        {
            return fn(obj, param, what, szin, arg, szout);
        };

    return doArgType(bound, where, base, std::forward<Args>(args)...);
}


/*****************************************************************************/
/* OPENCL DEVICE INFO                                                        */
/*****************************************************************************/

DEFINE_ENUM_DESCRIPTION_INLINE(OpenCLFpConfig)
{
    addValue("DENORM", OpenCLFpConfig::DENORM);
    addValue("INF_NAN", OpenCLFpConfig::INF_NAN);
    addValue("ROUND_TO_NEAREST", OpenCLFpConfig::ROUND_TO_NEAREST);
    addValue("ROUND_TO_ZERO", OpenCLFpConfig::ROUND_TO_ZERO);
    addValue("ROUND_TO_INF", OpenCLFpConfig::ROUND_TO_INF);
    addValue("FMA", OpenCLFpConfig::FMA);
    addValue("SOFT_FLOAT", OpenCLFpConfig::SOFT_FLOAT);
    addValue("CORRECTLY_ROUNDED_DIVIDE_SQRT",
             OpenCLFpConfig::CORRECTLY_ROUNDED_DIVIDE_SQRT);
}

DEFINE_ENUM_DESCRIPTION_INLINE(OpenCLCacheType)
{
    addValue("NONE", OpenCLCacheType::NONE);
    addValue("READ_ONLY", OpenCLCacheType::READ_ONLY);
    addValue("READ_WRITE", OpenCLCacheType::READ_WRITE);
}

DEFINE_ENUM_DESCRIPTION_INLINE(OpenCLExecutionCapabilities)
{
    addValue("NONE", OpenCLExecutionCapabilities::NONE);
    addValue("KERNEL", OpenCLExecutionCapabilities::KERNEL);
    addValue("NATIVE_KERNEL", OpenCLExecutionCapabilities::NATIVE_KERNEL);
}

DEFINE_ENUM_DESCRIPTION_INLINE(OpenCLLocalMemoryType)
{
    addValue("NONE", OpenCLLocalMemoryType::NONE);
    addValue("LOCAL", OpenCLLocalMemoryType::LOCAL);
    addValue("GLOBAL", OpenCLLocalMemoryType::GLOBAL);
}

DEFINE_ENUM_DESCRIPTION_INLINE(OpenCLPartitionProperty)
{
    addValue("EQUALLY", OpenCLPartitionProperty::EQUALLY);
    addValue("BY_COUNTS", OpenCLPartitionProperty::BY_COUNTS);
    addValue("BY_COUNTS_LIST_END", OpenCLPartitionProperty::BY_COUNTS_LIST_END);
    addValue("BY_AFFINITY_DOMAIN",
             OpenCLPartitionProperty::BY_AFFINITY_DOMAIN);
    
}

DEFINE_ENUM_DESCRIPTION_INLINE(OpenCLPartitionAffinityDomain)
{
    addValue("NUMA", OpenCLPartitionAffinityDomain::NUMA);
    addValue("L4_CACHE", OpenCLPartitionAffinityDomain::L4_CACHE);
    addValue("L3_CACHE", OpenCLPartitionAffinityDomain::L3_CACHE);
    addValue("L2_CACHE", OpenCLPartitionAffinityDomain::L2_CACHE);
    addValue("L1_CACHE", OpenCLPartitionAffinityDomain::L1_CACHE);
    addValue("NEXT_PARTITIONABLE",
             OpenCLPartitionAffinityDomain::NEXT_PARTITIONABLE);
}

DEFINE_ENUM_DESCRIPTION_INLINE(OpenCLDeviceQueueProperties)
{
    addValue("OUT_OF_ORDER_EXEC_MODE_ENABLE",
             OpenCLDeviceQueueProperties::OUT_OF_ORDER_EXEC_MODE_ENABLE);
    addValue("PROFILING_ENABLE",
             OpenCLDeviceQueueProperties::PROFILING_ENABLE);
    addValue("ON_DEVICE", OpenCLDeviceQueueProperties::ON_DEVICE);
    addValue("ON_DEVICE_DEFAULT",
             OpenCLDeviceQueueProperties::ON_DEVICE_DEFAULT);
}

DEFINE_ENUM_DESCRIPTION_INLINE(OpenCLDeviceType)
{
    addValue("DEFAULT", OpenCLDeviceType::DEFAULT);
    addValue("CPU", OpenCLDeviceType::CPU);
    addValue("GPU", OpenCLDeviceType::GPU);
    addValue("ACCELERATOR", OpenCLDeviceType::ACCELERATOR);
    addValue("CUSTOM", OpenCLDeviceType::CUSTOM);
}

DEFINE_ENUM_DESCRIPTION_INLINE(OpenCLDeviceSvmCapabilities)
{
    addValue("COARSE_GRAIN_BUFFER",
             OpenCLDeviceSvmCapabilities::COARSE_GRAIN_BUFFER);
    addValue("FINE_GRAIN_BUFFER",
             OpenCLDeviceSvmCapabilities::FINE_GRAIN_BUFFER);
    addValue("FINE_GRAIN_SYSTEM",
             OpenCLDeviceSvmCapabilities::FINE_GRAIN_SYSTEM);
    addValue("ATOMICS",
             OpenCLDeviceSvmCapabilities::ATOMICS);
}


/*****************************************************************************/
/* OPENCL DEVICE INFO                                                        */
/*****************************************************************************/

OpenCLDeviceInfo::
OpenCLDeviceInfo(cl_device_id device)
    : device(device)
{
#define DO_FIELD(name, id) doField(CL_DEVICE_##id, name)
    DO_FIELD(addressBits, ADDRESS_BITS);
    DO_FIELD(available, AVAILABLE);
    static const std::regex splitExtensionsOn("[^ ]+");
    doField(CL_DEVICE_BUILT_IN_KERNELS, builtInKernels, splitExtensionsOn);
    DO_FIELD(compilerAvailable, COMPILER_AVAILABLE);
    DO_FIELD(singleFpConfig, SINGLE_FP_CONFIG);
    DO_FIELD(doubleFpConfig, DOUBLE_FP_CONFIG);
    DO_FIELD(endianLittle, ENDIAN_LITTLE);
    DO_FIELD(errorCorrection, ERROR_CORRECTION_SUPPORT);
    DO_FIELD(executionCapabilities, EXECUTION_CAPABILITIES);
    doField(CL_DEVICE_EXTENSIONS, extensions, splitExtensionsOn);
    DO_FIELD(globalMemCacheSize, GLOBAL_MEM_CACHE_SIZE);
    DO_FIELD(globalMemCacheType, GLOBAL_MEM_CACHE_TYPE);
    DO_FIELD(globalMemCacheLineSize, GLOBAL_MEM_CACHELINE_SIZE);
    DO_FIELD(globalMemSize, GLOBAL_MEM_SIZE);
    doField(CL_DEVICE_HALF_FP_CONFIG, halfFpConfig, OpenCLIgnoreExceptions());
    DO_FIELD(unifiedMemory, HOST_UNIFIED_MEMORY);
    DO_FIELD(imageSupport, IMAGE_SUPPORT);
    DO_FIELD(image2dMaxDimensions, IMAGE2D_MAX_WIDTH);
    DO_FIELD(image3dMaxDimensions, IMAGE3D_MAX_WIDTH);
    DO_FIELD(imageMaxBufferSize, IMAGE_MAX_BUFFER_SIZE);
    DO_FIELD(imageMaxArraySize, IMAGE_MAX_ARRAY_SIZE);
    DO_FIELD(linkerAvailable, LINKER_AVAILABLE);
    DO_FIELD(localMemSize, LOCAL_MEM_SIZE);
    DO_FIELD(localMemType, LOCAL_MEM_TYPE);
    DO_FIELD(maxClockFrequency, MAX_CLOCK_FREQUENCY);
    DO_FIELD(maxComputeUnits, MAX_COMPUTE_UNITS);
    DO_FIELD(maxConstantArgs, MAX_CONSTANT_ARGS);
    DO_FIELD(maxConstantBufferSize, MAX_CONSTANT_BUFFER_SIZE);
    DO_FIELD(maxMemAllocSize, MAX_MEM_ALLOC_SIZE);
    DO_FIELD(maxParameterSize, MAX_PARAMETER_SIZE);
    DO_FIELD(maxReadImageArgs, MAX_READ_IMAGE_ARGS);
    DO_FIELD(maxSamplers, MAX_SAMPLERS);
    DO_FIELD(maxWorkGroupSize, MAX_WORK_GROUP_SIZE);
    DO_FIELD(maxWorkItemDimensions, MAX_WORK_ITEM_DIMENSIONS);
    DO_FIELD(maxWorkItemSizes, MAX_WORK_ITEM_SIZES);
    DO_FIELD(maxWriteImageArgs, MAX_WRITE_IMAGE_ARGS);
    DO_FIELD(memBaseAddrAlign, MEM_BASE_ADDR_ALIGN);
    DO_FIELD(name, NAME);
    DO_FIELD(nativeVectorWidth, NATIVE_VECTOR_WIDTH_CHAR);
    DO_FIELD(openCLCVersion, OPENCL_C_VERSION);
    DO_FIELD(partitionMaxSubDevices, PARTITION_MAX_SUB_DEVICES);
    DO_FIELD(partitionProperties, PARTITION_PROPERTIES);
    DO_FIELD(partitionAffinityDomain, PARTITION_AFFINITY_DOMAIN);
    DO_FIELD(partitionType, PARTITION_TYPE);
    DO_FIELD(preferredVectorWidth, PREFERRED_VECTOR_WIDTH_CHAR);
    DO_FIELD(printfBufferSize, PRINTF_BUFFER_SIZE);
    DO_FIELD(preferredInteropUserSync, PREFERRED_INTEROP_USER_SYNC);
    DO_FIELD(profile, PROFILE);
    DO_FIELD(profilingTimerResolution, PROFILING_TIMER_RESOLUTION);
    DO_FIELD(queueProperties, QUEUE_PROPERTIES);
    DO_FIELD(referenceCount, REFERENCE_COUNT);
    DO_FIELD(type, TYPE);
    DO_FIELD(vendor, VENDOR);
    DO_FIELD(vendorId, VENDOR_ID);
    DO_FIELD(version, VERSION);
    doField(CL_DRIVER_VERSION, driverVersion);
    doField(CL_DEVICE_PREFERRED_VECTOR_WIDTH_HALF, preferredVectorWidth[6]);


#define DO_OPTIONAL(name, id) doField(CL_DEVICE_##id, name, OpenCLIgnoreExceptions());

    DO_OPTIONAL(svmCapabilities, SVM_CAPABILITIES);
    DO_OPTIONAL(imagePitchAlignment, IMAGE_PITCH_ALIGNMENT);
    DO_OPTIONAL(imageBaseAddressAlignment, IMAGE_BASE_ADDRESS_ALIGNMENT);
    DO_OPTIONAL(maxReadWriteImageArgs, MAX_READ_WRITE_IMAGE_ARGS);
    DO_OPTIONAL(maxGlobalVariableSize, MAX_GLOBAL_VARIABLE_SIZE);
    DO_OPTIONAL(globalVariablePreferredTotalSize,
                GLOBAL_VARIABLE_PREFERRED_TOTAL_SIZE);
    DO_OPTIONAL(pipeMaxActiveReservations, PIPE_MAX_ACTIVE_RESERVATIONS);
    DO_OPTIONAL(pipeMaxPacketSize, PIPE_MAX_PACKET_SIZE);
    DO_OPTIONAL(maxOnDeviceQueues, MAX_ON_DEVICE_QUEUES);
    DO_OPTIONAL(maxOnDeviceEvents, MAX_ON_DEVICE_EVENTS);
    DO_OPTIONAL(queueOnDeviceMaxSize, QUEUE_ON_DEVICE_MAX_SIZE);
    DO_OPTIONAL(queueOnDevicePreferredSize, QUEUE_ON_DEVICE_PREFERRED_SIZE);
    DO_OPTIONAL(queueOnDeviceProperties, QUEUE_ON_DEVICE_PROPERTIES);
    DO_OPTIONAL(maxPipeArgs, MAX_PIPE_ARGS);
    DO_OPTIONAL(pipeMaxActiveReservations, PIPE_MAX_ACTIVE_RESERVATIONS);
    DO_OPTIONAL(pipeMaxPacketSize, PIPE_MAX_PACKET_SIZE);
    DO_OPTIONAL(preferredPlatformAtomicAlignment,
                PREFERRED_PLATFORM_ATOMIC_ALIGNMENT);
    DO_OPTIONAL(preferredGlobalAtomicAlignment,
                PREFERRED_GLOBAL_ATOMIC_ALIGNMENT);
    DO_OPTIONAL(preferredLocalAtomicAlignment,
                PREFERRED_LOCAL_ATOMIC_ALIGNMENT);
    DO_OPTIONAL(partitionProperties, PARTITION_PROPERTIES);
#undef DO_FIELD
#undef DO_OPTIONAL
}

template<typename T, typename... Args>
void
OpenCLDeviceInfo::
doField(cl_uint what, T & where, Args&&... args)
{
    clInfoCall(clGetDeviceInfo, device, what, where, this,
               std::forward<Args>(args)...);
}

template<typename T, size_t N>
void
OpenCLDeviceInfo::
doField(cl_device_info what, std::array<T, N> & where)
{
    for (size_t i = 0;  i < N;  ++i) {
        doField(what + i, where[i]);
    }
}

#if 0
template<typename T>
void
OpenCLDeviceInfo::
doField(cl_device_info id, T & val,
        const char * call,
        std::enable_if<std::is_pod<T>::value>::type *)
{
    int res = clGetDeviceInfo(device, id, sizeof(val), &val, nullptr);
    checkOpenCLError(res, "glGetDeviceInfo(" + string(call) + ")");
}

void
OpenCLDeviceInfo::
doField(cl_device_info what, std::string & where,
        const char * call)
{
    size_t len = 0;
    int res = clGetDeviceInfo(device, what, 0, nullptr, &len);
    checkOpenCLError(res, "glGetDeviceInfo(" + string(call) + ")");

    char buf[len];
        
    res = clGetDeviceInfo(device, what, len, buf, nullptr);

    if (res != CL_SUCCESS) {
        throw AnnotatedException(400, "clGetDeviceInfo: "
                                  + std::to_string(res));
    }

    where = string(buf, buf + len);
}

void
OpenCLDeviceInfo::
doField(cl_device_info what, std::vector<std::string> & where,
        const char * call)
{
    std::string unsplit;
    doField(what, unsplit, call);

    static const std::regex splitOn("[^ ]+");
    where = { std::sregex_token_iterator(unsplit.begin(),
                                         unsplit.end(),
                                         splitOn),
              std::sregex_token_iterator() };

    if (where.size() == 1 && where[0] == "") {
        where.clear();
    }
}

#endif

DEFINE_STRUCTURE_DESCRIPTION_INLINE(OpenCLDeviceInfo)
{
#define DO_FIELD(name) addField(#name, &OpenCLDeviceInfo::name, "")
    DO_FIELD(addressBits);
    DO_FIELD(available);
    DO_FIELD(builtInKernels);
    DO_FIELD(compilerAvailable);
    DO_FIELD(singleFpConfig);
    DO_FIELD(doubleFpConfig);
    DO_FIELD(endianLittle);
    DO_FIELD(errorCorrection);
    DO_FIELD(executionCapabilities);
    DO_FIELD(extensions);
    DO_FIELD(globalMemCacheSize);
    DO_FIELD(globalMemCacheType);
    DO_FIELD(globalMemCacheLineSize);
    DO_FIELD(globalMemSize);
    DO_FIELD(halfFpConfig);
    DO_FIELD(unifiedMemory);
    DO_FIELD(imageSupport);
    DO_FIELD(image2dMaxDimensions);
    DO_FIELD(image3dMaxDimensions);
    DO_FIELD(imageMaxBufferSize);
    DO_FIELD(imageMaxArraySize);
    DO_FIELD(linkerAvailable);
    DO_FIELD(localMemSize);
    DO_FIELD(localMemType);
    DO_FIELD(maxClockFrequency);
    DO_FIELD(maxComputeUnits);
    DO_FIELD(maxConstantArgs);
    DO_FIELD(maxConstantBufferSize);
    DO_FIELD(maxMemAllocSize);
    DO_FIELD(maxParameterSize);
    DO_FIELD(maxReadImageArgs);
    DO_FIELD(maxSamplers);
    DO_FIELD(maxWorkGroupSize);
    DO_FIELD(maxWorkItemDimensions);
    DO_FIELD(maxWorkItemSizes);
    DO_FIELD(maxWriteImageArgs);
    DO_FIELD(memBaseAddrAlign);
    DO_FIELD(name);
    DO_FIELD(nativeVectorWidth);
    DO_FIELD(openCLCVersion);
    DO_FIELD(partitionMaxSubDevices);
    DO_FIELD(partitionProperties);
    DO_FIELD(partitionAffinityDomain);
    DO_FIELD(partitionType);
    DO_FIELD(preferredVectorWidth);
    DO_FIELD(printfBufferSize);
    DO_FIELD(preferredInteropUserSync);
    DO_FIELD(profile);
    DO_FIELD(profilingTimerResolution);
    DO_FIELD(queueProperties);
    DO_FIELD(referenceCount);
    DO_FIELD(type);
    DO_FIELD(vendor);
    DO_FIELD(vendorId);
    DO_FIELD(version);
    DO_FIELD(driverVersion);

    DO_FIELD(svmCapabilities);
    DO_FIELD(imagePitchAlignment);
    DO_FIELD(imageBaseAddressAlignment);
    DO_FIELD(maxReadWriteImageArgs);
    DO_FIELD(maxGlobalVariableSize);
    DO_FIELD(globalVariablePreferredTotalSize);
    DO_FIELD(pipeMaxActiveReservations);
    DO_FIELD(pipeMaxPacketSize);
    DO_FIELD(maxOnDeviceQueues);
    DO_FIELD(maxOnDeviceEvents);
    DO_FIELD(queueOnDeviceMaxSize);
    DO_FIELD(queueOnDevicePreferredSize);
    DO_FIELD(queueOnDeviceProperties);
    DO_FIELD(maxPipeArgs);

    DO_FIELD(preferredPlatformAtomicAlignment);
    DO_FIELD(preferredGlobalAtomicAlignment);
    DO_FIELD(preferredLocalAtomicAlignment);

#undef DO_FIELD
}


/*****************************************************************************/
/* OPENCL PLATFORM INFO                                                      */
/*****************************************************************************/

OpenCLPlatformInfo::
OpenCLPlatformInfo(cl_platform_id platform)
    : platform(platform)
{
    get(CL_PLATFORM_PROFILE, profile, "PROFILE");
    get(CL_PLATFORM_VERSION, version, "VERSION");
    get(CL_PLATFORM_NAME, name, "NAME");
    get(CL_PLATFORM_VENDOR, vendor, "VENDOR");

    std::string extensionsStr;
    get(CL_PLATFORM_EXTENSIONS, extensionsStr, "EXTENSIONS");

    static const std::regex splitOn("[^ ]+");
    extensions = { std::sregex_token_iterator(extensionsStr.begin(),
                                              extensionsStr.end(),
                                              splitOn),
                   std::sregex_token_iterator() };
}
    
void
OpenCLPlatformInfo::
get(cl_platform_info what, std::string & where,
    const char * call)
{
    size_t len = 0;
    int res = clGetPlatformInfo(platform, what, 0, nullptr, &len);
    checkOpenCLError(res, "glGetPlatformInfo(" + string(call) + ")");

    char buf[len];
        
    res = clGetPlatformInfo(platform, what, len, buf, nullptr);
    checkOpenCLError(res, "glGetPlatformInfo(" + string(call) + ")");

    where = string(buf, buf + len);
}
    
IMPLEMENT_STRUCTURE_DESCRIPTION(OpenCLPlatformInfo)
{
    addField("profile", &OpenCLPlatformInfo::profile,
             "OpenCL profile version of platform");
    addField("version", &OpenCLPlatformInfo::version,
             "OpenCL profile version number of platform");
    addField("name", &OpenCLPlatformInfo::name,
             "OpenCL profile name of platform");
    addField("vendor", &OpenCLPlatformInfo::vendor,
             "OpenCL platform vendor name");
    addField("extensions", &OpenCLPlatformInfo::extensions,
             "OpenCL platform vendor extensions");
}


/*****************************************************************************/
/* OPENCL PROGRAM BUILD INFO                                                 */
/*****************************************************************************/

DEFINE_ENUM_DESCRIPTION_INLINE(OpenCLBuildStatus)
{
    addValue("NONE", OpenCLBuildStatus::NONE);
    addValue("ERROR", OpenCLBuildStatus::ERROR);
    addValue("SUCCESS", OpenCLBuildStatus::SUCCESS);
    addValue("IN_PROGRESS", OpenCLBuildStatus::IN_PROGRESS);
}

DEFINE_ENUM_DESCRIPTION_INLINE(OpenCLBinaryType)
{
    addValue("NONE", OpenCLBinaryType::NONE);
    addValue("COMPILED_OBJECT", OpenCLBinaryType::COMPILED_OBJECT);
    addValue("LIBRARY", OpenCLBinaryType::LIBRARY);
    addValue("EXECUTABLE", OpenCLBinaryType::EXECUTABLE);
}

OpenCLProgramBuildInfo::
OpenCLProgramBuildInfo(cl_program program, cl_device_id device)
    : program(program), device(device)
{
    doField(CL_PROGRAM_BUILD_STATUS, buildStatus);
    doField(CL_PROGRAM_BUILD_OPTIONS, buildOptions);
    static const std::regex splitBuildLogOn("[^\n]*");
    doField(CL_PROGRAM_BUILD_LOG, buildLog, splitBuildLogOn);
    doField(CL_PROGRAM_BINARY_TYPE, binaryType);
}

template<typename T, typename... Args>
void
OpenCLProgramBuildInfo::
doField(cl_uint what, T & where, Args&&... args)
{
    clInfoCall(clGetProgramBuildInfo, program, device, what, where, this,
               std::forward<Args>(args)...);
}

DEFINE_STRUCTURE_DESCRIPTION_INLINE(OpenCLProgramBuildInfo)
{
    addField("buildStatus", &OpenCLProgramBuildInfo::buildStatus, "");
    addField("buildOptions", &OpenCLProgramBuildInfo::buildOptions, "");
    addField("buildLog", &OpenCLProgramBuildInfo::buildLog, "");
    addField("binaryType", &OpenCLProgramBuildInfo::binaryType, "");
}


/*****************************************************************************/
/* OPENCL PROGRAM INFO                                                       */
/*****************************************************************************/

OpenCLProgramInfo::
OpenCLProgramInfo(cl_program program)
    : program(program)
{
    cl_uint numDevices;
    doField(CL_PROGRAM_NUM_DEVICES, numDevices);

    std::vector<size_t> binarySizes(numDevices);
    doField(CL_PROGRAM_BINARY_SIZES, binarySizes);

    binaries.resize(numDevices);
    std::vector<char *> binaryPointers(numDevices);
    for (int i = 0;  i < numDevices;  ++i) {
        binaries[i].resize(binarySizes[i]);
        binaryPointers[i] = binaries[i].data();
    }
    doField(CL_PROGRAM_BINARIES, binaryPointers);

    doField(CL_PROGRAM_SOURCE, source);
}

template<typename T, typename... Args>
void
OpenCLProgramInfo::
doField(cl_uint what, T & where, Args&&... args)
{
    clInfoCall(clGetProgramInfo, program, what, where, this,
               std::forward<Args>(args)...);
}

DEFINE_STRUCTURE_DESCRIPTION_INLINE(OpenCLProgramInfo)
{
    addField("source", &OpenCLProgramInfo::source, "");
    addField("binaries", &OpenCLProgramInfo::binaries, "");
}


/*****************************************************************************/
/* OPENCL KERNEL INFO                                                        */
/*****************************************************************************/

DEFINE_ENUM_DESCRIPTION_INLINE(OpenCLArgAddressQualifier)
{
    addValue("GLOBAL", OpenCLArgAddressQualifier::GLOBAL);
    addValue("LOCAL", OpenCLArgAddressQualifier::LOCAL);
    addValue("CONSTANT", OpenCLArgAddressQualifier::CONSTANT);
    addValue("PRIVATE", OpenCLArgAddressQualifier::PRIVATE);
}

DEFINE_ENUM_DESCRIPTION_INLINE(OpenCLArgAccessQualifier)
{
    addValue("READ_ONLY", OpenCLArgAccessQualifier::READ_ONLY);
    addValue("WRITE_ONLY", OpenCLArgAccessQualifier::WRITE_ONLY);
    addValue("READ_WRITE", OpenCLArgAccessQualifier::READ_WRITE);
    addValue("NONE", OpenCLArgAccessQualifier::NONE);
}

DEFINE_ENUM_DESCRIPTION_INLINE(OpenCLArgTypeQualifier)
{
    addValue("NONE", OpenCLArgTypeQualifier::NONE);
    addValue("CONST", OpenCLArgTypeQualifier::CONST);
    addValue("RESTRICT", OpenCLArgTypeQualifier::RESTRICT);
    addValue("VOLATILE", OpenCLArgTypeQualifier::VOLATILE);
    addValue("PIPE", OpenCLArgTypeQualifier::PIPE);
}

OpenCLKernelArgInfo::
OpenCLKernelArgInfo(cl_kernel kernel,
                    cl_uint argNum)
    : kernel(kernel), argNum(argNum)
{
    doField(CL_KERNEL_ARG_ADDRESS_QUALIFIER, addressQualifier);
    doField(CL_KERNEL_ARG_ACCESS_QUALIFIER, accessQualifier);
    doField(CL_KERNEL_ARG_TYPE_NAME, typeName);
    doField(CL_KERNEL_ARG_TYPE_QUALIFIER, typeQualifier);
    doField(CL_KERNEL_ARG_NAME, name);
}

template<typename T, typename... Args>
void
OpenCLKernelArgInfo::
doField(cl_uint what, T & where, Args&&... args)
{
    clInfoCall(clGetKernelArgInfo, kernel, argNum, what, where, this,
               std::forward<Args>(args)...);
}

DEFINE_STRUCTURE_DESCRIPTION_INLINE(OpenCLKernelArgInfo)
{
    addField("addressQualifier", &OpenCLKernelArgInfo::addressQualifier,
             "");
    addField("accessQualifier", &OpenCLKernelArgInfo::accessQualifier,
             "");
    addField("typeName", &OpenCLKernelArgInfo::typeName, "");
    addField("typeQualifier", &OpenCLKernelArgInfo::typeQualifier, "");
    addField("name", &OpenCLKernelArgInfo::name, "");
}

OpenCLKernelInfo::
OpenCLKernelInfo(cl_kernel kernel)
    : kernel(kernel)
{
    doField(CL_KERNEL_FUNCTION_NAME, functionName);
    doField(CL_KERNEL_NUM_ARGS, numArgs);
    static const std::regex splitAttributesOn("[^ ]+");
    doField(CL_KERNEL_ATTRIBUTES, attributes, splitAttributesOn);
    
    args.reserve(numArgs);
    for (size_t i = 0;  i < numArgs;  ++i) {
        args.emplace_back(kernel, i);
    }
}

template<typename T, typename... Args>
void
OpenCLKernelInfo::
doField(cl_uint what, T & where, Args&&... args)
{
    clInfoCall(clGetKernelInfo, kernel, what, where, this,
               std::forward<Args>(args)...);
}

DEFINE_STRUCTURE_DESCRIPTION_INLINE(OpenCLKernelInfo)
{
    addField("functionName", &OpenCLKernelInfo::functionName, "");
    addField("numArgs", &OpenCLKernelInfo::numArgs, "");
    addField("attributes", &OpenCLKernelInfo::attributes, "");
    addField("args", &OpenCLKernelInfo::args, "");
}


/*****************************************************************************/
/* OPENCL KERNEL WORKGROUP INFO                                              */
/*****************************************************************************/

OpenCLKernelWorkgroupInfo::
OpenCLKernelWorkgroupInfo(cl_kernel kernel, cl_device_id device)
    : kernel(kernel), device(device)
{
    doField(CL_KERNEL_GLOBAL_WORK_SIZE, globalWorkSize,
            OpenCLIgnoreExceptions());
    doField(CL_KERNEL_WORK_GROUP_SIZE, workGroupSize);
    doField(CL_KERNEL_COMPILE_WORK_GROUP_SIZE, compileWorkGroupSize);
    doField(CL_KERNEL_LOCAL_MEM_SIZE, localMemSize);
    doField(CL_KERNEL_PREFERRED_WORK_GROUP_SIZE_MULTIPLE,
            preferredWorkGroupSizeMultiple);
    doField(CL_KERNEL_PRIVATE_MEM_SIZE, privateMemSize);
}

template<typename T, typename... Args>
void
OpenCLKernelWorkgroupInfo::
doField(cl_uint what, T & where, Args&&... args)
{
    //cerr << "doField at " << ((const char *)(&where) - (const char *)this)
    //     << endl;
    try {
        clInfoCall(clGetKernelWorkGroupInfo, kernel, device, what, where, this,
                   std::forward<Args>(args)...);
    } MLDB_CATCH_ALL {
        cerr << "failing call was " << what << endl;
        throw;
    }
}

DEFINE_STRUCTURE_DESCRIPTION_INLINE(OpenCLKernelWorkgroupInfo)
{
    addField("workGroupSize", &OpenCLKernelWorkgroupInfo::workGroupSize, "");
    addField("compileWorkGroupSize",
             &OpenCLKernelWorkgroupInfo::compileWorkGroupSize, "");
    addField("localMemSize", &OpenCLKernelWorkgroupInfo::localMemSize, "");
    addField("preferredWorkGroupSizeMultiple",
             &OpenCLKernelWorkgroupInfo::preferredWorkGroupSizeMultiple, "");
    addField("privateMemSize", &OpenCLKernelWorkgroupInfo::privateMemSize, "");
    addField("globalWorkSize", &OpenCLKernelWorkgroupInfo::globalWorkSize, "");
}


/*****************************************************************************/
/* OPENCL EVENT INFO                                                         */
/*****************************************************************************/

DEFINE_ENUM_DESCRIPTION_INLINE(OpenCLEventCommandType)
{
#define DO_VALUE(name) addValue(#name, OpenCLEventCommandType::name)
    DO_VALUE(ND_RANGE_KERNEL);
    DO_VALUE(NATIVE_KERNEL);
    DO_VALUE(READ_BUFFER);
    DO_VALUE(WRITE_BUFFER);
    DO_VALUE(COPY_BUFFER);
    DO_VALUE(READ_IMAGE);
    DO_VALUE(WRITE_IMAGE);
    DO_VALUE(COPY_IMAGE);
    DO_VALUE(COPY_BUFFER_TO_IMAGE);
    DO_VALUE(COPY_IMAGE_TO_BUFFER);
    DO_VALUE(MAP_BUFFER);
    DO_VALUE(MAP_IMAGE);
    DO_VALUE(UNMAP_MEM_OBJECT);
    DO_VALUE(MARKER);
    DO_VALUE(ACQUIRE_GL_OBJECTS);
    DO_VALUE(RELEASE_GL_OBJECTS);
    DO_VALUE(READ_BUFFER_RECT);
    DO_VALUE(WRITE_BUFFER_RECT);
    DO_VALUE(COPY_BUFFER_RECT);
    DO_VALUE(USER);
    DO_VALUE(BARRIER);
    DO_VALUE(MIGRATE_MEM_OBJECTS);
    DO_VALUE(FILL_BUFFER);
    DO_VALUE(FILL_IMAGE);
    DO_VALUE(SVM_FREE);
    DO_VALUE(SVM_MEMCPY);
    DO_VALUE(SVM_MEMFILL);
    DO_VALUE(SVM_MAP);
    DO_VALUE(SVM_UNMAP);
#undef DO_VALUE
}

DEFINE_ENUM_DESCRIPTION_INLINE(OpenCLEventCommandExecutionStatus)
{
#define DO_VALUE(name) addValue(#name, OpenCLEventCommandExecutionStatus::name)
    DO_VALUE(COMPLETE);
    DO_VALUE(QUEUED);
    DO_VALUE(SUBMITTED);
    DO_VALUE(RUNNING);
    DO_VALUE(ERROR);
#undef DO_VALUE
};

template<typename T, typename... Args>
void
OpenCLEventInfo::
doField(cl_uint what, T & where, Args&&... args)
{
    clInfoCall(clGetEventInfo, event, what, where, this,
               std::forward<Args>(args)...);
}

OpenCLEventInfo::
OpenCLEventInfo(cl_event event)
{
    doField(CL_EVENT_COMMAND_TYPE, commandType);
    doField(CL_EVENT_COMMAND_EXECUTION_STATUS, executionStatus);
    if ((int)executionStatus < 0) {
        error = (OpenCLStatus)executionStatus;
        executionStatus = OpenCLEventCommandExecutionStatus::ERROR;
    }
    else error = OpenCLStatus::SUCCESS;
}

DEFINE_STRUCTURE_DESCRIPTION_INLINE(OpenCLEventInfo)
{
    addField("commandType", &OpenCLEventInfo::commandType, "");
    addField("error", &OpenCLEventInfo::error, "");
    addField("executionStatus", &OpenCLEventInfo::executionStatus, "");
}


/*****************************************************************************/
/* OPENCL PROFILING INFO                                                     */
/*****************************************************************************/

OpenCLProfilingInfo::
OpenCLProfilingInfo(cl_event event)
    : event(event)
{
    doField(CL_PROFILING_COMMAND_QUEUED, queued);
    doField(CL_PROFILING_COMMAND_SUBMIT, submit);
    doField(CL_PROFILING_COMMAND_START, start);
    doField(CL_PROFILING_COMMAND_END, end);

    //doField(CL_PROFILING_COMMAND_COMPLETE, complete, OpenCLIgnoreExceptions());
}

template<typename T, typename... Args>
void
OpenCLProfilingInfo::
doField(cl_uint what, T & where, Args&&... args)
{
    clInfoCall(clGetEventProfilingInfo, event, what, where, this,
               std::forward<Args>(args)...);
}

DEFINE_STRUCTURE_DESCRIPTION_INLINE(OpenCLProfilingInfo)
{
    addField("queued", &OpenCLProfilingInfo::queued, "");
    addField("submit", &OpenCLProfilingInfo::submit, "");
    addField("start", &OpenCLProfilingInfo::start, "");
    addField("end", &OpenCLProfilingInfo::end, "");
    addField("complete", &OpenCLProfilingInfo::complete, "");
}


/*****************************************************************************/
/* OPENCL COMMAND QUEUE                                                      */
/*****************************************************************************/

DEFINE_ENUM_DESCRIPTION_INLINE(OpenCLCommandQueueProperties)
{
    addValue("OUT_OF_ORDER_EXEC_MODE_ENABLE",
             OpenCLCommandQueueProperties::OUT_OF_ORDER_EXEC_MODE_ENABLE);
    addValue("PROFILING_ENABLE",
             OpenCLCommandQueueProperties::PROFILING_ENABLE);
}

/*****************************************************************************/
/* OPENCL MEM OBJECT                                                         */
/*****************************************************************************/

size_t
OpenCLMemObject::
size() const
{
    size_t res;
    auto ret = clGetMemObjectInfo(buffer, CL_MEM_SIZE, sizeof(res), &res, nullptr);
    checkOpenCLError(ret, "clGetMemObjectInfo CL_MEM_SIZE");
    return res;
}

OpenCLContext
OpenCLMemObject::
context() const
{
    cl_context res;
    auto ret = clGetMemObjectInfo(buffer, CL_MEM_CONTEXT, sizeof(res), &res, nullptr);
    checkOpenCLError(ret, "clGetMemObjectInfo CL_MEM_CONTEXT");
    return res;
}

std::vector<OpenCLDevice>
OpenCLMemObject::
devices() const
{
    return context().getDevices();
}


} // namespace MLDB
