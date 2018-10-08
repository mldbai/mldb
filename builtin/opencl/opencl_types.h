/** opencl_types.h                                                 -*- C++ -*-
    Jeremy Barnes, 21 September 2017
    Copyright (c) 2017 Element AI Inc.  All rights reserved.
    This file is part of MLDB. Copyright 2015 mldb.ai inc. All rights reserved.

    OpenCL plugin, to allow execution of OpenCL code and OpenCL functions
    to be defined.
*/

#pragma once

#include "bitset.h"
#include <vector>
#include <array>
#include <string>
#include "mldb/types/annotated_exception.h"
#include <CL/cl.h>

namespace MLDB {

std::string clGetErrorString(int errorCode);


/*****************************************************************************/
/* OPENCL STATUS                                                             */
/*****************************************************************************/

// Wrap the OpenCL error codes so that we understand them
enum class OpenCLStatus : cl_int {
    // ... content ...
};

DECLARE_ENUM_DESCRIPTION(OpenCLStatus);

struct OpenCLException: public AnnotatedException {
    template<typename... Args>
    OpenCLException(cl_int returnCode,
                    const Utf8String & call,
                    Args&&... args)
        : AnnotatedException(returnCode == CL_SUCCESS ? 200 : 400,
                             "OpenCL error: " + call + ": "
                             + printCode(returnCode),
                             "returnCode", (OpenCLStatus)returnCode,
                             "call", call,
                             std::forward<Args>(args)...)
    {
    }

    static std::string printCode(cl_int returnCode);
};

void checkOpenCLError(cl_int returnCode,
                      const char * operation);
void checkOpenCLError(cl_int returnCode,
                      const std::string & operation);


/*****************************************************************************/
/* OPENCL DEVICE ENUMS                                                       */
/*****************************************************************************/

enum class OpenCLFpConfig: cl_bitfield {
    DENORM = CL_FP_DENORM,
    INF_NAN = CL_FP_INF_NAN,
    ROUND_TO_NEAREST = CL_FP_ROUND_TO_NEAREST,
    ROUND_TO_ZERO = CL_FP_ROUND_TO_ZERO,
    ROUND_TO_INF = CL_FP_ROUND_TO_INF,
    FMA = CL_FP_FMA,
    SOFT_FLOAT = CL_FP_SOFT_FLOAT,
    CORRECTLY_ROUNDED_DIVIDE_SQRT = CL_FP_CORRECTLY_ROUNDED_DIVIDE_SQRT        
};

DECLARE_ENUM_DESCRIPTION(OpenCLFpConfig);

enum class OpenCLCacheType: cl_uint {
    NONE = CL_NONE,
    READ_ONLY = CL_READ_ONLY_CACHE,
    READ_WRITE = CL_READ_WRITE_CACHE
};

DECLARE_ENUM_DESCRIPTION(OpenCLCacheType);

enum class OpenCLExecutionCapabilities: cl_bitfield {
    NONE = CL_NONE,
    KERNEL = CL_EXEC_KERNEL,
    NATIVE_KERNEL = CL_EXEC_NATIVE_KERNEL
};

DECLARE_ENUM_DESCRIPTION(OpenCLExecutionCapabilities);

enum class OpenCLLocalMemoryType: cl_uint {
    NONE = CL_NONE,
    LOCAL = CL_LOCAL,
    GLOBAL = CL_GLOBAL
};

DECLARE_ENUM_DESCRIPTION(OpenCLLocalMemoryType);

enum class OpenCLPartitionProperty: cl_uint {
    EQUALLY = CL_DEVICE_PARTITION_EQUALLY,
    BY_COUNTS = CL_DEVICE_PARTITION_BY_COUNTS,
    BY_COUNTS_LIST_END = CL_DEVICE_PARTITION_BY_COUNTS_LIST_END,
    BY_AFFINITY_DOMAIN = CL_DEVICE_PARTITION_BY_AFFINITY_DOMAIN
};

DECLARE_ENUM_DESCRIPTION(OpenCLPartitionProperty);

enum class OpenCLPartitionAffinityDomain: cl_bitfield {
    NUMA = CL_DEVICE_AFFINITY_DOMAIN_NUMA,
    L4_CACHE = CL_DEVICE_AFFINITY_DOMAIN_L4_CACHE,
    L3_CACHE = CL_DEVICE_AFFINITY_DOMAIN_L3_CACHE,
    L2_CACHE = CL_DEVICE_AFFINITY_DOMAIN_L2_CACHE,
    L1_CACHE = CL_DEVICE_AFFINITY_DOMAIN_L1_CACHE,
    NEXT_PARTITIONABLE = CL_DEVICE_AFFINITY_DOMAIN_NEXT_PARTITIONABLE
};

DECLARE_ENUM_DESCRIPTION(OpenCLPartitionAffinityDomain)

enum class OpenCLDeviceQueueProperties: cl_bitfield {
    OUT_OF_ORDER_EXEC_MODE_ENABLE = CL_QUEUE_OUT_OF_ORDER_EXEC_MODE_ENABLE,
    PROFILING_ENABLE = CL_QUEUE_PROFILING_ENABLE,
    ON_DEVICE = CL_QUEUE_ON_DEVICE,
    ON_DEVICE_DEFAULT = CL_QUEUE_ON_DEVICE_DEFAULT
};

DECLARE_ENUM_DESCRIPTION(OpenCLDeviceQueueProperties);

enum class OpenCLDeviceType: cl_bitfield {
    DEFAULT = CL_DEVICE_TYPE_DEFAULT,
    CPU = CL_DEVICE_TYPE_CPU,
    GPU = CL_DEVICE_TYPE_GPU,
    ACCELERATOR = CL_DEVICE_TYPE_ACCELERATOR,
    CUSTOM = CL_DEVICE_TYPE_CUSTOM
};

DECLARE_ENUM_DESCRIPTION(OpenCLDeviceType);

enum class OpenCLDeviceSvmCapabilities: cl_bitfield {
    COARSE_GRAIN_BUFFER = CL_DEVICE_SVM_COARSE_GRAIN_BUFFER,
    FINE_GRAIN_BUFFER = CL_DEVICE_SVM_FINE_GRAIN_BUFFER,
    FINE_GRAIN_SYSTEM = CL_DEVICE_SVM_FINE_GRAIN_SYSTEM,
    ATOMICS = CL_DEVICE_SVM_ATOMICS
};

DECLARE_ENUM_DESCRIPTION(OpenCLDeviceSvmCapabilities);

#define CL_DEVICE_HALF_FP_CONFIG 0x1033


/*****************************************************************************/
/* OPENCL DEVICE INFO                                                        */
/*****************************************************************************/

struct OpenCLDeviceInfo {
    OpenCLDeviceInfo()
    {
    }

    OpenCLDeviceInfo(cl_device_id device);

    cl_device_id device = nullptr;

    template<typename T, typename... Args>
    void doField(cl_uint what, T & where, Args&&... args);

    template<typename T, size_t N>
    void doField(cl_device_info what, std::array<T, N> & where);
    
#if 0
    template<typename T>
    void doField(cl_device_info id, T & val,
                 const char * call,
                 std::enable_if<std::is_pod<T>::value> * = 0);

    void doField(cl_device_info what, std::string & where,
                 const char * call);

    void doField(cl_device_info what, std::vector<size_t> & where,
                 const char * call);

    void doField(cl_device_info what, std::vector<std::string> & where,
                 const char * call);

    template<typename T, size_t N>
    void doField(cl_device_info what, std::array<T, N> & where,
                 const char * call);
#endif
    
    cl_uint addressBits = 0;
    cl_bool available = false;
    std::vector<std::string> builtInKernels;
    cl_bool compilerAvailable = false;
    Bitset<OpenCLFpConfig> doubleFpConfig;
    cl_bool endianLittle = true;
    cl_bool errorCorrection = false;
    Bitset<OpenCLExecutionCapabilities> executionCapabilities;
    std::vector<std::string> extensions;
    cl_ulong globalMemCacheSize = 0;
    OpenCLCacheType globalMemCacheType = OpenCLCacheType::NONE;
    cl_uint globalMemCacheLineSize = 0;
    size_t globalMemSize = 0;
    Bitset<OpenCLFpConfig> halfFpConfig;
    cl_bool unifiedMemory = false;
    cl_bool imageSupport = false;
    std::array<size_t, 2> image2dMaxDimensions = { {0, 0} };
    std::array<size_t, 3> image3dMaxDimensions = { {0, 0, 0} };
    size_t imageMaxBufferSize = 0;
    size_t imageMaxArraySize = 0;
    cl_bool linkerAvailable = true;
    cl_ulong localMemSize = 0;
    OpenCLLocalMemoryType localMemType = OpenCLLocalMemoryType::NONE;
    cl_uint maxClockFrequency = 0;
    cl_uint maxComputeUnits = 0;
    cl_uint maxConstantArgs = 0;
    cl_ulong maxConstantBufferSize = 0;
    cl_ulong maxMemAllocSize = 0;
    size_t maxParameterSize = 0;
    cl_uint maxReadImageArgs = 0;
    cl_uint maxSamplers = 0;
    size_t maxWorkGroupSize = 0;
    cl_uint maxWorkItemDimensions = 0;
    std::vector<size_t> maxWorkItemSizes;
    cl_uint maxWriteImageArgs = 0;
    cl_uint memBaseAddrAlign = 0;
    std::string name;
    std::array<cl_uint, 7> nativeVectorWidth;
    std::string openCLCVersion;
    cl_uint partitionMaxSubDevices = 0;
    std::vector<OpenCLPartitionProperty> partitionProperties;
    Bitset<OpenCLPartitionAffinityDomain> partitionAffinityDomain;
    std::vector<OpenCLPartitionProperty> partitionType;
    std::array<cl_uint, 7> preferredVectorWidth;
    size_t printfBufferSize = 0;
    cl_bool preferredInteropUserSync = false;
    std::string profile;
    size_t profilingTimerResolution = 0;
    Bitset<OpenCLDeviceQueueProperties> queueProperties;
    cl_uint referenceCount = 0;
    Bitset<OpenCLFpConfig> singleFpConfig;
    Bitset<OpenCLDeviceType> type;
    std::string vendor;
    cl_uint vendorId = 0;
    std::string version;
    std::string driverVersion;

    cl_uint imagePitchAlignment = 0;
    cl_uint imageBaseAddressAlignment = 0;
    Bitset<OpenCLDeviceSvmCapabilities> svmCapabilities;
    cl_uint maxReadWriteImageArgs = 0;
    size_t maxGlobalVariableSize = 0;
    size_t globalVariablePreferredTotalSize = 0;
    cl_uint pipeMaxActiveReservations = 0;
    cl_uint pipeMaxPacketSize = 0;
    cl_uint maxOnDeviceQueues = 0;
    cl_uint maxOnDeviceEvents = 0;
    cl_uint queueOnDeviceMaxSize = 0;
    cl_uint queueOnDevicePreferredSize = 0;
    Bitset<OpenCLDeviceQueueProperties> queueOnDeviceProperties;
    cl_uint maxPipeArgs = 0;

    cl_uint preferredPlatformAtomicAlignment = 0;
    cl_uint preferredGlobalAtomicAlignment = 0;
    cl_uint preferredLocalAtomicAlignment = 0;
};

DECLARE_STRUCTURE_DESCRIPTION(OpenCLDeviceInfo);


/*****************************************************************************/
/* OPENCL PLATFORM INFO                                                      */
/*****************************************************************************/

struct OpenCLPlatformInfo {
    OpenCLPlatformInfo()
    {
    }

    OpenCLPlatformInfo(cl_platform_id platform);

    cl_platform_id platform = nullptr;
    
    std::string profile;
    std::string version;
    std::string name;
    std::string vendor;
    std::set<std::string> extensions;

private:
    void get(cl_platform_info what, std::string & where, const char * call);
};

DECLARE_STRUCTURE_DESCRIPTION(OpenCLPlatformInfo);


/*****************************************************************************/
/* OPENCL PROGRAM BUILD INFO                                                 */
/*****************************************************************************/

enum class OpenCLBuildStatus: cl_int {
    NONE = CL_BUILD_NONE,
    ERROR = CL_BUILD_ERROR,
    SUCCESS = CL_BUILD_SUCCESS,
    IN_PROGRESS = CL_BUILD_IN_PROGRESS
};

DECLARE_ENUM_DESCRIPTION(OpenCLBuildStatus);

enum class OpenCLBinaryType: cl_uint {
    NONE = CL_PROGRAM_BINARY_TYPE_NONE,
    COMPILED_OBJECT = CL_PROGRAM_BINARY_TYPE_COMPILED_OBJECT,
    LIBRARY = CL_PROGRAM_BINARY_TYPE_LIBRARY,
    EXECUTABLE = CL_PROGRAM_BINARY_TYPE_EXECUTABLE
};

DECLARE_ENUM_DESCRIPTION(OpenCLBinaryType);

struct OpenCLProgramBuildInfo {
    OpenCLProgramBuildInfo()
    {
    }

    OpenCLProgramBuildInfo(cl_program program, cl_device_id device);

    template<typename T, typename... Args>
    void doField(cl_uint what, T & where, Args&&... args);

    cl_program program = nullptr;
    cl_device_id device = nullptr;

    OpenCLBuildStatus buildStatus = OpenCLBuildStatus::NONE;
    std::string buildOptions;
    std::string buildLog;
    OpenCLBinaryType binaryType = OpenCLBinaryType::NONE;
};

DECLARE_STRUCTURE_DESCRIPTION(OpenCLProgramBuildInfo);


/*****************************************************************************/
/* OPENCL KERNEL INFO                                                        */
/*****************************************************************************/

enum class OpenCLArgAddressQualifier: cl_uint {
    GLOBAL = CL_KERNEL_ARG_ADDRESS_GLOBAL,
    LOCAL = CL_KERNEL_ARG_ADDRESS_LOCAL,
    CONSTANT = CL_KERNEL_ARG_ADDRESS_CONSTANT,
    PRIVATE = CL_KERNEL_ARG_ADDRESS_PRIVATE
};

DECLARE_ENUM_DESCRIPTION(OpenCLArgAddressQualifier);

enum class OpenCLArgAccessQualifier: cl_uint {
    READ_ONLY = CL_KERNEL_ARG_ACCESS_READ_ONLY,
    WRITE_ONLY = CL_KERNEL_ARG_ACCESS_WRITE_ONLY,
    READ_WRITE = CL_KERNEL_ARG_ACCESS_READ_WRITE,
    NONE = CL_KERNEL_ARG_ACCESS_NONE
};

DECLARE_ENUM_DESCRIPTION(OpenCLArgAccessQualifier);

enum class OpenCLArgTypeQualifier: cl_bitfield {
    NONE = CL_KERNEL_ARG_TYPE_NONE,
    CONST = CL_KERNEL_ARG_TYPE_CONST,
    RESTRICT = CL_KERNEL_ARG_TYPE_RESTRICT,
    VOLATILE = CL_KERNEL_ARG_TYPE_VOLATILE,
    PIPE = CL_KERNEL_ARG_TYPE_PIPE
};

DECLARE_ENUM_DESCRIPTION(OpenCLArgTypeQualifier);

struct OpenCLKernelArgInfo;

DECLARE_STRUCTURE_DESCRIPTION(OpenCLKernelArgInfo);

struct OpenCLKernelArgInfo {
    OpenCLKernelArgInfo()
    {
    }

    OpenCLKernelArgInfo(cl_kernel kernel,
                        cl_uint argNum);

    template<typename T, typename... Args>
    void doField(cl_uint what, T & where, Args&&... args);

    cl_kernel kernel = nullptr;
    cl_uint argNum = -1;
    OpenCLArgAddressQualifier addressQualifier;
    OpenCLArgAccessQualifier accessQualifier;
    std::string typeName;
    Bitset<OpenCLArgTypeQualifier> typeQualifier;
    std::string name;
};


struct OpenCLKernelInfo;

DECLARE_STRUCTURE_DESCRIPTION(OpenCLKernelInfo);

struct OpenCLKernelInfo {

    OpenCLKernelInfo()
    {
    }
    
    OpenCLKernelInfo(cl_kernel kernel);

    template<typename T, typename... Args>
    void doField(cl_uint what, T & where, Args&&... args);

    cl_kernel kernel = nullptr;
    std::string functionName;
    cl_uint numArgs = 0;
    std::vector<std::string> attributes;
    std::vector<OpenCLKernelArgInfo> args;
};


/*****************************************************************************/
/* OPENCL PROFILING INFO                                                     */
/*****************************************************************************/

struct OpenCLProfilingInfo {
    OpenCLProfilingInfo()
    {
    }

    OpenCLProfilingInfo(cl_event event);

    template<typename T, typename... Args>
    void doField(cl_uint what, T & where, Args&&... args);

    cl_event event = nullptr;
    cl_ulong queued = 0;
    cl_ulong submit = 0;
    cl_ulong start = 0;
    cl_ulong end = 0;
    cl_ulong complete = 0;
};

DECLARE_STRUCTURE_DESCRIPTION(OpenCLProfilingInfo);

} // namespace MLDB
