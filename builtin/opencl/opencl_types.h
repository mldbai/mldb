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
#include <iostream>
#include <CL/cl.h>

namespace MLDB {

std::string clGetErrorString(int errorCode);
struct OpenCLContext;
struct OpenCLCommandQueue;

OpenCLContext openCLCreateContext(cl_context context);

/*****************************************************************************/
/* OPENCL STATUS                                                             */
/*****************************************************************************/

// Wrap the OpenCL error codes so that we understand them
enum class OpenCLStatus : cl_int {
    SUCCESS = CL_SUCCESS
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
    std::array<uint32_t /*cl_uint*/, 7> nativeVectorWidth;
    std::string openCLCVersion;
    cl_uint partitionMaxSubDevices = 0;
    std::vector<OpenCLPartitionProperty> partitionProperties;
    Bitset<OpenCLPartitionAffinityDomain> partitionAffinityDomain;
    std::vector<OpenCLPartitionProperty> partitionType;
    std::array<uint32_t /*cl_uint*/, 7> preferredVectorWidth;
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
    std::vector<std::string> buildLog;
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
    OpenCLArgAddressQualifier addressQualifier
        = OpenCLArgAddressQualifier::GLOBAL;
    OpenCLArgAccessQualifier accessQualifier
        = OpenCLArgAccessQualifier::READ_ONLY;
    std::string typeName;
    Bitset<OpenCLArgTypeQualifier> typeQualifier;
    std::string name;
};


struct OpenCLKernelInfo {

    OpenCLKernelInfo() = default;
    OpenCLKernelInfo(cl_kernel kernel);

    template<typename T, typename... Args>
    void doField(cl_uint what, T & where, Args&&... args);

    cl_kernel kernel = nullptr;
    std::string functionName;
    cl_uint numArgs = 0;
    std::vector<std::string> attributes;
    std::vector<OpenCLKernelArgInfo> args;
};

DECLARE_STRUCTURE_DESCRIPTION(OpenCLKernelInfo);


/*****************************************************************************/
/* OPENCL KERNEL WORKGROUP INFO                                              */
/*****************************************************************************/

struct OpenCLKernelWorkgroupInfo {

    OpenCLKernelWorkgroupInfo() = default;
    OpenCLKernelWorkgroupInfo(cl_kernel kernel, cl_device_id device);

    template<typename T, typename... Args>
    void doField(cl_uint what, T & where, Args&&... args);

    cl_kernel kernel = nullptr;
    cl_device_id device = nullptr;
    
    std::array<size_t, 3> globalWorkSize = { 0, 0, 0 };
    size_t workGroupSize = 0;
    std::array<size_t, 3> compileWorkGroupSize = { 0, 0, 0 };
    cl_ulong localMemSize = 0;
    size_t preferredWorkGroupSizeMultiple = 0;
    cl_ulong privateMemSize = 0;
};

DECLARE_STRUCTURE_DESCRIPTION(OpenCLKernelWorkgroupInfo);


/*****************************************************************************/
/* OPENCL EVENT INFO                                                         */
/*****************************************************************************/

enum OpenCLEventCommandType : cl_uint {
    ND_RANGE_KERNEL = CL_COMMAND_NDRANGE_KERNEL,
    NATIVE_KERNEL = CL_COMMAND_NATIVE_KERNEL,
    READ_BUFFER = CL_COMMAND_READ_BUFFER,
    WRITE_BUFFER = CL_COMMAND_WRITE_BUFFER,
    COPY_BUFFER = CL_COMMAND_COPY_BUFFER,
    READ_IMAGE = CL_COMMAND_READ_IMAGE,
    WRITE_IMAGE = CL_COMMAND_WRITE_IMAGE,
    COPY_IMAGE = CL_COMMAND_COPY_IMAGE,
    COPY_BUFFER_TO_IMAGE = CL_COMMAND_COPY_BUFFER_TO_IMAGE,
    COPY_IMAGE_TO_BUFFER = CL_COMMAND_COPY_IMAGE_TO_BUFFER,
    MAP_BUFFER = CL_COMMAND_MAP_BUFFER,
    MAP_IMAGE = CL_COMMAND_MAP_IMAGE,
    UNMAP_MEM_OBJECT = CL_COMMAND_UNMAP_MEM_OBJECT,
    MARKER = CL_COMMAND_MARKER,
    ACQUIRE_GL_OBJECTS = CL_COMMAND_ACQUIRE_GL_OBJECTS,
    RELEASE_GL_OBJECTS = CL_COMMAND_RELEASE_GL_OBJECTS,
    READ_BUFFER_RECT = CL_COMMAND_READ_BUFFER_RECT,
    WRITE_BUFFER_RECT = CL_COMMAND_WRITE_BUFFER_RECT,
    COPY_BUFFER_RECT = CL_COMMAND_COPY_BUFFER_RECT,
    USER = CL_COMMAND_USER,
    BARRIER = CL_COMMAND_BARRIER,
    MIGRATE_MEM_OBJECTS = CL_COMMAND_MIGRATE_MEM_OBJECTS,
    FILL_BUFFER = CL_COMMAND_FILL_BUFFER,
    FILL_IMAGE = CL_COMMAND_FILL_IMAGE,
    SVM_FREE = CL_COMMAND_SVM_FREE,
    SVM_MEMCPY = CL_COMMAND_SVM_MEMCPY,
    SVM_MEMFILL = CL_COMMAND_SVM_MEMFILL,
    SVM_MAP = CL_COMMAND_SVM_MAP,
    SVM_UNMAP = CL_COMMAND_SVM_UNMAP
};

DECLARE_ENUM_DESCRIPTION(OpenCLEventCommandType);

enum OpenCLEventCommandExecutionStatus: cl_int {
    COMPLETE = CL_COMPLETE,
    QUEUED = CL_QUEUED,
    SUBMITTED = CL_SUBMITTED,
    RUNNING = CL_RUNNING,
    ERROR = 1000 /* extension */
};

DECLARE_ENUM_DESCRIPTION(OpenCLEventCommandExecutionStatus);

struct OpenCLEventInfo {

    OpenCLEventInfo() = default;
    OpenCLEventInfo(cl_event event);

    template<typename T, typename... Args>
    void doField(cl_uint what, T & where, Args&&... args);

    OpenCLCommandQueue getCommandQueue() const;
    OpenCLContext getContext() const;

    cl_event event = nullptr;

    OpenCLEventCommandType commandType;
    OpenCLStatus error;
    OpenCLEventCommandExecutionStatus executionStatus;
};

DECLARE_STRUCTURE_DESCRIPTION(OpenCLEventInfo);


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

    OpenCLProfilingInfo relative() const
    {
        OpenCLProfilingInfo newInfo(*this);
        if (newInfo.complete != 0) newInfo.complete -= newInfo.end;
        newInfo.end -= newInfo.start;
        newInfo.start -= newInfo.submit;
        newInfo.submit -= newInfo.queued;
        newInfo.queued = 0;
        return newInfo;
    }

    OpenCLProfilingInfo operator - (cl_ulong offset) const
    {
        OpenCLProfilingInfo result = *this;
        result.queued   -= offset;
        result.submit   -= offset;
        result.start    -= offset;
        result.end      -= offset;
        result.complete -= offset;
        return result;
    }
};

DECLARE_STRUCTURE_DESCRIPTION(OpenCLProfilingInfo);


/*****************************************************************************/
/* OPENCL NOTIFY HANDLER                                                     */
/*****************************************************************************/

template<typename Obj>
struct OpenCLNotify {
    typedef void (*notify_fn) (Obj obj, void * param);

    static void notify(Obj obj, void * param)
    {
    }
    
    operator notify_fn()
    {
        return notify;
    }

    operator void * ()
    {
        return this;
    }
};


/*****************************************************************************/
/* OPENCL DEVICE                                                             */
/*****************************************************************************/

struct OpenCLDevice {
    OpenCLDevice()
    {
    }

    OpenCLDevice(cl_device_id device)
        : device(device)
    {
    }

    cl_device_id device = nullptr;

    OpenCLDeviceInfo getDeviceInfo() const
    {
        ExcCheck(device, "Uninitialized OpenCL device");
        return OpenCLDeviceInfo(device);
    }

    operator cl_device_id () const
    {
        ExcCheck(device, "Uninitialized OpenCL device");
        return device;
    }
};


/*****************************************************************************/
/* OPENCL PLATFORM                                                           */
/*****************************************************************************/

struct OpenCLPlatform {
    OpenCLPlatform()
    {
    }
    
    OpenCLPlatform(cl_platform_id platform)
        : platform(platform)
    {
    }

    ~OpenCLPlatform()
    {
    }

    cl_platform_id platform = nullptr;

    OpenCLPlatformInfo getPlatformInfo() const
    {
        ExcCheck(platform, "Platform not initialized");
        return OpenCLPlatformInfo(platform);
    }

    OpenCLDevice getDevice(Bitset<OpenCLDeviceType> types
                           = (OpenCLDeviceType)CL_DEVICE_TYPE_ALL) const
    {
        auto devs = getDevices(1, types);
        if (devs.empty())
            throw AnnotatedException(400, "No OpenCL devices found");
        return std::move(devs[0]);
    }
    
    size_t getDeviceCount(Bitset<OpenCLDeviceType> types
                          = (OpenCLDeviceType)CL_DEVICE_TYPE_ALL) const
    {
        ExcCheck(platform, "Platform not initialized");

        cl_uint deviceIdCount = 0;

        cl_int error
            = clGetDeviceIDs (platform, (cl_device_type)types.val, 0, nullptr,
                              &deviceIdCount);
        checkOpenCLError(error, "clGetDeviceIds");

        return deviceIdCount;
    }
    
    std::vector<OpenCLDevice>
    getDevices(ssize_t count = -1,
            Bitset<OpenCLDeviceType> types
            = (OpenCLDeviceType)CL_DEVICE_TYPE_ALL) const
    {
        ExcCheck(platform, "Platform not initialized");

        cl_uint deviceIdCount = getDeviceCount(types);

        if (count != -1 && count < deviceIdCount)
            deviceIdCount = count;
        
        std::vector<cl_device_id> deviceIds (deviceIdCount);
        cl_int error
            = clGetDeviceIDs (platform, (cl_device_type)types.val,
                              deviceIdCount,
                              deviceIds.data (), nullptr);
        checkOpenCLError(error, "clGetDeviceIds");

        std::vector<OpenCLDevice> devices(deviceIds.begin(), deviceIds.end());
        return devices;
    }
};

inline std::vector<OpenCLPlatform>
getOpenCLPlatforms()
{
    cl_uint platformIdCount = 0;
    cl_int error = clGetPlatformIDs (0, nullptr, &platformIdCount);
    checkOpenCLError(error, "clGetPlatformIds");

    std::vector<cl_platform_id> platformIds (platformIdCount);
    error = clGetPlatformIDs (platformIdCount, platformIds.data(), nullptr);
    checkOpenCLError(error, "clGetPlatformIds");

    return { platformIds.begin(), platformIds.end() };
}


/*****************************************************************************/
/* OPENCL REFCOUNTED                                                         */
/*****************************************************************************/

inline void clRetain(cl_context context)
{
    cl_int res = clRetainContext(context);
    checkOpenCLError(res, "clRetainContext");
}

inline void clRelease(cl_context context)
{
    cl_int res = clReleaseContext(context);
    checkOpenCLError(res, "clReleseContext");
}

inline int clRefCount(cl_context context)
{
    cl_uint refCount;
    cl_int res = clGetContextInfo(context, CL_CONTEXT_REFERENCE_COUNT,
                                  sizeof(refCount), &refCount, nullptr);
    checkOpenCLError(res, "clGetContextInfo CL_CONTEXT_REFERENCE_COUNT");
    return refCount;
}

inline void clRetain(cl_command_queue queue)
{
    cl_int res = clRetainCommandQueue(queue);
    checkOpenCLError(res, "clRetainQueue");
}

inline void clRelease(cl_command_queue queue)
{
    cl_int res = clReleaseCommandQueue(queue);
    checkOpenCLError(res, "clReleseQueue");
}

inline int clRefCount(cl_command_queue command_queue)
{
    cl_uint refCount;
    cl_int res = clGetCommandQueueInfo(command_queue, CL_QUEUE_REFERENCE_COUNT,
                                  sizeof(refCount), &refCount, nullptr);
    checkOpenCLError(res, "clGetCommandQueueInfo CL_QUEUE_REFERENCE_COUNT");
    return refCount;
}

inline void clRetain(cl_event event)
{
    cl_int res = clRetainEvent(event);
    checkOpenCLError(res, "clRetainEvent");
}

inline void clRelease(cl_event event)
{
    cl_int res = clReleaseEvent(event);
    checkOpenCLError(res, "clReleaseEvent");
}

inline int clRefCount(cl_event event)
{
    cl_uint refCount;
    cl_int res = clGetEventInfo(event, CL_EVENT_REFERENCE_COUNT,
                                  sizeof(refCount), &refCount, nullptr);
    checkOpenCLError(res, "clGetEventInfo CL_EVENT_REFERENCE_COUNT");
    return refCount;
}

inline void clRetain(cl_program program)
{
    cl_int res = clRetainProgram(program);
    checkOpenCLError(res, "clRetainProgram");
}

inline void clRelease(cl_program program)
{
    cl_int res = clReleaseProgram(program);
    checkOpenCLError(res, "clReleaseProgram");
}

inline int clRefCount(cl_program program)
{
    cl_uint refCount;
    cl_int res = clGetProgramInfo(program, CL_PROGRAM_REFERENCE_COUNT,
                                  sizeof(refCount), &refCount, nullptr);
    checkOpenCLError(res, "clGetProgramInfo CL_PROGRAM_REFERENCE_COUNT");
    return refCount;
}

inline void clRetain(cl_kernel kernel)
{
    cl_int res = clRetainKernel(kernel);
    checkOpenCLError(res, "clRetainKernel");
}

inline void clRelease(cl_kernel kernel)
{
    cl_int res = clReleaseKernel(kernel);
    checkOpenCLError(res, "clReleaseKernel");
}

inline int clRefCount(cl_kernel kernel)
{
    cl_uint refCount;
    cl_int res = clGetKernelInfo(kernel, CL_KERNEL_REFERENCE_COUNT,
                                 sizeof(refCount), &refCount, nullptr);
    checkOpenCLError(res, "clGetKernelInfo CL_KERNEL_REFERENCE_COUNT");
    return refCount;
}

inline void clRetain(cl_mem buffer)
{
    cl_int res = clRetainMemObject(buffer);
    checkOpenCLError(res, "clRetainMemObject");
}

inline void clRelease(cl_mem buffer)
{
    cl_int res = clReleaseMemObject(buffer);
    checkOpenCLError(res, "clReleaseMemObject");
}

inline int clRefCount(cl_mem buffer)
{
    cl_uint refCount;
    cl_int res = clGetMemObjectInfo(buffer, CL_MEM_REFERENCE_COUNT,
                                    sizeof(refCount), &refCount, nullptr);
    checkOpenCLError(res, "clGetMemObjectInfo CL_MEM_REFERENCE_COUNT");
    return refCount;
}

template<typename Handle>
struct OpenCLRefCounted {
    Handle handle = nullptr;

    OpenCLRefCounted()
    {
    }
    
    OpenCLRefCounted(Handle handle, bool alreadyRetained)
        : handle(handle)
    {
        if (!alreadyRetained)
            clRetain(handle);
    }

    OpenCLRefCounted(OpenCLRefCounted && other)
        : handle(other.handle)
    {
        other.handle = nullptr;
    }

    OpenCLRefCounted(const OpenCLRefCounted & other)
        : OpenCLRefCounted(other.handle, false /* already retained */)
    {
    }

    OpenCLRefCounted & operator = (OpenCLRefCounted && other)
    {
        OpenCLRefCounted newMe(std::move(other));
        swap(newMe);
        return *this;
    }
    
    OpenCLRefCounted & operator = (const OpenCLRefCounted & other)
    {
        OpenCLRefCounted newMe(other);
        swap(newMe);
        return *this;
    }

    void reset(Handle handle, bool alreadyRetained)
    {
        OpenCLRefCounted newMe(handle, alreadyRetained);
        *this = std::move(newMe);
    }
    
#if 0    
    OpenCLRefCounted(cl_device_id device)
        : OpenCLRefCounted(&device, 1 /* numDevices */)
    {
    }
#endif
    
    ~OpenCLRefCounted()
    {
        if (!handle)
            return;

        clRelease(handle);
    }

    void swap(OpenCLRefCounted & other)
    {
        std::swap(handle, other.handle);
    }

    operator Handle() const
    {
        ExcCheck(handle, "Use of uninitialized OpenCL handle "
                 + MLDB::type_name<Handle>());
        return handle;
    }

    int referenceCount() const
    {
        return clRefCount(handle);
    }
};


/*****************************************************************************/
/* OPENCL EVENT                                                              */
/*****************************************************************************/

struct OpenCLEvent {
    OpenCLRefCounted<cl_event> event;

    OpenCLEvent()
    {
    }

    OpenCLEvent(cl_event event)
        : event(event, true /* already retained */)
    {
    }

    // Used to pass to functions that need a place to put their event.
    // This will return a pointer to its own handle, first clearing the
    // current event.
    operator cl_event * ()
    {
        OpenCLRefCounted<cl_event> oldEvent;
        event.swap(oldEvent);
        return &event.handle;
    }

    void waitUntilFinished() const
    {
        event.operator cl_event();  // check it's not null

        cl_int error
            = clWaitForEvents(1 /* numEvents */,
                              &event.handle);
        checkOpenCLError(error, "clWaitForEvents");
    }

    OpenCLProfilingInfo getProfilingInfo() const
    {
        return OpenCLProfilingInfo(event.operator cl_event());
    }

    OpenCLEventCommandExecutionStatus getStatus() const
    {
        ExcCheck(event, "Uninitialized OpenCL event");
        cl_int result;
        cl_int res
            = clGetEventInfo(event, CL_EVENT_COMMAND_EXECUTION_STATUS,
                             sizeof(result), &result, nullptr);
        checkOpenCLError(res, "clGetEventInfo(EVENT_COMMAND_EXECUTION_STATUS)");

        if (result < 0)
            return OpenCLEventCommandExecutionStatus::ERROR;
        return (OpenCLEventCommandExecutionStatus)result;
    }

    OpenCLStatus getError() const
    {
        ExcCheck(event, "Uninitialized OpenCL event");
        cl_int result;
        cl_int res
            = clGetEventInfo(event, CL_EVENT_COMMAND_EXECUTION_STATUS,
                             sizeof(result), &result, nullptr);
        checkOpenCLError(res, "clGetEventInfo(EVENT_COMMAND_EXECUTION_STATUS)");

        if (result < 0)
            return OpenCLStatus(result);

        if (result == OpenCLEventCommandExecutionStatus::COMPLETE)
            return OpenCLStatus::SUCCESS;

        if (getStatus() != OpenCLEventCommandExecutionStatus::COMPLETE) {
            throw OpenCLException(CL_INVALID_EVENT,
                                  "OpenCLEvent::getError(): operation is not complete: " + jsonEncode((OpenCLEventCommandExecutionStatus)result).asString());
        }
        
        return (OpenCLStatus)result;
    }

    void assertSuccess() const
    {
        if (getError() != OpenCLStatus::SUCCESS) {
            throw OpenCLException((cl_int)getError(),
                                  "OpenCLEvent::assertSuccess(): operation failed: ",
                                  "info", getInfo());
        }
        if (getStatus() != OpenCLEventCommandExecutionStatus::COMPLETE) {
            throw OpenCLException(CL_INVALID_EVENT,
                                  "OpenCLEvent::assertSuccess(): operation is not complete: " + jsonEncode(getStatus()).asString(),
                                  "info", getInfo());
        }
    }
    
    OpenCLEventInfo getInfo() const
    {
        return OpenCLEventInfo(event);
    }

    typedef std::function<void (const OpenCLEvent & event, cl_int status)> Callback;
    
    struct CallbackInfo {
        Callback callback;

        // If we set a callback, keep
        OpenCLRefCounted<cl_event> event;
    };
    
    static void doCallback(cl_event event, cl_int status, void * userData)
    {
        auto info = reinterpret_cast<CallbackInfo*>(userData);

        // Ensure that it's destroyed no matter what
        std::unique_ptr<CallbackInfo> infoPtr(info);
        
        try {
            info->callback(OpenCLEvent(event), status);
        } MLDB_CATCH_ALL {
            using namespace std;
            cerr << "An exception thrown by an OpenCL callback of type "
                 << demangle(info->callback.target_type().name())
                 << " threw an exception.  This leads to immediate termination "
                 << "of the program, which is happening now." << endl;
            abort();
        }
    }
    
    void addCallback(Callback callback,
                     OpenCLEventCommandExecutionStatus status
                         = OpenCLEventCommandExecutionStatus::COMPLETE)
    {
        std::unique_ptr<CallbackInfo> info
            (new CallbackInfo({std::move(callback), event}));

        cl_int error
            = clSetEventCallback(event, status,
                                 doCallback, info.get());

        checkOpenCLError(error, "clSetEventCallback");

        info.release();
    }

    int referenceCount() const
    {
        return event.referenceCount();
    }
};


/*****************************************************************************/
/* OPENCL EVENT LIST                                                         */
/*****************************************************************************/

struct OpenCLEventList {
    OpenCLEventList()
    {
    }
    
    OpenCLEventList(const OpenCLEvent & event)
        : events({event})
    {
    }

    OpenCLEventList(OpenCLEvent && event)
        : events({std::move(event)})
    {
    }

    OpenCLEventList(std::initializer_list<OpenCLEvent> ev)
        : events(ev)
    {
    }

    // Relies on events in memory being just an array
    operator const cl_event * () const
    {
        return events.empty() ? nullptr : &events[0].event.handle;
    }

    size_t size() const
    {
        return events.size();
    }
    
    std::vector<OpenCLEvent> events;
};


/*****************************************************************************/
/* OPENCL COMMAND QUEUE                                                      */
/*****************************************************************************/

enum class OpenCLCommandQueueProperties: cl_command_queue_properties {
    OUT_OF_ORDER_EXEC_MODE_ENABLE = CL_QUEUE_OUT_OF_ORDER_EXEC_MODE_ENABLE,
    PROFILING_ENABLE = CL_QUEUE_PROFILING_ENABLE
};

DECLARE_ENUM_DESCRIPTION(OpenCLCommandQueueProperties);

struct OpenCLCommandQueue {
    OpenCLRefCounted<cl_command_queue> queue;

    OpenCLCommandQueue()
    {
    }

    OpenCLCommandQueue(cl_command_queue queue)
        : queue(queue, true /* implicitly retained */)
    {
    }
    
    operator cl_command_queue() const
    {
        return queue;
    }

    void flush()
    {
        cl_int error = clFlush(queue);
        checkOpenCLError(error, "clFlush");
    }

    void finish()
    {
        cl_int error = clFinish(queue);
        checkOpenCLError(error, "clFinish");
    }

    void wait(const OpenCLEventList & events)
    {
        cl_int error = clWaitForEvents(events.size(), events);
        checkOpenCLError(error, "clWaitForEvents");
    }
    
    OpenCLEvent launch(cl_kernel kernel,
                       std::vector<size_t> range,
                       std::vector<size_t> work = std::vector<size_t>(),
                       OpenCLEventList before = OpenCLEventList())
    {
        OpenCLEvent result;

        ExcAssert(work.empty() || work.size() == range.size());

        // TODO: check for too much local memory on enqueue failure
        //cl_ulong localMemSize = clGetKernelWorkGroupInfo(kernel, ...);
        
        cl_int error = clEnqueueNDRangeKernel
            (queue, kernel,
             range.size(),
             nullptr /* offsets */,
             range.data(),
             work.empty() ? nullptr : work.data(),
             before.size(), before,
             result);
        
        checkOpenCLError(error, "clEnqueueNDRangeKernel");

        return result;
    }

    OpenCLEvent enqueueReadBuffer(cl_mem buffer,
                                  size_t offset,
                                  size_t length,
                                  void * target,
                                  OpenCLEventList before = OpenCLEventList())
    {
        OpenCLEvent result;
        
        cl_int error
            = clEnqueueReadBuffer(queue, buffer, CL_FALSE /* blocking */,
                                  offset, length, target,
                                  before.size(), before,
                                  result);

        checkOpenCLError(error, "clEnqueueReadBuffer");

        return result;
    }

    OpenCLEvent enqueueWriteBuffer(cl_mem buffer,
                                   size_t offset,
                                   size_t length,
                                   const void * source,
                                   OpenCLEventList before = OpenCLEventList())
    {
        OpenCLEvent result;
        
        cl_int error
            = clEnqueueWriteBuffer(queue, buffer, CL_FALSE /* blocking */,
                                   offset, length, source,
                                   before.size(), before,
                                   result);

        checkOpenCLError(error, "clEnqueueWriteBuffer");

        return result;
    }

    OpenCLEvent enqueueFillBuffer(cl_mem buffer,
                                  const void * pattern,
                                  size_t patternLength,
                                  size_t offset,
                                  size_t length,
                                  OpenCLEventList before = OpenCLEventList())
    {
        OpenCLEvent result;
        
        cl_int error
            = clEnqueueFillBuffer(queue, buffer,
                                  pattern, patternLength,
                                  offset, length,
                                  before.size(), before,
                                  result);

        checkOpenCLError(error, "clEnqueueFillBuffer");

        return result;
    }

    template<typename T>
    OpenCLEvent enqueueFillBuffer(cl_mem buffer,
                                  const T & pattern,
                                  size_t offset,
                                  size_t length,
                                  OpenCLEventList before = OpenCLEventList())
    {
        return enqueueFillBuffer(std::move(buffer), &pattern, sizeof(pattern),
                                 offset, length, std::move(before));
    }

    /** Enqueue an operation to map the buffer.  Once the operation is done
        (the returned event tells us when), the shared pointer can be used
        to refer to the memory.  When the shared pointer is destroyed,
        the mapping will be undone.
    */
    std::pair<std::shared_ptr<void>,
              OpenCLEvent>
    enqueueMapBuffer(cl_mem buffer,
                     int flags,
                     size_t offset,
                     size_t length,
                     OpenCLEventList before = OpenCLEventList())
    {
        cl_int error = 0;
        OpenCLEvent result;
        
        void * addr = clEnqueueMapBuffer
            (queue, buffer, CL_FALSE /* blocking */, flags,
             offset, length,
             before.size(), before, result, &error);

        checkOpenCLError(error, "clEnqueueMapBuffer");

        auto unmap = [=] (void * addr) throw()
            {
                int res = clEnqueueUnmapMemObject(queue, buffer, addr, 0, nullptr, nullptr);
                try {
                    checkOpenCLError(res, "clEnququeUnmapMemObject");
                } MLDB_CATCH_ALL {
                    using namespace std;
                    cerr << getExceptionString() << endl;
                    cerr << "Error in unmapping OpenCL object aborts program"
                         << endl;
                    abort();
                }
            };

        return std::make_pair(std::shared_ptr<void>(addr, unmap),
                              std::move(result));
    }
    
    OpenCLEvent enqueueMarker(OpenCLEventList waitFor)
    {
        OpenCLEvent result;
        
        cl_int error
            = clEnqueueMarkerWithWaitList
                (queue, waitFor.size(), waitFor, result);

        checkOpenCLError(error, "clEnqueueMarkerWithWaitList");

        return result;
    }

    OpenCLEvent enqueueBarrier(OpenCLEventList waitFor)
    {
        OpenCLEvent result;
        
        cl_int error
            = clEnqueueBarrierWithWaitList
                (queue, waitFor.size(), waitFor, result);

        checkOpenCLError(error, "clEnqueueBarrierWithWaitList");

        return result;
    }

#if 0    
    OpenCLContext getContext() const
    {
    }

    OpenCLDevice getDevice() const
    {
    }

    cl_uint getReferenceCountUnsafe() const
    {
    }

    Bitset<OpenCLQueueProperties> getProperties() const
    {
        Bitset<OpenCLQueueProperties> result;
    }
#endif

#if 0    
    template<typename A1>
    void doEnqueue(cl_int (*fn) (cl_command_queue, A1 a1,
                                 cl_uint numEventsInWaitList,
                                 const cl_event * waitList,
                                 cl_event * event),
                   A1 a1,
                   const OpenCLEventList & waitFor,
                   OpenCLEvent & waitOn)
    {
    }

    template<typename Res, typename A1>
     Res doEnqueue(Res (*fn) (cl_command_queue, A1 a1,
                              cl_uint numEventsInWaitList,
                              const cl_event * waitList,
                              cl_event * event,
                              cl_int * error),
                   A1 a1,
                   const OpenCLEventList & waitFor,
                   OpenCLEvent & waitOn);
#endif

    int referenceCount() const
    {
        return queue.referenceCount();
    }
};


/*****************************************************************************/
/* OPENCL MEM OBJECT                                                         */
/*****************************************************************************/

struct OpenCLMemObject {
    OpenCLRefCounted<cl_mem> buffer;

    OpenCLMemObject()
    {
    }

    OpenCLMemObject(cl_mem buffer, bool alreadyRetained)
        : buffer(buffer, alreadyRetained)
    {
    }
    
    operator cl_mem () const
    {
        return buffer;
    }

    int referenceCount() const
    {
        return buffer.referenceCount();
    }
};


/*****************************************************************************/
/* OPENCL KERNEL                                                             */
/*****************************************************************************/

/// Used to bind a local array of the given size into a kernel
template<typename T>
struct LocalArray {
    LocalArray(size_t n)
        : n(n)
    {
    }

    size_t n;

    size_t bytes() const { return n * sizeof(T); }
};

struct OpenCLKernel {
    OpenCLRefCounted<cl_kernel> kernel;

    OpenCLKernel()
    {
    }

    OpenCLKernel(cl_kernel kernel)
        : kernel(kernel, true /* already retained */)
    {
    }

    OpenCLKernelInfo getInfo() const
    {
        return OpenCLKernelInfo(kernel);
    }
    
    operator cl_kernel () const
    {
        return kernel;
    }

    OpenCLContext getContext() const;
    
    template<typename T>
    void bindArg(int argNum, const T & data,
                 typename std::enable_if<std::is_pod<T>::value>::type * = 0)
    {
        cl_int error = clSetKernelArg(kernel, argNum, sizeof(data), &data);
        checkOpenCLError(error, "clSetKernelArg: arg " + std::to_string(argNum)
                         + " of type " + type_name<T>());
    }

#if 0    
    template<typename T>
    void bindArg(int argNum, T * data)
    {
        using namespace std;
        auto mem = getContext().createBuffer(0, sizeof(T), data);
        cl_mem memValue = mem;
        cl_int error = clSetKernelArg(kernel, argNum, sizeof(memValue), &memValue);
        checkOpenCLError(error, "clSetKernelArg: arg " + std::to_string(argNum)
                         + " of type " + type_name<T>());
    }
#endif
    
    template<typename T>
    void bindArg(int argNum, const LocalArray<T> & data)
    {
        if (data.bytes() > 32767) {
            using namespace std;
            cerr << "warning: asking for " << data.bytes() / 1024.0
                 << "kb of local memory; many devices will fail to launch"
                 << endl;
        }
        cl_int error = clSetKernelArg(kernel, argNum, data.bytes(), nullptr);
        checkOpenCLError(error, "clSetKernelArg: arg " + std::to_string(argNum)
                         + " of type " + type_name<T>());
    }

    void bindArg(int argNum, cl_mem data)
    {
        cl_int error = clSetKernelArg(kernel, argNum, sizeof(data), &data);
        checkOpenCLError(error, "clSetKernelArg: arg " + std::to_string(argNum)
                         + " of type cl_mem");
    }

    template<typename T>
    void bindArg(int argNum, std::vector<T> & data);

    template<typename T>
    void bindArg(int argNum, const std::vector<T> & data);

    void bindArgs(int argNum)
    {
        // verify that the required number of arguments are there
        if (argNum != getInfo().args.size()) {
            throw AnnotatedException(400, "Attempt to bind wrong number of "
                                     "arguments to kernel");
        }
    }
    
    template<typename Arg1, typename... Args>
    void bindArgs(int argNum, Arg1&& arg1, Args&&... args)
    {
        bindArg(argNum, std::forward<Arg1>(arg1));
        bindArgs(argNum + 1, std::forward<Args>(args)...);
    }
    
    template<typename... Args>
    void bind(Args&&... args)
    {
        if (sizeof...(args) != getInfo().args.size()) {
            throw AnnotatedException(400, "Attempt to bind wrong number of "
                                     "arguments to kernel: got "
                                     + std::to_string(sizeof...(args))
                                     + " expected "
                                     + std::to_string(getInfo().args.size()),
                                     "args", getInfo());
        }
        bindArgs(0, std::forward<Args>(args)...);
    }

    int referenceCount() const
    {
        return kernel.referenceCount();
    }

    std::vector<OpenCLMemObject> boundMemObjects;
};


/*****************************************************************************/
/* OPENCL PROGRAM                                                            */
/*****************************************************************************/

struct OpenCLProgram {
    OpenCLRefCounted<cl_program> program;

    OpenCLProgram()
    {
    }

    OpenCLProgram(cl_program program)
        : program(program, false /* already retained */)
    {
    }
    
    operator cl_program () const
    {
        return program;
    }

    OpenCLContext getContext() const;

    std::vector<OpenCLProgramBuildInfo>
    build(const std::vector<OpenCLDevice> & devices,
          const std::string & options)
    {
        cl_int error MLDB_UNUSED
            = clBuildProgram(program, devices.size() /*deviceIdCount*/,
                             &devices[0].device,
                             options.c_str(),
                             nullptr, nullptr);
        //checkOpenCLError(error, "clBuildProgram");

        std::vector<OpenCLProgramBuildInfo> result;
        for (auto & device: devices) {
            result.emplace_back(program, device);
        }
        return result;
    }

    OpenCLProgramBuildInfo getProgramInfo(const OpenCLDevice & device)
    {
        return OpenCLProgramBuildInfo(program, device);
        
    }
    
    OpenCLKernel createKernel(const std::string & name)
    {
        cl_int error;
        cl_kernel result = clCreateKernel (program, name.c_str(), &error);
        checkOpenCLError(error, "clCreateKernel");
        return result;
    }

    int referenceCount() const
    {
        return program.referenceCount();
    }
};


/*****************************************************************************/
/* OPENCL CONTEXT                                                            */
/*****************************************************************************/

struct OpenCLContext {
    OpenCLRefCounted<cl_context> context;

    OpenCLContext()
    {
    }

    OpenCLContext(cl_context context)
        : context(context, false /* implicitly retained */)
    {
    }

    OpenCLContext(cl_device_id device)
        : OpenCLContext(&device, 1 /* numDevices */)
    {
    }

    OpenCLContext(Bitset<uint32_t /*cl_device_type*/> type,
                  cl_platform_id platform)
    {
        const cl_context_properties contextProperties [] =
            {
                CL_CONTEXT_PLATFORM,
                reinterpret_cast<cl_context_properties> (platform),
                //CL_CONTEXT_INTEROP_USER_SYNC,
                //reinterpret_cast<cl_context_properties> (userSync),
                0, 0
            };

        cl_int error;

        this->context
            .reset(clCreateContextFromType(contextProperties,
                                           type.val,
                                           &staticErrorHandler, nullptr, &error),
                   true /* already retained */);
            
        checkOpenCLError(error, "clCreateContextFromType");
    }

    OpenCLContext(const std::vector<OpenCLDevice> & devices)
        : OpenCLContext(&devices[0].device, devices.size())
    {
    }
    
    OpenCLContext(const cl_device_id * first,
                  size_t numDevices,
                  Bitset<uint32_t /*cl_device_type*/> type = 0)
    {
        const cl_context_properties contextProperties [] =
            {
                //CL_CONTEXT_PLATFORM,
                //reinterpret_cast<cl_context_properties> (platform),
                //CL_CONTEXT_INTEROP_USER_SYNC,
                //reinterpret_cast<cl_context_properties> (userSync),
                0, 0
            };

        cl_int error;

        this->context
            .reset(clCreateContext(contextProperties,
                                   numDevices,
                                   first,
                                   &staticErrorHandler, nullptr, &error),
                   true /* already retained */);

        checkOpenCLError(error, "clCreateContext");
    }

    ~OpenCLContext()
    {
    }

    static void staticErrorHandler(const char * error,
                                   const void * param,
                                   size_t paramSize,
                                   void * This)
    {
        using namespace std;
        cerr << "got context error " << error << " with param of size "
             << paramSize << endl;
    }
    
    cl_uint getReferenceCountUnsafe() const
    {
        ExcCheck(context, "Uninitialized OpenCL context");
        cl_uint result;
        cl_int res
            = clGetContextInfo(context, CL_CONTEXT_REFERENCE_COUNT,
                               sizeof(result), &result, nullptr);
        checkOpenCLError(res, "clGetContextInfo(REFERENCE_COUNT)");
        return result;
    }

    std::vector<OpenCLDevice>
    getDevices() const
    {
        auto ndev = getDeviceCount();

        ExcCheck(context, "Uninitialized OpenCL context");
        std::vector<cl_device_id> result(ndev);
        cl_int res
            = clGetContextInfo(context, CL_CONTEXT_DEVICES,
                               result.size() * sizeof(cl_device_id),
                               result.data(), nullptr);
        checkOpenCLError(res, "clGetContextInfo(DEVICES)");
        return std::vector<OpenCLDevice>(result.begin(), result.end());
    }
    
    size_t getDeviceCount() const
    {
        ExcCheck(context, "Uninitialized OpenCL context");
        cl_uint result;
        cl_int res
            = clGetContextInfo(context, CL_CONTEXT_NUM_DEVICES,
                               sizeof(result), &result, nullptr);
        checkOpenCLError(res, "clGetContextInfo(NUM_DEVICES)");
        return result;
    }

    OpenCLCommandQueue
    createCommandQueue(const OpenCLDevice & device,
                    Bitset<OpenCLCommandQueueProperties> props
                        = (OpenCLCommandQueueProperties)0)
    {
        cl_int error;
        cl_command_queue queue
            = clCreateCommandQueue (context, device,
                                    (cl_command_queue_properties)props.val,
                                    &error);
        checkOpenCLError (error, "clCreateCommandQueue");
        return queue;
    }

    OpenCLMemObject
    createBuffer(cl_mem_flags options,
                 size_t bytes)
    {
        cl_int error;
        cl_mem result
            = clCreateBuffer (context,
                              options,
                              bytes, nullptr, &error);
        checkOpenCLError(error, "clCreateBuffer");
        return OpenCLMemObject(result, true /* already retained */);
    }

    OpenCLMemObject
    createBuffer(cl_mem_flags options,
                 void * buf, size_t bytes)
    {
        cl_int error;
        cl_mem result
            = clCreateBuffer (context,
                              options | CL_MEM_USE_HOST_PTR,
                              bytes, buf, &error);
        checkOpenCLError(error, "clCreateBuffer");
        return OpenCLMemObject(result, true /* already retained */);
    }

    OpenCLMemObject
    createBuffer(cl_mem_flags options,
                 const void * buf, size_t bytes)
    {
        cl_int error;
        cl_mem result
            = clCreateBuffer (context,
                              options | CL_MEM_READ_ONLY | CL_MEM_USE_HOST_PTR,
                              bytes, (void *)buf, &error);
        checkOpenCLError(error, "clCreateBuffer");
        return OpenCLMemObject(result, true /* already retained */);
    }

    template<typename T>
    OpenCLMemObject
    createBuffer(std::vector<T> & data)
    {
        return createBuffer(0, data.data(), sizeof(T) * data.size());
    }

    template<typename T>
    OpenCLMemObject
    createBuffer(const std::vector<T> & data)
    {
        return createBuffer(0, data.data(), sizeof(T) * data.size());
    }
    
    OpenCLProgram
    createProgram(const Utf8String & programSource)
    {
	size_t lengths [1] = { programSource.rawLength() };
	const char* sources [1] = { programSource.rawData() };

        cl_int error;
        cl_program result = clCreateProgramWithSource(context, 1, sources, lengths, &error);
        checkOpenCLError(error, "clCreateProgramWithSource");
        return result;
    }
    
    operator cl_context() const
    {
        ExcCheck(context, "Uninitialized OpenCL context");
        return context;
    }
};

inline OpenCLContext
OpenCLProgram::
getContext() const
{
    cl_context context;
    cl_int error = clGetProgramInfo(program, CL_PROGRAM_CONTEXT,
                                    sizeof(context), &context, 0);
    checkOpenCLError(error, "clGetProgramInfo CL_PROGRAM_CONTEXT");
    return context;
}

inline OpenCLContext
OpenCLKernel::
getContext() const
{
    cl_context context;
    cl_int error = clGetKernelInfo(kernel, CL_KERNEL_CONTEXT,
                                   sizeof(context), &context, 0);
    checkOpenCLError(error, "clGetKernelInfo CL_KERNEL_CONTEXT");
    return context;
}

inline OpenCLContext openCLCreateContext(cl_context context)
{
    return context;
}

template<typename T>
void
OpenCLKernel::
bindArg(int argNum, std::vector<T> & data)
{
    auto context = getContext();
    auto mem = context.createBuffer(0, data.data(),
                                    sizeof(T) * data.size());
    bindArg(argNum, mem);
    boundMemObjects.emplace_back(std::move(mem));
}

template<typename T>
void
OpenCLKernel::
bindArg(int argNum, const std::vector<T> & data)
{
    auto context = getContext();
    auto mem = context.createBuffer(0, data.data(),
                                    sizeof(T) * data.size());
    bindArg(argNum, mem);
    boundMemObjects.emplace_back(std::move(mem));
}


} // namespace MLDB
