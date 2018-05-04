/** opencl.cc
    Jeremy Barnes, 21 September 2017
    Copyright (c) 2017 Element AI Inc.  All rights reserved.
    This file is part of MLDB. Copyright 2015 mldb.ai inc. All rights reserved.

    OpenCL plugin, to allow execution of OpenCL code and OpenCL functions
    to be defined.
*/

#include "mldb/core/plugin.h"
#include "mldb/server/plugin_resource.h"
#include "mldb/rest/rest_request_binding.h"
#include "mldb/types/basic_value_descriptions.h"
#include "mldb/types/set_description.h"
#include "mldb/types/array_description.h"
#include "mldb/block/memory_region.h"
#include "opencl_types.h"

#include <regex>


using namespace std;

namespace MLDB {


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
            throw HttpReturnException(400, "No OpenCL devices found");
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

template<typename Handle>
struct OpenCLRefCounted {
    Handle handle = nullptr;

    OpenCLRefCounted()
    {
    }
    
    OpenCLRefCounted(Handle handle)
        : handle(handle)
    {
        clRetain(handle);
    }

    OpenCLRefCounted(OpenCLRefCounted && other)
        : handle(other.handle)
    {
        other.handle = nullptr;
    }

    OpenCLRefCounted(const OpenCLRefCounted & other)
        : OpenCLRefCounted(other.handle)
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
    
    OpenCLRefCounted(cl_device_id device)
        : OpenCLRefCounted(&device, 1 /* numDevices */)
    {
    }

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
        : event(event)
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
};


/*****************************************************************************/
/* OPENCL COMMAND QUEUE                                                      */
/*****************************************************************************/

enum class OpenCLCommandQueueProperties: cl_command_queue_properties {
    OUT_OF_ORDER_EXEC_MODE_ENABLE = CL_QUEUE_OUT_OF_ORDER_EXEC_MODE_ENABLE,
    PROFILING_ENABLE = CL_QUEUE_PROFILING_ENABLE
};

DECLARE_ENUM_DESCRIPTION(OpenCLCommandQueueProperties);

DEFINE_ENUM_DESCRIPTION_INLINE(OpenCLCommandQueueProperties)
{
    addValue("OUT_OF_ORDER_EXEC_MODE_ENABLE",
             OpenCLCommandQueueProperties::OUT_OF_ORDER_EXEC_MODE_ENABLE);
    addValue("PROFILING_ENABLE",
             OpenCLCommandQueueProperties::PROFILING_ENABLE);
}



struct OpenCLCommandQueue {
    OpenCLRefCounted<cl_command_queue> queue;

    OpenCLCommandQueue()
    {
    }

    OpenCLCommandQueue(cl_command_queue queue)
        : queue(queue)
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
};


/*****************************************************************************/
/* OPENCL KERNEL                                                             */
/*****************************************************************************/

struct OpenCLKernel {
    OpenCLRefCounted<cl_kernel> kernel;

    OpenCLKernel()
    {
    }

    OpenCLKernel(cl_kernel kernel)
        : kernel(kernel)
    {
    }
    
    operator cl_kernel () const
    {
        return kernel;
    }

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
        : program(program)
    {
    }
    
    operator cl_program () const
    {
        return program;
    }

    OpenCLKernel createKernel(const std::string & name)
    {
        cl_int error;
        cl_kernel result = clCreateKernel (program, name.c_str(), &error);
        checkOpenCLError(error, "clCreateKernel");
        return result;
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

    OpenCLMemObject(cl_mem buffer)
        : buffer(buffer)
    {
    }
    
    operator cl_mem () const
    {
        return buffer;
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
        : context(context)
    {
    }

    OpenCLContext(cl_device_id device)
        : OpenCLContext(&device, 1 /* numDevices */)
    {
    }

    OpenCLContext(Bitset<cl_device_type> type,
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
            = clCreateContextFromType(contextProperties,
                                      type.val,
                                      &staticErrorHandler, nullptr, &error);
            
        checkOpenCLError(error, "clCreateContextFromType");
    }

    OpenCLContext(const std::vector<OpenCLDevice> & devices)
        : OpenCLContext(&devices[0].device, devices.size())
    {
    }
    
    OpenCLContext(const cl_device_id * first,
                  size_t numDevices,
                  Bitset<cl_device_type> type = 0)
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
            = clCreateContext(contextProperties,
                              numDevices,
                              first,
                              &staticErrorHandler, nullptr, &error);

        cerr << "got context " << context << endl;
        
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

#if 0    
    std::vector<OpenCLDevice> getDevices() const
    {
    }
#endif
    
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
        return result;
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
        return result;
    }

    OpenCLMemObject
    createBuffer(cl_mem_flags options,
                 const void * buf, size_t bytes)
    {
        cl_int error;
        cl_mem result
            = clCreateBuffer (context,
                              options | CL_MAP_READ | CL_MEM_USE_HOST_PTR,
                              bytes, (void *)buf, &error);
        checkOpenCLError(error, "clCreateBuffer");
        return result;
    }
    
    operator cl_context() const
    {
        ExcCheck(context, "Uninitialized OpenCL context");
        return context;
    }
};




/*****************************************************************************/
/* OPENCL PLUGIN                                                             */
/*****************************************************************************/

struct OpenCLPlugin: public Plugin {
    OpenCLPlugin(MldbServer * server,
                     PolyConfig config,
                     std::function<bool (const Json::Value & progress)> onProgress);
    
    ~OpenCLPlugin();
    
    virtual Any getStatus() const;

    virtual RestRequestMatchResult
    handleRequest(RestConnection & connection,
                  const RestRequest & request,
                  RestRequestParsingContext & context) const;

#if 0
    static ScriptOutput
    runOpenCLScript(MldbServer * server,
                        const PluginResource & scriptConfig);
#endif

    static RestRequestMatchResult
    handleTypeRoute(RestDirectory * server, RestConnection & conn,
                    const RestRequest & request,
                    RestRequestParsingContext & context);

    //std::unique_ptr<JsPluginContext> itl;
};

OpenCLPlugin::
OpenCLPlugin(MldbServer * server,
             PolyConfig config,
             std::function<bool (const Json::Value & progress)> onProgress)
    : Plugin(server)
{
    std::vector<OpenCLPlatform> platforms
        = getOpenCLPlatforms();

    cerr << platforms.size() << " OpenCL platforms available" << endl;

    for (size_t i = 0;  i < platforms.size();  ++i) {

        cerr << "platform" << jsonEncode(platforms[i].getPlatformInfo())
             << endl;

        const OpenCLPlatform & platform
            = platforms[i];
        
        std::vector<OpenCLDevice> devices
            = platform.getDevices();

        for (auto & d: platform.getDevices()) {
            cerr << jsonEncode(d.getDeviceInfo()) << endl;
        }

        if (devices.empty())
            continue;
        devices.resize(1);
        
        OpenCLContext context(devices);

        OpenCLCommandQueue queue
            = context.createCommandQueue
                (devices.at(0),
                 OpenCLCommandQueueProperties::PROFILING_ENABLE);
        
        constexpr size_t testDataSize = 100000000;
        std::vector<float> a(testDataSize), b(testDataSize);
        for (size_t i = 0;  i < a.size();  ++i) {
            a[i] = i;
            b[i] = i % 1000;
        }

        auto aBuffer
            = context.createBuffer(CL_MEM_READ_ONLY,
                                   a.data(), a.size() * sizeof(a[0]));
        auto bBuffer
            = context.createBuffer(CL_MEM_READ_WRITE,
                                   b.data(), b.size() * sizeof(b[0]));

        cl_int error;
        string source =
            "__kernel void SAXPY (__global const float* x, __global float* y, float a)\n"
            "{\n"
            "    const int i = get_global_id (0);\n"
            "    y [i] += a * x [i];\n"
            "    //if (i % 100000 == 0) printf(\"hello %d\\n\", i);\n"
            "}";        

        
        
	size_t lengths [1] = { source.size () };
	const char* sources [1] = { source.data () };

	OpenCLProgram program(clCreateProgramWithSource (context, 1, sources, lengths, &error));

        checkOpenCLError(error, "clCreateProgramWithSource");

        string options = "-cl-kernel-arg-info";//" -cl-mad-enable -cl-unsafe-math-optimizations";

        std::vector<cl_device_id> deviceIds = { devices.begin(), devices.end() };
        
        error = clBuildProgram (program, deviceIds.size() /*deviceIdCount*/,
                                deviceIds.data (), options.c_str(),
                                nullptr, nullptr);

        OpenCLProgramBuildInfo buildInfo(program, deviceIds[0]);

        cerr << jsonEncode(buildInfo) << endl;
        
        checkOpenCLError(error, "clBuildProgram");

        OpenCLKernel kernel
            = program.createKernel("SAXPY");

        OpenCLKernelInfo info(kernel);
        cerr << jsonEncode(info) << endl;
        
        double maxgf = 0, maxgb = 0, maxgfo = 0, maxgbo = 0;
        
        for (size_t i = 0;  i < 10;  ++i) {
            Date before = Date::now();

            clSetKernelArg (kernel, 0, sizeof (cl_mem), &aBuffer);
            clSetKernelArg (kernel, 1, sizeof (cl_mem), &bBuffer);
            static const float two = 2.0f;
            clSetKernelArg (kernel, 2, sizeof (float), &two);        
            

            OpenCLEvent event;
            
            const size_t globalWorkSize [] = { testDataSize, 0, 0 };
            error = clEnqueueNDRangeKernel (queue, kernel,
                                            1, // One dimension
                                            nullptr,
                                            globalWorkSize,
                                            nullptr,
                                            0, nullptr,
                                            event);
            
            checkOpenCLError(error, "clEnqueueNDRangeKernel");

            //error = clFinish(queue);

            //checkOpenCLError(error, "clFinish");

            event.waitUntilFinished();

            auto profile = event.getProfilingInfo();

            cerr << "time to queue: " << profile.submit - profile.queued << endl;
            cerr << "time to start: " << profile.start - profile.submit << endl;
            cerr << "time to run:   " << profile.end - profile.start << endl;
            
            cerr << jsonEncode(profile);

            double gflops = testDataSize * 2 / ((profile.end - profile.start) * 1.0);
            cerr << "gflops for kernel = " << gflops << endl;

            
            Date middle = Date::now();
            
            // Get the results back to the host
	// http://www.khronos.org/registry/cl/sdk/1.1/docs/man/xhtml/clEnqueueReadBuffer.html
            error = clEnqueueReadBuffer (queue, bBuffer, CL_TRUE, 0,
                                         sizeof (float) * testDataSize,
                                         b.data (),
                                         0, nullptr, nullptr);
            checkOpenCLError(error, "clEnqueueReadBuffer");

            //error = clFinish(queue);
            
            //checkOpenCLError(error, "clFinish");

            Date after = Date::now();

            double computation = middle.secondsSince(before);
            double flops = testDataSize * 2;

            double transfer = after.secondsSince(middle);
            double bytes = sizeof(float) * testDataSize;

            double overall = computation + transfer;
            
            cerr << flops / computation / 1000000000.0 << " gflop/s" << endl;
            cerr << bytes / transfer / 1000000000.0 << " gbytes/s" << endl;
            cerr << flops / overall / 1000000000.0 << " gflop/s overall" << endl;
            cerr << bytes / overall / 1000000000.0 << " gbytes/s overall" << endl;

            maxgf = std::max(maxgf, flops / computation / 1000000000.0);
            maxgb = std::max(maxgb, bytes / transfer / 1000000000.0);
            maxgfo = std::max(maxgfo, flops / overall / 1000000000.0);
            maxgbo = std::max(maxgbo, bytes / overall / 1000000000.0);

            cerr << maxgf << "gF/s\t" << maxgb << "gB/s\t"
                 << maxgfo << "gF/s\t" << maxgbo << " gB/s\t" << endl;
        }
    }
    
}
    
OpenCLPlugin::
~OpenCLPlugin()
{
}
    
Any
OpenCLPlugin::
getStatus() const
{
    return Any();
}

RestRequestMatchResult
OpenCLPlugin::
handleRequest(RestConnection & connection,
              const RestRequest & request,
              RestRequestParsingContext & context) const
{
#if 0
    // First, check for a route
    auto res = itl->router.processRequest(connection, request, context);
    if (res != RestRequestRouter::MR_NO)
        return res;

    // Second, check for a generic request handler
    if (itl->handleRequest)
        return itl->handleRequest(connection, request, context);
#endif

    // Otherwise we simply don't handle it
    return RestRequestRouter::MR_NO;
}

RestRequestMatchResult
OpenCLPlugin::
handleTypeRoute(RestDirectory * server,
                RestConnection & conn,
                const RestRequest & request,
                RestRequestParsingContext & context)
{
#if 0
    //cerr << "context.remaining = " << context.remaining << endl;

    if (context.resources.back() == "run") {
        //cerr << "request.payload = " << request.payload << endl;
        
        auto scriptConfig = jsonDecodeStr<ScriptResource>(request.payload).toPluginConfig();
        
        auto result = runOpenCLScript(static_cast<MldbServer *>(server),
                                          scriptConfig);
        conn.sendResponse(result.exception ? 400 : 200,
                          jsonEncodeStr(result), "application/json");
        return RestRequestRouter::MR_YES;
    }
#endif
    return RestRequestRouter::MR_NO;
}


/*****************************************************************************/
/* REGISTRY                                                                  */
/*****************************************************************************/

RegisterPluginType<OpenCLPlugin, PluginResource>
regOpenCL(builtinPackage(),
          "opencl",
          "OpenCL plugin loader",
          "lang/OpenCL.md.html",
          &OpenCLPlugin::handleTypeRoute);

} // namespace MLDB
