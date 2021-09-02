/** opencl.cc
    Jeremy Barnes, 21 September 2017
    Copyright (c) 2017 Element AI Inc.  All rights reserved.
    This file is part of MLDB. Copyright 2015 mldb.ai inc. All rights reserved.

    OpenCL plugin, to allow execution of OpenCL code and OpenCL functions
    to be defined.
*/

#include "mldb/core/plugin.h"
#include "mldb/builtin/plugin_resource.h"
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
/* OPENCL PLUGIN                                                             */
/*****************************************************************************/

struct OpenCLPlugin: public Plugin {
    OpenCLPlugin(MldbEngine * engine,
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
    runOpenCLScript(MldbEngine * engine,
                        const PluginResource & scriptConfig);
#endif
    
    static RestRequestMatchResult
    handleTypeRoute(RestDirectory * engine, RestConnection & conn,
                    const RestRequest & request,
                    RestRequestParsingContext & context);

    //std::unique_ptr<JsPluginContext> itl;
};

OpenCLPlugin::
OpenCLPlugin(MldbEngine * engine,
             PolyConfig config,
             std::function<bool (const Json::Value & progress)> onProgress)
    : Plugin(engine)
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

        for (auto & d: devices) {
            cerr << "device " << d << ": " << jsonEncode(d.getDeviceInfo()) << endl;
        }

        if (devices.empty())
            continue;
        devices.resize(1);
        
        OpenCLContext context(devices);

        OpenCLCommandQueue queue
            = context.createCommandQueue
                (devices.at(0),
                 OpenCLCommandQueueProperties::PROFILING_ENABLE);

        constexpr size_t stride = 1;
        
        constexpr size_t testDataSize = 100 * 1024 * 1024;

        typedef float Float;

        std::vector<Float> a(testDataSize), b(testDataSize);
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
            "__kernel void SAXPY (__global const Float* x, __global Float* y, Float a, int stride)\n"
            "{\n"
            "    const int id = get_global_id (0);\n"
            "    for (int i = id * stride;  i < (id + 1) * stride;  ++i) {\n"
            "        y [i] += a * x [i];\n"
            "    }\n"
            "    //if (i % 100000 == 0) printf(\"hello %d\\n\", i);\n"
            "}";        

        
        
	size_t lengths [1] = { source.size () };
	const char* sources [1] = { source.data () };

	OpenCLProgram program(clCreateProgramWithSource (context, 1, sources, lengths, &error));

        string options = "-cl-kernel-arg-info -cl-mad-enable -cl-fast-relaxed-math -cl-unsafe-math-optimizations -DFloat=" + type_name<Float>();

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
            static const Float two = 2.0f;
            clSetKernelArg (kernel, 2, sizeof (Float), &two);
            clSetKernelArg (kernel, 3, sizeof (int), &stride);
            

            OpenCLEvent event;
            
            const size_t globalWorkSize [] = { testDataSize / stride, 0, 0 };

            for (size_t i = 0;  i < 10;  ++i) {
                error = clEnqueueNDRangeKernel (queue, kernel,
                                                1, // One dimension
                                                nullptr,
                                                globalWorkSize,
                                                nullptr,
                                                0, nullptr,
                                                event);
                checkOpenCLError(error, "clEnqueueNDRangeKernel");

                event.waitUntilFinished();

                cerr << "getting profiling info" << endl;
                auto profile = event.getProfilingInfo();

                cerr << "time to queue (ns): " << profile.submit - profile.queued << endl;
                cerr << "time to start (ns): " << profile.start - profile.submit << endl;
                cerr << "time to run (ns):   " << profile.end - profile.start << endl;
            
                cerr << jsonEncode(profile);

                // units: flop / ns = flop / s / 10^-9 = GFLOP/s
                
                double gflops = testDataSize * 2 / ((profile.end - profile.start) * 1.0);
                cerr << "gflops for kernel = " << gflops << endl;

                // Read bandwidth is twice the array length in bytes
                cerr << "mem read bandwidth for kernel = "
                     << 2 * testDataSize * sizeof(Float) / ((profile.end - profile.start) * 1.0)
                     << " gB/s" << endl;
                cerr << "mem write bandwidth for kernel = "
                     << testDataSize * sizeof(Float) / ((profile.end - profile.start) * 1.0)
                     << " gB/s" << endl;
                cerr << "total bandwidth for kernel = "
                     << 3 * testDataSize * sizeof(Float) / ((profile.end - profile.start) * 1.0)
                     << " gB/s" << endl;
            }
            
            //error = clFinish(queue);

            //checkOpenCLError(error, "clFinish");

            Date middle = Date::now();
            
            // Get the results back to the host
	// http://www.khronos.org/registry/cl/sdk/1.1/docs/man/xhtml/clEnqueueReadBuffer.html
            error = clEnqueueReadBuffer (queue, bBuffer, CL_TRUE, 0,
                                         sizeof (Float) * testDataSize,
                                         b.data (),
                                         0, nullptr, nullptr);
            checkOpenCLError(error, "clEnqueueReadBuffer");

            //error = clFinish(queue);
            
            //checkOpenCLError(error, "clFinish");

            Date after = Date::now();

            double computation = middle.secondsSince(before);
            double flops = testDataSize * 2;

            double transfer = after.secondsSince(middle);
            double bytes = sizeof(Float) * testDataSize;

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
handleTypeRoute(RestDirectory * engine,
                RestConnection & conn,
                const RestRequest & request,
                RestRequestParsingContext & context)
{
#if 0
    //cerr << "context.remaining = " << context.remaining << endl;

    if (context.resources.back() == "run") {
        //cerr << "request.payload = " << request.payload << endl;
        
        auto scriptConfig = jsonDecodeStr<ScriptResource>(request.payload).toPluginConfig();
        
        auto result = runOpenCLScript(static_cast<MldbEngine *>(engine),
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
