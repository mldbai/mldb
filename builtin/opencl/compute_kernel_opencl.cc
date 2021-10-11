/** compute_kernel_opencl.h                                                -*- C++ -*-
    Jeremy Barnes, 27 March 2016
    This file is part of MLDB. Copyright 2016 mldb.ai inc. All rights reserved.

    Compute kernel runtime for CPU devices.
*/

#include "compute_kernel_opencl.h"
#include "mldb/types/basic_value_descriptions.h"
#include "opencl_types.h"

using namespace std;

namespace MLDB {

struct OpenCLComputeRuntime: public ComputeRuntime {

    std::vector<OpenCLPlatform> clPlatforms;
    std::vector<std::vector<OpenCLDevice>> clDevices;
    std::vector<ComputeDevice> devices;

    OpenCLComputeRuntime()
    {
        clPlatforms = getOpenCLPlatforms();
        clDevices.reserve(clPlatforms.size());

        for (size_t i = 0;  i < clPlatforms.size();  ++i) {
            clDevices.emplace_back(clPlatforms[i].getDevices());
            for (size_t j = 0;  j < clDevices[i].size();  ++j) {
                devices.push_back({ComputeRuntimeId::OPENCL, (uint8_t)i, (uint16_t)j, 0, 0});
            }
        }
    }

    OpenCLDevice convertDevice(ComputeDevice device) const
    {
        if (device.runtime != ComputeRuntimeId::OPENCL) {
            throw MLDB::Exception("Attempt to pass non-OpenCL device " + device.info() + " to OpenCL");
        }
        return clDevices.at(device.runtimeInstance).at(device.deviceInstance);
    }

    virtual ~OpenCLComputeRuntime()
    {
    }

    virtual ComputeRuntimeId getId() const
    {
        return ComputeRuntimeId::OPENCL;
    }

    virtual std::string printRestOfDevice(ComputeDevice device) const
    {
        return std::to_string(device.runtimeInstance) + ":" + std::to_string(device.deviceInstance);
    }

    virtual std::string printHumanReadableDeviceInfo(ComputeDevice device) const
    {
        if (device.runtimeInstance >= clDevices.size()
            || device.deviceInstance >= clDevices[device.runtimeInstance].size()) {
            return "<<INVALID OPENCL PLATFORM OR DEVICE INDEX>>";
        }
        std::string result = clPlatforms[device.runtimeInstance].getPlatformInfo().name
             + " " + clDevices[device.runtimeInstance][device.deviceInstance].getDeviceInfo().name;
        return result;
    }

    virtual ComputeDevice getDefaultDevice() const
    {
        if (clPlatforms.empty()) {
            return ComputeDevice::none();
        }

        // Look for a device that's a GPU with non-unified memory
        for (size_t i = 0;  i < clDevices.size();  ++i) {
            for (size_t j = 0;  j < clDevices[i].size();  ++j) {
                auto info = clDevices[i][j].getDeviceInfo();
                if (info.type.test(OpenCLDeviceType::GPU) && info.unifiedMemory == false) {
                    return {ComputeRuntimeId::OPENCL, (uint8_t)i, (uint16_t)j, 0, 0};
                }
            }
        }

        // Look for a device that's a GPU
        for (size_t i = 0;  i < clDevices.size();  ++i) {
            for (size_t j = 0;  j < clDevices[i].size();  ++j) {
                auto info = clDevices[i][j].getDeviceInfo();
                if (info.type.test(OpenCLDeviceType::GPU)) {
                    return {ComputeRuntimeId::OPENCL, (uint8_t)i, (uint16_t)j, 0, 0};
                }
            }
        }

        // Fall back on the first device
        return devices[0];
    }

    // Enumerate the devices available for this runtime
    virtual std::vector<ComputeDevice> enumerateDevices() const
    {
        return devices;
    }

    // Get a compute context for this runtime
    virtual std::shared_ptr<ComputeContext>
    getContext(std::span<const ComputeDevice> devices) const;
};

namespace {

std::mutex kernelRegistryMutex;
struct KernelRegistryEntry {
    std::function<std::shared_ptr<ComputeKernel>(OpenCLComputeContext & context)> generate;
};

std::map<std::string, KernelRegistryEntry> kernelRegistry;

} // file scope

std::shared_ptr<ComputeKernel>
OpenCLComputeContext::
getKernel(const std::string & kernelName)
{
    std::unique_lock guard(kernelRegistryMutex);
    auto it = kernelRegistry.find(kernelName);
    cerr << "looking for " << kernelName << endl;
    for (auto [k,g]: kernelRegistry) {
        cerr << "  we have " << k << endl;
    }
    if (it == kernelRegistry.end()) {
        throw AnnotatedException(400, "Unable to find OpenCL compute kernel '" + kernelName + "'",
                                        "kernelName", kernelName);
    }
    auto result = it->second.generate(*this);
    result->context = this;
    return result;
}


struct OpenCLComputeEvent: public ComputeEvent {
    virtual ~OpenCLComputeEvent() = default;
    virtual ComputeProfilingInfo getProfilingInfo() const
    {
        return ComputeProfilingInfo();
    }

    virtual void await() const
    {
        ev.waitUntilFinished();
    }

    OpenCLEvent ev;
};

// Get a compute context for this runtime
std::shared_ptr<ComputeContext>
OpenCLComputeRuntime::
getContext(std::span<const ComputeDevice> devices) const
{
    std::vector<OpenCLDevice> clDevices;
    for (auto & d: devices) {
        clDevices.emplace_back(convertDevice(d));
    }
    return std::make_shared<OpenCLComputeContext>(clDevices);
}


void registerOpenCLComputeKernel(const std::string & kernelName,
                                 std::function<std::shared_ptr<OpenCLComputeKernel>(OpenCLComputeContext & context)> generator)
{
    cerr << "registering OpenCL kernel " << kernelName << endl;
    kernelRegistry[kernelName].generate = generator;
}

namespace {

static struct Init {
    Init()
    {
        ComputeRuntime::registerRuntime(ComputeRuntimeId::OPENCL, "opencl",
                                        [] () { return new OpenCLComputeRuntime(); });
    }

} init;

} // file scope
} // namespace MLDB
