// ...
#include "mldb/block/zip_serializer.h"
#include "mldb/vfs/filter_streams.h"
#include "mldb/types/vector_description.h"
#include "mldb/types/map_description.h"
#include "mldb/types/meta_value_description.h"
#include "mldb/arch/timers.h"
#include "compute_kernel_opencl.h"

using namespace MLDB;
using namespace std;

int main(int argc, char ** argv)
{
    string kernelName = "updateBuckets";

    ZipStructuredReconstituter traceReader(Url("file://./kernels.zip"));

    //auto entries = traceReader.getDirectory();

    //for (auto & entry: entries) {
    //    cerr << "got entry " << entry.name << endl;
    //}

    auto listItems = [] (const char * things, const char * subThings, const StructuredReconstituter & rec)
    {
        auto dir = rec.getDirectory();
        cerr << dir.size() << " " << things << endl;
        for (auto & item: dir) {
            auto subItems = item.getStructure();
            size_t numSubItems = subItems->getDirectory().size();
            cerr << format("%-30s (%4zd %s)", item.name.toUtf8String().rawString().c_str(), numSubItems, subThings) << endl;
        }
    };

    auto kernels = traceReader.getStructure("kernels");

    listItems("kernels", "runs", *kernels);

    auto programs = traceReader.getStructure("programs");

    //listItems("programs", "", *programs);

    auto regions = traceReader.getStructure("regions");

    //listItems("regions", "versions", *regions);

    auto traceKernel = kernels->getStructure(kernelName);
    auto programId = traceKernel->getObject<std::string>("program");
    auto entryPoint = traceKernel->getObject<std::string>("kernel");
    auto program = programs->getStructure(programId);
    auto source = program->getStream("source");
    auto buildInfo = program->getObject<OpenCLProgramBuildInfo>("build");
    auto runs = traceKernel->getDirectory();

    auto runtime = ComputeRuntime::getRuntimeForId(ComputeRuntimeId::OPENCL);
    auto genericContext = runtime->getContext(vector{runtime->getDefaultDevice()});
    auto context = std::dynamic_pointer_cast<OpenCLComputeContext>(genericContext);
    ExcAssert(context);

    auto genericQueue = context->getQueue();
    auto queue = std::dynamic_pointer_cast<OpenCLComputeQueue>(genericQueue);
    ExcAssert(queue);

    std::map<Path, MemoryRegionHandle> cachedRegions;

    auto getCachedRegion = [&] (const std::string & regionName, int version) -> MemoryRegionHandle
    {
        Path p{regionName, version};
        auto it = cachedRegions.find(p);
        if (it == cachedRegions.end()) {
            auto region = regions->getRegionRecursive(p);
            auto handle = context->transferToDeviceSyncImpl("get cached " + p.toUtf8String().rawString(), region, typeid(void), true /* isConst */);
            it = cachedRegions.emplace(p, handle).first;
        }
        return it->second;
    };

    std::stringstream sourceStr;
    sourceStr << source.rdbuf();
    OpenCLProgram clProgram = context->clContext.createProgram(sourceStr.str());
    //cerr << jsonEncode(clProgram.getProgramInfo()) << endl;
    auto clBuildInfo = clProgram.build(context->clDevices, buildInfo.buildOptions);
    //cerr << jsonEncode(buildInfo) << endl;

    for (auto & run: traceKernel->getStructure("runs")->getDirectory()) {
        auto clKernel = clProgram.createKernel(entryPoint);
        auto clKernelInfo = clKernel.getInfo();
        //cerr << jsonEncode(clKernelInfo) << endl;
        auto args = run.getStructure()->getObject<Json::Value>("args");
        auto grid = run.getStructure()->getObject<std::vector<size_t>>("grid");
        auto block = run.getStructure()->getObject<std::vector<size_t>>("block");
        auto knownsJson = run.getStructure()->getObject<std::map<std::string, Json::Value>>("knowns");
        cerr << "run " << run.name << " grid " << grid << " block " << block << " args " << endl << args << endl;
        //cerr << "knowns" << endl;
        //for (auto & [key,value]: knownsJson) {
        //    cerr << "  " << key << " = " << value.toStringNoNewLine() << endl;
        //}
        std::vector<MemoryRegionHandle> handles;
        std::vector<std::shared_ptr<const void>> pins;

        CommandExpressionContext knowns;
        for (auto & [name, value]: knownsJson) {
            knowns.setValue(name, value);
        }

        for (size_t i = 0;  i < clKernelInfo.numArgs;  ++i) {

            const auto & argInfo = clKernelInfo.args[i];
            auto argName = argInfo.name;
            auto passedArg = args[argName];
            if (passedArg.isNull()) {
                cerr << "skipping parameter " << argName << ": " << jsonEncodeStr(argInfo) << endl;
                continue;
            }
            std::string typeName = passedArg["type"]["typeName"].asString();

            // Check to see if the type is already known, before we do any parsing
            std::shared_ptr<const ValueDescription> desc
                = ValueDescription::get(typeName);

            if (!desc) {
                // We don't know the type... register it
                //cerr << "registering unknown type " << passedArg["type"]["typeName"].asString();
                //cerr << passedArg << endl;
                desc.reset(jsonDecode<const ValueDescription *>(passedArg["type"]));
                std::vector<std::string> aliases = jsonDecode<std::vector<std::string>>(passedArg["aliases"]);
                registerForeignValueDescription(typeName, desc, aliases);
            }

            auto type = OpenCLComputeKernel::getKernelType(argInfo);
            auto formalType = parseType(passedArg["spec"].asString());

            switch (argInfo.addressQualifier) {
            case OpenCLArgAddressQualifier::GLOBAL: {
                auto regionName = passedArg["value"]["name"].asString();
                auto version = passedArg["value"]["version"].asInt();
                size_t length = passedArg["value"]["length"].asUInt();
                size_t align = passedArg["value"]["elAlign"].asUInt();
                size_t size = passedArg["value"]["elWidth"].asUInt();
                size_t lengthInBytes = length * size;
                //cerr << "length = " << length << " align = " << align << " size = " << size << " lengthInBytes = " << lengthInBytes << endl;

                MemoryRegionHandle memIn, mem;

                if (formalType.access & MemoryRegionAccess::ACC_READ) {
                    memIn = getCachedRegion(regionName, version);
                }
                if (formalType.access & MemoryRegionAccess::ACC_WRITE) {
                    mem = context->allocateSyncImpl(regionName, lengthInBytes, align, typeid(void), false /* isConst */,
                                                    MemoryRegionInitialization::INIT_NONE);
                    if (formalType.access == MemoryRegionAccess::ACC_READ_WRITE) {
                        context->copyBetweenDeviceRegionsSyncImpl("copy", memIn, mem, 0, 0, lengthInBytes);
                    }
                }
                else { // read only
                    mem = memIn;
                }

                auto [pin, clMem, clMemOffset] = context->getMemoryRegion("bind arg", *mem.handle, formalType.access);
                ExcAssertEqual(clMemOffset, 0);

                clKernel.bindArg(i, clMem);
                pins.push_back(pin);
                handles.push_back(mem);
                break;
            }
            case OpenCLArgAddressQualifier::PRIVATE: {
                std::shared_ptr<void> mem(type.baseType->constructDefault(), [=] (void * p) { type.baseType->destroy(p); });
                Json::Value arg = passedArg["value"];
                if (arg.isNull())
                    arg = knowns.getValue(argName);

                StructuredJsonParsingContext context(arg);
                type.baseType->parseJson(mem.get(), context);
                clKernel.bindArg(i, mem.get(), type.baseType->width);
                pins.push_back(mem);
                break;
            }
            case OpenCLArgAddressQualifier::LOCAL: {
                ExcAssertEqual(formalType.dims.size(), 1);
                ExcAssert(formalType.dims[0].bound);
                auto len = formalType.dims[0].bound->apply(knowns).asUInt();
                size_t nbytes = len * formalType.baseType->width;
                //cerr << "local argument: type " << type.print() << " formalType " << formalType.print() << " nbytes " << nbytes << endl;
                clKernel.bindArg(i, LocalArray<std::byte>(nbytes));
                break;
            }
            default:
                throw MLDB::Exception("not (yet) implemented: replay CONSTANT trace arguments");
            }
            if (passedArg.isObject()) {
                // must be an array
            }
            else {
                // A simple parameter
            }
        }

        //cerr << "launching" << endl;
        Timer before;
        auto event = queue->clQueue.launch(clKernel, grid, block);
        event.waitUntilFinished();
        double elapsed = before.elapsed_wall();

        cerr << "run " << run.name << " ran in " << elapsed * 1000 << "ms" << endl;

    }
    
}