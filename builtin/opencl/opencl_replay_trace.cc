// ...
#include "mldb/block/zip_serializer.h"
#include "mldb/vfs/filter_streams.h"
#include "mldb/types/vector_description.h"
#include "mldb/types/map_description.h"
#include "mldb/types/meta_value_description.h"
#include "mldb/arch/timers.h"
#include "mldb/arch/ansi.h"
#include "compute_kernel_opencl.h"

#include <boost/program_options/cmdline.hpp>
#include <boost/program_options/options_description.hpp>
#include <boost/program_options/positional_options.hpp>
#include <boost/program_options/parsers.hpp>
#include <boost/program_options/variables_map.hpp>
#include <boost/algorithm/string.hpp>
#include <boost/exception/diagnostic_information.hpp> 


using namespace MLDB;
using namespace std;

int main(int argc, char ** argv)
{
    std::string traceFile = "file://./kernels.zip";
    string kernelName = "updateBuckets";

    using namespace boost::program_options;

    options_description all_opt;
    all_opt
        .add_options()
        ("trace-file,f", value(&traceFile), "File to read trace from")
        ("trace-kernel,k", value(&kernelName), "Kernel to trace")
        ("help", "print this message");

    variables_map vm;
    // command line has precendence over config
    try {
        store(command_line_parser(argc, argv)
              .options(all_opt)
              //.positional(p)
              .run(),
              vm);
    }
    catch (const boost::exception & exc) {
        cerr << boost::diagnostic_information(exc) << endl;
        return 1;
    }

    notify(vm);

    if (traceFile.find("://") == string::npos)
        traceFile = "file://" + traceFile;
    ZipStructuredReconstituter traceReader{Url(traceFile)};

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
    //auto source = program->getStream("source");
    filter_istream source("mldb/plugins/jml/randomforest_kernels.cl");
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
    std::vector<FrozenMemoryRegion> savedRegions;

    auto getCachedRegion = [&] (const std::string & regionName, int version) -> MemoryRegionHandle
    {
        Path p{regionName, version};
        auto it = cachedRegions.find(p);
        if (it == cachedRegions.end()) {
            auto region = regions->getRegionRecursive(p);
            //cerr << "  getting " << region.length() << " bytes for " << p << endl;
            if (regionName == "expandedRowData" && false) {
                auto p = (const float *)region.data();
                for (size_t i = 0;  i < 100;  ++i) {
                    cerr << "row " << i << " decoded " << p[i] << endl;
                }
            }
            auto handle = context->allocateSyncImpl("get cached " + p.toUtf8String().rawString(), region.length(), 64, typeid(void),
                                                    true, MemoryRegionInitialization::INIT_NONE);
            context->fillDeviceRegionFromHostSyncImpl("get cached " + p.toUtf8String().rawString(), handle,
                                                      {(const std::byte *)region.data(), region.length()});
            savedRegions.push_back(region);

            it = cachedRegions.emplace(p, handle).first;
        }

        if (regionName == "expandedRowData" && false) {
            auto region = context->transferToHostSyncImpl("debug", it->second);
            auto p = (const float *)region.data();
            for (size_t i = 0;  i < 100;  ++i) {
                cerr << "got back row " << i << " decoded " << p[i] << endl;
            }
        }

        return it->second;
    };

    std::stringstream sourceStr;
    sourceStr << source.rdbuf();
    OpenCLProgram clProgram = context->clContext.createProgram(sourceStr.str());
    //cerr << jsonEncode(clProgram.getProgramInfo()) << endl;
    auto clBuildInfo = clProgram.build(context->clDevices, buildInfo.buildOptions);
    //cerr << jsonEncode(buildInfo) << endl;

    auto timeRun = [&] (const StructuredReconstituter::Entry & run, uint32_t & tunedGrid, uint32_t & tunedBlock)
    {
        Timer start;

        auto clKernel = clProgram.createKernel(entryPoint);
        auto clKernelInfo = clKernel.getInfo();
        //cerr << jsonEncode(clKernelInfo) << endl;
        auto args = run.getStructure()->getObject<Json::Value>("args");
        auto grid = run.getStructure()->getObject<std::vector<size_t>>("grid");
        auto block = run.getStructure()->getObject<std::vector<size_t>>("block");
        auto knownsJson = run.getStructure()->getObject<std::map<std::string, Json::Value>>("knowns");
        auto tuneables = run.getStructure()->getObject<std::vector<ComputeTuneable>>("tuneables");

        if (tunedGrid == 0)
            tunedGrid = grid.at(0);
        else
            grid.at(0) = tunedGrid;

        if (tunedBlock == 0)
            tunedBlock = block.at(0);
        else
            block.at(0) = tunedBlock;

        //cerr << "tuneables = " << jsonEncode(tuneables) << endl;
        //cerr << "run " << run.name << " grid " << grid << " block " << block << " args " << endl << args << endl;
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
            //cerr << "arg " << i << ": " << argName << " = " << jsonEncodeStr(passedArg) << endl;
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
                //cerr << "arg " << argInfo.name << " length = " << length << " align = " << align << " size = " << size << " lengthInBytes = " << lengthInBytes << endl;

                MemoryRegionHandle memIn, mem;

                if (formalType.access & MemoryRegionAccess::ACC_READ) {
                    //cerr << "  getting for read" << endl;
                    memIn = getCachedRegion(regionName, version);
                }
                if (formalType.access & MemoryRegionAccess::ACC_WRITE) {
                    //cerr << "  getting for write" << endl;
                    mem = context->allocateSyncImpl(regionName, lengthInBytes, align, typeid(void), false /* isConst */,
                                                    MemoryRegionInitialization::INIT_NONE);
                    if (formalType.access == MemoryRegionAccess::ACC_READ_WRITE) {
                        //cerr << "  getting for read/write" << endl;
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

        //cerr << "setup took " << start.elapsed_wall() * 1000 << " ms" << endl; 

        //cerr << "launching " << kernelName << " " << grid << " " << block << endl;
        Timer before;
        auto event = queue->clQueue.launch(clKernel, grid, block);
        event.waitUntilFinished();
        double elapsed = before.elapsed_wall();

        //cerr << "run " << run.name << " ran in " << elapsed * 1000 << "ms" << endl;
        return elapsed * 1000.0;
    };

    double bestMin = INFINITY, bestMean = INFINITY;

    auto printRun = [&] (PathElement id, uint32_t grid, uint32_t block, const std::vector<double> & runValues)
    {
        double total = 0.0;
        double min = INFINITY;
        double max = -INFINITY;
        for (auto & run: runValues) {
            total += run;
            min = std::min(min, run);
            max = std::max(max, run);
        }
        double mean = total / runValues.size();

        double stderr = 0.0;
        for (auto & run: runValues) {
            stderr += (run - mean) * (run - mean);
        }
        double stddev = sqrt(stderr);

        if (min <= bestMin || mean <= bestMean) {
            bestMin = std::min(bestMin, min);
            bestMean = std::min(bestMean, mean);
            cerr << ansi::green;
        }

        cerr << format("%-25s %-10s %8d %6d %6d %8.2f %8.2f %8.2f %8.2f\n",
        //printf("%-25s %-10s %6d %6d %6d %8.2f %8.2f %8.2f %8.2f\n",
                       kernelName.c_str(),
                       id.toUtf8String().rawString().c_str(),
                       (int)runValues.size(), (int)grid, (int)block,
                       min, mean, stddev, max);

        cerr << ansi::reset;    
    };

    auto gridSizes = { 0, 256, 512, 768, 1024, 1536, 2048, 3072, 4096, 8192, 16384, 32768, 65536 };
    auto blockSizes = { 32, 64, 96, 128, 160, 192, 224, 256 };

    auto runDirectory = traceKernel->getStructure("runs")->getDirectory();
    int numIterations = 5;

    cerr << ansi::bold << "kernel                    run         iters      grid  block      min     mean      std      max" << ansi::reset << endl;

    auto doRun = [&] (uint32_t testGrid, uint32_t testBlock)
    {
        if (testGrid % testBlock != 0)
            return;

        try {
            std::vector<double> totalValues(numIterations);
            std::vector<std::vector<double>> runValues(runDirectory.size());
            for (size_t i = 0;  i < numIterations;  ++i) {
                for (size_t j = 0;  j < runDirectory.size();  ++j) {
                    const StructuredReconstituter::Entry & run = runDirectory[j];
                    double ms = timeRun(run, testGrid, testBlock);
                    runValues[j].push_back(ms);
                    totalValues[i] += ms;
                }
            }

            //for (size_t i = 0;  i < runDirectory.size();  ++i) {
            //    printRun(runDirectory[i].name, runValues[i]);
            //}
        
            printRun("total", testGrid, testBlock, totalValues);
        } MLDB_CATCH_ALL {

        }
    };

    // Benchmark the initial version
    uint32_t defaultGrid = 0, defaultBlock = 0;
    doRun(defaultGrid, defaultBlock);

    for (auto & testGrid: gridSizes) {
        for (auto & testBlock: blockSizes) {

            if (testGrid % testBlock != 0)
                continue;

            doRun(testGrid, testBlock);
        }
    }
}