/** compute_kernel_metal.cc                                                -*- C++ -*-
    Jeremy Barnes, 27 March 2016
    This file is part of MLDB. Copyright 2016 mldb.ai inc. All rights reserved.

    Compute kernel runtime for CPU devices.
*/

#include "compute_kernel_metal.h"
#include "mldb/types/basic_value_descriptions.h"
#include "mldb/types/meta_value_description.h"
#include "mldb/types/structure_description.h"
#include "mldb/types/set_description.h"
#include "mldb/types/generic_array_description.h"
#include "mldb/types/generic_atom_description.h"
#include "mldb/vfs/filter_streams.h"
#include "mldb/utils/environment.h"
#include "mldb/arch/ansi.h"
#include "mldb/block/zip_serializer.h"
#include "mldb/utils/command_expression_impl.h"
#include "mldb/arch/spinlock.h"
#include "mldb/arch/exception.h"
#include <compare>

using namespace std;

namespace MLDB {

struct MetalException: public AnnotatedException {
    static std::string getDomain(const ns::Error & error)
    {
        if (!error) return "<<no error>>";
        return error.GetDomain().GetCStr();
    }

    static std::string getDescription(const ns::Error & error)
    {
        if (!error) return "<<no error>>";
        return error.GetLocalizedDescription().GetCStr();
    }

    template<typename... NamesAndParams>
    MetalException(const ns::Error & error, const std::string & message, NamesAndParams&&... namesAndParams)
        : AnnotatedException(500, "METAL exception: " + message,
                             "domain", getDomain(error),
                             "description", getDescription(error),
                             std::forward<NamesAndParams>(namesAndParams)...),
          error(error), message(message)
    {
    }

    ns::Error error;
    std::string message;
};

template<typename... NamesAndParams>
void checkMetalError(const ns::Error & error, const std::string & message, NamesAndParams&&... namesAndParams)
{
    if (!error)
        return;

    throw MetalException(error, message, std::forward<NamesAndParams>(namesAndParams)...);
}

DEFINE_ENUM_DESCRIPTION_NAMED(MtlCommandBufferStatusDescription, mtlpp::CommandBufferStatus);

MtlCommandBufferStatusDescription::MtlCommandBufferStatusDescription()
{
    addValue("NotEnqueued", mtlpp::CommandBufferStatus::NotEnqueued, "");
    addValue("Enqueued", mtlpp::CommandBufferStatus::Enqueued, "");
    addValue("Scheduled", mtlpp::CommandBufferStatus::Scheduled, "");
    addValue("Committed", mtlpp::CommandBufferStatus::Committed, "");
    addValue("Error", mtlpp::CommandBufferStatus::Error, "");
    addValue("Completed", mtlpp::CommandBufferStatus::Completed, "");
}

namespace {

std::mutex kernelRegistryMutex;
struct KernelRegistryEntry {
    std::function<std::shared_ptr<MetalComputeKernel>(MetalComputeContext & context)> generate;
};

std::map<std::string, KernelRegistryEntry> kernelRegistry;

std::mutex libraryRegistryMutex;
struct LibraryRegistryEntry {
    std::function<std::shared_ptr<MetalComputeFunctionLibrary>(MetalComputeContext & context)> generate;
};

std::map<std::string, LibraryRegistryEntry> libraryRegistry;

struct MetalBindInfo: public GridBindInfo {
    MetalBindInfo(const MetalComputeKernel * owner)
        : owner(owner)
    {        
    }

    virtual ~MetalBindInfo() = default;

    const MetalComputeKernel * owner = nullptr;
    std::shared_ptr<StructuredSerializer> traceSerializer;

    // Pins that control the lifetime of the arguments and allow the system to know
    // when an argument is no longer needed
    std::vector<std::shared_ptr<const void>> argumentPins;
};

EnvOption<int> METAL_TRACE_API_CALLS("METAL_COMPUTE_TRACE_API_CALLS", 0);
EnvOption<std::string, true> METAL_KERNEL_TRACE_FILE("METAL_KERNEL_TRACE_FILE", "");
EnvOption<std::string, true> METAL_CAPTURE_FILE("METAL_CAPTURE_FILE", "");
EnvOption<bool, false> METAL_ENABLED("METAL_ENABLED", true);

std::shared_ptr<ZipStructuredSerializer> traceSerializer;
std::shared_ptr<StructuredSerializer> regionsSerializer;
std::shared_ptr<StructuredSerializer> kernelsSerializer;
std::shared_ptr<StructuredSerializer> programsSerializer;

using TracedRegionKey = std::tuple<std::string, int>;

struct TracedRegionEntry {
    size_t length = 0;
};

std::mutex tracedRegionMutex;
std::map<std::string, std::shared_ptr<StructuredSerializer>> regionSerializers;
std::map<TracedRegionKey, TracedRegionEntry> tracedRegions;

std::mutex tracedProgramMutex;
std::set<std::string> tracedPrograms;

bool versionIsTraced(const std::string & name, int version)
{
    std::unique_lock guard(tracedRegionMutex);
    return tracedRegions.count({name, version});
}

void traceVersion(std::string name, int version, const FrozenMemoryRegion & region)
{
    if (name == "") {
        name = format("%016llx", (unsigned long long)region.data());
    }
    ExcAssert(name != "");
    ExcAssertGreaterEqual(version, 0);

    if (!traceSerializer)
        return;

    std::unique_lock guard(tracedRegionMutex);
    auto it = tracedRegions.find({name, version});
    if (it != tracedRegions.end()) {
        ExcAssertEqual(it->second.length, region.length());
        return;
    }

    if (!regionSerializers.count(name)) {
        regionSerializers[name] = regionsSerializer->newStructure(name);
    }
    auto thisRegionSerializer = regionSerializers[name];
    ExcAssert(thisRegionSerializer);
    thisRegionSerializer->addRegion(region, version);

    tracedRegions[{name, version}] = {region.length()};
}

struct InitTrace {
    InitTrace()
    {
        if (METAL_KERNEL_TRACE_FILE.specified()) {
            traceSerializer = std::make_shared<ZipStructuredSerializer>(METAL_KERNEL_TRACE_FILE.get());
            regionsSerializer = traceSerializer->newStructure("regions");
            kernelsSerializer = traceSerializer->newStructure("kernels");
            programsSerializer = traceSerializer->newStructure("programs");
        }
    }

    ~InitTrace()
    {
        if (traceSerializer) {
            regionsSerializer->commit();
            kernelsSerializer->commit();
            traceSerializer->commit();
            programsSerializer->commit();
        }
    }
} initTrace;

// Use the Metal capture facility to create a file that xcode can open
struct InitCapture {
    mtlpp::CaptureManager captureManager;

    InitCapture()
    {
        mtlpp::CaptureManager::GetShared().StopCapture();
    }

    void startCapture(const mtlpp::CommandQueue & commandQueue)
    {
        if (captureManager)
            return;
        
        ExcAssert(commandQueue);

        if (METAL_CAPTURE_FILE.specified()) {
            captureManager = mtlpp::CaptureManager::GetShared();
            mtlpp::CaptureDescriptor captureDescriptor;
            //captureDescriptor.SetCaptureDevice(commandQueue.GetDevice());
            if (METAL_CAPTURE_FILE.get() == "xcode") {
                //captureDescriptor.SetDestination(mtlpp::CaptureDestination::DeveloperTools);
            }
            else {
                captureDescriptor.SetDestination(mtlpp::CaptureDestination::TraceDocument);
                captureDescriptor.SetOutputURL(METAL_CAPTURE_FILE.get().c_str());
            }

            ns::Error error{ns::Handle()};
            captureManager.StartCapture(commandQueue.GetDevice());

//            if (!captureManager.StartCapture(captureDescriptor, &error)) {
            if (!captureManager.IsCapturing()) {
                throw MetalException(error, "Error initializing capture (maybe you need to set METAL_DEVICE_WRAPPER_TYPE=1 in the environment?)");
            }
        }
    }

    ~InitCapture()
    {
        if (captureManager) {
            captureManager.StopCapture();
            char buf[PATH_MAX + 1024];
            std::string fullPath = METAL_CAPTURE_FILE.get();
            if (!fullPath.empty() && fullPath[0] != '/')
                fullPath = getcwd(buf, PATH_MAX + 1024) + string("/") + METAL_CAPTURE_FILE.get();
            cerr << "Metal capture file (should have a .gputrace extension) can be inspected with the following command:" << endl;
            cerr << "osascript -e '" << endl;
            cerr << "  tell application \"Xcode\"\n    open \"" << fullPath << "\"\n  end tell" << endl;
            cerr << "'";
        }
    }
} initCapture;

__thread int opCount = 0;
Timer startTimer;
double lastTime = 0;

static std::string printTime(double elapsed)
{
    std::string timerStr;
    if (elapsed < 0.000001) {
        timerStr = format("%.1fns", elapsed * 1000000000.0);
    }
    else if (elapsed < 0.001) {
        timerStr = format("%.1fus", elapsed * 1000000.0);
    }
    else if (elapsed < 1) {
        timerStr = format("%.1fms", elapsed * 1000.0);
    }
    else {
        timerStr = format("%.1fs", elapsed);
    }
    return timerStr;
}

enum class OperationType {
    METAL_COMPUTE = 1,
    USER = 2
};

enum class OperationScope {
    ENTER = 1,
    EXIT = 2,
    EXIT_EXC = 3,
    EVENT = 4
};

template<typename... Args>
void traceOperation(OperationScope opScope, OperationType opType, const std::string & opName, Args&&... args)
{
    if (METAL_TRACE_API_CALLS.get()) {
        using namespace MLDB::ansi;
        int tid = std::hash<std::thread::id>()(std::this_thread::get_id());
        double elapsed = startTimer.elapsed_wall();

        std::string opTypeName;
        std::string opColor;

        switch (opType) {
        case OperationType::METAL_COMPUTE:
            opTypeName = "METAL:";
            opColor = ansi::ansi_str_blue();
            break;
        case OperationType::USER:
            opTypeName = "USER:";
            opColor = ansi::ansi_str_bright_blue();
            break;
        }

        std::string opPrefix;

        switch (opScope) {
        case OperationScope::ENTER:
            opPrefix = "BEGIN ";
            break;

        case OperationScope::EVENT:
            opPrefix = "";
            break;

        case OperationScope::EXIT:
            opPrefix = "END ";
            break;

        case OperationScope::EXIT_EXC:
            opPrefix = "EXCEPTION ";
            opColor = ansi::ansi_str_bright_red();
            break;
        };

        if (true || (opScope == OperationScope::EXIT && (elapsed - lastTime) > 0.001)) {
            std::string header = format("%10.6f%9lld t%8x %-8s", elapsed, (long long)((elapsed-lastTime)*1000000000), tid, opTypeName.c_str());
            std::string indent(4 * opCount, ' ');
            std::string toDump = opColor + header + indent + opPrefix + opName
                               + ansi_str_reset() + "\n";
            cerr << toDump << flush;
        }

        lastTime = startTimer.elapsed_wall();
    }
}

template<typename... Args>
void traceOperation(OperationType opType, const std::string & opName, Args&&... args)
{
    traceOperation(OperationScope::EVENT, opType, opName, std::forward<Args>(args)...);
}

template<typename... Args>
void traceMetalOperation(const std::string & opName, Args&&... args)
{
    traceOperation(OperationScope::EVENT, OperationType::METAL_COMPUTE, opName, std::forward<Args>(args)...);
}

struct ScopedOperation {
    ScopedOperation(const ScopedOperation &) = delete;
    auto operator = (const ScopedOperation &) = delete;

    template<typename... Args>
    ScopedOperation(OperationType opType,
                    const std::string & opName, Args&&... args)
        : opType(opType), opName(opName)
    {
        traceOperation(OperationScope::ENTER, opType, opName, std::forward<Args>(args)...);
        ++opCount;
    }

    ~ScopedOperation()
    {
        if (!opName.empty()) {
            --opCount;

            if (std::uncaught_exceptions()) {
                traceOperation(OperationScope::EXIT_EXC, opType, opName);
                return;
            }

            double elapsed = timer.elapsed_wall();
            traceOperation(OperationScope::EXIT, opType, opName + " [" + ansi::ansi_str_underline() + printTime(elapsed) + "]");
        }
    }

    OperationType opType;
    std::string opName;
    Timer timer;
};

template<typename... Args>
ScopedOperation MLDB_WARN_UNUSED_RESULT scopedOperation(OperationType opType, const std::string & opName, Args&&... args) 
{
    return ScopedOperation(opType, opName, std::forward<Args>(args)...);
}

struct MetalMemoryRegionHandleInfo: public GridMemoryRegionHandleInfo {
    mtlpp::Buffer buffer;

    virtual MetalMemoryRegionHandleInfo * clone() const
    {
        return new MetalMemoryRegionHandleInfo(*this);
    }

    void init(mtlpp::Buffer mem, size_t offset)
    {
        this->buffer = std::move(mem);
        this->offset = offset;
        this->version = 0;
    }
};

} // file scope

// MetalComputeEvent

MetalComputeEvent::
MetalComputeEvent(const std::string & label, bool isResolvedIn, const MetalComputeQueue * queue)
    : GridComputeEvent(label, isResolvedIn, queue)
{
}

void
MetalComputeEvent::
resolveFromCommandBuffer(const mtlpp::CommandBuffer & buffer)
{
    ExcAssert(!isResolved);
    ExcAssert(!this->commandBuffer);

    this->commandBuffer = buffer;

    if (buffer.GetStatus() == mtlpp::CommandBufferStatus::Completed) {
        this->resolve();
        return;
    }

    if (buffer.GetStatus() == mtlpp::CommandBufferStatus::Error) {
        throw MLDB::Exception("Can't resolve command buffer with an error (TODO)");
        this->resolve();
        return;
    }

    if (buffer.GetStatus() >= mtlpp::CommandBufferStatus::Committed) {
        // https://developer.apple.com/documentation/metal/mtlcommandbuffer/1442997-addcompletedhandler?language=objc
        // You can’t add a completion handler after you commit the command buffer.
        cerr << "buffer.GetStatus() = " << jsonEncodeStr(buffer.GetStatus()) << endl;
        throw MLDB::Exception("cannot create event for committed command buffer");
    }

    std::weak_ptr<GridComputeEvent> weakThis = this->weak_from_this();

    auto onComplete = [weakThis] (const mtlpp::CommandBuffer & buffer)
    {
        auto sharedThis = weakThis.lock();
        if (!sharedThis)
            return;

        //cerr << ansi::red << "onComplete for event " << sharedThis->label() << ansi::reset << endl;
        sharedThis->resolve();
    };

    //cerr << ansi::red << "event from command buffer: status = " << (int)commandBuffer.GetStatus() << ansi::reset << endl;
    commandBuffer.AddCompletedHandler(onComplete);
}

void
MetalComputeEvent::
await() const
{
    //cerr << "await(): isResolved = " << isResolved << " commandBuffer = " << commandBuffer << endl;
    if (isResolved)
        return;

    // If we're actively waiting, we can do so in this thread and avoid having to wait for
    // the asynchronous mechanisms to eventually notice that it's done.
    if (commandBuffer) {
        auto tr = scopedOperation(OperationType::METAL_COMPUTE, "awaiting " + label() + " on command buffer");
        auto & mutableCommandBuffer = const_cast<mtlpp::CommandBuffer &>(commandBuffer);

        //cerr << "await(): status = " << jsonEncodeStr(commandBuffer.GetStatus()) << endl;

        // Try a busy-ish wait
        //while (mutableCommandBuffer.GetStatus() < mtlpp::CommandBufferStatus::Completed) {
        //    std::this_thread::sleep_for(std::chrono::milliseconds(1));
        //    std::this_thread::yield();
        //}

        mutableCommandBuffer.WaitUntilCompleted();

        //cerr << "await(): status = " << jsonEncodeStr(commandBuffer.GetStatus()) << endl;

        ((MetalComputeEvent *)this)->resolve();
    }
    else {
        auto tr = scopedOperation(OperationType::METAL_COMPUTE, "awaiting " + label() + " on future");        
        future.get();
        ExcAssert(isResolved);
    }
}

std::shared_ptr<MetalComputeEvent>
MetalComputeEvent::
makeAlreadyResolvedEvent(const std::string & label, const MetalComputeQueue * queue)
{
    auto result = std::make_shared<MetalComputeEvent>(label, true /* already resolved */, queue);
    return result;
}

std::shared_ptr<MetalComputeEvent>
MetalComputeEvent::
makeUnresolvedEvent(const std::string & label, const MetalComputeQueue * queue)
{
    auto result = std::make_shared<MetalComputeEvent>(label, false /* already resolved */, queue);
    return result;
}

// MetalComputeQueue

GridDispatchType generalizeDispatchType(mtlpp::DispatchType dispatchType)
{
    return dispatchType == mtlpp::DispatchType::Concurrent ? GridDispatchType::PARALLEL : GridDispatchType::SERIAL;
}

MetalComputeQueue::
MetalComputeQueue(MetalComputeContext * owner, MetalComputeQueue * parent,
                  const std::string & label,
                  mtlpp::CommandQueue queueIn, mtlpp::DispatchType dispatchType)
    : GridComputeQueue(owner, parent, label, generalizeDispatchType(dispatchType)), mtlOwner(owner),
      mtlQueue(std::move(queueIn)),
      dispatchType(dispatchType)
{
    if (!mtlQueue) {
        mtlQueue = owner->mtlDevice.NewCommandQueue(32 /* max command buffer count */);
        ExcAssert(mtlQueue);
        mtlQueue.SetLabel(ns::String(label.c_str()));
        initCapture.startCapture(mtlQueue);
    }

    commandBuffer = mtlQueue.CommandBuffer();
    ExcAssert(commandBuffer);
    commandBuffer.SetLabel(label.c_str());
}

MetalComputeQueue::~MetalComputeQueue()
{
}

std::shared_ptr<ComputeQueue>
MetalComputeQueue::
parallel(const std::string & opName)
{
    return std::make_shared<MetalComputeQueue>(this->mtlOwner, this, opName, this->mtlQueue, mtlpp::DispatchType::Concurrent);
}

std::shared_ptr<ComputeQueue>
MetalComputeQueue::
serial(const std::string & opName)
{
    return std::make_shared<MetalComputeQueue>(this->mtlOwner, this, opName, this->mtlQueue, mtlpp::DispatchType::Serial);
}

void
MetalComputeQueue::
enqueueZeroFillArrayConcrete(const std::string & opName,
                                MemoryRegionHandle region,
                                size_t startOffsetInBytes, ssize_t lengthInBytes)
{
    auto [pin, buffer, offset] = MetalComputeContext::getMemoryRegion(opName, *region.handle, ACC_WRITE);
    auto length = buffer.GetLength();
    ExcAssertLessEqual(offset + startOffsetInBytes + lengthInBytes, length);

    auto blitEncoder = commandBuffer.BlitCommandEncoder();
    beginEncoding(opName, blitEncoder);
    blitEncoder.SetLabel((opName + " zero fill blit").c_str());
    blitEncoder.Fill(buffer, { uint32_t(offset + startOffsetInBytes), uint32_t(lengthInBytes) }, 0);
    endEncoding(opName, blitEncoder);
}                                
                                
void
MetalComputeQueue::
enqueueBlockFillArrayConcrete(const std::string & opName,
                              MemoryRegionHandle region,
                              size_t startOffsetInBytes, ssize_t lengthInBytes,
                              std::span<const std::byte> block)
{
    ComputeQueue::enqueueFillArrayImpl(opName, region, MemoryRegionInitialization::INIT_BLOCK_FILLED, startOffsetInBytes, lengthInBytes, block);
}                                
                                
void
MetalComputeQueue::
enqueueCopyFromHostConcrete(const std::string & opName,
                            MemoryRegionHandle toRegion,
                            FrozenMemoryRegion fromRegion,
                            size_t deviceStartOffsetInBytes)
{
    auto [pin, buffer, offset] = MetalComputeContext::getMemoryRegion(opName, *toRegion.handle, ACC_WRITE);
    auto length = buffer.GetLength();
    ExcAssertLessEqual(offset + deviceStartOffsetInBytes + fromRegion.length(), length);

    switch (buffer.GetStorageMode()) {
    case mtlpp::StorageMode::Managed:
    case mtlpp::StorageMode::Shared: {
        //cerr << opName << " is managed/shared" << endl;
        std::byte * contents = (std::byte *)buffer.GetContents();
        ExcAssert(contents);
        memcpy(contents + offset + deviceStartOffsetInBytes, fromRegion.data(), fromRegion.length());
        buffer.DidModify({(uint32_t)(deviceStartOffsetInBytes + offset), (uint32_t)fromRegion.length()});
        break;
    }
    case mtlpp::StorageMode::Private: {
        //cerr << opName << " is private" << endl;
        auto blitEncoder = commandBuffer.BlitCommandEncoder();
        beginEncoding(opName, blitEncoder);

        // It's a private region; we need to create a new region to hold the result, and then
        // synchronize
        mtlpp::ResourceOptions options = mtlpp::ResourceOptions::StorageModeManaged;
        auto tmpBuffer = mtlOwner->mtlDevice.NewBuffer((const void *)fromRegion.data(), fromRegion.length(), options);
        tmpBuffer.SetLabel((opName + " copyFromHostSync private copy").c_str());
        blitEncoder.Copy(tmpBuffer, 0 /* sourceOffset */, buffer, offset + deviceStartOffsetInBytes /* dstOffset */, fromRegion.length());
        endEncoding(opName, blitEncoder);

        // We pin the temp buffer instead
        auto freeTmpBuffer = [tmpBuffer, pin=pin] (auto arg) {};
        pin = std::shared_ptr<const void>(nullptr, freeTmpBuffer);

        break;
    }
    default:
        throw MLDB::Exception("Cannot manage MemoryLess storage mode");
    }
}                            
                            

FrozenMemoryRegion
MetalComputeQueue::
enqueueTransferToHostConcrete(const std::string & opName, MemoryRegionHandle handle)
{
    MLDB_THROW_UNIMPLEMENTED();
}


FrozenMemoryRegion
MetalComputeQueue::
transferToHostSyncConcrete(const std::string & opName, MemoryRegionHandle handle)
{
    MLDB_THROW_UNIMPLEMENTED();
}

struct MetalBindContext: public GridBindContext {
    MetalBindContext(MetalComputeQueue * queue,
                     const std::string & opName,
                     const GridComputeKernel * kernel,
                     const GridBindInfo * bindInfo)
        : kernel(dynamic_cast<const MetalComputeKernel *>(kernel)),
          bindInfo(dynamic_cast<const MetalBindInfo *>(bindInfo)),
          queue(queue), opName(opName)
    {
        ExcAssert(this->kernel);
        ExcAssert(this->bindInfo);

        commandEncoder = queue->commandBuffer.ComputeCommandEncoder();
        queue->beginEncoding(opName, commandEncoder);
        commandEncoder.SetComputePipelineState(this->kernel->mtlFunction->computePipelineState);
    }

    ~MetalBindContext()
    {
        queue->endEncoding(opName, commandEncoder);
    }

    const MetalComputeKernel * kernel = nullptr;
    const MetalBindInfo * bindInfo = nullptr;
    MetalComputeQueue * queue = nullptr;
    std::string opName;
    
    mtlpp::ComputeCommandEncoder commandEncoder;

    virtual void
    setPrimitive(const std::string & opName, int argNum, std::span<const std::byte> bytes) override
    {
        cerr << "setPrimitive: argNum " << argNum << " bytes " << bytes.size_bytes() << endl;
        commandEncoder.SetBytes(bytes.data(), bytes.size_bytes(), argNum);
    }

    virtual void
    setBuffer(const std::string & opName, int argNum,
              std::shared_ptr<GridMemoryRegionHandleInfo> handle,
              MemoryRegionAccess access) override
    {
        cerr << "setBuffer: argNum " << argNum << " bytes " << handle->lengthInBytes << endl;
        ExcAssert(handle);
        auto [pin, memBuffer, offset] = MetalComputeContext::getMemoryRegion(opName, *handle, access);
        // TODO: where do we put the pin?
        commandEncoder.SetBuffer(memBuffer, offset, argNum);
    }

    virtual void
    setThreadGroupMemory(const std::string & opName, int argNum, size_t nBytes) override
    {
        // Set it up in the command encoder
        commandEncoder.SetThreadgroupMemory(nBytes, argNum);
    }
};

std::shared_ptr<GridBindContext>
MetalComputeQueue::
newBindContext(const std::string & opName,
               const GridComputeKernel * kernel,
               const GridBindInfo * bindInfo)
{
    ExcAssert(bindInfo);
    return std::make_shared<MetalBindContext>(this, opName, kernel, bindInfo);
}

void
MetalComputeQueue::
launch(const std::string & opName, GridBindContext & context, std::vector<size_t> grid, std::vector<size_t> block)
{
    MetalBindContext & bindContext = dynamic_cast<MetalBindContext &>(context);
    auto * kernel = bindContext.kernel;

    try {
        grid.resize(3, 1);
        block.resize(3, 1);

        mtlpp::Size gridSize(grid[0], grid[1], grid[2]);
        mtlpp::Size blockSize(block[0], block[1], block[2]);

        auto invocations = grid[0] * grid[1] * grid[2] * block[0] * block[1] * block[2];
        cerr << "kernel " << kernel->kernelName << " grid " << grid << " block " << block
             << " requires " << invocations << " invocations and "
             << invocations * 2048 / 1024.0 / 1024.0 << "MB of wired scratchpad memory" << endl;

        bindContext.commandEncoder.DispatchThreadgroups(gridSize, blockSize);
    } MLDB_CATCH_ALL {
        rethrowException(400, "Error launching Metal kernel " + kernel->kernelName);
    }
}

template<typename CommandEncoder>
void
MetalComputeQueue::
beginEncodingImpl(const std::string & opName, CommandEncoder & encoder, bool force)
{
    //cerr << "BEGIN ENCODING " << this << " " << opName << " " << type_name<CommandEncoder>() << endl;

    if (numChildren > 0) {
        throw MLDB::Exception("Cannot use a MetalComputeQueue that has a child queue once it has been created");
    }

    ExcAssert(encoder);
    encoder.SetLabel(opName.c_str());
    if (force || this->dispatchType == mtlpp::DispatchType::Serial) {
        //cerr << "awaiting " << activeCommands.size() << " fences before " << opName << endl;
        for (auto fence: activeCommands)
            encoder.WaitForFence(fence);
        activeCommands.clear();
    }
}

template<typename CommandEncoder>
void
MetalComputeQueue::
endEncodingImpl(const std::string & opName, CommandEncoder & encoder, bool force)
{
    //cerr << "END ENCODING " << this << " " << opName << " " << type_name<CommandEncoder>() << endl;
    auto fence = mtlOwner->mtlDevice.NewFence();
    ExcAssert(fence);
    encoder.UpdateFence(fence);
    encoder.EndEncoding();
    activeCommands.emplace_back(std::move(fence));
}

void
MetalComputeQueue::
beginEncoding(const std::string & opName, mtlpp::ComputeCommandEncoder & encoder, bool force)
{
    beginEncodingImpl(opName, encoder, force);
}

void
MetalComputeQueue::
beginEncoding(const std::string & opName, mtlpp::BlitCommandEncoder & encoder, bool force)
{
    beginEncodingImpl(opName, encoder, force);
}

void
MetalComputeQueue::
endEncoding(const std::string & opName, mtlpp::ComputeCommandEncoder & encoder, bool force)
{
    endEncodingImpl(opName, encoder, force);
}

void
MetalComputeQueue::
endEncoding(const std::string & opName, mtlpp::BlitCommandEncoder & encoder, bool force)
{
    endEncodingImpl(opName, encoder, force);
}


#if 0
void
MetalComputeQueue::
enqueueFillArrayImpl(const std::string & opName,
                     MemoryRegionHandle region, MemoryRegionInitialization init,
                     size_t startOffsetInBytes, ssize_t lengthInBytes,
                     std::span<const std::byte> block)
{
    auto op = scopedOperation(OperationType::METAL_COMPUTE, "enqueueFillArrayImpl " + opName);

    if (startOffsetInBytes > region.lengthInBytes()) {
        throw MLDB::Exception("region is too long");
    }
    if (lengthInBytes == -1)
        lengthInBytes = region.lengthInBytes() - startOffsetInBytes;
    
    if (startOffsetInBytes + lengthInBytes > region.lengthInBytes()) {
        throw MLDB::Exception("overflowing memory region");
    }

    switch (init) {
    case MemoryRegionInitialization::INIT_NONE:
        return;
    case MemoryRegionInitialization::INIT_ZERO_FILLED: {
        auto [pin, buffer, offset] = MetalComputeContext::getMemoryRegion(opName, *region.handle, ACC_WRITE);
        auto length = buffer.GetLength();
        ExcAssertLessEqual(offset + startOffsetInBytes + lengthInBytes, length);

        auto blitEncoder = commandBuffer.BlitCommandEncoder();
        beginEncoding(opName, blitEncoder);
        blitEncoder.SetLabel((opName + " zero fill blit").c_str());
        blitEncoder.Fill(buffer, { uint32_t(offset + startOffsetInBytes), uint32_t(lengthInBytes) }, 0);
        endEncoding(opName, blitEncoder);

        return;
    }
    default:
        ComputeQueue::enqueueFillArrayImpl(opName, region, init, startOffsetInBytes, lengthInBytes, block);
        return;
    }

}

void
MetalComputeQueue::
enqueueCopyFromHostImpl(const std::string & opName,
                        MemoryRegionHandle toRegion,
                        FrozenMemoryRegion fromRegion,
                        size_t deviceStartOffsetInBytes)
{
    MetalMemoryRegionHandleInfo * upcastHandle = dynamic_cast<MetalMemoryRegionHandleInfo *>(toRegion.handle.get());
    if (!upcastHandle) {
        throw MLDB::Exception("Wrong MetalComputeContext handle: got " + demangle(typeid(toRegion.handle)));
    }
    if (upcastHandle->backingHostMem) {
        throw MLDB::Exception("cannot copy from host into a read-only host backed array");
    }

}

void
MetalComputeQueue::
copyFromHostSyncImpl(const std::string & opName,
                            MemoryRegionHandle toRegion,
                            FrozenMemoryRegion fromRegion,
                            size_t deviceStartOffsetInBytes)
{
    enqueueCopyFromHostImpl(opName, toRegion, fromRegion, deviceStartOffsetInBytes);
    finish();
}
#endif

FrozenMemoryRegion
MetalComputeQueue::
enqueueTransferToHostImpl(const std::string & opName,
                          MemoryRegionHandle handle)
{
    auto op = scopedOperation(OperationType::METAL_COMPUTE, "MetalComputeQueue enqueueTransferToHostSyncImpl " + opName);
    MLDB_THROW_UNIMPLEMENTED();
}

FrozenMemoryRegion
MetalComputeQueue::
transferToHostSyncImpl(const std::string & opName,
                       MemoryRegionHandle handle)
{
    auto op = scopedOperation(OperationType::METAL_COMPUTE, "MetalComputeQueue transferToHostSyncImpl " + opName);

    MetalMemoryRegionHandleInfo * upcastHandle = dynamic_cast<MetalMemoryRegionHandleInfo *>(handle.handle.get());
    if (!upcastHandle) {
        throw MLDB::Exception("Wrong MetalComputeContext handle: got " + demangle(typeid(handle)));
    }
    if (upcastHandle->backingHostMem) {
        // It's a read-only, host-first mapping... so nothing to do
        return { handle.handle, (const char *)upcastHandle->backingHostMem, (size_t)upcastHandle->lengthInBytes };
    }

    auto [pin, buffer, offset] = MetalComputeContext::getMemoryRegion(opName, *handle.handle, ACC_READ);

    //cerr << "transferToHostSyncImpl " << this << ": status " << jsonEncodeStr(commandBuffer.GetStatus()) << endl;

    auto blitEncoder = commandBuffer.BlitCommandEncoder();
    beginEncoding(opName, blitEncoder);

    const void * contents;
    auto length = buffer.GetLength();
    ExcAssertLessEqual(handle.handle->lengthInBytes, length);

    switch (buffer.GetStorageMode()) {
    case mtlpp::StorageMode::Managed:
        blitEncoder.Synchronize(buffer);
        // fall through
    case mtlpp::StorageMode::Shared:
        contents = buffer.GetContents();
        break;
    case mtlpp::StorageMode::Private: {
        // It's a private region; we need to create a new region to hold the result, and then
        // synchronize
        mtlpp::ResourceOptions options = mtlpp::ResourceOptions::StorageModeManaged;
        auto tmpBuffer = mtlOwner->mtlDevice.NewBuffer(length == 0 ? 4 : length, options);
        tmpBuffer.SetLabel((opName + " transferToHostSync private copy").c_str());
        blitEncoder.Copy(buffer, offset, tmpBuffer, 0 /* offset */, length - offset);
        blitEncoder.Synchronize(tmpBuffer);
        contents = tmpBuffer.GetContents();

        // We pin the temp buffer instead
        auto freeTmpBuffer = [tmpBuffer] (auto arg) {};
        pin = std::shared_ptr<const void>(nullptr, freeTmpBuffer);
        break;
    }
    default:
        throw MLDB::Exception("Cannot manage MemoryLess storage mode");
    }
    endEncoding(opName, blitEncoder);

    auto myCommandBuffer = commandBuffer;

    finish();

    //ExcAssert(commandBuffer);
    //commandBuffer.Commit();
    //commandBuffer.WaitUntilCompleted();

    auto status = myCommandBuffer.GetStatus();
    if (status != mtlpp::CommandBufferStatus::Completed) {
        auto error = commandBuffer.GetError();
        throw MetalException(error, "Command buffer is not completed", "status", status);
    }

    //auto storageMode = buffer.GetStorageMode();
    //cerr << "storage mode is " << (int)storageMode << endl;


    //cerr << "contents " << contents << endl;
    //cerr << "length " << length << endl;

    //cerr << "region length for region " << handle.handle->name
    //     << " with lengthInBytes " << handle.handle->lengthInBytes
    //     << " is " << length << " and contents " << contents << endl;

    FrozenMemoryRegion result(std::move(pin), ((const char *)contents) + offset, handle.handle->lengthInBytes);
    return result;
}

MutableMemoryRegion
MetalComputeQueue::
enqueueTransferToHostMutableImpl(const std::string & opName, MemoryRegionHandle handle)
{
    MLDB_THROW_UNIMPLEMENTED();

#if 0
    auto op = scopedOperation(OperationType::METAL_COMPUTE, "MetalComputeContext transferToHostMutableImpl " + opName);
    ExcAssert(handle.handle);

    auto [pin, mem, offset] = getMemoryRegion(opName, *handle.handle, ACC_READ_WRITE);
    MetalEvent clEvent;
    std::shared_ptr<void> memPtr;
    std::tie(memPtr, clEvent)
        = clQueue->clQueue.enqueueMapBuffer(mem, CL_MAP_READ | CL_MAP_WRITE,
                                    offset, handle.lengthInBytes());

    auto event = std::make_shared<MetalComputeEvent>(std::move(clEvent));
    auto promise = std::shared_ptr<std::promise<std::any>>();

    auto cb = [handle, promise, memPtr, pin=pin] (const MetalEvent & event, auto status)
    {
        if (status == MetalEventCommandExecutionStatus::ERROR)
            promise->set_exception(std::make_exception_ptr(MLDB::Exception("Metal error mapping host memory")));
        else {
            promise->set_value(MutableMemoryRegion(memPtr, (char *)memPtr.get(), handle.lengthInBytes()));
        }
    };

    clEvent.addCallback(cb);

    return { std::move(promise), std::move(event) };
#endif
}

MutableMemoryRegion
MetalComputeQueue::
transferToHostMutableSyncImpl(const std::string & opName,
                              MemoryRegionHandle handle)
{
    MLDB_THROW_UNIMPLEMENTED();

#if 0
    auto op = scopedOperation(OperationType::METAL_COMPUTE, "MetalComputeContext transferToHostMutableSyncImpl " + opName);

    ExcAssert(handle.handle);

    auto [pin, mem, offset] = getMemoryRegion(opName, *handle.handle, ACC_READ_WRITE);
    auto memPtr = clQueue->clQueue.enqueueMapBufferBlocking(mem, CL_MAP_READ | CL_MAP_WRITE,
                                                            offset, handle.lengthInBytes());
    MutableMemoryRegion result(std::move(memPtr), (char *)memPtr.get(), handle.lengthInBytes());
    return result;
#endif
}

MemoryRegionHandle
MetalComputeQueue::
enqueueManagePinnedHostRegionImpl(const std::string & opName, std::span<const std::byte> region, size_t align,
                                  const std::type_info & type, bool isConst)
{
    auto op = scopedOperation(OperationType::METAL_COMPUTE, "MetalComputeContext managePinnedHostRegionImpl " + opName);
    return managePinnedHostRegionSyncImpl(opName, region, align, type, isConst);
}

MemoryRegionHandle
MetalComputeQueue::
managePinnedHostRegionSyncImpl(const std::string & opName,
                               std::span<const std::byte> region, size_t align,
                               const std::type_info & type, bool isConst)
{
    auto op = scopedOperation(OperationType::METAL_COMPUTE, "MetalComputeContext managePinnedHostRegionSyncImpl " + opName);

    cerr << "managing pinned host region " << opName << " of length " << region.size() << endl;

    mtlpp::ResourceOptions options
         = mtlpp::ResourceOptions::StorageModeShared;


    traceMetalOperation("region size " + std::to_string(region.size()));

    // This overload calls newBufferWithBytesNoCopy
    // Note that we require a page-aligned address and size, and a single VM region
    // See https://developer.apple.com/documentation/metal/mtldevice/1433382-makebuffer
    // Thus, for now we don't try; instead we copy and blit
    mtlpp::Buffer buffer;
    if (false) {
        auto deallocator = [=] (auto ptr, auto len)
        {
            cerr << ansi::bright_red << "DEALLOCATING PINNED REGION " << opName << ansi::reset
                << " of length " << len << endl;
        };

        buffer = mtlOwner->mtlDevice.NewBuffer((void *)region.data(), region.size(), options, deallocator);
    }
    else {
        if (region.empty()) {
            buffer = mtlOwner->mtlDevice.NewBuffer(4, options);
        }
        else {
            buffer = mtlOwner->mtlDevice.NewBuffer((const void *)region.data(), region.size(), options);
        }
    }
    buffer.SetLabel(opName.c_str());

    auto handle = std::make_shared<MetalMemoryRegionHandleInfo>();
    handle->buffer = std::move(buffer);
    handle->offset = 0;
    handle->type = &type;
    handle->isConst = isConst;
    handle->lengthInBytes = region.size();
    handle->version = 0;
    handle->name = opName;
    handle->backingHostMem = region.data();
    MemoryRegionHandle result{std::move(handle)};
    return result;
}

void
MetalComputeQueue::
enqueueCopyBetweenDeviceRegionsImpl(const std::string & opName,
                                    MemoryRegionHandle from, MemoryRegionHandle to,
                                    size_t fromOffset, size_t toOffset,
                                    size_t length)
{
    auto op = scopedOperation(OperationType::METAL_COMPUTE, "MetalComputeQueue enqueueCopyBetweenDeviceRegionsImpl " + opName);

    MetalMemoryRegionHandleInfo * upcastFromHandle = dynamic_cast<MetalMemoryRegionHandleInfo *>(from.handle.get());
    if (!upcastFromHandle) {
        throw MLDB::Exception("Wrong MetalComputeContext from handle: got " + demangle(typeid(from.handle)));
    }

    auto [fromPin, fromBuffer, fromBaseOffset] = MetalComputeContext::getMemoryRegion(opName, *from.handle, ACC_READ);

    ExcAssertLessEqual(fromBaseOffset + fromOffset + length, from.lengthInBytes());

    MetalMemoryRegionHandleInfo * upcastToHandle = dynamic_cast<MetalMemoryRegionHandleInfo *>(to.handle.get());
    if (!upcastToHandle) {
        throw MLDB::Exception("Wrong MetalComputeContext to handle: got " + demangle(typeid(to.handle)));
    }

    auto [toPin, toBuffer, toBaseOffset] = MetalComputeContext::getMemoryRegion(opName, *to.handle, ACC_WRITE);

    ExcAssertLessEqual(toBaseOffset + toOffset + length, to.lengthInBytes());

    auto blitEncoder = commandBuffer.BlitCommandEncoder();
    beginEncoding(opName, blitEncoder);
    blitEncoder.Copy(fromBuffer, fromBaseOffset + fromOffset, toBuffer, toBaseOffset + toOffset, length);
    endEncoding(opName, blitEncoder);
}

void
MetalComputeQueue::
copyBetweenDeviceRegionsSyncImpl(const std::string & opName,
                                 MemoryRegionHandle from, MemoryRegionHandle to,
                                 size_t fromOffset, size_t toOffset,
                                 size_t length)
{
    enqueueCopyBetweenDeviceRegionsImpl(opName, from, to, fromOffset, toOffset, length);
    finish();
}

std::shared_ptr<ComputeEvent>
MetalComputeQueue::
flush()
{
    auto op = scopedOperation(OperationType::METAL_COMPUTE, "MetalComputeQueue flush");

    auto result = std::make_shared<MetalComputeEvent>("flush", false /* resolved */, this);
    ExcAssert(commandBuffer);

    //cerr << "flush(): " << this << " status = " << jsonEncodeStr(commandBuffer.GetStatus()) << endl;

    if (commandBuffer.GetStatus() < mtlpp::CommandBufferStatus::Completed) {
        result->resolveFromCommandBuffer(commandBuffer);
        if (commandBuffer.GetStatus() < mtlpp::CommandBufferStatus::Committed)
            commandBuffer.Commit();
    }
    else {
        // TODO: error
        result->resolve();
    }

    //cerr << "flush(): " << this << " status now " << jsonEncodeStr(commandBuffer.GetStatus()) << endl;

    auto label = commandBuffer.GetLabel();

    commandBuffer = mtlQueue.CommandBuffer();
    commandBuffer.SetLabel(label);
    ExcAssert(commandBuffer);

    //cerr << "flush(): " << this << " status at end is " << jsonEncodeStr(commandBuffer.GetStatus()) << endl;

    return result;
}

void
MetalComputeQueue::
enqueueBarrier(const std::string & label)
{
    auto op = scopedOperation(OperationType::METAL_COMPUTE, "MetalComputeQueue enqueueBarrier " + label);
    auto encoder = commandBuffer.ComputeCommandEncoder(mtlpp::DispatchType::Serial);
    beginEncoding("enqueueBarrier " + label, encoder, true /* force */);
    endEncoding("enqueueBarrier " + label, encoder, true /* force */);
}

void
MetalComputeQueue::
finish()
{
    auto op = scopedOperation(OperationType::METAL_COMPUTE, "MetalComputeQueue finish");
    flush()->await();
}

std::shared_ptr<ComputeEvent>
MetalComputeQueue::
makeAlreadyResolvedEvent(const std::string & label) const
{
    return MetalComputeEvent::makeAlreadyResolvedEvent(label, this);
}


// MetalComputeContext

MetalComputeContext::
MetalComputeContext(mtlpp::Device mtlDevice, ComputeDevice device)
    : GridComputeContext(device), mtlDevice(mtlDevice)
{
#if 0
    mtlpp::HeapDescriptor heapDescriptor;
    heapDescriptor.SetStorageMode(mtlpp::StorageMode::Private);
    heapDescriptor.SetSize(1 << 30);  // 1GB

    this->heap = mtlDevice.NewHeap(heapDescriptor);
    ExcAssert(this->heap);
#endif
}


std::tuple<std::shared_ptr<const void>, mtlpp::Buffer, size_t>
MetalComputeContext::
getMemoryRegion(const std::string & opName, MemoryRegionHandleInfo & handle, MemoryRegionAccess access)
{
    MetalMemoryRegionHandleInfo * upcastHandle = dynamic_cast<MetalMemoryRegionHandleInfo *>(&handle);
    if (!upcastHandle) {
        throw MLDB::Exception("TODO: get memory region from block handled from elsewhere: got " + demangle(typeid(handle)));
    }
    auto pin = upcastHandle->pinAccess(opName, access);

    return { std::move(pin), upcastHandle->buffer, upcastHandle->offset };
}

std::tuple<FrozenMemoryRegion, int /* version */>
MetalComputeContext::
getFrozenHostMemoryRegion(const std::string & opName, MemoryRegionHandleInfo & handle,
                          size_t offset, ssize_t length,
                          bool ignoreHazards) const
{
    MetalMemoryRegionHandleInfo * upcastHandle = dynamic_cast<MetalMemoryRegionHandleInfo *>(&handle);
    if (!upcastHandle) {
        throw MLDB::Exception("TODO: get memory region from block handled from elsewhere: got " + demangle(typeid(handle)));
    }
    return upcastHandle->getReadOnlyHostAccessSync(*this, opName, offset, length, ignoreHazards);
}

static MemoryRegionHandle
doMetalAllocate(mtlpp::Heap & mtlHeap,
                mtlpp::Device & mtlDevice,
                 const std::string & regionName,
                 size_t length, size_t align,
                 const std::type_info & type,
                 bool isConst)
{
    cerr << "allocating " << length << " bytes for " << regionName << endl;
    // TODO: align...
    //mtlpp::ResourceOptions options = mtlpp::ResourceOptions::StorageModeManaged;
    mtlpp::ResourceOptions options = mtlpp::ResourceOptions::StorageModePrivate;
    auto buffer = mtlDevice.NewBuffer(length == 0 ? 4 : length, options);
    //auto buffer = mtlHeap.NewBuffer(length == 0 ? 4 : length, options);
    if (!buffer) {
        throw MLDB::Exception("Error allocating buffer from heap");
    }
    buffer.SetLabel(regionName.c_str());

    auto handle = std::make_shared<MetalMemoryRegionHandleInfo>();
    handle->buffer = std::move(buffer);
    handle->offset = 0;
    handle->type = &type;
    handle->isConst = isConst;
    handle->lengthInBytes = length;
    handle->name = regionName;
    handle->version = 0;

    MemoryRegionHandle result{std::move(handle)};
    return result;
}

MemoryRegionHandle
MetalComputeContext::
allocateSyncImpl(const std::string & regionName,
                 size_t length, size_t align,
                 const std::type_info & type, bool isConst)
{
    auto op = scopedOperation(OperationType::METAL_COMPUTE, "MetalComputeContext allocateSyncImpl " + regionName);
    auto result = doMetalAllocate(this->heap, this->mtlDevice, regionName, length, align, type, isConst);
    return result;
}

static MemoryRegionHandle
doMetalTransferToDevice(mtlpp::Device & mtlDevice,
                         const std::string & opName, FrozenMemoryRegion region,
                         const std::type_info & type, bool isConst)
{
    Timer timer;

    mtlpp::ResourceOptions options = mtlpp::ResourceOptions::StorageModeManaged;
    
    mtlpp::Buffer buffer;
    if (region.length() == 0)
        buffer = mtlDevice.NewBuffer(4, options);
    else {
        buffer = mtlDevice.NewBuffer(region.data(), region.length(), options);
    }
    buffer.SetLabel(opName.c_str());

    auto handle = std::make_shared<MetalMemoryRegionHandleInfo>();
    handle->buffer = std::move(buffer);
    handle->offset = 0;
    handle->type = &type;
    handle->isConst = isConst;
    handle->lengthInBytes = region.length();
    handle->name = opName;
    handle->version = 0;
    MemoryRegionHandle result{std::move(handle)};

    using namespace std;
    cerr << "transferring " << opName << " " << region.memusage() / 1000000.0 << " Mbytes of type "
        << demangle(type.name()) << " isConst " << isConst << " to device in "
            << timer.elapsed_wall() << " at "
            << region.memusage() / 1000000.0 / timer.elapsed_wall() << "MB/sec" << endl;

    return result;
}

#if 0
MemoryRegionHandle
MetalComputeContext::
transferToDeviceImpl(const std::string & opName, FrozenMemoryRegion region,
                     const std::type_info & type, bool isConst)
{
    auto op = scopedOperation(OperationType::METAL_COMPUTE, "MetalComputeContext transferToDeviceImpl " + opName);
    auto result = doMetalTransferToDevice(mtlDevice, opName, region, type, isConst);
    return {std::move(result), MetalComputeEvent::makeAlreadyResolvedEvent(opName) };
}

MemoryRegionHandle
MetalComputeContext::
transferToDeviceSyncImpl(const std::string & opName,
                         FrozenMemoryRegion region,
                         const std::type_info & type, bool isConst)
{
    MLDB_THROW_UNIMPLEMENTED();
#if 0
    auto op = scopedOperation(OperationType::METAL_COMPUTE, "MetalComputeContext transferToDeviceSyncImpl " + opName);
    auto result = doMetalTransferToDevice(clContext, opName, region, type, isConst);
    return result;
#endif
}


FrozenMemoryRegion
MetalComputeContext::
transferToHostImpl(const std::string & opName, MemoryRegionHandle handle)
{
    // TODO: async
    return transferToHostSyncImpl(opName, handle);
}

FrozenMemoryRegion
MetalComputeContext::
transferToHostSyncImpl(const std::string & opName,
                       MemoryRegionHandle handle)
{
    auto op = scopedOperation(OperationType::METAL_COMPUTE, "MetalComputeContext transferToHostSyncImpl " + opName);
    return queue->transferToHostSyncImpl(opName, std::move(handle));
}

#endif

#if 0
std::shared_ptr<ComputeKernel>
MetalComputeContext::
getKernel(const std::string & kernelName)
{
    auto op = scopedOperation(OperationType::METAL_COMPUTE, "MetalComputeContext getKernel " + kernelName);

    std::unique_lock guard(kernelRegistryMutex);
    auto it = kernelRegistry.find(kernelName);
    if (it == kernelRegistry.end()) {
        throw AnnotatedException(400, "Unable to find Metal compute kernel '" + kernelName + "'",
                                        "kernelName", kernelName);
    }
    auto result = it->second.generate(*this);
    result->context = this;
    if (traceSerializer) {
        result->traceSerializer = kernelsSerializer->newStructure(kernelName);
        result->runsSerializer = result->traceSerializer->newStructure("runs");
    }
    return result;
}
#endif

#if 0
void
MetalComputeContext::
fillDeviceRegionFromHostImpl(const std::string & opName,
                             MemoryRegionHandle deviceHandle,
                             std::shared_ptr<std::span<const std::byte>> pinnedHostRegion,
                             size_t deviceOffset)
{
    MLDB_THROW_UNIMPLEMENTED();
#if 0
    FrozenMemoryRegion region(pinnedHostRegion, (const char *)pinnedHostRegion->data(), pinnedHostRegion->size_bytes());

    return clQueue
        ->enqueueCopyFromHostImpl(opName, deviceHandle, region, deviceOffset, {}).event();
#endif
}                                     

void
MetalComputeContext::
fillDeviceRegionFromHostSyncImpl(const std::string & opName,
                                 MemoryRegionHandle deviceHandle,
                                 std::span<const std::byte> hostRegion,
                                 size_t deviceOffset)
{
    FrozenMemoryRegion region(nullptr, (const char *)hostRegion.data(), hostRegion.size_bytes());

    auto [pin, buffer, offset]
        = MetalComputeContext::getMemoryRegion(opName, *deviceHandle.handle, ACC_WRITE);

    std::byte * contents = (std::byte *)buffer.GetContents();
    if (contents == nullptr) {
        MLDB_THROW_UNIMPLEMENTED();
    }
    uint32_t length = buffer.GetLength();

    ExcAssertLessEqual(offset + deviceOffset + hostRegion.size(), length);

    memcpy(contents + offset + deviceOffset, hostRegion.data(), hostRegion.size_bytes());

    buffer.DidModify({(uint32_t)deviceOffset, (uint32_t)hostRegion.size_bytes()});

#if 0
    auto commandQueue = mtlDevice.NewCommandQueue();
    ExcAssert(commandQueue);
    auto commandBuffer = commandQueue.CommandBuffer();
    ExcAssert(commandBuffer);
    auto blitEncoder = commandBuffer.BlitCommandEncoder();
    ExcAssert(blitEncoder);

    blitEncoder.
    blitEncoder.Synchronize(buffer);
    blitEncoder.EndEncoding();
    
    commandBuffer.Commit();
    commandBuffer.WaitUntilCompleted();

    auto status = commandBuffer.GetStatus();
    if (status != mtlpp::CommandBufferStatus::Completed) {
        auto error = commandBuffer.GetError();
        throw MLDB::Exception(std::string(error.GetDomain().GetCStr()) + ": " + error.GetLocalizedDescription().GetCStr());
    }

    clQueue
        ->enqueueCopyFromHostImpl(opName, deviceHandle, region, deviceOffset, {})
    .await();
#endif
}
#endif

std::shared_ptr<ComputeQueue>
MetalComputeContext::
getQueue(const std::string & queueName)
{
    return std::make_shared<MetalComputeQueue>(this, nullptr /* parent */, queueName, mtlpp::CommandQueue(), mtlpp::DispatchType::Serial);
}

std::shared_ptr<GridComputeFunctionLibrary>
MetalComputeContext::
getLibrary(const std::string & libraryName)
{
    auto op = scopedOperation(OperationType::METAL_COMPUTE, "MetalComputeContext getLibrary " + libraryName);

    std::unique_lock guard(libraryRegistryMutex);
    auto it = libraryRegistry.find(libraryName);
    if (it == libraryRegistry.end()) {
        throw AnnotatedException(400, "Unable to find Metal compute library '" + libraryName + "'",
                                        "libraryName", libraryName);
    }
    auto result = it->second.generate(*this);
    //if (traceSerializer) {
    //    result->traceSerializer = kernelsSerializer->newStructure(kernelName);
    //    result->runsSerializer = result->traceSerializer->newStructure("runs");
    //}
    return result;
}

std::shared_ptr<GridComputeKernelSpecialization>
MetalComputeContext::
specializeKernel(const GridComputeKernelTemplate & tmplate)
{
    return std::make_shared<MetalComputeKernel>(this, tmplate);
}

#if 0
MemoryRegionHandle
MetalComputeContext::
getSliceImpl(const MemoryRegionHandle & handle, const std::string & regionName,
             size_t startOffsetInBytes, size_t lengthInBytes,
             size_t align, const std::type_info & type, bool isConst)
{
    auto op = scopedOperation(OperationType::METAL_COMPUTE, "MetalComputeContext getSliceImpl " + regionName);

    auto info = std::dynamic_pointer_cast<const MetalMemoryRegionHandleInfo>(std::move(handle.handle));
    ExcAssert(info);

    if (info->isConst && !isConst) {
        throw MLDB::Exception("getSliceImpl: attempt to take a non-const slice of a const region");
    }

    if (*info->type != type) {
        throw MLDB::Exception("getSliceImpl: attempt to cast a slice");
    }

    if (startOffsetInBytes % align != 0) {
        throw MLDB::Exception("getSliceImpl: unaligned start offset");
    }

    if (lengthInBytes % align != 0) {
        throw MLDB::Exception("getSliceImpl: unaligned length");
    }

    if (startOffsetInBytes > info->lengthInBytes) {
        throw MLDB::Exception("getSliceImpl: start offset past the end");
    }

    if (startOffsetInBytes + lengthInBytes > info->lengthInBytes) {
        throw MLDB::Exception("getSliceImpl: end offset past the end");
    }

    auto newInfo = std::make_shared<MetalMemoryRegionHandleInfo>();

    newInfo->buffer = info->buffer;
    newInfo->offset = info->offset + startOffsetInBytes;
    newInfo->isConst = isConst;
    newInfo->type = &type;
    newInfo->name = regionName;
    newInfo->lengthInBytes = lengthInBytes;
    newInfo->parent = info;
    newInfo->ownerOffset = startOffsetInBytes;
    newInfo->version = info->version;

    return { newInfo };
}
#endif


// MetalComputeKernel

template<typename T>
ComputeKernelType
makeBasicType(std::initializer_list<int> vals = {1})
{
    ComputeKernelType result;
    result.simd.insert(result.simd.begin(), vals.begin(), vals.end());
    result.baseType = getDefaultDescriptionSharedT<T>();
    return result;
}

static ComputeKernelType
getKernelTypeFromDataType(mtlpp::DataType dataType)
{
    switch (dataType) {
        case mtlpp::DataType::None: 
        case mtlpp::DataType::Struct: 
        case mtlpp::DataType::Array:
            throw MLDB::Exception("getKernelTypeFromDataType: not a basic type"); 
        case mtlpp::DataType::Float:        return makeBasicType<float>();
        case mtlpp::DataType::Float2:       return makeBasicType<float>({2});
        case mtlpp::DataType::Float3:       return makeBasicType<float>({3});
        case mtlpp::DataType::Float4:       return makeBasicType<float>({4});
        case mtlpp::DataType::Float2x2:     return makeBasicType<float>({2,2});
        case mtlpp::DataType::Float2x3:     return makeBasicType<float>({2,3});
        case mtlpp::DataType::Float2x4:     return makeBasicType<float>({2,4});
        case mtlpp::DataType::Float3x2:     return makeBasicType<float>({3,2});
        case mtlpp::DataType::Float3x3:     return makeBasicType<float>({3,3});
        case mtlpp::DataType::Float3x4:     return makeBasicType<float>({3,4});
        case mtlpp::DataType::Float4x2:     return makeBasicType<float>({4,2});
        case mtlpp::DataType::Float4x3:     return makeBasicType<float>({4,3});
        case mtlpp::DataType::Float4x4:     return makeBasicType<float>({4,4});
        case mtlpp::DataType::Half:         return makeBasicType<half>();
        case mtlpp::DataType::Half2:        return makeBasicType<half>({2});
        case mtlpp::DataType::Half3:        return makeBasicType<half>({3});
        case mtlpp::DataType::Half4:        return makeBasicType<half>({4});
        case mtlpp::DataType::Half2x2:      return makeBasicType<half>({2,2});
        case mtlpp::DataType::Half2x3:      return makeBasicType<half>({2,3});
        case mtlpp::DataType::Half2x4:      return makeBasicType<half>({2,4});
        case mtlpp::DataType::Half3x2:      return makeBasicType<half>({3,2});
        case mtlpp::DataType::Half3x3:      return makeBasicType<half>({3,3});
        case mtlpp::DataType::Half3x4:      return makeBasicType<half>({3,4});
        case mtlpp::DataType::Half4x2:      return makeBasicType<half>({4,2});
        case mtlpp::DataType::Half4x3:      return makeBasicType<half>({4,3});
        case mtlpp::DataType::Half4x4:      return makeBasicType<half>({4,4});
        case mtlpp::DataType::Int:          return makeBasicType<int32_t>();
        case mtlpp::DataType::Int2:         return makeBasicType<int32_t>({2});
        case mtlpp::DataType::Int3:         return makeBasicType<int32_t>({3});
        case mtlpp::DataType::Int4:         return makeBasicType<int32_t>({4});
        case mtlpp::DataType::UInt:         return makeBasicType<uint32_t>();
        case mtlpp::DataType::UInt2:        return makeBasicType<uint32_t>({2});
        case mtlpp::DataType::UInt3:        return makeBasicType<uint32_t>({3});
        case mtlpp::DataType::UInt4:        return makeBasicType<uint32_t>({4});
        case mtlpp::DataType::Short:        return makeBasicType<int16_t>();
        case mtlpp::DataType::Short2:       return makeBasicType<int16_t>({2});
        case mtlpp::DataType::Short3:       return makeBasicType<int16_t>({3});
        case mtlpp::DataType::Short4:       return makeBasicType<int16_t>({4});
        case mtlpp::DataType::UShort:       return makeBasicType<uint16_t>();
        case mtlpp::DataType::UShort2:      return makeBasicType<uint16_t>({2});
        case mtlpp::DataType::UShort3:      return makeBasicType<uint16_t>({3});
        case mtlpp::DataType::UShort4:      return makeBasicType<uint16_t>({4});
        case mtlpp::DataType::Char:         return makeBasicType<int8_t>();
        case mtlpp::DataType::Char2:        return makeBasicType<int8_t>({2});
        case mtlpp::DataType::Char3:        return makeBasicType<int8_t>({3});
        case mtlpp::DataType::Char4:        return makeBasicType<int8_t>({4});
        case mtlpp::DataType::UChar:        return makeBasicType<uint8_t>();
        case mtlpp::DataType::UChar2:       return makeBasicType<uint8_t>({2});
        case mtlpp::DataType::UChar3:       return makeBasicType<uint8_t>({3});
        case mtlpp::DataType::UChar4:       return makeBasicType<uint8_t>({4});
        case mtlpp::DataType::Bool:         return makeBasicType<bool>();
        case mtlpp::DataType::Bool2:        return makeBasicType<bool>({2});
        case mtlpp::DataType::Bool3:        return makeBasicType<bool>({3});
        case mtlpp::DataType::Bool4:        return makeBasicType<bool>({4});
        case mtlpp::DataType::Long:         return makeBasicType<int64_t>();
        case mtlpp::DataType::Long2:        return makeBasicType<int64_t>({2});
        case mtlpp::DataType::Long3:        return makeBasicType<int64_t>({3});
        case mtlpp::DataType::Long4:        return makeBasicType<int64_t>({4});
        case mtlpp::DataType::ULong:        return makeBasicType<uint64_t>();
        case mtlpp::DataType::ULong2:       return makeBasicType<uint64_t>({2});
        case mtlpp::DataType::ULong3:       return makeBasicType<uint64_t>({3});
        case mtlpp::DataType::ULong4:       return makeBasicType<uint64_t>({4});
        default:
            throw MLDB::Exception("getKernelTypeFromDataType: unknown type"); 
    }
}

static MemoryRegionAccess convertAccess(mtlpp::ArgumentAccess access)
{
    switch (access) {
        case mtlpp::ArgumentAccess::ReadOnly: return ACC_READ;
        case mtlpp::ArgumentAccess::ReadWrite: return ACC_READ_WRITE;
        case mtlpp::ArgumentAccess::WriteOnly: return ACC_WRITE;
        default:
            throw MLDB::Exception("Converting unknown MtlArgumentAccess value");
    }
}

ComputeKernelType
getKernelTypeFromArrayType(const mtlpp::ArrayType & arrayType, const std::string & name, int align = -1, int width = -1);

ComputeKernelType
getKernelTypeFromStructType(const mtlpp::StructType & structType, const std::string & name,
                            int align = -1, int width = -1)
{
    auto desc = std::make_shared<GenericStructureDescription>(false, name);
    if (align != -1)
        desc->align = align;
    if (width != -1)
        desc->width = width;

    auto members = structType.GetMembers();
    for (size_t i = 0;  i < members.GetSize();  ++i) {
        auto member = members[i];
        std::string memberName = member.GetName().GetCStr();
        //cerr << member.GetName().GetCStr() << " at offset " << member.GetOffset() << " with data type "
        //        << (int)member.GetDataType() << endl;

        ComputeKernelType kernelType;

        switch (member.GetDataType()) {
        case mtlpp::DataType::None:
            throw MLDB::Exception("Can't handle void (MtlDataType::None) types");
        case mtlpp::DataType::Struct:
            kernelType = getKernelTypeFromStructType(member.GetStructType(), name + "__m__" + memberName);
            break;
        case mtlpp::DataType::Array:
            kernelType = getKernelTypeFromArrayType(member.GetArrayType(), name + "__m__" + memberName);
            break;
        default:
            kernelType = getKernelTypeFromDataType(member.GetDataType());
        }
        //cerr << "  data type " << (int)member.GetDataType() << " " << kernelType.print() << endl;
        //cerr << "  array type " << member.GetArrayType()

        desc->addFieldDesc(memberName, member.GetOffset(), "", kernelType.baseType);
    }
    
    ComputeKernelType type;
    type.baseType = desc;
    return type;
}

ComputeKernelType
getKernelTypeFromArrayType(const mtlpp::ArrayType & arrayType, const std::string & name, int align, int width)
{
    uint32_t length = arrayType.GetArrayLength();
    uint32_t stride = arrayType.GetStride();

    auto dataType = arrayType.GetElementType();
    ComputeKernelType elementType;

    switch (dataType) {
    case mtlpp::DataType::None:
        throw MLDB::Exception("Can't handle void (MtlDataType::None) types");
    case mtlpp::DataType::Struct:
        elementType = getKernelTypeFromStructType(arrayType.GetElementStructType(), name + "__ul__", -1 /* align */, stride /* width */);
        break;
    case mtlpp::DataType::Array:
        elementType = getKernelTypeFromArrayType(arrayType.GetElementArrayType(), name + "__ul__", -1 /* align */, stride /* width */);
        break;
    default:
        elementType = getKernelTypeFromDataType(dataType);
    }

    if (width == -1) {
        width = length * stride;
    }
    else {
        ExcAssertGreater(width, 0);
        ExcAssertEqual(elementType.baseType->width, stride);
        ExcAssertEqual(elementType.baseType->width * length, width);
    }

    if (align == -1) {
        align = elementType.baseType->align;
    }
    else {
        ExcAssertGreater(align, 0);
        // ... TODO more conditions here...
    }

    auto desc = std::make_shared<GenericFixedLengthArrayDescription>(width, align, name, elementType.baseType, length);

    ComputeKernelType result;
    result.baseType = desc;
    return result;
}

// Parses an Metal kernel argument info structure, and turns it into a ComputeKernel type
ComputeKernelType
MetalComputeKernel::
getKernelType(const mtlpp::Argument & arg)
{
    ComputeKernelType type;

    mtlpp::ArgumentType argType = arg.GetType();
    mtlpp::DataType dataType;

    static std::atomic<int> idx(0);
    std::string name = "__metal_arg_" + string(arg.GetName().GetCStr()) + std::to_string(idx++);

    if (argType == mtlpp::ArgumentType::Buffer) {
        //cerr << "buffer argument " << arg.GetName().GetCStr() << endl;

        dataType = arg.GetBufferDataType();
        auto structType = arg.GetBufferStructType();
        auto align = arg.GetBufferAlignment();
        auto width = arg.GetBufferDataSize();
        auto pointer = arg.GetBufferPointerType();

        //cerr << "align " << align << " width " << width << " pointer " << pointer << endl;

        if (pointer && false) {
            cerr << "  pointer: acc " << (int)pointer.GetAccess() << " data " << (int)pointer.GetElementType()
                 << " array " << pointer.GetElementArrayType() << " struct " << pointer.GetElementStructType()
                 << " datasize " << pointer.GetDataSize() << endl;
        }

        switch (dataType) {
        case mtlpp::DataType::None:
            throw MLDB::Exception("Can't handle void (MtlDataType::None) types");
        case mtlpp::DataType::Struct:
            type = getKernelTypeFromStructType(arg.GetBufferStructType(), name, align, width);
            break;
        case mtlpp::DataType::Array:
            throw MLDB::Exception("Can't handle arrays as Metal arguments");
            break;
        default:
            type = getKernelTypeFromDataType(dataType);
            type.dims.emplace_back();  // if it's a basic type, it must be an array
        }
    }
    else if (argType == mtlpp::ArgumentType::ThreadgroupMemory) {
        // We know very little about this type, just its alignment and data size
        auto align = arg.GetThreadgroupMemoryAlignment();
        auto width = arg.GetThreadgroupMemoryDataSize();
        type.baseType.reset(new GenericAtomDescription(width, align, name));
    }
    else {
        throw MLDB::Exception("Not implemented: non-buffer and non-thread group argument types");
    }

    type.access = convertAccess(arg.GetAccess());
    ExcAssert(type.baseType);
    return type;
}

#if 0
void
MetalBindFieldAction::
apply(void * object,
      const ValueDescription & desc,
      MetalComputeQueue & queue,
      const std::vector<ComputeKernelArgument> & args,
      ComputeKernelConstraintSolution & knowns) const
{
    auto & fieldDesc = desc.getFieldByNumber(fieldNumber);
    std::string opName = "bind: fill field " + fieldDesc.fieldName;
    switch (action) {
    case MetalBindFieldActionType::SET_FIELD_FROM_PARAM: opName += " from arg " + std::to_string(argNum);  break;
    case MetalBindFieldActionType::SET_FIELD_FROM_KNOWN: opName += " from known " + jsonEncodeStr(expr);  break;
    }

    auto op = scopedOperation(OperationType::METAL_COMPUTE, opName);
    Json::Value value;
    bool done = false;

    switch (action) {
    case MetalBindFieldActionType::SET_FIELD_FROM_PARAM: {
        if (argNum == -1 || argNum >= args.size() || !args.at(argNum).handler) {
            // Problem... this parameter wasn't set from an argument.  It must be in known.
            value = knowns.evaluate(*expr);
            break;
        }
        auto & arg = args.at(argNum);
        ExcAssert(arg.handler);
        value = arg.handler->toJson();
        if (arg.handler->type.baseType->kind == ValueKind::ENUM) {
            // we need to convert to an integer
            auto bytes = arg.handler->getPrimitive(opName, queue);
            ExcAssertEqual(bytes.size_bytes(), fieldDesc.width);
            fieldDesc.description->copyValue(bytes.data(), fieldDesc.getFieldPtr(object));
            done = true;
        }
        break;
    }
    case MetalBindFieldActionType::SET_FIELD_FROM_KNOWN: {
        value = knowns.evaluate(*expr);
        break;
    }
    default:
        throw MLDB::Exception("Can't handle unknown MetalBindFieldAction value");
    }

    traceMetalOperation("value " + jsonEncodeStr(value));

    // We go through the JSON conversion so that we can handle compatible type coversions,
    // eg uint16_t -> uint32_t.
    if (!done) {
        StructuredJsonParsingContext jsonContext(value);
        fieldDesc.description->parseJson(fieldDesc.getFieldPtr(object), jsonContext);
    }
}

void
MetalBindAction::
applyArg(MetalComputeQueue & queue,
         const std::vector<ComputeKernelArgument> & args,
         ComputeKernelConstraintSolution & knowns,
         bool setKnowns,
         mtlpp::CommandBuffer & commandBuffer,
         mtlpp::ComputeCommandEncoder & commandEncoder) const
{
    auto & context = dynamic_cast<MetalComputeContext &>(*queue.owner);

    const ComputeKernelArgument & arg = args.at(argNum);

    if (!arg.handler) {
        throw MLDB::Exception("argument " + std::to_string(argNum) + " (" + argName
                              + ") was not passed");
    }

    auto j = arg.handler->toJson();
    std::string jStr;
    if (j.isObject()) {
        jStr = j["name"].asString() + ":" + j["version"].toStringNoNewLine();
    }
    else {
        jStr = j.toStringNoNewLine();
    }
    auto op = scopedOperation(OperationType::METAL_COMPUTE, 
                              "MetalComputeKernel bind arg " + argName + " type " + arg.handler->type.print()
                              + " from " + jStr 
                              + " at index " + std::to_string(this->arg.GetIndex()));

#if 0
    if (traceSerializer) {
        Json::Value thisArgInfo;
        if (arg.handler)
            thisArgInfo["value"] = arg.handler->toJson();
        thisArgInfo["spec"] = paramType.print();
        static auto vdd = getValueDescriptionDescription(true /* detailed */);
        thisArgInfo["type"] = vdd->printJsonStructured(paramType.baseType);
        thisArgInfo["aliases"] = jsonEncode(getValueDescriptionAliases(*paramType.baseType->type));
        argInfo[this->clKernelInfo.args[i].name] = thisArgInfo;
    }
#endif

    static std::atomic<int> disamb = 0;
    std::string opName = "bind " + argName + " " + std::to_string(++disamb);

    ExcAssert(arg.handler);  // if there is no handler, we should be in applyStruct

    mtlpp::Buffer memBuffer;
    size_t offset = 0;
    std::shared_ptr<const void> pin;

    if (arg.handler->canGetPrimitive()) {
        auto bytes = arg.handler->getPrimitive(opName, queue);
        traceMetalOperation("binding primitive with " + std::to_string(bytes.size()) + " bytes and value " + jsonEncodeStr(arg.handler->toJson()));
        commandEncoder.SetBytes(bytes.data(), bytes.size_bytes(), this->arg.GetIndex());
    }
    else if (arg.handler->canGetHandle()) {
        auto handle = arg.handler->getHandle(opName, queue);
        traceMetalOperation("binding handle with " + std::to_string(handle.lengthInBytes()) + " bytes");
        MemoryRegionAccess access = type.access;

        if (setKnowns) {
            Json::Value known;
            ExcAssert(type.baseType);
            known["elAlign"] = type.baseType->align;
            known["elWidth"] = type.baseType->width;
            known["elType"] = type.baseType->typeName;
            known["length"] = handle.lengthInBytes() / type.baseType->width;
            known["type"] = type.print();
            known["name"] = handle.handle->name;
            known["version"] = handle.handle->version;

            //cerr << "setting known " << argName << " to " << known << endl;

            knowns.setValue(argName, known);
        }

        if (traceSerializer && (access & ACC_READ)) {
            auto [region, version] = context
                .getFrozenHostMemoryRegion(opName + " trace", *handle.handle, 0 /* offset */, -1,
                                            true /* ignore hazards */);
            //bindInfo->traceSerializer->addRegion(region, this->clKernelInfo.args[i].name);
            traceVersion(handle.handle->name, version, region);
        }

        std::tie(pin, memBuffer, offset)
            = context.getMemoryRegion(opName, *handle.handle, access);

        commandEncoder.SetBuffer(memBuffer, offset, this->arg.GetIndex());
    }
    else if (arg.handler->canGetConstRange()) {
        throw MLDB::Exception("param.getConstRange");
    }
    else {
        throw MLDB::Exception("don't know how to handle passing parameter to Metal");
    }
}

void
MetalBindAction::
applyStruct(MetalComputeQueue & queue,
            const std::vector<ComputeKernelArgument> & args,
            ComputeKernelConstraintSolution & knowns,
            bool setKnowns,
            mtlpp::CommandBuffer & commandBuffer,
            mtlpp::ComputeCommandEncoder & commandEncoder) const
{
    auto op = scopedOperation(OperationType::METAL_COMPUTE, 
                              "MetalComputeKernel bind struct to arg " + argName + " at index " + std::to_string(arg.GetIndex()));

    void * ptr = nullptr;
    if (type.baseType->align < alignof(void *)) {
        ptr = malloc(type.baseType->width);
        if (!ptr)
            throw MLDB::Exception(errno, "malloc");
    }
    else {
        int res = posix_memalign(&ptr, type.baseType->align, type.baseType->width);
        if (res != 0)
            throw MLDB::Exception(res, "posix_memalign");
    }

    try {
        type.baseType->initializeDefault(ptr);
    } MLDB_CATCH_ALL {
        free(ptr);
        throw;
    }

    auto destruct = [desc=type.baseType] (void * ptr)
    {
        try {
            desc->destruct(ptr);
        } MLDB_CATCH_ALL {
            free(ptr);
            throw;
        }
        free(ptr);
    };

    std::shared_ptr<void> result(ptr, std::move(destruct));

    // Fill it in
    for (auto & field: this->fields) {
        field.apply(result.get(), *type.baseType, queue, args, knowns);
    }

    commandEncoder.SetBytes(result.get(), type.baseType->width, this->arg.GetIndex());
}

void
MetalBindAction::
applyThreadGroup(MetalComputeQueue & queue,
                 const std::vector<ComputeKernelArgument> & args,
                 ComputeKernelConstraintSolution & knowns,
                 bool setKnowns,
                 mtlpp::CommandBuffer & commandBuffer,
                 mtlpp::ComputeCommandEncoder & commandEncoder) const
{
    // Calculate the array bound
    auto len = knowns.evaluate(*expr).asUInt();

    // Get the number of bytes
    size_t nbytes = len * type.baseType->width;

    // Must be a multiple of 16 bytes
    while (nbytes % 16 != 0)
        ++nbytes;

    traceMetalOperation("binding local array handle with " + std::to_string(nbytes) + " bytes " + " at index " + std::to_string(arg.GetIndex()));

    // Set the knowns for this binding
    if (setKnowns) {
        Json::Value known;
        known["elAlign"] = type.baseType->align;
        known["elWidth"] = type.baseType->width;
        known["elType"] = type.baseType->typeName;
        known["length"] = len;
        known["type"] = type.print();
        known["name"] = "<<<Local array>>>";

        knowns.setValue(this->argName, std::move(known));
    }

    // Set it up in the command encoder
    commandEncoder.SetThreadgroupMemory(nbytes, this->arg.GetIndex());
}

void
MetalBindAction::
apply(MetalComputeQueue & queue,
      const std::vector<ComputeKernelArgument> & args,
      ComputeKernelConstraintSolution & knowns,
      bool setKnowns,
      mtlpp::CommandBuffer & buffer,
      mtlpp::ComputeCommandEncoder & commandEncoder) const
{
    switch (action) {
        case MetalBindActionType::SET_BUFFER_FROM_ARG:
            applyArg(queue, args, knowns, setKnowns, buffer, commandEncoder);
            break;
        case MetalBindActionType::SET_BUFFER_FROM_STRUCT:
            applyStruct(queue, args, knowns, setKnowns, buffer, commandEncoder);
            break;
        case MetalBindActionType::SET_BUFFER_THREAD_GROUP:
            applyThreadGroup(queue, args, knowns, setKnowns, buffer, commandEncoder);
            break;
        default:
            throw MLDB::Exception("Unknown metal action");
    }
}

DEFINE_STRUCTURE_DESCRIPTION_INLINE(MetalBindAction)
{
    addField("action", &MetalBindAction::action, "");
    addField("type", &MetalBindAction::type, "");
    addField("argNum", &MetalBindAction::argNum, "");
    addField("argName", &MetalBindAction::argName, "");
    addField("expr", &MetalBindAction::expr, "");
    addField("fields", &MetalBindAction::fields, "");
}
#endif

#if 0
void
MetalComputeKernel::
setComputeFunction(mtlpp::Library libraryIn,
                   std::string kernelName)
{
    ExcAssert(libraryIn);

    this->mtlLibrary = std::move(libraryIn);
    this->kernelName = kernelName;

    ns::Error error{ns::Handle()};
    mtlpp::FunctionConstantValues constantValues;
    this->mtlFunction = this->mtlLibrary.NewFunction(kernelName.c_str(), constantValues, &error);
    checkMetalError(error, "Error making metal function");
    ExcAssert(this->mtlFunction);

    //cerr << this->mtlFunction.GetName().GetCStr() << endl;

    mtlpp::PipelineOption options = (mtlpp::PipelineOption((int)mtlpp::PipelineOption::ArgumentInfo | (int)mtlpp::PipelineOption::BufferTypeInfo));
    this->computePipelineState
        = mtlContext->mtlDevice.NewComputePipelineState(this->mtlFunction, options, reflection, &error);
    checkMetalError(error, "Error making pipeline state");
    ExcAssert(computePipelineState);
    ExcAssert(reflection);

    auto arguments = reflection.GetArguments();

    correspondingArgumentNumbers.resize(arguments.GetSize());

    std::vector<GridBindAction> actions;

    for (size_t i = 0;  i < arguments.GetSize();  ++i) {
        mtlpp::Argument arg = arguments[i];
        std::string argName = arg.GetName().GetCStr();
        mtlpp::ArgumentType argType = arg.GetType();

        GridBindAction action;
        auto it = paramIndex.find(argName);

        if (argType == mtlpp::ArgumentType::ThreadgroupMemory) {
            if (it == paramIndex.end()) {
                throw MLDB::Exception("Metal ThreadGroup kernel parameter " + argName + " to kernel " + this->kernelName
                                      + " has no corresponding argument to learn its length from");
            }
            int argNum = it->second;
            auto & paramType = params[argNum].type;

            auto width = arg.GetThreadgroupMemoryDataSize();
            auto align = arg.GetThreadgroupMemoryAlignment();

            ExcAssertEqual(paramType.dims.size(), 1);
            ExcAssert(paramType.dims[0].bound);
            ExcAssertEqual(width, paramType.baseType->width);
            ExcAssertEqual(align, paramType.baseType->align);

            action.action = GridBindActionType::SET_BUFFER_THREAD_GROUP;
            action.arg = arg;
            action.type = paramType;
            action.argName = argName;
            action.expr = paramType.dims[0].bound;
        }
        else if (argType == mtlpp::ArgumentType::Buffer) {
            auto type = getKernelType(arg);
            //cerr << "argument " << i << " has name " << argName
            //    << " and index " << arg.GetIndex()
            //    << " and type " << type.print() << endl;

            if (it == paramIndex.end()) {
                if (type.baseType->kind == ValueKind::STRUCTURE && type.dims.empty()) {
                    std::vector<GridBindFieldAction> fieldActions;

                    // Check if we know the unpacked types
                    auto numFields = type.baseType->getFixedFieldCount();
                    std::vector<std::string> fieldsNotFound;

                    for (size_t i = 0;  i < numFields;  ++i) {
                        const auto & field = type.baseType->getFieldByNumber(i);
                        auto it = paramIndex.find(field.fieldName);
                        if (it != paramIndex.end()) {
                            GridBindFieldAction action;
                            action.action = GridBindFieldActionType::SET_FIELD_FROM_PARAM;
                            action.argNum = it->second;
                            action.fieldNumber = i;
                            // Set expr in case the argument isn't passed, in which case it needs to come
                            // from the known values in the context.
                            action.expr = CommandExpression::parseArgumentExpression(field.fieldName);
                            fieldActions.emplace_back(std::move(action));
                            continue;
                        }
                        else {
                            // We're assuming it comes from somewhere...
                            GridBindFieldAction action;
                            action.action = GridBindFieldActionType::SET_FIELD_FROM_KNOWN;
                            action.fieldNumber = i;
                            action.expr = CommandExpression::parseArgumentExpression(field.fieldName);
                            fieldActions.emplace_back(std::move(action));
                            continue;
                        }
                        fieldsNotFound.push_back(field.fieldName);
                    }

                    if (!fieldsNotFound.empty()) {
                        throw MLDB::Exception("Kernel parameter " + std::to_string(i)
                                        + " (" + argName + ") to Metal kernel " + kernelName
                                        + " cannot be assembled because of missing fields " + jsonEncodeStr(fieldsNotFound));
                    }

                    action.action = GridBindActionType::SET_BUFFER_FROM_STRUCT;
                    action.arg = arg;
                    action.type = type;
                    action.argName = argName;
                    action.fields = std::move(fieldActions);
                }
                else if (argType == mtlpp::ArgumentType::ThreadgroupMemory) {
                    throw MLDB::Exception("Thread group parameter in kernel with no setters defined; "
                                            "implement a setter to avoid launch failure");
                }
                else {
                    throw MLDB::Exception("Kernel parameter " + std::to_string(i)
                                            + " (" + argName + ") to Metal kernel " + kernelName
                                            + " has no counterpart in formal parameter list");
                }
            }
            else {
                int argNum = it->second;
                auto & param = params.at(it->second);
                correspondingArgumentNumbers.at(i) = argNum;

                type.dims = param.type.dims;

                action.action = GridBindActionType::SET_BUFFER_FROM_ARG;
                action.arg = arg;
                action.type = type;  // TODO: get information from param type
                action.argNum = it->second;
                action.argName = arg.GetName().GetCStr();

                //cerr << "checking compatibility" << endl;
                //cerr << "param type " << param.type.print() << endl;
                //cerr << "arg type " << type.print() << endl;

                std::string reason;

                if (!param.type.isCompatibleWith(type, &reason)) {
                    throw MLDB::Exception("Kernel parameter " + std::to_string(i)
                                            + " (" + argName + ") to Metal kernel " + kernelName
                                            + ": declared parameter type " + type.print()
                                            + " is not compatible with kernel type " + param.type.print()
                                            + ": " + reason);
                }
            }
        }
        else {
            throw MLDB::Exception("Can't do Metal buffer or texture arguments (yet)");
        }
        //cerr << "got action\n" << jsonEncode(action) << endl;

        actions.emplace_back(std::move(action));
    }

    //cerr << "got actions\n" << jsonEncode(actions) << endl;
    this->bindActions = std::move(actions);
}
#endif

MetalComputeKernel::
MetalComputeKernel(MetalComputeContext * owner, const GridComputeKernelTemplate & tmplate)
    : GridComputeKernelSpecialization(owner, tmplate)
{
    this->mtlContext = owner;
    this->mtlFunction = dynamic_cast<const MetalComputeFunction *>(this->gridFunction.get());
    ExcAssert(this->mtlFunction);
}

std::shared_ptr<GridBindInfo>
MetalComputeKernel::
getGridBindInfo() const
{
    return std::make_shared<MetalBindInfo>(this);
}


// MetalComputeFunction

MetalComputeFunction::
MetalComputeFunction(MetalComputeContext & context, mtlpp::Function mtlFunction)
    : mtlFunction(std::move(mtlFunction))
{
    ExcAssert(this->mtlFunction);
    ns::Error error{ns::Handle()};

    mtlpp::PipelineOption options = (mtlpp::PipelineOption((int)mtlpp::PipelineOption::ArgumentInfo | (int)mtlpp::PipelineOption::BufferTypeInfo));
    this->computePipelineState
        = context.mtlDevice.NewComputePipelineState(this->mtlFunction, options, reflection, &error);
    checkMetalError(error, "Error making pipeline state for function");
    ExcAssert(computePipelineState);
    ExcAssert(reflection);
}

GridComputeFunctionArgumentDisposition getDisposition(mtlpp::ArgumentType argType)
{
    switch (argType) {
    case mtlpp::ArgumentType::Buffer: return GridComputeFunctionArgumentDisposition::BUFFER;
    case mtlpp::ArgumentType::ThreadgroupMemory: return GridComputeFunctionArgumentDisposition::THREADGROUP;
    default:
        MLDB_THROW_UNIMPLEMENTED("Metal sampler or texture arguments");
    }
}

std::vector<GridComputeFunctionArgument>
MetalComputeFunction::
getArgumentInfo() const
{
    auto arguments = reflection.GetArguments();

    std::vector<GridComputeFunctionArgument> result;

    for (size_t i = 0;  i < arguments.GetSize();  ++i) {
        mtlpp::Argument arg = arguments[i];
        std::string argName = arg.GetName().GetCStr();
        GridComputeFunctionArgumentDisposition disposition = getDisposition(arg.GetType());
        auto type = MetalComputeKernel::getKernelType(arg);

        GridComputeFunctionArgument entry;
        entry.name = argName;
        entry.disposition = disposition;
        entry.type = type;
        entry.computeFunctionArgIndex = arg.GetIndex();

        result.emplace_back(std::move(entry));
    }

    return result;
}


// MetalComputeFunctionLibrary

MetalComputeFunctionLibrary::
MetalComputeFunctionLibrary(MetalComputeContext & context, mtlpp::Library mtlLibrary)
    : context(context), mtlLibrary(mtlLibrary)
{
}

std::shared_ptr<GridComputeFunction>
MetalComputeFunctionLibrary::
getFunction(const std::string & functionName)
{
    ns::Error error{ns::Handle()};

    mtlpp::Function function = this->mtlLibrary.NewFunction(functionName.c_str(), {} /* constantValues */, &error);

    checkMetalError(error, "Error getting function from Metal library", "functionName", functionName);

    ExcAssert(function);
    return std::make_shared<MetalComputeFunction>(context, function);
}

std::string
MetalComputeFunctionLibrary::
getId() const
{
    MLDB_THROW_UNIMPLEMENTED();
}

Json::Value
MetalComputeFunctionLibrary::
getMetadata() const
{
    MLDB_THROW_UNIMPLEMENTED();
}

std::shared_ptr<MetalComputeFunctionLibrary>
MetalComputeFunctionLibrary::
compileFromSourceFile(MetalComputeContext & context, const std::string & fileName)
{
    filter_istream stream(fileName);
    Utf8String source = /*"#line 1 \"" + fileName + "\"\n" +*/ stream.readAll();

    return compileFromSource(context, source, fileName);
}

std::shared_ptr<MetalComputeFunctionLibrary>
MetalComputeFunctionLibrary::
compileFromSource(MetalComputeContext & context, const Utf8String & source, const std::string & fileNameToAppearInErrorMessages)
{
    ns::Error error{ns::Handle()};

    mtlpp::CompileOptions compileOptions;
    mtlpp::Library library = context.mtlDevice.NewLibrary(source.rawData(), compileOptions, &error);

    checkMetalError(error, "Error compiling Metal library from source", "fileName", fileNameToAppearInErrorMessages);

    return std::make_shared<MetalComputeFunctionLibrary>(context, std::move(library));
}

std::shared_ptr<MetalComputeFunctionLibrary>
MetalComputeFunctionLibrary::
loadMtllib(MetalComputeContext & context, const std::string & libraryFilename)
{
    ns::Error error{ns::Handle()};

    mtlpp::Library library = context.mtlDevice.NewLibrary(libraryFilename.c_str(), &error);

    checkMetalError(error, "Error loading Metal library", "fileName", libraryFilename);

    return std::make_shared<MetalComputeFunctionLibrary>(context, std::move(library));
}


// MetalComputeRuntime

EnvOption<int> METAL_DEFAULT_DEVICE("METAL_DEFAULT_DEVICE", -1);

struct MetalComputeRuntime: public ComputeRuntime {

    std::vector<mtlpp::Device> mtlDevices;
    std::vector<ComputeDevice> devices;

    MetalComputeRuntime()
    {
        auto queriedDevices = mtlpp::Device::CopyAllDevices();
        for (size_t i = 0;  i < queriedDevices.GetSize();  ++i) {
            auto device = queriedDevices[i];
            //cerr << "metal device " << i << " is " << device.GetName().GetCStr() << endl;
            mtlDevices.push_back(queriedDevices[i]);
            devices.push_back({ComputeRuntimeId::METAL, 0 /* runtime instance */, (uint8_t)i, 0, 0});
        }
    }

    mtlpp::Device convertDevice(ComputeDevice device) const
    {
        if (device.runtime != ComputeRuntimeId::METAL) {
            throw MLDB::Exception("Attempt to pass non-Metal device " + device.info() + " to Metal");
        }
        return mtlDevices.at(device.deviceInstance);
    }

    virtual ~MetalComputeRuntime()
    {
    }

    virtual ComputeRuntimeId getId() const
    {
        return ComputeRuntimeId::METAL;
    }

    virtual std::string printRestOfDevice(ComputeDevice device) const
    {
        return std::to_string(device.deviceInstance);
    }

    virtual std::string printHumanReadableDeviceInfo(ComputeDevice device) const
    {
        if (device.runtimeInstance > 0 || device.deviceInstance >= mtlDevices.size()) {
            return "<<INVALID METAL PLATFORM OR DEVICE INDEX>>";
        }
        std::string result = mtlDevices[device.deviceInstance].GetName().GetCStr();
        return result;
    }

    virtual ComputeDevice getDefaultDevice() const
    {
        if (!METAL_ENABLED || mtlDevices.empty())
            return ComputeDevice::none();

        if (METAL_DEFAULT_DEVICE.specified()) {
            ExcAssertLess(METAL_DEFAULT_DEVICE.get(), mtlDevices.size());
            return {ComputeRuntimeId::METAL, 0 /* platform */,
                    (uint16_t)METAL_DEFAULT_DEVICE.get(), 0, 0};
        }

        // TODO: deal with the possibility of the default not being at index zero...
        return {ComputeRuntimeId::METAL, 0 /* platform */, 0 /* device */,0, 0};
    }

    // Enumerate the devices available for this runtime
    virtual std::vector<ComputeDevice> enumerateDevices() const
    {
        return devices;
    }

    // Get a compute context for this runtime
    virtual std::shared_ptr<ComputeContext>
    getContext(std::span<const ComputeDevice> devices) const
    {
        if (devices.size() != 1) {
            throw MLDB::Exception("Metal Compute Kernel driver only can accept a single device");
        }
        return std::make_shared<MetalComputeContext>(convertDevice(devices[0]), devices[0]);
    }

};

void registerMetalComputeKernel(const std::string & kernelName,
                                 std::function<std::shared_ptr<MetalComputeKernel>(MetalComputeContext & context)> generator)
{
    std::unique_lock guard{kernelRegistryMutex};
    kernelRegistry[kernelName].generate = generator;
}

void registerMetalLibrary(const std::string & libraryName,
                          std::function<std::shared_ptr<MetalComputeFunctionLibrary>(MetalComputeContext &)> generator)
{
    std::unique_lock guard{libraryRegistryMutex};
    libraryRegistry[libraryName].generate = generator;
}

namespace {

static struct Init {
    Init()
    {
        ComputeRuntime::registerRuntime(ComputeRuntimeId::METAL, "metal",
                                        [] () { return new MetalComputeRuntime(); });

        auto compileLibrary = [] (MetalComputeContext & context) -> std::shared_ptr<MetalComputeFunctionLibrary>
        {
            if (false) {
                std::string fileName = "mldb/builtin/metal/base_kernels.metal";
                return MetalComputeFunctionLibrary::compileFromSourceFile(context, fileName);
            }
            else {
                std::string fileName = "build/arm64/lib/base_kernels_metal.metallib";
                return MetalComputeFunctionLibrary::loadMtllib(context, fileName);
            }
        };

        registerMetalLibrary("base_kernels", compileLibrary);
        

#if 0


        auto getLibrary = [] (MetalComputeContext & context) -> mtlpp::Library
        {
            auto compileLibrary = [&] () -> mtlpp::Library
            {
                ns::Error error{ns::Handle()};
                mtlpp::Library library;

                if (false) {
                    std::string fileName = "mldb/builtin/metal/base_kernels.metal";
                    filter_istream stream(fileName);
                    Utf8String source = "#line 1 \"" + fileName + "\"\n" + stream.readAll();

                    mtlpp::CompileOptions compileOptions;
                    library = context.mtlDevice.NewLibrary(source.rawData(), compileOptions, &error);
                }
                else {
                    std::string fileName = "build/arm64/lib/base_kernels_metal.metallib";
                    library = context.mtlDevice.NewLibrary(fileName.c_str(), &error);
                }

                if (error) {
                    cerr << "Error compiling" << endl;
                    cerr << "domain: " << error.GetDomain().GetCStr() << endl;
                    cerr << "descrption: " << error.GetLocalizedDescription().GetCStr() << endl;
                    if (error.GetLocalizedFailureReason()) {
                        cerr << "reason: " << error.GetLocalizedFailureReason().GetCStr() << endl;
                    }
                }

                ExcAssert(library);

                return library;
            };

            static const std::string cacheKey = "base_kernels";
            mtlpp::Library library = context.getCacheEntry(cacheKey, compileLibrary);
            return library;
        };
    
        auto createBlockFillArrayKernel = [getLibrary] (MetalComputeContext& context) -> std::shared_ptr<MetalComputeKernel>
        {
            auto library = getLibrary(context);
            auto result = std::make_shared<MetalComputeKernel>(&context);
            result->kernelName = "__blockFillArray";
            result->allowGridExpansion();
            result->addParameter("region", "w", "u8[regionLength]");
            result->addParameter("startOffsetInBytes", "r", "u64");
            result->addParameter("lengthInBytes", "r", "u64");
            result->addParameter("blockData", "r", "u8[blockLengthInBytes]");
            result->addParameter("blockLengthInBytes", "r", "u64");
            result->addConstraint("blockLengthInBytes", ">", "0", "Avoid divide by zero");
            result->addConstraint("(lengthInBytes-startOffsetInBytes) % blockLengthInBytes", "==", "0", "Block must divide evenly in region");
            result->addConstraint("numBlocks", "==", "(lengthInBytes-startOffsetInBytes)/blockLengthInBytes");
            result->addTuneable("threadsPerBlock", 256);
            result->addTuneable("blocksPerGrid", 16);
            result->allowGridPadding();
            result->setGridExpression("[min(blocksPerGrid,ceilDiv(numBlocks, threadsPerBlock))]", "[threadsPerBlock]");
            result->setComputeFunction(library, "__blockFillArrayKernel");

            return result;
        };

        registerMetalComputeKernel("__blockFillArray", createBlockFillArrayKernel);

        auto createZeroFillArrayKernel = [getLibrary] (MetalComputeContext& context) -> std::shared_ptr<MetalComputeKernel>
        {
            auto library = getLibrary(context);
            auto result = std::make_shared<MetalComputeKernel>(&context);
            result->kernelName = "__zeroFillArray";
            result->allowGridExpansion();
            result->addParameter("region", "w", "u8[regionLength]");
            result->addParameter("startOffsetInBytes", "r", "u64");
            result->addParameter("lengthInBytes", "r", "u64");
            result->addTuneable("threadsPerBlock", 256);
            result->addTuneable("blocksPerGrid", 16);
            result->allowGridPadding();
            result->setGridExpression("[blocksPerGrid]", "[threadsPerBlock]");
            result->setComputeFunction(library, "__zeroFillArrayKernel");

            return result;
        };

        registerMetalComputeKernel("__zeroFillArray", createZeroFillArrayKernel);
#endif
    }

} init;


} // file scope

} // namespace MLDB
