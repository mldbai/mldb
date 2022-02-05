/** compute_kernel_cuda.cc                                                -*- C++ -*-
    Jeremy Barnes, 27 March 2016
    This file is part of MLDB. Copyright 2016 mldb.ai inc. All rights reserved.

    Compute kernel runtime for CPU devices.
*/

#include "compute_kernel_cuda.h"
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

struct CudaException: public AnnotatedException {
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
    CudaException(const ns::Error & error, const std::string & message, NamesAndParams&&... namesAndParams)
        : AnnotatedException(500, "CUDA exception: " + message,
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
void checkCudaError(const ns::Error & error, const std::string & message, NamesAndParams&&... namesAndParams)
{
    if (!error)
        return;

    throw CudaException(error, message, std::forward<NamesAndParams>(namesAndParams)...);
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

std::mutex libraryRegistryMutex;
struct LibraryRegistryEntry {
    std::function<std::shared_ptr<CudaComputeFunctionLibrary>(CudaComputeContext & context)> generate;
};

std::map<std::string, LibraryRegistryEntry> libraryRegistry;

EnvOption<int> CUDA_TRACE_API_CALLS("CUDA_COMPUTE_TRACE_API_CALLS", 0);
EnvOption<std::string, true> CUDA_KERNEL_TRACE_FILE("CUDA_KERNEL_TRACE_FILE", "");
EnvOption<std::string, true> CUDA_CAPTURE_FILE("CUDA_CAPTURE_FILE", "");
EnvOption<bool, false> CUDA_ENABLED("CUDA_ENABLED", true);

// Use the Cuda capture facility to create a file that xcode can open
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

        if (CUDA_CAPTURE_FILE.specified()) {
            captureManager = mtlpp::CaptureManager::GetShared();
            mtlpp::CaptureDescriptor captureDescriptor;
            //captureDescriptor.SetCaptureDevice(commandQueue.GetDevice());
            if (CUDA_CAPTURE_FILE.get() == "xcode") {
                //captureDescriptor.SetDestination(mtlpp::CaptureDestination::DeveloperTools);
            }
            else {
                captureDescriptor.SetDestination(mtlpp::CaptureDestination::TraceDocument);
                captureDescriptor.SetOutputURL(CUDA_CAPTURE_FILE.get().c_str());
            }

            ns::Error error{ns::Handle()};
            captureManager.StartCapture(commandQueue.GetDevice());

//            if (!captureManager.StartCapture(captureDescriptor, &error)) {
            if (!captureManager.IsCapturing()) {
                throw CudaException(error, "Error initializing capture (maybe you need to set CUDA_DEVICE_WRAPPER_TYPE=1 in the environment?)");
            }
        }
    }

    ~InitCapture()
    {
        if (captureManager) {
            captureManager.StopCapture();
            char buf[PATH_MAX + 1024];
            std::string fullPath = CUDA_CAPTURE_FILE.get();
            if (!fullPath.empty() && fullPath[0] != '/')
                fullPath = getcwd(buf, PATH_MAX + 1024) + string("/") + CUDA_CAPTURE_FILE.get();
            cerr << "Cuda capture file (should have a .gputrace extension) can be inspected with the following command:" << endl;
            cerr << "osascript -e '" << endl;
            cerr << "  tell application \"Xcode\"\n    open \"" << fullPath << "\"\n  end tell" << endl;
            cerr << "'";
        }
    }
} initCapture;

struct CudaMemoryRegionHandleInfo: public GridMemoryRegionHandleInfo {
    mtlpp::Buffer buffer;

    virtual ~CudaMemoryRegionHandleInfo()
    {
        cerr << "destroying CudaMemoryRegionHandleInfo " << name << " with buffer use count " << buffer.GetRetainCount() << endl;
    }

    virtual CudaMemoryRegionHandleInfo * clone() const
    {
        return new CudaMemoryRegionHandleInfo(*this);
    }

    void init(mtlpp::Buffer mem, size_t offset)
    {
        this->buffer = std::move(mem);
        this->offset = offset;
        this->version = 0;
    }
};

} // file scope


// CudaComputeEvent

CudaComputeEvent::
CudaComputeEvent(const std::string & label, bool isResolvedIn, const CudaComputeQueue * queue)
    : GridComputeEvent(label, isResolvedIn, queue)
{
}

void
CudaComputeEvent::
resolveFromCommandBuffer(mtlpp::CommandBuffer & buffer)
{
    std::unique_lock guard{mutex};

    ExcAssert(!isResolved);
    ExcAssert(!this->commandBuffer);

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
        // https://developer.apple.com/documentation/cuda/mtlcommandbuffer/1442997-addcompletedhandler?language=objc
        // You can’t add a completion handler after you commit the command buffer.
        cerr << "buffer.GetStatus() = " << jsonEncodeStr(buffer.GetStatus()) << endl;
        throw MLDB::Exception("cannot create event for committed command buffer");
    }

    std::weak_ptr<GridComputeEvent> weakThis = this->weak_from_this();

    auto onComplete = [weakThis] (const mtlpp::CommandBuffer & buffer)
    {
        auto sharedThis = std::reinterpret_pointer_cast<CudaComputeEvent>(weakThis.lock());
        if (!sharedThis)
            return;

        std::unique_lock guard(sharedThis->mutex);
        cerr << ansi::red << "onComplete for event " << sharedThis->label() << ansi::reset << endl;
        sharedThis->resolve();
        sharedThis->commandBuffer = {};
    };

    //cerr << ansi::red << "event from command buffer: status = " << (int)commandBuffer.GetStatus() << ansi::reset << endl;
    this->commandBuffer = buffer;
    commandBuffer.AddCompletedHandler(onComplete);
}

void
CudaComputeEvent::
await() const
{
    //cerr << "await(): isResolved = " << isResolved << " commandBuffer = " << commandBuffer << endl;
    if (isResolved)
        return;

    std::unique_lock guard{mutex};

    // If we're actively waiting, we can do so in this thread and avoid having to wait for
    // the asynchronous mechanisms to eventually notice that it's done.
    if (commandBuffer) {
        auto tr = scopedOperation(OperationType::CUDA_COMPUTE, "awaiting " + label() + " on command buffer");
        auto mutableCommandBuffer = const_cast<mtlpp::CommandBuffer &>(commandBuffer);
        guard.unlock();

        //cerr << "await(): status = " << jsonEncodeStr(commandBuffer.GetStatus()) << endl;

        // Try a busy-ish wait
        //while (mutableCommandBuffer.GetStatus() < mtlpp::CommandBufferStatus::Completed) {
        //    std::this_thread::sleep_for(std::chrono::milliseconds(1));
        //    std::this_thread::yield();
        //}

        mutableCommandBuffer.WaitUntilCompleted();


        guard.lock();
        //cerr << "await(): status = " << jsonEncodeStr(commandBuffer.GetStatus()) << endl;

        if (!isResolved)
            ((CudaComputeEvent *)this)->resolve();
    }
    else {
        guard.unlock();
        auto tr = scopedOperation(OperationType::CUDA_COMPUTE, "awaiting " + label() + " on future");        
        future.get();
        ExcAssert(isResolved);
    }
}

std::shared_ptr<CudaComputeEvent>
CudaComputeEvent::
makeAlreadyResolvedEvent(const std::string & label, const CudaComputeQueue * queue)
{
    auto result = std::make_shared<CudaComputeEvent>(label, true /* already resolved */, queue);
    return result;
}

std::shared_ptr<CudaComputeEvent>
CudaComputeEvent::
makeUnresolvedEvent(const std::string & label, const CudaComputeQueue * queue)
{
    auto result = std::make_shared<CudaComputeEvent>(label, false /* already resolved */, queue);
    return result;
}

// CudaComputeQueue

GridDispatchType generalizeDispatchType(mtlpp::DispatchType dispatchType)
{
    return dispatchType == mtlpp::DispatchType::Concurrent ? GridDispatchType::PARALLEL : GridDispatchType::SERIAL;
}

CudaComputeQueue::
CudaComputeQueue(CudaComputeContext * owner, CudaComputeQueue * parent,
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

CudaComputeQueue::~CudaComputeQueue()
{
}

std::shared_ptr<ComputeQueue>
CudaComputeQueue::
parallel(const std::string & opName)
{
    return std::make_shared<CudaComputeQueue>(this->mtlOwner, this, opName, this->mtlQueue, mtlpp::DispatchType::Concurrent);
}

std::shared_ptr<ComputeQueue>
CudaComputeQueue::
serial(const std::string & opName)
{
    return std::make_shared<CudaComputeQueue>(this->mtlOwner, this, opName, this->mtlQueue, mtlpp::DispatchType::Serial);
}

void
CudaComputeQueue::
enqueueZeroFillArrayConcrete(const std::string & opName,
                                MemoryRegionHandle region,
                                size_t startOffsetInBytes, ssize_t lengthInBytes)
{
    auto [pin, buffer, offset] = CudaComputeContext::getMemoryRegion(opName, *region.handle, ACC_WRITE);
    auto length = buffer.GetLength();
    ExcAssertLessEqual(offset + startOffsetInBytes + lengthInBytes, length);

    auto blitEncoder = commandBuffer.BlitCommandEncoder();
    beginEncoding(opName, blitEncoder);
    blitEncoder.SetLabel((opName + " zero fill blit").c_str());
    blitEncoder.Fill(buffer, { uint32_t(offset + startOffsetInBytes), uint32_t(lengthInBytes) }, 0);
    endEncoding(opName, blitEncoder);
}                                
                                
void
CudaComputeQueue::
enqueueBlockFillArrayConcrete(const std::string & opName,
                              MemoryRegionHandle region,
                              size_t startOffsetInBytes, ssize_t lengthInBytes,
                              std::span<const std::byte> block)
{
    ComputeQueue::enqueueFillArrayImpl(opName, region, MemoryRegionInitialization::INIT_BLOCK_FILLED, startOffsetInBytes, lengthInBytes, block);
}                                
                                
void
CudaComputeQueue::
enqueueCopyFromHostConcrete(const std::string & opName,
                            MemoryRegionHandle toRegion,
                            FrozenMemoryRegion fromRegion,
                            size_t deviceStartOffsetInBytes)
{
    auto [pin, buffer, offset] = CudaComputeContext::getMemoryRegion(opName, *toRegion.handle, ACC_WRITE);
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
CudaComputeQueue::
enqueueTransferToHostConcrete(const std::string & opName, MemoryRegionHandle handle)
{
    MLDB_THROW_UNIMPLEMENTED();
}


FrozenMemoryRegion
CudaComputeQueue::
transferToHostSyncConcrete(const std::string & opName, MemoryRegionHandle handle)
{
    MLDB_THROW_UNIMPLEMENTED();
}

struct CudaBindContext: public GridBindContext {
    CudaBindContext(CudaComputeQueue * queue,
                     const std::string & opName,
                     const GridComputeKernel * kernel,
                     const GridBindInfo * bindInfo)
        : kernel(dynamic_cast<const CudaComputeKernel *>(kernel)),
          queue(queue), opName(opName)
    {
        ExcAssert(this->kernel);

        commandEncoder = queue->commandBuffer.ComputeCommandEncoder();
        queue->beginEncoding(opName, commandEncoder);
        commandEncoder.SetComputePipelineState(this->kernel->mtlFunction->computePipelineState);
    }

    virtual ~CudaBindContext()
    {
        queue->endEncoding(opName, commandEncoder);
    }

    const CudaComputeKernel * kernel = nullptr;
    CudaComputeQueue * queue = nullptr;
    std::string opName;
    
    mtlpp::ComputeCommandEncoder commandEncoder;

    virtual void
    setPrimitive(const std::string & opName, int argNum, std::span<const std::byte> bytes) override
    {
        traceCudaOperation("setPrimitive " + opName, "argNum %d with %zd bytes", argNum, bytes.size_bytes());
        commandEncoder.SetBytes(bytes.data(), bytes.size_bytes(), argNum);
    }

    virtual void
    setBuffer(const std::string & opName, int argNum,
              std::shared_ptr<GridMemoryRegionHandleInfo> handle,
              MemoryRegionAccess access) override
    {
        traceCudaOperation("setBuffer " + opName, "argNum %d handle %s:%d with %zd bytes",
                            argNum, handle->name.c_str(), handle->version, handle->lengthInBytes);
        ExcAssert(handle);
        auto [pin, memBuffer, offset] = CudaComputeContext::getMemoryRegion(opName, *handle, access);
        // TODO: where do we put the pin?
        commandEncoder.SetBuffer(memBuffer, offset, argNum);
    }

    virtual void
    setThreadGroupMemory(const std::string & opName, int argNum, size_t nBytes) override
    {
        traceCudaOperation("setPrimitive " + opName, "argNum %d with %zd bytes", argNum, nBytes);
        // Set it up in the command encoder
        commandEncoder.SetThreadgroupMemory(nBytes, argNum);
    }

    virtual void launch(const std::string & opName,
                        GridBindContext & context, std::vector<size_t> grid,
                        std::vector<size_t> block) override
    {
        CudaBindContext & bindContext = dynamic_cast<CudaBindContext &>(context);
        auto * kernel = bindContext.kernel;

        try {
            auto op = scopedOperation(OperationType::CUDA_COMPUTE, "launch kernel " + opName);
            grid.resize(3, 1);
            block.resize(3, 1);

            auto invocations = grid[0] * grid[1] * grid[2] * block[0] * block[1] * block[2];
            double memoryRequiredMb = invocations * 2048 / 1024.0 / 1024.0;

            traceCudaOperation("grid size " + jsonEncodeStr(grid));
            traceCudaOperation("block size " + jsonEncodeStr(block));
            traceCudaOperation(std::to_string(invocations) + " invocations requiring " + std::to_string(memoryRequiredMb)
                                + "MB of wired scratchpad memory");

            mtlpp::Size gridSize(grid[0], grid[1], grid[2]);
            mtlpp::Size blockSize(block[0], block[1], block[2]);

            bindContext.commandEncoder.DispatchThreadgroups(gridSize, blockSize);
        } MLDB_CATCH_ALL {
            rethrowException(400, "Error launching Cuda kernel " + kernel->kernelName);
        }
    }
};

std::shared_ptr<GridBindContext>
CudaComputeQueue::
newBindContext(const std::string & opName,
               const GridComputeKernel * kernel,
               const GridBindInfo * bindInfo)
{
    ExcAssert(bindInfo);
    return std::make_shared<CudaBindContext>(this, opName, kernel, bindInfo);
}

template<typename CommandEncoder>
void
CudaComputeQueue::
beginEncodingImpl(const std::string & opName, CommandEncoder & encoder, bool force)
{
    //cerr << "BEGIN ENCODING " << this << " " << opName << " " << type_name<CommandEncoder>() << endl;

    if (numChildren > 0) {
        throw MLDB::Exception("Cannot use a CudaComputeQueue that has a child queue once it has been created");
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
CudaComputeQueue::
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
CudaComputeQueue::
beginEncoding(const std::string & opName, mtlpp::ComputeCommandEncoder & encoder, bool force)
{
    beginEncodingImpl(opName, encoder, force);
}

void
CudaComputeQueue::
beginEncoding(const std::string & opName, mtlpp::BlitCommandEncoder & encoder, bool force)
{
    beginEncodingImpl(opName, encoder, force);
}

void
CudaComputeQueue::
endEncoding(const std::string & opName, mtlpp::ComputeCommandEncoder & encoder, bool force)
{
    endEncodingImpl(opName, encoder, force);
}

void
CudaComputeQueue::
endEncoding(const std::string & opName, mtlpp::BlitCommandEncoder & encoder, bool force)
{
    endEncodingImpl(opName, encoder, force);
}

FrozenMemoryRegion
CudaComputeQueue::
enqueueTransferToHostImpl(const std::string & opName,
                          MemoryRegionHandle handle)
{
    auto op = scopedOperation(OperationType::CUDA_COMPUTE, "CudaComputeQueue enqueueTransferToHostSyncImpl " + opName);
    MLDB_THROW_UNIMPLEMENTED();
}

FrozenMemoryRegion
CudaComputeQueue::
transferToHostSyncImpl(const std::string & opName,
                       MemoryRegionHandle handle)
{
    auto op = scopedOperation(OperationType::CUDA_COMPUTE, "CudaComputeQueue transferToHostSyncImpl " + opName);

    CudaMemoryRegionHandleInfo * upcastHandle = dynamic_cast<CudaMemoryRegionHandleInfo *>(handle.handle.get());
    if (!upcastHandle) {
        throw MLDB::Exception("Wrong CudaComputeContext handle: got " + demangle(typeid(handle)));
    }
    if (upcastHandle->backingHostMem) {
        // It's a read-only, host-first mapping... so nothing to do
        return { handle.handle, (const char *)upcastHandle->backingHostMem, (size_t)upcastHandle->lengthInBytes };
    }

    auto [pin, buffer, offset] = CudaComputeContext::getMemoryRegion(opName, *handle.handle, ACC_READ);

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

    finish("transferToHostSync");

    //ExcAssert(commandBuffer);
    //commandBuffer.Commit();
    //commandBuffer.WaitUntilCompleted();

    auto status = myCommandBuffer.GetStatus();
    if (status != mtlpp::CommandBufferStatus::Completed) {
        auto error = commandBuffer.GetError();
        throw CudaException(error, "Command buffer is not completed", "status", status);
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
CudaComputeQueue::
enqueueTransferToHostMutableImpl(const std::string & opName, MemoryRegionHandle handle)
{
    MLDB_THROW_UNIMPLEMENTED();

#if 0
    auto op = scopedOperation(OperationType::CUDA_COMPUTE, "CudaComputeContext transferToHostMutableImpl " + opName);
    ExcAssert(handle.handle);

    auto [pin, mem, offset] = getMemoryRegion(opName, *handle.handle, ACC_READ_WRITE);
    CudaEvent clEvent;
    std::shared_ptr<void> memPtr;
    std::tie(memPtr, clEvent)
        = clQueue->clQueue.enqueueMapBuffer(mem, CL_MAP_READ | CL_MAP_WRITE,
                                    offset, handle.lengthInBytes());

    auto event = std::make_shared<CudaComputeEvent>(std::move(clEvent));
    auto promise = std::shared_ptr<std::promise<std::any>>();

    auto cb = [handle, promise, memPtr, pin=pin] (const CudaEvent & event, auto status)
    {
        if (status == CudaEventCommandExecutionStatus::ERROR)
            promise->set_exception(std::make_exception_ptr(MLDB::Exception("Cuda error mapping host memory")));
        else {
            promise->set_value(MutableMemoryRegion(memPtr, (char *)memPtr.get(), handle.lengthInBytes()));
        }
    };

    clEvent.addCallback(cb);

    return { std::move(promise), std::move(event) };
#endif
}

MutableMemoryRegion
CudaComputeQueue::
transferToHostMutableSyncImpl(const std::string & opName,
                              MemoryRegionHandle handle)
{
    MLDB_THROW_UNIMPLEMENTED();

#if 0
    auto op = scopedOperation(OperationType::CUDA_COMPUTE, "CudaComputeContext transferToHostMutableSyncImpl " + opName);

    ExcAssert(handle.handle);

    auto [pin, mem, offset] = getMemoryRegion(opName, *handle.handle, ACC_READ_WRITE);
    auto memPtr = clQueue->clQueue.enqueueMapBufferBlocking(mem, CL_MAP_READ | CL_MAP_WRITE,
                                                            offset, handle.lengthInBytes());
    MutableMemoryRegion result(std::move(memPtr), (char *)memPtr.get(), handle.lengthInBytes());
    return result;
#endif
}

MemoryRegionHandle
CudaComputeQueue::
enqueueManagePinnedHostRegionImpl(const std::string & opName, std::span<const std::byte> region, size_t align,
                                  const std::type_info & type, bool isConst)
{
    auto op = scopedOperation(OperationType::CUDA_COMPUTE, "CudaComputeContext managePinnedHostRegionImpl " + opName);
    return managePinnedHostRegionSyncImpl(opName, region, align, type, isConst);
}

MemoryRegionHandle
CudaComputeQueue::
managePinnedHostRegionSyncImpl(const std::string & opName,
                               std::span<const std::byte> region, size_t align,
                               const std::type_info & type, bool isConst)
{
    auto op = scopedOperation(OperationType::CUDA_COMPUTE, "CudaComputeContext managePinnedHostRegionSyncImpl " + opName);

    //cerr << "managing pinned host region " << opName << " of length " << region.size() << endl;

    mtlpp::ResourceOptions options
         = mtlpp::ResourceOptions::StorageModeShared;


    traceCudaOperation("region size " + std::to_string(region.size()));

    // This overload calls newBufferWithBytesNoCopy
    // Note that we require a page-aligned address and size, and a single VM region
    // See https://developer.apple.com/documentation/cuda/mtldevice/1433382-makebuffer
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

    auto handle = std::make_shared<CudaMemoryRegionHandleInfo>();
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
CudaComputeQueue::
enqueueCopyBetweenDeviceRegionsImpl(const std::string & opName,
                                    MemoryRegionHandle from, MemoryRegionHandle to,
                                    size_t fromOffset, size_t toOffset,
                                    size_t length)
{
    auto op = scopedOperation(OperationType::CUDA_COMPUTE, "CudaComputeQueue enqueueCopyBetweenDeviceRegionsImpl " + opName);

    CudaMemoryRegionHandleInfo * upcastFromHandle = dynamic_cast<CudaMemoryRegionHandleInfo *>(from.handle.get());
    if (!upcastFromHandle) {
        throw MLDB::Exception("Wrong CudaComputeContext from handle: got " + demangle(typeid(from.handle)));
    }

    auto [fromPin, fromBuffer, fromBaseOffset] = CudaComputeContext::getMemoryRegion(opName, *from.handle, ACC_READ);

    ExcAssertLessEqual(fromBaseOffset + fromOffset + length, from.lengthInBytes());

    CudaMemoryRegionHandleInfo * upcastToHandle = dynamic_cast<CudaMemoryRegionHandleInfo *>(to.handle.get());
    if (!upcastToHandle) {
        throw MLDB::Exception("Wrong CudaComputeContext to handle: got " + demangle(typeid(to.handle)));
    }

    auto [toPin, toBuffer, toBaseOffset] = CudaComputeContext::getMemoryRegion(opName, *to.handle, ACC_WRITE);

    ExcAssertLessEqual(toBaseOffset + toOffset + length, to.lengthInBytes());

    auto blitEncoder = commandBuffer.BlitCommandEncoder();
    beginEncoding(opName, blitEncoder);
    blitEncoder.Copy(fromBuffer, fromBaseOffset + fromOffset, toBuffer, toBaseOffset + toOffset, length);
    endEncoding(opName, blitEncoder);
}

void
CudaComputeQueue::
copyBetweenDeviceRegionsSyncImpl(const std::string & opName,
                                 MemoryRegionHandle from, MemoryRegionHandle to,
                                 size_t fromOffset, size_t toOffset,
                                 size_t length)
{
    enqueueCopyBetweenDeviceRegionsImpl(opName, from, to, fromOffset, toOffset, length);
    finish("copyBetweenDeviceRegionsSyncImpl");
}

std::shared_ptr<ComputeEvent>
CudaComputeQueue::
flush(const std::string & opName)
{
    auto op = scopedOperation(OperationType::CUDA_COMPUTE, opName + " CudaComputeQueue flush");

    auto result = std::make_shared<CudaComputeEvent>(opName, false /* resolved */, this);
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
CudaComputeQueue::
enqueueBarrier(const std::string & label)
{
    auto op = scopedOperation(OperationType::CUDA_COMPUTE, "CudaComputeQueue enqueueBarrier " + label);
    auto encoder = commandBuffer.ComputeCommandEncoder(mtlpp::DispatchType::Serial);
    beginEncoding("enqueueBarrier " + label, encoder, true /* force */);
    endEncoding("enqueueBarrier " + label, encoder, true /* force */);
}

void
CudaComputeQueue::
finish(const std::string & opName)
{
    auto op = scopedOperation(OperationType::CUDA_COMPUTE, opName + " CudaComputeQueue finish");
    flush(opName)->await();
}

std::shared_ptr<ComputeEvent>
CudaComputeQueue::
makeAlreadyResolvedEvent(const std::string & label) const
{
    return CudaComputeEvent::makeAlreadyResolvedEvent(label, this);
}


// CudaComputeContext

CudaComputeContext::
CudaComputeContext(mtlpp::Device mtlDevice, ComputeDevice device)
    : GridComputeContext(device), mtlDevice(mtlDevice)
{
    {
#if 0
        mtlpp::Buffer buffer;

        for (size_t i = 0;  i < 1000;  ++i) {
            cerr << endl << "creating buffer " << i << endl;
            buffer = mtlDevice.NewBuffer(100000000, mtlpp::ResourceOptions::StorageModePrivate);
            cerr << endl << "done creating buffer " << i << endl;
            if (!buffer) {
                throw MLDB::Exception("Error creating buffer number " + std::to_string(i));
            }
            //buffer.SetLabel(format("test buffer %zd", i).c_str());
            //ns::GarbageCollectExhaustively();
        }

        cerr << "done creating buffers" << endl;
#endif
    }

#if 0
    mtlpp::HeapDescriptor heapDescriptor;
    heapDescriptor.SetStorageMode(mtlpp::StorageMode::Private);
    heapDescriptor.SetSize(1 << 30);  // 1GB

    this->heap = mtlDevice.NewHeap(heapDescriptor);
    ExcAssert(this->heap);
#endif
}


std::tuple<std::shared_ptr<const void>, mtlpp::Buffer, size_t>
CudaComputeContext::
getMemoryRegion(const std::string & opName, MemoryRegionHandleInfo & handle, MemoryRegionAccess access)
{
    CudaMemoryRegionHandleInfo * upcastHandle = dynamic_cast<CudaMemoryRegionHandleInfo *>(&handle);
    if (!upcastHandle) {
        throw MLDB::Exception("TODO: get memory region from block handled from elsewhere: got " + demangle(typeid(handle)));
    }
    auto pin = upcastHandle->pinAccess(opName, access);

    return { std::move(pin), upcastHandle->buffer, upcastHandle->offset };
}

std::tuple<FrozenMemoryRegion, int /* version */>
CudaComputeContext::
getFrozenHostMemoryRegion(const std::string & opName, MemoryRegionHandleInfo & handle,
                          size_t offset, ssize_t length,
                          bool ignoreHazards) const
{
    CudaMemoryRegionHandleInfo * upcastHandle = dynamic_cast<CudaMemoryRegionHandleInfo *>(&handle);
    if (!upcastHandle) {
        throw MLDB::Exception("TODO: get memory region from block handled from elsewhere: got " + demangle(typeid(handle)));
    }
    return upcastHandle->getReadOnlyHostAccessSync(*this, opName, offset, length, ignoreHazards);
}

static MemoryRegionHandle
doCudaAllocate(mtlpp::Heap & mtlHeap,
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

    auto handle = std::make_shared<CudaMemoryRegionHandleInfo>();
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
CudaComputeContext::
allocateSyncImpl(const std::string & regionName,
                 size_t length, size_t align,
                 const std::type_info & type, bool isConst)
{
    auto op = scopedOperation(OperationType::CUDA_COMPUTE, "CudaComputeContext allocateSyncImpl " + regionName);
    auto result = doCudaAllocate(this->heap, this->mtlDevice, regionName, length, align, type, isConst);
    return result;
}

static MemoryRegionHandle
doCudaTransferToDevice(mtlpp::Device & mtlDevice,
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

    auto handle = std::make_shared<CudaMemoryRegionHandleInfo>();
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

std::shared_ptr<ComputeQueue>
CudaComputeContext::
getQueue(const std::string & queueName)
{
    return std::make_shared<CudaComputeQueue>(this, nullptr /* parent */, queueName, mtlpp::CommandQueue(), mtlpp::DispatchType::Serial);
}

std::shared_ptr<GridComputeFunctionLibrary>
CudaComputeContext::
getLibrary(const std::string & libraryName)
{
    auto op = scopedOperation(OperationType::CUDA_COMPUTE, "CudaComputeContext getLibrary " + libraryName);

    std::unique_lock guard(libraryRegistryMutex);
    auto it = libraryRegistry.find(libraryName);
    if (it == libraryRegistry.end()) {
        throw AnnotatedException(400, "Unable to find Cuda compute library '" + libraryName + "'",
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
CudaComputeContext::
specializeKernel(const GridComputeKernelTemplate & tmplate)
{
    return std::make_shared<CudaComputeKernel>(this, tmplate);
}


// CudaComputeKernel

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

// Parses an Cuda kernel argument info structure, and turns it into a ComputeKernel type
ComputeKernelType
CudaComputeKernel::
getKernelType(const mtlpp::Argument & arg)
{
    ComputeKernelType type;

    mtlpp::ArgumentType argType = arg.GetType();
    mtlpp::DataType dataType;

    static std::atomic<int> idx(0);
    std::string name = "__cuda_arg_" + string(arg.GetName().GetCStr()) + std::to_string(idx++);

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
            throw MLDB::Exception("Can't handle arrays as Cuda arguments");
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



CudaComputeKernel::
CudaComputeKernel(CudaComputeContext * owner, const GridComputeKernelTemplate & tmplate)
    : GridComputeKernelSpecialization(owner, tmplate)
{
    this->mtlContext = owner;
    this->mtlFunction = dynamic_cast<const CudaComputeFunction *>(this->gridFunction.get());
    ExcAssert(this->mtlFunction);
}


// CudaComputeFunction

CudaComputeFunction::
CudaComputeFunction(CudaComputeContext & context, mtlpp::Function mtlFunction)
    : mtlFunction(std::move(mtlFunction))
{
    ExcAssert(this->mtlFunction);
    ns::Error error{ns::Handle()};

    mtlpp::PipelineOption options = (mtlpp::PipelineOption((int)mtlpp::PipelineOption::ArgumentInfo | (int)mtlpp::PipelineOption::BufferTypeInfo));
    this->computePipelineState
        = context.mtlDevice.NewComputePipelineState(this->mtlFunction, options, reflection, &error);
    checkCudaError(error, "Error making pipeline state for function");
    ExcAssert(computePipelineState);
    ExcAssert(reflection);
}

GridComputeFunctionArgumentDisposition getDisposition(mtlpp::ArgumentType argType)
{
    switch (argType) {
    case mtlpp::ArgumentType::Buffer: return GridComputeFunctionArgumentDisposition::BUFFER;
    case mtlpp::ArgumentType::ThreadgroupMemory: return GridComputeFunctionArgumentDisposition::THREADGROUP;
    default:
        MLDB_THROW_UNIMPLEMENTED("Cuda sampler or texture arguments");
    }
}

std::vector<GridComputeFunctionArgument>
CudaComputeFunction::
getArgumentInfo() const
{
    auto arguments = reflection.GetArguments();

    std::vector<GridComputeFunctionArgument> result;

    for (size_t i = 0;  i < arguments.GetSize();  ++i) {
        mtlpp::Argument arg = arguments[i];
        std::string argName = arg.GetName().GetCStr();
        GridComputeFunctionArgumentDisposition disposition = getDisposition(arg.GetType());
        auto type = CudaComputeKernel::getKernelType(arg);

        GridComputeFunctionArgument entry;
        entry.name = argName;
        entry.disposition = disposition;
        entry.type = type;
        entry.computeFunctionArgIndex = arg.GetIndex();

        result.emplace_back(std::move(entry));
    }

    return result;
}


// CudaComputeFunctionLibrary

CudaComputeFunctionLibrary::
CudaComputeFunctionLibrary(CudaComputeContext & context, mtlpp::Library mtlLibrary)
    : context(context), mtlLibrary(mtlLibrary)
{
}

std::shared_ptr<GridComputeFunction>
CudaComputeFunctionLibrary::
getFunction(const std::string & functionName)
{
    ns::Error error{ns::Handle()};

    mtlpp::Function function = this->mtlLibrary.NewFunction(functionName.c_str(), {} /* constantValues */, &error);

    checkCudaError(error, "Error getting function from Cuda library", "functionName", functionName);

    ExcAssert(function);
    return std::make_shared<CudaComputeFunction>(context, function);
}

std::string
CudaComputeFunctionLibrary::
getId() const
{
    MLDB_THROW_UNIMPLEMENTED();
}

Json::Value
CudaComputeFunctionLibrary::
getMetadata() const
{
    MLDB_THROW_UNIMPLEMENTED();
}

std::shared_ptr<CudaComputeFunctionLibrary>
CudaComputeFunctionLibrary::
compileFromSourceFile(CudaComputeContext & context, const std::string & fileName)
{
    filter_istream stream(fileName);
    Utf8String source = /*"#line 1 \"" + fileName + "\"\n" +*/ stream.readAll();

    return compileFromSource(context, source, fileName);
}

std::shared_ptr<CudaComputeFunctionLibrary>
CudaComputeFunctionLibrary::
compileFromSource(CudaComputeContext & context, const Utf8String & source, const std::string & fileNameToAppearInErrorMessages)
{
    ns::Error error{ns::Handle()};

    mtlpp::CompileOptions compileOptions;
    mtlpp::Library library = context.mtlDevice.NewLibrary(source.rawData(), compileOptions, &error);

    checkCudaError(error, "Error compiling Cuda library from source", "fileName", fileNameToAppearInErrorMessages);

    return std::make_shared<CudaComputeFunctionLibrary>(context, std::move(library));
}

std::shared_ptr<CudaComputeFunctionLibrary>
CudaComputeFunctionLibrary::
loadMtllib(CudaComputeContext & context, const std::string & libraryFilename)
{
    ns::Error error{ns::Handle()};

    mtlpp::Library library = context.mtlDevice.NewLibrary(libraryFilename.c_str(), &error);

    checkCudaError(error, "Error loading Cuda library", "fileName", libraryFilename);

    return std::make_shared<CudaComputeFunctionLibrary>(context, std::move(library));
}


// CudaComputeRuntime

EnvOption<int> CUDA_DEFAULT_DEVICE("CUDA_DEFAULT_DEVICE", -1);

struct CudaComputeRuntime: public ComputeRuntime {

    std::vector<mtlpp::Device> mtlDevices;
    std::vector<ComputeDevice> devices;

    CudaComputeRuntime()
    {
        auto queriedDevices = mtlpp::Device::CopyAllDevices();
        for (size_t i = 0;  i < queriedDevices.GetSize();  ++i) {
            auto device = queriedDevices[i];
            //cerr << "cuda device " << i << " is " << device.GetName().GetCStr() << endl;
            mtlDevices.push_back(queriedDevices[i]);
            devices.push_back({ComputeRuntimeId::CUDA, 0 /* runtime instance */, (uint8_t)i, 0, 0});
        }
    }

    mtlpp::Device convertDevice(ComputeDevice device) const
    {
        if (device.runtime != ComputeRuntimeId::CUDA) {
            throw MLDB::Exception("Attempt to pass non-Cuda device " + device.info() + " to Cuda");
        }
        return mtlDevices.at(device.deviceInstance);
    }

    virtual ~CudaComputeRuntime()
    {
    }

    virtual ComputeRuntimeId getId() const
    {
        return ComputeRuntimeId::CUDA;
    }

    virtual std::string printRestOfDevice(ComputeDevice device) const
    {
        return std::to_string(device.deviceInstance);
    }

    virtual std::string printHumanReadableDeviceInfo(ComputeDevice device) const
    {
        if (device.runtimeInstance > 0 || device.deviceInstance >= mtlDevices.size()) {
            return "<<INVALID CUDA PLATFORM OR DEVICE INDEX>>";
        }
        std::string result = mtlDevices[device.deviceInstance].GetName().GetCStr();
        return result;
    }

    virtual ComputeDevice getDefaultDevice() const
    {
        if (!CUDA_ENABLED || mtlDevices.empty())
            return ComputeDevice::none();

        if (CUDA_DEFAULT_DEVICE.specified()) {
            ExcAssertLess(CUDA_DEFAULT_DEVICE.get(), mtlDevices.size());
            return {ComputeRuntimeId::CUDA, 0 /* platform */,
                    (uint16_t)CUDA_DEFAULT_DEVICE.get(), 0, 0};
        }

        // TODO: deal with the possibility of the default not being at index zero...
        return {ComputeRuntimeId::CUDA, 0 /* platform */, 0 /* device */,0, 0};
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
            throw MLDB::Exception("Cuda Compute Kernel driver only can accept a single device");
        }
        return std::make_shared<CudaComputeContext>(convertDevice(devices[0]), devices[0]);
    }

};

void registerCudaLibrary(const std::string & libraryName,
                          std::function<std::shared_ptr<CudaComputeFunctionLibrary>(CudaComputeContext &)> generator)
{
    std::unique_lock guard{libraryRegistryMutex};
    libraryRegistry[libraryName].generate = generator;
}

namespace {

static struct Init {
    Init()
    {
        ComputeRuntime::registerRuntime(ComputeRuntimeId::CUDA, "cuda",
                                        [] () { return new CudaComputeRuntime(); });

        auto compileLibrary = [] (CudaComputeContext & context) -> std::shared_ptr<CudaComputeFunctionLibrary>
        {
            if (false) {
                std::string fileName = "mldb/builtin/cuda/base_kernels.cuda";
                return CudaComputeFunctionLibrary::compileFromSourceFile(context, fileName);
            }
            else {
                std::string fileName = "build/arm64/lib/base_kernels_cuda.cudalib";
                return CudaComputeFunctionLibrary::loadMtllib(context, fileName);
            }
        };

        registerCudaLibrary("base_kernels", compileLibrary);
    }

} init;


} // file scope
} // namespace MLDB
