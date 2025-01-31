/** for_each_line.cc
    Jeremy Barnes, 29 November 2013
    Copyright (c) 2013 mldb.ai inc.  All rights reserved.
    This file is part of MLDB. Copyright 2015 mldb.ai inc. All rights reserved.

*/

#include "for_each_line.h"
#include <atomic>
#include <exception>
#include <mutex>
#include <chrono>
#include <thread>
#include <cstring>
#include <coroutine>
#include "mldb/ext/concurrentqueue/blockingconcurrentqueue.h"
#include <future>
#include "mldb/vfs/filter_streams.h"
#include "mldb/base/thread_pool.h"
#include "mldb/base/exc_assert.h"
#include "mldb/types/date.h"
#include "mldb/utils/log.h"
#include "mldb/base/hex_dump.h"
#include "mldb/block/content_descriptor.h"
#include "mldb/arch/spinlock.h"
#include "mldb/arch/atomic_min_max.h"
#include "mldb/base/scope.h"
#include "mldb/utils/scoreboard.h"
#include "mldb/utils/coalesced_range.h"
#include "mldb/base/processing_state.h"

using namespace std;
using moodycamel::BlockingConcurrentQueue;


namespace MLDB {


// First phase: blocks are processed in sequence, with each block creating
// a set of lines plus a state containing leftover data to pass to the
// next block.

namespace {


struct ForEachLineOptions {
    int maxParallelism = -1;
    ssize_t defaultBlockSize = -1;
    size_t startOffset = 0;
    size_t skipLines = 0;
    ssize_t maxLines = -1;
    const BlockSplitter * splitter = &newLineSplitter;
};

struct ForEachLineProcessor: public ProcessingState {

    ForEachLineProcessor(ForEachLineOptions options)
        : ProcessingState(options.maxParallelism), options(options)
    {
    }

    ForEachLineOptions options;

    enum {
        CHUNK_STREAM_PRIORITY = 5,
    };

    struct JobPromise;

    struct CoroutineJob: public std::coroutine_handle<JobPromise> {
        using promise_type = JobPromise;
    };

    struct JobPromise {
        CoroutineJob get_return_object() { return {CoroutineJob::from_promise(*this)}; }
        std::suspend_always initial_suspend() noexcept { return {}; }
        std::suspend_always final_suspend() noexcept { return {}; }
        void return_void() {}
        void unhandled_exception() {}
    };

    // A chunk of memory plus metadata
    struct Chunk {
        size_t chunkNumber = 0;
        size_t startOffset = 0;
        bool lastChunk = false;
        std::span<const std::byte> data;
        shared_ptr<const void> keep;      // Pins data in place
    };

    // Pushes a series of jobs that call onChunk
    bool chunk_source(std::istream & stream)
    {
        filter_istream * fistream = dynamic_cast<filter_istream *>(&stream);
        size_t extraCapacity = options.splitter->requiredBlockPadding();

        if (fistream) {
            size_t chunkOffset = stream.tellg();
            // Can we get a memory mapped version of our stream?  It
            // saves us having to copy data.  mapped will be set to
            // nullptr if it's not possible to memory map this stream.
            auto [mapped, mappedSize, mappedCapacity] = fistream->mapped();

            if (mapped) {
                ExcCheckLessEqual(options.startOffset + chunkOffset, mappedSize, "startOffset is beyond the size of the file");
                std::span<const std::byte> mem(reinterpret_cast<const std::byte *>(mapped) + options.startOffset + chunkOffset,
                                            mappedSize - options.startOffset - chunkOffset);
                return chunk_source(mem, options.startOffset + chunkOffset);
            }
        }
        size_t blockSize = options.defaultBlockSize == -1 ? 2'000'000 : options.defaultBlockSize;
        if (options.startOffset > 0)
            stream.seekg(options.startOffset);
        std::shared_ptr<std::byte[]> block(new std::byte[blockSize]);

        bool lastChunk = false;

        for (size_t chunkNumber = 0; stream; ++chunkNumber) {
            if (stopped()) return false;
            size_t chunkOffset = stream.tellg();
            stream.read(reinterpret_cast<char *>(block.get()), blockSize + extraCapacity);
            ssize_t bytesRead = stream.gcount();
            if (bytesRead <= 0)
                break;
            if (!stream)
                lastChunk = true;
            std::span<const std::byte> mem(block.get(), bytesRead);
            if (!handle_chunk({chunkNumber, chunkOffset, lastChunk, mem, {block}}))
                return false;
        }
        return true;
    }

    size_t block_size_for_content_length(size_t contentLength)
    {
        size_t blockSize = options.defaultBlockSize;
        if (blockSize == -1) {
            blockSize = std::max<int64_t>(1000000, contentLength / options.maxParallelism);
            blockSize = std::min<int64_t>(blockSize, 20000000);
        }
        return blockSize;
    }

    bool chunk_source(std::span<const std::byte> mem, size_t startOffset)
    {
        size_t numBytes = mem.size();
        size_t blockSize = block_size_for_content_length(numBytes);

        const std::byte * p = mem.data();
        const std::byte * e = p + mem.size();

        for (size_t chunkNumber = 0; p < e; p += blockSize) {
            if (stopped()) return false;
            const std::byte * eb = std::min(e, p + blockSize);
            std::span<const std::byte> mem(p, eb-p);
            size_t chunkOffset = startOffset + p - mem.data();
            bool lastChunk = eb == e;
            if (!handle_chunk({chunkNumber, chunkOffset, lastChunk, mem, {}}))
                return false;
        }
        return true;
    }

    bool chunk_source(const ContentHandler & content)
    {
        size_t contentSize = content.getSize();
        size_t blockSize = block_size_for_content_length(contentSize);

        auto doBlock = [&] (size_t chunkNumber, uint64_t chunkOffset, FrozenMemoryRegion region)
        {
            if (stopped()) return false;
            bool lastChunk = chunkOffset + region.length() == contentSize;
            std::span<const std::byte> mem(reinterpret_cast<const std::byte *>(region.data()), region.length());
            return handle_chunk({chunkNumber, chunkOffset, lastChunk, mem, {}});
        };
    
        return content.forEachBlockParallel(options.startOffset, blockSize, options.maxParallelism, doBlock);
    }

    bool handle_chunk(Chunk chunk)
    {
        cerr << "handling chunk " << chunk.chunkNumber << " last " << chunk.lastChunk << endl;
        size_t numChunksOutstanding = 0;
        {
            std::unique_lock guard{chunks_mutex_};
            if (chunk.chunkNumber == chunks_done_) {
                if (chunksFinished)
                    return true;
                guard.unlock();
                return handle_next_chunk(std::move(chunk));
            }
            else {
                chunks_.emplace(chunk.chunkNumber, std::move(chunk));
                numChunksOutstanding = chunks_.size();
            }
        }
        
        // Slow down the production of chunks if there are too many by contributing to
        // other work.
        while (numChunksOutstanding > numCpus() && !stopped()) {
            {
                // Make sure there is no data race on chunksFinished
                // We could remove this lock if it becomes a problem by using the right sequencing
                std::unique_lock guard{chunks_mutex_};
                if (chunksFinished)
                    break;
            }

            tp_.work();

            std::unique_lock guard{chunks_mutex_};
            numChunksOutstanding = chunks_.size();
        }

        return true;
    }

    struct LineBlock {
        size_t blockNumber = 0;
        size_t startLine = 0;
        size_t startOffset = 0;
        size_t endOffset = 0;
        bool lastBlock = false;
        std::vector<std::span<const char>> lines;
        std::vector<std::shared_ptr<const void>> keep;
    };

    // Information passed from one chunk to the next
    size_t outputChunkNumber = 0;
    size_t outputStartOffset = 0;
    size_t outputLineNumber = 0;
    CoalescedRange<const char> chunkData;
    std::any splitterState;
    std::vector<std::shared_ptr<const void>> keep;  // pins for each block in chunkData

    bool handle_next_chunk(Chunk chunk)
    {
        cerr << "handling in-order chunk " << chunk.chunkNumber << endl;
        cerr << "chunkData.range_count() = " << chunkData.range_count() << endl;
        cerr << "keep.size() = " << keep.size() << endl;

        // Precondition: chunks_done_ is equal to this chunk number
        // Only one thread in this block at a time, which is staisfied by the condition above
        ExcAssertEqual(chunks_done_, chunk.chunkNumber);
        ExcAssertEqual(chunkData.range_count(), keep.size());

        chunkData.add(reinterpret_cast<const char *>(chunk.data.data()), chunk.data.size());
        keep.emplace_back(chunk.keep);

        LineBlock lineBlock;
        lineBlock.keep = keep;
        lineBlock.blockNumber = outputChunkNumber;
        lineBlock.startLine = outputLineNumber;
        lineBlock.startOffset = outputStartOffset;
        lineBlock.lastBlock = chunk.lastChunk;

        auto & lines = lineBlock.lines;

        auto it = chunkData.begin(), end = chunkData.end();

        while (it != end) {
            auto another = options.splitter->nextRecord(chunkData, it, chunk.lastChunk, splitterState);
            if (!another)
                break;
            auto [next_it, next_state] = another.value();

            if (auto lineSpan = chunkData.get_span(it, next_it)) {
                // Push the contiguous line
                lines.push_back(std::move(lineSpan.value()));
            }
            else {
                // Line is not in a contiguous range; need to extract as a string
                size_t len = next_it - it;
                std::shared_ptr<char[]> contiguous(new char[len]);
                using std::copy;
                copy(it, next_it, contiguous.get());
                lineBlock.lines.emplace_back(contiguous.get(), len);
                lineBlock.keep.emplace_back(std::move(contiguous));
            }
            splitterState = std::move(next_state);
            it = next_it;
        }

        size_t lineCount = lines.size();

        cerr << "found " << lineCount << " lines" << endl;
        cerr << "outputLineNumber = " << outputLineNumber << endl;
        cerr << "options.maxLines = " << options.maxLines << endl;

        if (options.maxLines != -1 && lineCount + outputLineNumber >= options.maxLines) {
            // We've reached the maximum number of lines
            lines.resize(options.maxLines - outputLineNumber);
            lineBlock.lastBlock = true;
        }

        // Reduce the chunk data to the leftover parts and remove the associated
        // data pins.

        ExcAssertEqual(keep.size(), chunkData.range_count());
        auto [firstChunk, lastChunk] = chunkData.reduce(it, end);
        cerr << "keeping chunks " << firstChunk << " to " << lastChunk << " of " << keep.size() << endl;
        keep.erase(keep.begin() + lastChunk, keep.end());
        keep.erase(keep.begin(), keep.begin() + firstChunk);

        ExcAssertEqual(keep.size(), chunkData.range_count());

        if (lineCount > 0) {
            // Submit the line block for processing before we schedule another chunk so
            // we don't get ahead of ourselves.
            int priority = 1;

            std::function<void ()> job = [lineBlock=std::move(lineBlock),this] ()
            {
                if (this->stopped())
                    return;
                if (!this->handle_line_block(std::move(lineBlock)))
                    this->stop();
            };

            if (options.maxParallelism > 1) {
                submit(priority, "process_line_block " + std::to_string(chunk.chunkNumber), std::move(job));
            }
            else {
                // We're not processing in parallel, so there is no other thread
                try {
                    job();
                } MLDB_CATCH_ALL {
                    takeException("handle_line_block");
                }
            }
            ++outputChunkNumber;
        }
        outputLineNumber += lineCount;
        outputStartOffset = chunk.startOffset + chunk.data.size();

        // Allow another chunk to happen
        {
            std::unique_lock guard{chunks_mutex_};
            // Nothing should have happened in the meantime...
            ExcAssertEqual(chunks_done_, chunk.chunkNumber);
            ++chunks_done_;
            if (lineBlock.lastBlock) {
                // We're done
                chunksFinished = true;
            }
        }
        
        return true;
    }

    std::function<bool (LineBlock)> handle_line_block;

    template<typename Source>
    bool process(Source&& source, 
                 std::function<bool (const char * line,
                                     size_t lineLength,
                                     int64_t blockNumber,
                                     int64_t lineNumber)> onLine,
                 std::function<bool (int64_t blockNumber, int64_t lineNumber, uint64_t numLinesInBlock)> startBlock,
                 std::function<bool (int64_t blockNumber, int64_t lineNumber, uint64_t numLinesInBlock)> endBlock)
    {
        handle_line_block = [onLine,startBlock,endBlock] (LineBlock block)
        {
            if (startBlock && !startBlock(block.blockNumber, block.startLine, block.lines.size()))
                return false;

            for (size_t i = 0; i < block.lines.size();  ++i) {
                if (onLine && !onLine(block.lines[i].data(), block.lines[i].size(), block.blockNumber, block.startLine + i))
                    return false;
            }

            if (endBlock && !endBlock(block.blockNumber, block.startLine, block.lines.size()))
                return false;

            return true;
        };

        return chunk_source(source);


#if 0
        auto pipeline
            = getLazyStream(source, block_size=blockSize, start_offset=startOffset)
              .map(handle_chunk, parallel=true, in_order=true)
              .reduce(LeftoverChunk(), split_chunk, in_order=true)
              .map(handle_block, parallel=true);

        auto executor = pipeline.compile();
        return executor();
#endif
    }

    mutable std::mutex chunks_mutex_;
    std::map<size_t, Chunk> chunks_;
    std::atomic<size_t> chunks_done_ = 0;
    std::atomic<bool> chunksFinished = false;
};

// Structure that keep track of the overall status of processing
struct Processing: public ProcessingState {
    Processing(int numThreads) : ProcessingState(numThreads) {}
    BlockingConcurrentQueue<pair<int64_t, vector<string> > > decompressedLines;
};

} // file scope

size_t
forEachLine(std::istream & stream,
            const std::function<void (const char *, size_t,
                                      int64_t)> & processLine,
            shared_ptr<spdlog::logger> logger,
            int numThreads,
            bool ignoreStreamExceptions,
            int64_t maxLines)
{
    auto onLineStr = [&] (const string & line, int64_t lineNum) {
        processLine(line.c_str(), line.size(), lineNum);
    };

    return forEachLineStr(stream, onLineStr, logger, numThreads,
                          ignoreStreamExceptions, maxLines);
}

size_t
forEachLineStr(std::istream & stream,
               const std::function<void (const std::string &,
                                         int64_t)> & processLine,
               shared_ptr<spdlog::logger> logger,
               int numThreads,
               bool ignoreStreamExceptions,
               int64_t maxLines)
{

    ForEachLineOptions options;
    options.maxParallelism = numThreads;
    options.defaultBlockSize = -1;
    options.startOffset = 0;
    options.skipLines = 0;
    options.maxLines = maxLines;
    options.splitter = &newLineSplitter;

    ForEachLineProcessor processor(options);
    std::atomic<int64_t> done(0);
    Date start = Date::now();
    Date lastCheck = start;

    auto onLine = [&] (const char * line, size_t length, int64_t blockNumber, int64_t lineNumber) -> bool
    {
        string lineStr(line, length);
        try {
            processLine(lineStr, lineNumber);
            auto new_done = done.fetch_add(1) + 1;

            if (new_done % 1000000 == 0 && 
                logger->should_log(spdlog::level::info)) {

                logger->info() << "done " << new_done << " lines";
                Date now = Date::now();

                double elapsed = now.secondsSince(start);
                double instElapsed = now.secondsSince(lastCheck);
                logger->info() << MLDB::format("doing %.3fMlines/second total, %.3f instantaneous",
                                               done / elapsed / 1000000.0,
                                               1000000 / instElapsed / 1000000.0);
                lastCheck = now;
            }
        } MLDB_CATCH_ALL {
            WARNING_MSG(logger) << "error dealing with line " << lineStr
                                << ": " << getExceptionString();
            if (!ignoreStreamExceptions) {
                throw;
            }
        }
        return true;
    };

    return processor.process(stream, onLine, nullptr /* startBlock */, nullptr /* endBlock */);
}

size_t
forEachLine(const std::string & filename,
            const std::function<void (const char *, size_t, int64_t)> & processLine,
            shared_ptr<spdlog::logger> logger,
            int numThreads,
            bool ignoreStreamExceptions,
            int64_t maxLines)
{
    filter_istream stream(filename);
    return forEachLine(stream, processLine, logger, numThreads,
                       ignoreStreamExceptions, maxLines);
}

size_t
forEachLineStr(const std::string & filename,
               const std::function<void (const std::string &, int64_t)> & processLine,
               shared_ptr<spdlog::logger> logger,
               int numThreads,
               bool ignoreStreamExceptions,
               int64_t maxLines)
{
    filter_istream stream(filename);
    return forEachLineStr(stream, processLine, logger, numThreads,
                          ignoreStreamExceptions, maxLines);
}


/*****************************************************************************/
/* FOR EACH LINE BLOCK (ISTREAM)                                             */
/*****************************************************************************/

bool forEachLineBlock(std::istream & stream,
                      std::function<bool (const char * line,
                                          size_t lineLength,
                                          int64_t blockNumber,
                                          int64_t lineNumber)> onLine,
                      int64_t maxLines,
                      int maxParallelism,
                      std::function<bool (int64_t blockNumber, int64_t lineNumber, uint64_t numLinesInBlock)> startBlock,
                      std::function<bool (int64_t blockNumber, int64_t lineNumber, uint64_t numLinesInBlock)> endBlock,
                      ssize_t defaultBlockSize,
                      const BlockSplitter & splitter)
{
    ForEachLineOptions options;
    options.maxParallelism = maxParallelism;
    options.defaultBlockSize = defaultBlockSize;
    options.startOffset = 0;
    options.skipLines = 0;
    options.maxLines = maxLines;
    options.splitter = &splitter;

    ForEachLineProcessor processor(options);
    return processor.process(stream, onLine, startBlock, endBlock);
}

/*****************************************************************************/
/* FOR EACH LINE BLOCK (CONTENT HANDLER)                                     */
/*****************************************************************************/

bool forEachLineBlock(std::shared_ptr<const ContentHandler> content,
                      uint64_t startOffset,
                      std::function<bool (const char * line,
                                          size_t lineLength,
                                          int64_t blockNumber,
                                          int64_t lineNumber)> onLine,
                      int64_t maxLines,
                      int maxParallelism,
                      std::function<bool (int64_t blockNumber,
                                          int64_t lineNumber,
                                          uint64_t numLinesInBlock)> startBlock,
                      std::function<bool (int64_t blockNumber,
                                          int64_t lineNumber,
                                          uint64_t numLinesInBlock)> endBlock,
                      size_t blockSize,
                      const BlockSplitter & splitter)
{
    ForEachLineOptions options;
    options.maxParallelism = maxParallelism;
    options.defaultBlockSize = blockSize;
    options.startOffset = startOffset;
    options.skipLines = 0;
    options.maxLines = maxLines;
    options.splitter = &splitter;

    ForEachLineProcessor processor(options);
    return processor.process(*content, onLine, startBlock, endBlock);
}


/*****************************************************************************/
/* FOR EACH CHUNK                                                            */
/*****************************************************************************/

void forEachChunk(std::istream & stream,
                  std::function<bool (const char * chunk,
                                      size_t chunkLength,
                                      int64_t chunkNumber)> onChunk,
                  size_t chunkLength,
                  int64_t maxChunks,
                  int maxParallelism)
{
    std::atomic<int> chunkNumber(0);

    ThreadPool tp(maxParallelism);

    std::atomic<int> stop(false);
    std::atomic<int> hasExc(false);
    std::exception_ptr exc;

    std::function<void ()> doBlock = [&] ()
        {
            try {
                std::shared_ptr<char> block(new char[chunkLength],
                                            [] (char * c) { delete[] c; });

                if (stop)
                    return;
                stream.read((char *)block.get(), chunkLength);

                if (stop)
                    return;
                // Check how many bytes we actually read
                size_t bytesRead = stream.gcount();
                
                if (bytesRead < chunkLength) {
                    ExcAssert(!stream || stream.eof());
                }

                int myChunkNumber = chunkNumber++;

                if (stream && !stream.eof() &&
                    (maxChunks == -1 || chunkNumber < maxChunks)) {
                    // Ready for another chunk
                    // After this, there could be a concurrent thread in this
                    // lambda
                    tp.add(doBlock);
                }

                if (!onChunk(block.get(), bytesRead, myChunkNumber)) {
                    // We decided to stop.  We should probably stop everything
                    // else, too.
                    stop = true;
                    return;
                }
            } MLDB_CATCH_ALL {
                if (hasExc.fetch_add(1) == 0) {
                    exc = std::current_exception();
                }
            }
        };
    
    tp.add(doBlock);
    tp.waitForAll();

    // If there was an exception, rethrow it rather than returning
    // cleanly
    if (hasExc) {
        std::rethrow_exception(exc);
    }
}

} // namespace MLDB
