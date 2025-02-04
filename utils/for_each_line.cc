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
#include "mldb/base/compute_context.h"

using namespace std;


namespace MLDB {


// First phase: blocks are processed in sequence, with each block creating
// a set of lines plus a state containing leftover data to pass to the
// next block.

namespace {

struct ForEachLineProcessor: public ComputeContext {

    ForEachLineProcessor(ForEachLineOptions options)
        : ComputeContext(options.maxParallelism), options(std::move(options))
    {
    }

    ForEachLineOptions options;

#if 0
    enum {
        CHUNK_STREAM_PRIORITY = 5,
    };
#endif

#if 0
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
#endif

    // A chunk of memory plus metadata
    struct Chunk {
        size_t chunkNumber = 0;
        size_t startOffset = 0;
        bool lastChunk = false;
        std::span<const std::byte> data;
        shared_ptr<const void> keep;      // Pins data in place
    };

    // Pushes a series of jobs that call onChunk
    // Returns true if and only if all continuations returned true
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
            if (relaxed_stopped() || chunks_in_finished_ || chunks_out_finished_) return false;
            size_t chunkOffset = stream.tellg();
            stream.read(reinterpret_cast<char *>(block.get()), blockSize + extraCapacity);
            ssize_t bytesRead = stream.gcount();
            if (bytesRead <= 0)
                break;
            if (!stream)
                lastChunk = true;
            std::span<const std::byte> mem(block.get(), bytesRead);
            if (!handle_out_of_order_chunk({chunkNumber, chunkOffset, lastChunk, mem, {block}}))
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

    // Returns true if and only if all continuations returned true
    bool chunk_source(std::span<const std::byte> mem, size_t startOffset)
    {
        size_t numBytes = mem.size();
        size_t blockSize = block_size_for_content_length(numBytes);

        const std::byte * p = mem.data();
        const std::byte * e = p + mem.size();

        for (size_t chunkNumber = 0; p < e; p += blockSize, ++chunkNumber) {
            if (relaxed_stopped() || chunks_in_finished_ || chunks_out_finished_) return false;
            const std::byte * eb = std::min(e, p + blockSize);
            std::span<const std::byte> mem(p, eb-p);
            size_t chunkOffset = startOffset + p - mem.data();
            bool lastChunk = eb == e;
            if (!handle_out_of_order_chunk({chunkNumber, chunkOffset, lastChunk, mem, {}}))
                return false;
        }
        return true;
    }

    bool chunk_source(const ContentHandler & content)
    {
        size_t contentSize = content.getSize();
        size_t blockSize = block_size_for_content_length(contentSize);

        auto doBlock = [&] (size_t chunkNumber, uint64_t chunkOffset, FrozenMemoryRegion region, bool lastBlock)
        {
            if (relaxed_stopped() || chunks_in_finished_ || chunks_out_finished_) return false;
            bool lastChunk = lastBlock || chunkOffset + region.length() == contentSize;
            std::span<const std::byte> mem(reinterpret_cast<const std::byte *>(region.data()), region.length());
            return handle_out_of_order_chunk({chunkNumber, chunkOffset, lastChunk, mem, {}});
        };
    
        auto res = content.forEachBlockParallel(options.startOffset, blockSize, *this /*compute*/, 5 /* priority */, doBlock);
        return res;
    }

    bool handle_out_of_order_chunk(Chunk chunk)
    {
        // The chunks need to be processed in order by handle_next_chunk
        // This function is called from multiple threads, so we need to reassemble and sychronize them.
        //
        // Basically:
        // - Take a lock on the queue
        // - Add it to the queue
        // - For as long as the chunk at the beginning of the queue is the next in-order one
        //   - drop the lock
        //   - process it (only we will do se because no other thread will get the next in-order chunk)
        //   - grab the lock again
        // - Otherwise
        // Note that this is the ONLY function which touches the queue. No other functions should touch it.

        std::unique_lock guard{chunks_mutex_};
        auto [it, inserted] = chunks_.emplace(chunk.chunkNumber, std::move(chunk));
        ExcAssert(inserted); // Failure means that chunk numbers are repeated by the upstream functions

        while (!chunks_.empty() && it->first == chunks_done_) {
            Chunk & chunk = it->second;

            guard.unlock();
            handle_next_chunk(it->second);
            guard.lock();

            // Nothing should have happened in the meantime...
            ExcAssertEqual(chunks_done_, chunk.chunkNumber);

            if (chunk.lastChunk || (options.maxLines != -1 && outputLineNumber >= options.maxLines)) {
                chunks_in_finished_ = true;
                chunks_out_finished_ = true;
                chunks_.clear();
                break;
            }
            chunks_.erase(it);
            ++chunks_done_;
            it = chunks_.begin();
        }
        guard.unlock();

        // At this point, we have processed all the chunks we can
        // TODO: if the list of chunks is too big, start slowing down the chunk producers by making them
        // contribute to work

        // TODO: uncommenting this and running with ASAN uncovers a bug in the ThreadPool whereby one
        // thread trying to steal work from another has its data freed. We should deal with that bug!
        // nice make -j16 -k SANITIZERS=address for_each_line_test
        //work();

        return !relaxed_stopped() && !chunks_out_finished_;

#if 0
        // Slow down the production of chunks if there are too many by contributing to
        // other work.
        while (numChunksOutstanding > numCpus() && !relaxed_stopped() && !chunks_out_finished_) {
            {
                // Make sure there is no data race on chunks_out_finished_
                // We could remove this lock if it becomes a problem by using the right sequencing
                std::unique_lock guard{chunks_mutex_};
                if (chunks_out_finished_)
                    break;
            }

            work();

            std::unique_lock guard{chunks_mutex_};
            numChunksOutstanding = chunks_.size();
            if (!chunks_.empty() && chunks_.begin()->first == chunks_done_) {
                if (chunks_.begin()->second.lastChunk)
                    chunks_in_finished_ = true;
                guard.unlock();
                if (!handle_next_chunk(chunks_.begin()->second))
                    return false;
            }
        }

        return true;
#endif
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

    // Handle the next chunk, which will be done in-order and one at a time. This should do the
    // smallest amount of work possible, submitting any further processing as a job to be
    // handled by another thread (if there is one).
    void handle_next_chunk(Chunk chunk)
    {
        if (chunks_out_finished_ || relaxed_stopped())
            return;

        // TODO: If there are too many chunks, then consolidate them
        // (needs careful thought as to how...)

        // Precondition: chunks_done_ is equal to this chunk number
        // Only one thread in this block at a time, which is staisfied by the condition above
        ExcAssertEqual(chunks_done_, chunk.chunkNumber);
        ExcAssertEqual(chunkData.range_count(), keep.size());

        if (chunkData.add(reinterpret_cast<const char *>(chunk.data.data()), chunk.data.size()))
            keep.emplace_back(chunk.keep);

        LineBlock lineBlock;
        lineBlock.keep = keep;
        lineBlock.blockNumber = outputChunkNumber;
        lineBlock.startLine = outputLineNumber;
        lineBlock.startOffset = outputStartOffset;
        lineBlock.lastBlock = chunk.lastChunk;

        auto & lines = lineBlock.lines;

        auto it = chunkData.begin(), end = chunkData.end();
        bool trailingSeparator = false;

        while (it != end) {
            auto [found_record, skip_to_end, skip_separator, next_state]
                = options.splitter->nextRecord(chunkData, it, chunk.lastChunk, splitterState);
            if (!found_record)
                break;
            auto next_it = it + skip_to_end;

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
            it = next_it + skip_separator;
            trailingSeparator = skip_separator > 0;
        }

        if (options.outputTrailingEmptyLine && trailingSeparator && chunk.lastChunk) {
            // We have a trailing separator at the end of the last chunk
            // We need to add an empty line
            lines.push_back({});
        }

        size_t lineCount = lines.size();

        if (options.maxLines != -1 && lineCount + outputLineNumber >= options.maxLines) {
            // We've reached the maximum number of lines
            lines.resize(options.maxLines - outputLineNumber);
            lineBlock.lastBlock = true;
        }

        // Reduce the chunk data to the leftover parts and remove the associated
        // data pins.

        ExcAssertEqual(keep.size(), chunkData.range_count());
        auto [firstChunk, lastChunk] = chunkData.reduce(it, end);
        keep.erase(keep.begin() + lastChunk, keep.end());
        keep.erase(keep.begin(), keep.begin() + firstChunk);

        ExcAssertEqual(keep.size(), chunkData.range_count());

        if (lineCount > 0) {
            // Submit the line block for processing before we schedule another chunk so
            // we don't get ahead of ourselves.
            int priority = 1;

            std::function<void ()> job = [lineBlock=std::move(lineBlock),this] ()
            {
                if (this->relaxed_stopped())
                    return;
                if (!this->handle_line_block(std::move(lineBlock)))
                    this->stop(ComputeContext::STOPPED_USER);
            };

            if (options.maxParallelism > 0) {
                submit(priority, "process_line_block " + std::to_string(chunk.chunkNumber), std::move(job));
            }
            else {
                // We're not processing in parallel, so there is no other thread
                try {
                    job();
                } MLDB_CATCH_ALL {
                    take_exception("handle_line_block");
                }
            }
            ++outputChunkNumber;
        }
        outputLineNumber += lineCount;
        outputStartOffset = chunk.startOffset + chunk.data.size();

    }

    std::function<bool (LineBlock)> handle_line_block;

    template<typename Source>
    bool process(Source&& source,
                 const LineContinuationFn & onLine,
                 const BlockContinuationFn & startBlock,
                 const BlockContinuationFn & endBlock)
    {
        std::atomic<size_t> done = 0;
        Date start = Date::now(), lastCheck = start;

        handle_line_block = [&] (LineBlock block)
        {
            LineBlockInfo blockInfo {
                .blockNumber     = static_cast<int64_t>(block.blockNumber),
                .startOffset     = static_cast<int64_t>(block.startOffset),
                .lineNumber      = static_cast<int64_t>(block.startLine),
                .numLinesInBlock = static_cast<int64_t>(block.lines.size()),
                .lastBlock       = block.lastBlock,
            };

            if (startBlock && !startBlock(blockInfo))
                return false;

            for (size_t i = 0; i < block.lines.size();  ++i) {
                if (options.logger) {
                    auto new_done = done.fetch_add(1) + 1;

                    if (new_done % options.logInterval == 0 && 
                        options.logger->should_log(spdlog::level::info)) {

                        options.logger->info() << "done " << new_done << " lines";
                        Date now = Date::now();

                        double elapsed = now.secondsSince(start);
                        double instElapsed = now.secondsSince(lastCheck);
                        options.logger->info() << MLDB::format("doing %.3fMlines/second total, %.3f instantaneous",
                                                    done / elapsed / 1000000.0,
                                                    options.logInterval / instElapsed / 1000000.0);
                        lastCheck = now;
                    }
                }

                LineInfo lineInfo {
                    .line         = { block.lines[i].data(), block.lines[i].size() },
                    .lineNumber   = static_cast<int64_t>(block.startLine + i),
                    .blockNumber  = static_cast<int64_t>(block.blockNumber),
                    .lastLine     = block.lastBlock && i == block.lines.size() - 1,
                };

                if (onLine && !onLine(lineInfo))
                    return false;
            }

            if (endBlock && !endBlock(blockInfo))
                return false;

            return true;
        };

        bool res = chunk_source(source);
        work_until_finished();
        rethrow_if_exception();
        return res && !is_stopped();


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
    std::atomic<bool> chunks_in_finished_ = false;
    std::atomic<bool> chunks_out_finished_ = false;
};

} // file scope

size_t
forEachLine(std::istream & stream,
            const LineContinuationFn & processLine,
            const ForEachLineOptions & options)
{
    ForEachLineProcessor processor(options);
    return processor.process(stream, processLine, nullptr /* startBlock */, nullptr /* endBlock */);
}


/*****************************************************************************/
/* FOR EACH LINE BLOCK (ISTREAM)                                             */
/*****************************************************************************/

bool forEachLineBlock(std::istream & stream,
                      const LineContinuationFn & onLine,
                      const BlockContinuationFn & startBlock,
                      const BlockContinuationFn & endBlock,
                      const ForEachLineOptions & options)
{
    ForEachLineProcessor processor(options);
    return processor.process(stream, onLine, startBlock, endBlock);
}


/*****************************************************************************/
/* FOR EACH LINE BLOCK (CONTENT HANDLER)                                     */
/*****************************************************************************/

bool forEachLineBlock(std::shared_ptr<const ContentHandler> content,
                      const LineContinuationFn & onLine,
                      const BlockContinuationFn & startBlock,
                      const BlockContinuationFn & endBlock,
                      const ForEachLineOptions & options)
{
    ForEachLineProcessor processor(options);
    return processor.process(*content, onLine, startBlock, endBlock);
}

#if 0
/*****************************************************************************/
/* FOR EACH CHUNK                                                            */
/*****************************************************************************/

void forEachChunk(std::istream & stream,
                  const ChunkContinuationFn & onChunk,
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

                MLDB::ChunkInfo chunkInfo {
                    .data = std::span<const char>(block.get(), bytesRead),
                    .chunkNumber = static_cast<int64_t>(myChunkNumber),
                    .startOffset = static_cast<int64_t>(myChunkNumber * chunkLength),
                    .lastChunk = !stream || stream.eof(),
                };

                if (!onChunk(chunkInfo)) {
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
#endif

} // namespace MLDB
