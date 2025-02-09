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

        bool lastChunk = false;

        for (size_t chunkNumber = 0; stream; ++chunkNumber) {
            if (relaxed_stopped() || chunks_in_finished_ || chunks_out_finished_) return false;
            size_t chunkOffset = stream.tellg();
            std::shared_ptr<std::byte[]> block(new std::byte[blockSize]);
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
            blockSize = std::max<int64_t>(1000000, contentLength / thread_count());
            blockSize = std::min<int64_t>(blockSize, 20000000);
        }
        return blockSize;
    }

    // Returns true if and only if all continuations returned true. Requires that the span remain
    // valid for the lifetime of the containing for_each_line call.
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
            return handle_out_of_order_chunk({chunkNumber, chunkOffset, lastChunk, mem, region.steal_handle()});
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
        size_t numLeadingEmptyLines = 0;
        std::vector<std::string_view> lines;
        std::vector<std::shared_ptr<const void>> keep;
        size_t numTrailingEmptyLines = 0; // only for the last block
        size_t lineCount() const { return numLeadingEmptyLines + lines.size() + numTrailingEmptyLines; }

        void addLine(std::string_view line, std::shared_ptr<const void> keep = nullptr)
        {
            if (line.empty()) {
                // Empty line, just accumulate it
                ++numTrailingEmptyLines;
            }
            else {
                // Add the empty line block we accumulated so far as it's no longer trailing
                lines.resize(lines.size() + numTrailingEmptyLines);
                numTrailingEmptyLines = 0;
                lines.push_back(line);
            }
            if (keep)
                this->keep.emplace_back(std::move(keep));
        }

        // Truncate to the given number of lines
        void truncate(size_t maxLines)
        {
            if (lineCount() <= maxLines)
                return;
            if (maxLines <= numLeadingEmptyLines) {
                numLeadingEmptyLines = maxLines;
                lines.clear();
                numTrailingEmptyLines = 0;
                return;
            }
            maxLines -= numLeadingEmptyLines;
            if (maxLines <= lines.size()) {
                lines.resize(maxLines);
                numTrailingEmptyLines = 0;
                return;
            }
            maxLines -= lines.size();
            ExcAssertLessEqual(maxLines, numTrailingEmptyLines);
            numTrailingEmptyLines -= maxLines;
        }

        void pushEmptyLinesToTrailing()
        {
            if (lines.empty()) {
                numTrailingEmptyLines += numLeadingEmptyLines;
                numLeadingEmptyLines = 0;
            }
        }

        void removeTrailingEmptyLines()
        {
            numTrailingEmptyLines = 0;
            if (lines.empty())
                numLeadingEmptyLines = 0;
        }
    };

    // Information passed from one chunk to the next
    size_t outputChunkNumber = 0;
    size_t outputStartOffset = 0;
    size_t outputLineNumber = 0;
    CoalescedRange<const char> chunkData;
    std::any splitterState;
    std::vector<std::shared_ptr<const void>> keep;  // pins for each block in chunkData
    size_t numTrailingEmptyLines = 0;
    std::shared_ptr<char[]> consolidated_;
    size_t consolidated_size_ = 0;
    size_t consolidated_capacity_ = 0;
    size_t consolidated_allocate_ = 1024;

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

        // If our chunk size is much smaller than our line size, the algorithm becomes
        // inefficient. We need to consolidate chunks until we have a reasonable size.
        if (consolidated_ || chunkData.range_count() > 10) {
            if (!consolidated_ || consolidated_size_ + chunk.data.size() > consolidated_capacity_) {

                // Doesn't fit. Make consolidated bigger
                auto required_capacity = consolidated_size_ + chunk.data.size();
                if (required_capacity > consolidated_allocate_)
                    consolidated_allocate_ = std::max(required_capacity, consolidated_allocate_ * 2);
                std::shared_ptr<char[]> new_consolidated(new char[consolidated_allocate_]);
                std::copy(consolidated_.get(), consolidated_.get() + consolidated_size_, new_consolidated.get());
                consolidated_ = new_consolidated;
                consolidated_capacity_ = consolidated_allocate_;
                chunkData.clear();
                chunkData.add(consolidated_.get(), consolidated_size_);
                keep = { consolidated_ };
            }

            // This new chunk fits in the already-allocated capacity
            char * first = consolidated_.get() + consolidated_size_;
            std::copy((const char *)chunk.data.data(), (const char *)chunk.data.data() + chunk.data.size(), first);
            consolidated_size_ += chunk.data.size();
            chunkData.add({first, chunk.data.size()});
            ExcAssertEqual(chunkData.range_count(), 1);
        }
        else {
            // Add this chunk
            if (chunkData.add(reinterpret_cast<const char *>(chunk.data.data()), chunk.data.size()))
                keep.emplace_back(chunk.keep);
        }


        LineBlock lineBlock {
            .blockNumber = outputChunkNumber,
            .startLine = outputLineNumber,
            .startOffset = outputStartOffset,
            .lastBlock = chunk.lastChunk,
            .numLeadingEmptyLines = numTrailingEmptyLines,
            .keep = keep,
        };

        auto it = chunkData.begin(), end = chunkData.end();
        bool trailingSeparator = false;

        while (it != end) {
            auto [found_record, skip_to_end, skip_separator, next_state]
                = options.splitter->nextRecord(chunkData, it, chunk.lastChunk, splitterState);
            if (!found_record)
                break;
            auto next_it = it + skip_to_end;

            if (auto lineView = chunkData.get_string_view(it, next_it)) {
                // Push the contiguous line
                lineBlock.addLine(lineView.value());
            }
            else {
                // Line is not in a contiguous range; need to extract as a string
                size_t len = next_it - it;
                std::shared_ptr<char[]> contiguous(new char[len]);
                using std::copy;
                copy(it, next_it, contiguous.get());
                std::string_view line(contiguous.get(), len);
                lineBlock.addLine(line, std::move(contiguous));
            }

            splitterState = std::move(next_state);
            it = next_it + skip_separator;
            trailingSeparator = skip_separator > 0;
        }

        // If there is a trailing separator and this is the last block, we need to add an empty
        // line.
        if (trailingSeparator && chunk.lastChunk) {
            lineBlock.addLine({});
        }

        // The output block consists of:
        // - numLeadingEmptyLines empty lines from the previous block
        // - lines.size() lines from this block
        // - numTrailingEmptyLines empty lines from this block
        //
        // We output the first and the second in this block; the rest are the responsibility of
        // the next block (unless this is the last block).

        lineBlock.pushEmptyLinesToTrailing();

        if (chunk.lastChunk && !options.outputTrailingEmptyLines) {
            lineBlock.removeTrailingEmptyLines();
        }

        if (options.maxLines != -1 && outputLineNumber + lineBlock.lineCount() >= options.maxLines) {
            // We've reached the maximum number of lines
            lineBlock.truncate(options.maxLines - outputLineNumber);
            lineBlock.lastBlock = true;
            numTrailingEmptyLines = 0;
        }
        else if (!chunk.lastChunk) {
            // The next block deals with trailing empty lines
            numTrailingEmptyLines = lineBlock.numTrailingEmptyLines;
            lineBlock.numTrailingEmptyLines = 0;
        }

        // Reduce the chunk data to the leftover parts and remove the associated
        // data pins.

        ExcAssertEqual(keep.size(), chunkData.range_count());
        auto [firstChunk, lastChunk] = chunkData.reduce(it, end);
        keep.erase(keep.begin() + lastChunk, keep.end());
        keep.erase(keep.begin(), keep.begin() + firstChunk);

        ExcAssertEqual(keep.size(), chunkData.range_count());
        outputLineNumber += lineBlock.lineCount();
        outputStartOffset = chunk.startOffset + chunk.data.size();

        if (lineBlock.lineCount() > 0) {

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

            if (!single_threaded()) {
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
            consolidated_ = nullptr;
            consolidated_size_ = 0;
        }
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
                .numLinesInBlock = static_cast<int64_t>(block.lineCount()),
                .lastBlock       = block.lastBlock,
            };

            if (startBlock && !startBlock(blockInfo))
                return false;

            auto outputLine = [&] (std::string_view line, int64_t lineNumber, int64_t blockNumber, bool lastLine)
            {
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

                LineInfo lineInfo { line, lineNumber, blockNumber, lastLine };

                if (onLine && !onLine(lineInfo))
                    return false;

                return true;
            };

            size_t l = 0;

            // Leading empty lines
            for (size_t i = 0; i < block.numLeadingEmptyLines; ++i, ++l) {
                bool lastLine = block.lastBlock && i == block.numLeadingEmptyLines - 1 && block.lines.empty() && block.numTrailingEmptyLines == 0;
                if (!outputLine({}, block.startLine + l, block.blockNumber, lastLine))
                    return false;
            }

            // Block lines
            for (size_t i = 0; i < block.lines.size();  ++i, ++l) {
                bool lastLine = i == block.lines.size() - 1 && block.lastBlock && block.numTrailingEmptyLines == 0;
                if (!outputLine(block.lines[i], block.startLine + l, block.blockNumber, lastLine))
                    return false;
            }

            // Trailing empty lines
            for (size_t i = 0; i < block.numTrailingEmptyLines; ++i, ++l) {
                bool lastLine = block.lastBlock && i == block.numTrailingEmptyLines - 1;
                if (!outputLine({}, block.startLine + l, block.blockNumber, lastLine))
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

} // namespace MLDB
