/** for_each_line.cc
    Jeremy Barnes, 29 November 2013
    Copyright (c) 2013 mldb.ai inc.  All rights reserved.
    This file is part of MLDB. Copyright 2015 mldb.ai inc. All rights reserved.

*/

#include "for_each_line.h"
#include <atomic>
#include <exception>
#include <mutex>
#include "mldb/arch/threads.h"
#include <chrono>
#include <thread>
#include <cstring>
#include "mldb/ext/concurrentqueue/blockingconcurrentqueue.h"
#include "mldb/vfs/filter_streams.h"
#include "mldb/base/thread_pool.h"
#include "mldb/base/exc_assert.h"
#include "mldb/types/date.h"
#include "mldb/utils/log.h"
#include "mldb/base/hex_dump.h"
#include "mldb/block/content_descriptor.h"
#include "mldb/arch/spinlock.h"

using namespace std;
using moodycamel::BlockingConcurrentQueue;


namespace {

struct Processing {

    Processing()
        : shutdown(false), hasException_(false),
          excPtr(nullptr), decompressedLines(16000)
    {
    }

    void takeLastException()
    {
        std::lock_guard<std::mutex> guard(excPtrLock);
        excPtr = std::current_exception();
        hasException_ = true;
    }

    bool hasException()
    {
        return hasException_;
    }

    atomic<bool> shutdown;
    atomic<bool> hasException_;
    std::mutex excPtrLock;
    std::exception_ptr excPtr;

    BlockingConcurrentQueue<pair<int64_t, vector<string> > > decompressedLines;
};

} // file scope

namespace MLDB {


/*****************************************************************************/
/* PARALLEL LINE PROCESSOR                                                   */
/*****************************************************************************/

static size_t
readStream(std::istream & stream,
           Processing & processing,
           shared_ptr<spdlog::logger> logger,
           bool ignoreStreamExceptions,
           int64_t maxLines)
{
    Date start = Date::now();

    pair<int64_t, vector<string> > current;
    current.second.reserve(1000);

    Date lastCheck = start;

    int64_t done = current.first = 0;  // 32 bit not enough

    try {
        while (stream && !stream.eof() && (maxLines == -1 || done < maxLines)) {

            if (processing.hasException()) {
                break;
            }
            string line;
            getline(stream, line);
            current.second.emplace_back(std::move(line));

            ++done;

            if (current.second.size() == 1000) {
                processing.decompressedLines.enqueue(std::move(current));
                current.first = done;
                current.second.clear();
                //current.clear();
                ExcAssertEqual(current.second.size(), 0);
                current.second.reserve(1000);
            }

            if (done % 1000000 == 0 && 
                logger->should_log(spdlog::level::info)) {

                logger->info() << "done " << done << " lines";
                Date now = Date::now();

                double elapsed = now.secondsSince(start);
                double instElapsed = now.secondsSince(lastCheck);
                logger->info() << MLDB::format("doing %.3fMlines/second total, %.3f instantaneous",
                                               done / elapsed / 1000000.0,
                                               1000000 / instElapsed / 1000000.0);
                lastCheck = now;
            }
        }
    } catch (const std::exception & exc) {
        if (!ignoreStreamExceptions) {
            processing.takeLastException();
        }
        else {
            WARNING_MSG(logger) << "stream threw ignored exception: " << exc.what();
        }
    }

    if (!current.second.empty() && !processing.hasException()) {
        processing.decompressedLines.enqueue(std::move(current));
    }

    return done;
};

static void
parseLinesThreadStr(Processing & processing,
                    const std::function<void (const std::string &,
                                              int64_t lineNum)> & processLine,
                    shared_ptr<spdlog::logger> logger)
{
    while (!processing.hasException()) {
        std::pair<int64_t, vector<string> > lines;
        if (!processing.decompressedLines.wait_dequeue_timed(lines, 1000 /* us */)) {
            if (processing.shutdown) {
                break;
            }
            continue;
        }
            
        for (unsigned i = 0;  i < lines.second.size();  ++i) {
            if (processing.hasException()) {
                break;
            }
            const string & line = lines.second[i];
            try {
                processLine(line, lines.first + i);
            } catch (const std::exception & exc) {
                processing.takeLastException();
                WARNING_MSG(logger) << "error dealing with line " << line
                                    << ": " << exc.what();
            } catch (...) {
                processing.takeLastException();
            }
        }
    }
};

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
    Processing processing;

    std::vector<std::thread> threads;
    for (unsigned i = 0;  i < numThreads;  ++i)
        threads.emplace_back(std::bind(parseLinesThreadStr,
                                       std::ref(processing),
                                       std::ref(processLine),
                                       logger));
        
    size_t result = readStream(stream, processing, logger,
                               ignoreStreamExceptions, maxLines);
        
    processing.shutdown = true;

    for (auto & t: threads)
        t.join();

    if (processing.hasException()) {
        std::rethrow_exception(processing.excPtr);
    }

    return result;
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

void forEachLineBlock(std::istream & stream,
                      std::function<bool (const char * line,
                                          size_t lineLength,
                                          int64_t blockNumber,
                                          int64_t lineNumber)> onLine,
                      int64_t maxLines,
                      int maxParallelism,
                      std::function<bool (int64_t blockNumber, int64_t lineNumber)> startBlock,
                      std::function<bool (int64_t blockNumber, int64_t lineNumber)> endBlock)
{
    //static constexpr int64_t BLOCK_SIZE = 100000000;  // 100MB blocks
    static constexpr int64_t BLOCK_SIZE = 20000000;  // 20MB blocks
    static constexpr int64_t READ_SIZE = 200000;  // read&scan 200kb to fit in cache

    std::atomic<int64_t> doneLines(0); //number of lines processed but not yet returned
    std::atomic<int64_t> returnedLines(0); //number of lines returned
    std::atomic<int64_t> byteOffset(0);
    std::atomic<int> chunkNumber(0);

    ThreadPool tp(ThreadPool::instance(), maxParallelism);

    // Memory map if possible
    const char * mapped = nullptr;
    size_t mappedSize = 0;

    filter_istream * fistream = dynamic_cast<filter_istream *>(&stream);

    if (fistream) {
        // Can we get a memory mapped version of our stream?  It
        // saves us having to copy data.  mapped will be set to
        // nullptr if it's not possible to memory map this stream.
        std::tie(mapped, mappedSize) = fistream->mapped();
    }

    //cerr << "mapped = " << (void *)mapped << endl;
    //cerr << "mappedSize = " << mappedSize << endl;
    //hex_dump(mapped, mappedSize, mappedSize);
    
    std::atomic<int> hasExc(false);
    std::exception_ptr exc;

    std::function<void ()> doBlock = [&] ()
        {
            std::shared_ptr<const char> blockOut;

            int64_t startOffset = byteOffset;
            int64_t startLine = doneLines;
            vector<size_t> lineOffsets = {0};
            bool lastBlock = false;
            size_t myChunkNumber = 0;

            
            try {
                //MLDB-1426
                if (mapped) {
                    std::streamsize pos = stream.tellg();
                    if (mappedSize == 0 || pos == EOF)
                        return;  // EOF, so nothing to read... probably empty
                    const char * start = mapped + pos;
                    const char * current = start;
                    const char * end = mapped + mappedSize;

                    while (current && current < end && (current - start) < BLOCK_SIZE
                           && (maxLines == -1 || doneLines < maxLines)) { //stop processing new line when we have enough)
                        current = (const char *)memchr(current, '\n', end - current);
                        if (current && current < end) {
                            ExcAssertEqual(*current, '\n');
                            lineOffsets.push_back(current - start);
                            ++doneLines;
                            ++current;
                        }
                    }
                
                    if (current)
                        stream.seekg(current - start, ios::cur);
                    else {
                        // Last line has no newline
                        lineOffsets.push_back(end - start);
                        ++doneLines;
                    }
                    
                    myChunkNumber = chunkNumber++;

                    if (current && current < end &&
                        (maxLines == -1 || doneLines < maxLines)) // don't schedule a new block if we have enough lines
                        {
                            // Ready for another chunk
                            tp.add(doBlock);
                        } else if (current == end) {
                        lastBlock = true;
                    }

                    blockOut = std::shared_ptr<const char>(start,
                                                           [] (const char *) {});
                }
                else {
                    // How far through our block are we?
                    size_t offset = 0;

                    // How much extra space to allocate for the last line?
                    static constexpr size_t EXTRA_SIZE = 10000;

                    std::shared_ptr<char> block(new char[BLOCK_SIZE + EXTRA_SIZE],
                                                [] (char * c) { delete[] c; });
                    blockOut = block;

                    // First line starts at offset 0

                    while (stream && !stream.eof()
                           && (maxLines == -1 || doneLines < maxLines)  //stop processing new line when we have enough
                           && (byteOffset - startOffset < BLOCK_SIZE)) {
                        
                        stream.read((char *)block.get() + offset,
                                    std::min<size_t>(READ_SIZE, BLOCK_SIZE - offset));

                        // Check how many bytes we actually read
                        size_t bytesRead = stream.gcount();
                        
                        offset += bytesRead;

                        // Scan for end of line characters
                        const char * current = block.get() + lineOffsets.back();
                        const char * end = block.get() + offset;

                        while (current && current < end) {
                            current = (const char *)memchr(current, '\n', end - current);
                            if (current && current < end) {
                                ExcAssertEqual(*current, '\n');
                                if (lineOffsets.back() != current - block.get()) {
                                    lineOffsets.push_back(current - block.get());
                                    ++doneLines;
                                }
                                ++current;
                            }
                        }

                        byteOffset += bytesRead;
                    }

                
                    if (stream.eof()) {
                        // If we are at the end of the stream
                        // make sure we include the last line 
                        // if there was no newline
                        if (lineOffsets.back() != offset - 1) {
                            lineOffsets.push_back(offset);
                            ++doneLines;
                        }
                    }
                    else {
                        // If we are not at the end of the stream
                        // get the last line, as we probably got just a partial
                        // line in the last one
                        std::string lastLine;
                        getline(stream, lastLine);
                
                        if (!lastLine.empty()) {
                            // Check for overflow on the buffer size
                            if (offset + lastLine.size() + 1 > BLOCK_SIZE + EXTRA_SIZE) {
                                // reallocate and copy
                                std::shared_ptr<char> newBlock(new char[offset + lastLine.size() + 1],
                                                               [] (char * c) { delete[] c; });
                                std::copy(block.get(), block.get() + offset,
                                          newBlock.get());
                                block = newBlock;
                                blockOut = block;
                            }

                            std::copy(lastLine.data(), lastLine.data() + lastLine.length(),
                                      block.get() + offset);
                    
                            lineOffsets.emplace_back(offset + lastLine.length());
                            ++doneLines;
                            offset += lastLine.size() + 1;
                        }                
                    }

                    myChunkNumber = chunkNumber++;

                    if (stream && !stream.eof() &&
                        (maxLines == -1 || doneLines < maxLines)) // don't schedule a new block if we have enough lines
                        {
                            // Ready for another chunk
                            tp.add(doBlock);
                        } else if (stream.eof()) {
                        lastBlock = true;
                    }
                }
                    
                int64_t chunkLineNumber = startLine;
                size_t lastLineOffset = lineOffsets[0];

                if (startBlock)
                    if (!startBlock(myChunkNumber, chunkLineNumber))
                        return;

                for (unsigned i = 1;  i < lineOffsets.size() && (maxLines == -1 || returnedLines++ < maxLines);  ++i) {
                    if (hasExc.load(std::memory_order_relaxed))
                        return;
                    const char * line = blockOut.get() + lastLineOffset;
                    size_t len = lineOffsets[i] - lastLineOffset;

                    // Skip \r for DOS line endings
                    if (len > 0 && line[len - 1] == '\r')
                        --len;

                    // if we are not at the last line
                    if (!lastBlock || len != 0 || i != lineOffsets.size() - 1)
                        if (!onLine(line, len, chunkNumber, chunkLineNumber++))
                            return;
                
                    lastLineOffset = lineOffsets[i] + 1;

                }

                if (endBlock)
                    if (!endBlock(myChunkNumber, chunkLineNumber))
                        return;

            } MLDB_CATCH_ALL {
                if (hasExc.fetch_add(1) == 0) {
                    exc = std::current_exception();
                }
            }
        };

    // Run the first block, which will enqueue the second before exiting
    doBlock();

    // Wait for all to be done
    tp.waitForAll();

    // If there was an exception, rethrow it rather than returning
    // cleanly
    if (hasExc) {
        std::rethrow_exception(exc);
    }
}

/*****************************************************************************/
/* FOR EACH LINE BLOCK (CONTENT HANDLER)                                     */
/*****************************************************************************/

void forEachLineBlock(std::shared_ptr<const ContentHandler> content,
                      uint64_t startOffset,
                      std::function<bool (const char * line,
                                          size_t lineLength,
                                          int64_t blockNumber,
                                          int64_t lineNumber)> onLine,
                      int64_t maxLines,
                      int maxParallelism,
                      std::function<bool (int64_t blockNumber,
                                          int64_t lineNumber,
                                          uint64_t numLines)> startBlock,
                      std::function<bool (int64_t blockNumber,
                                          int64_t lineNumber)> endBlock,
                      size_t blockSize)
{
    std::atomic<int> hasExc(false);
    std::exception_ptr exc;

    /// This is what we pass to the next block once we've finished scanning for
    /// line breaks.
    struct PassToNextBlock {
        FrozenMemoryRegion leftoverFromPreviousBlock;
        uint64_t doneLines = 0;
        bool bail = false;  ///< Should we bail out (stop) immediately?
    };

    // Set of queues, one per block, that grows with the amount of data
    // TODO: make this a deque so we can remove early entries
    Spinlock queuesMutex;
    std::vector<std::shared_ptr<BlockingConcurrentQueue<PassToNextBlock> > > queues;
    queues.reserve(1024);
    queues.emplace_back(new BlockingConcurrentQueue<PassToNextBlock>());
    
    auto getQueues = [&] (size_t blockNumber)
        -> std::pair<std::shared_ptr<BlockingConcurrentQueue<PassToNextBlock> >,
                     std::shared_ptr<BlockingConcurrentQueue<PassToNextBlock> > >
        {
            std::unique_lock<Spinlock> guard(queuesMutex);
            while (blockNumber + 1 >= queues.size())
                queues.emplace_back(new BlockingConcurrentQueue<PassToNextBlock>());
            return { queues[blockNumber], queues[blockNumber + 1] };
        };
    
    
    auto doBlock
        = [&hasExc,&exc,maxLines,&onLine,&startBlock,&endBlock,&getQueues]
        (int chunkNumber,
         uint64_t offset,
         FrozenMemoryRegion mem) -> bool
        {
            //cerr << "chunk " << chunkNumber << " with " << mem.length() << " bytes" << endl;
            
            // Contains the full first line of our block, which is made up
            // of whatever was leftover from the previous block plus
            // our current line
            FrozenMemoryRegion firstLine;

            // Contains the (partial) last line of our block
            FrozenMemoryRegion partialLastLine;
            
            // Offset in otherLines of line start characters
            vector<size_t> lineOffsets;
            
            // What we got from the last block
            PassToNextBlock fromPrev;

            std::shared_ptr<BlockingConcurrentQueue<PassToNextBlock> >
                fromPrevQueue, toNextQueue;
            std::tie(fromPrevQueue, toNextQueue) = getQueues(chunkNumber);
            
            // Call this to tell the next block that it should bail out.  It's
            // safe to call at any time, including before the next block has
            // been launched and after the next block has already been told to
            // do something else.
            auto bailNextBlock = [&] () {
                PassToNextBlock toNext;
                toNext.bail = true;
                toNextQueue->enqueue(std::move(toNext));
            };

            try {
                //cerr << endl << endl
                //     << "------------- starting block " << chunkNumber
                //     << " at offset "
                //     << offset << endl;
                //cerr << "with " << leftoverFromPreviousBlock.length()
                //     << " characters leftover" << endl;
                //hex_dump(leftoverFromPreviousBlock.data(),
                //         leftoverFromPreviousBlock.length());
                //cerr << "getting block at offset " << offset
                //     << " with " << leftoverFromPreviousBlock.length()
                //     << " bytes left over" << endl;

                //Date start = Date::now();
                
                //Date gotData = Date::now();

                //double elapsed = gotData.secondsSince(start);
                
                //cerr << "  chunk " << chunkNumber << " got "
                //     << mem.length() << " bytes in " << elapsed << " seconds"
                //     << " at " << mem.length() / elapsed / 1000000 << " MB/s"
                //     << endl;
                
                //cerr << "got " << mem.length() << " bytes at "
                //     << (void *)mem.data() << endl;

                //hex_dump(mem.data(), mem.length());
                    
                size_t length = mem.length();
                const char * start = mem.data();
                const char * current = (const char *)memchr(start, '\n', length);
                const char * end = start + length;
                size_t numLinesInBlock = 0;
                
                //cerr << "start = " << (void *)start
                //     << " current = " << (void *)current
                //     << " end = " << (void *)end
                //     << endl;

                bool noBreakInChunk = false;
                ssize_t charsUntilFirstLineBreak = -1;
                if (!current) {
                    noBreakInChunk = true;
                }
                else {
                    charsUntilFirstLineBreak = current - start;
                    //cerr << "firstLine is " << endl;
                    //hex_dump(firstLine.data(), firstLine.length());

                    ++current;
                    ++numLinesInBlock;
                    //cerr << "numLinesInBlock incremented for first line" << endl;
                        
                    // Second line starts here; record the start
                    lineOffsets.push_back(current - start);
                        
                    const char * lastLineStart = current;
                        
                    //cerr << "start = " << (void *)start
                    //     << " current = " << (void *)current
                    //     << " end = " << (void *)end
                    //     << endl;
                        
                    while (current && current < end) {
                        // Bail out on exception
                        if (numLinesInBlock % 256 == 0
                            && hasExc.load(std::memory_order_relaxed)) {
                            bailNextBlock();
                            return false;
                        }

                        current = (const char *)memchr(current, '\n', end - current);

                        //cerr << " current now = " << (void *)current << endl;

                        if (current)
                            lastLineStart = current + 1;
                            
                        if (current && current < end) {
                            ExcAssertEqual(*current, '\n');
                            lineOffsets.push_back(current - start);
                            ++numLinesInBlock;
                            //cerr << "doneLines incremented for other line"
                            //     << endl;
                            ++current;
                        }
                    }
                    
                    // Whatever is left over is the last line, which we pass
                    // through to the next block
                    if (!current && mem) {
                        //cerr << "lastLineStart - start = "
                        //     << lastLineStart - start << endl;
                        //cerr << "mem.length() = " << mem.length() << endl;
                        partialLastLine = mem.range(lastLineStart - start,
                                                    mem.length());
                        //cerr << "partial last line" << endl;
                    }
                }

                if (hasExc.load(std::memory_order_relaxed)) {
                    bailNextBlock();
                    return false;
                }
                    
                fromPrevQueue->wait_dequeue(fromPrev);

                // Do we bail out?  If our previous block says it has bailed,
                // then we should too.
                if (fromPrev.bail) {
                    bailNextBlock();
                    return false;
                }

                // Now we have information from the previous block, we can reconstruct
                // our first line and know our real line numbers
                FrozenMemoryRegion leftoverFromPreviousBlock
                    = std::move(fromPrev.leftoverFromPreviousBlock);
                int64_t startLine = fromPrev.doneLines;
                uint64_t doneLines = startLine + numLinesInBlock;
                    
                if (noBreakInChunk && mem) {
                    // No line break in the whole chunk; it's all a partial
                    // last line
                    partialLastLine
                        = FrozenMemoryRegion::combined
                        (leftoverFromPreviousBlock,
                         mem);
                }
                else if (mem) {
                    firstLine
                        = FrozenMemoryRegion::combined
                        (leftoverFromPreviousBlock,
                         mem.range(0, charsUntilFirstLineBreak));
                }
                else {
                    firstLine = std::move(leftoverFromPreviousBlock);
                    if (firstLine.length() == 0)
                        return false;
                }

                if (maxLines == -1 || doneLines < maxLines) {

                    //cerr << "sending on partial last line with "
                    //     << partialLastLine.length() << " characters"
                    //     << endl;
                    //hex_dump(partialLastLine.data(),
                    //         partialLastLine.length());


                    // What we pass on to the next block
                    PassToNextBlock toNext;
                    toNext.leftoverFromPreviousBlock
                        = std::move(partialLastLine);
                    toNext.doneLines = doneLines;
                    toNextQueue->enqueue(std::move(toNext));
                }
                else {
                    bailNextBlock();
                }
                    
                int64_t chunkLineNumber = startLine;
                size_t numLines = (firstLine ? 1 : 0)
                                + (lineOffsets.empty() ? 0 : lineOffsets.size() - 1);
                
                auto doLine = [&] (const char * line, size_t len)
                    {
                        // Skip \r for DOS line endings
                        if (len > 0 && line[len - 1] == '\r')
                            --len;

                        return onLine(line, len, chunkNumber, chunkLineNumber++);
                    };
            
                if (startBlock)
                    if (!startBlock(chunkNumber, chunkLineNumber, numLines))
                        return false;

                auto returnedLines = startLine;
                
                if (firstLine) {
                    //cerr << "doing first line" << endl;
                    if (maxLines == -1 || returnedLines++ < maxLines) {
                        if (!doLine(firstLine.data(), firstLine.length())) {
                            return false;
                        }
                    }
                }
                
                if (!lineOffsets.empty()) {
                    size_t lastLineOffset = lineOffsets[0];

                    for (unsigned i = 1;
                         i < lineOffsets.size()
                             && (maxLines == -1 || returnedLines++ < maxLines);
                         ++i) {

                        // Check for exception bailout every 256 lines
                        if (i % 256 == 0
                            && hasExc.load(std::memory_order_relaxed))
                            return false;

                        const char * line = mem.data() + lastLineOffset;
                        size_t len = lineOffsets[i] - lastLineOffset;

                        //cerr << "doing other line " << i << " with "
                        //     << len << " chars" << endl;
                        
                        if (!doLine(line, len))
                            return false;
                        
                        lastLineOffset = lineOffsets[i] + 1;  // skip \n
                    }
                }
                    
                if (endBlock)
                    if (!endBlock(chunkNumber, chunkLineNumber))
                        return false;
                
            } MLDB_CATCH_ALL {
                //cerr << "got exception in chunk " << myChunkNumber
                //<< " " << getExceptionString() << endl;
                if (hasExc.fetch_add(1) == 0) {
                    exc = std::current_exception();
                }

                // If the next block is waiting for instructions, tell it
                // to bail out.
                bailNextBlock();
                return false;
            }

            return true;
        };

    // Unblock the first block by writing to it
    PassToNextBlock pass;
    pass.doneLines = 0;
    queues[0]->enqueue(std::move(pass));
    
    content->forEachBlockParallel(startOffset, blockSize, maxParallelism, doBlock);

    // last chunk with single last line is in the last queue entry
    if (!hasExc && !queues.empty()) {
        cerr << "last one; queues.size() = " << queues.size() << endl;
        size_t chunkNumber = queues.size() - 1;
        doBlock(chunkNumber, -1 /* offset */, FrozenMemoryRegion());
    
#if 0
        if (!queues.back()->try_dequeue(pass)) {
            throw Exception("Queue issues");
        }
        cerr << "total doneLines = " << pass.doneLines << endl;
        if (pass.leftoverFromPreviousBlock) {

            if (!startBlock || startBlock(chunkNumber, pass.doneLines, 1 /* num in block*/)) {
                const char * line = pass.leftoverFromPreviousBlock.data();
                size_t len = pass.leftoverFromPreviouBlock.length();

                // Skip \r for DOS line endings
                if (len > 0 && line[len - 1] == '\r')
                    --len;

                if (onLine(line, len, chunkNumber, pass.doneLines)) {
                    if (endBlock) {
                        endBlock(chunkNumber, pass.doneLines + 1);
                    }
                }
            }
        }
#endif
    }
    
    // If there was an exception, rethrow it rather than returning
    // cleanly
    if (hasExc) {
        std::rethrow_exception(exc);
    }
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
