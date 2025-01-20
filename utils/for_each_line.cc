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

using namespace std;
using moodycamel::BlockingConcurrentQueue;


namespace MLDB {

const NewlineSplitter newLineSplitter;

std::span<const char>
BlockSplitter::
fixupBlock(std::span<const char> block) const
{
    return block;
}

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
    if (numThreads == -1)
        numThreads = numCpus();

    ThreadPool tp(ThreadPool::instance(), numThreads);

    for (unsigned i = 0;  i == 0 || i < numThreads;  ++i)
        tp.add(std::bind(parseLinesThreadStr, std::ref(processing), std::ref(processLine), logger));
        
    size_t result = readStream(stream, processing, logger,
                               ignoreStreamExceptions, maxLines);
        
    processing.shutdown = true;

    tp.waitForAll();

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
    static constexpr int64_t READ_SIZE = 200000;  // read&scan 200kb to fit in cache

    std::atomic<int64_t> doneLines(0); //number of lines processed but not yet returned
    std::atomic<int64_t> returnedLines(0); //number of lines returned
    std::atomic<int64_t> byteOffset(0);
    std::atomic<int> chunkNumber(0);

    ThreadPool tp(ThreadPool::instance(), maxParallelism);

    constexpr bool debug = false;

    // Memory map if possible
    const char * mapped = nullptr;
    size_t mappedSize = 0;
    size_t mappedCapacity = 0;

    filter_istream * fistream = dynamic_cast<filter_istream *>(&stream);

    if (fistream) {
        // Can we get a memory mapped version of our stream?  It
        // saves us having to copy data.  mapped will be set to
        // nullptr if it's not possible to memory map this stream.
        std::tie(mapped, mappedSize, mappedCapacity) = fistream->mapped();
        auto info = fistream->info();
        if (info.size > 0 && defaultBlockSize == -1) {
            defaultBlockSize = std::max<int64_t>(1000000, info.size / maxParallelism);
            defaultBlockSize = std::min<int64_t>(defaultBlockSize, 20000000);
        }
    }

    ExcCheckGreater(defaultBlockSize, 0, "defaultBlockSize must be > 0");

    //cerr << "defaultBlockSize = " << defaultBlockSize << endl;

    //cerr << "mapped = " << (void *)mapped << endl;
    //cerr << "mappedSize = " << mappedSize << endl;
    //hex_dump(mapped, mappedSize, mappedSize);
    
    std::atomic<int> hasExc(false);
    std::exception_ptr exc;

    std::any splitterState = splitter.newState();

    // These are used to pass leftover data from the previous block to the next
    // one
    std::shared_ptr<char> nextBlock;
    size_t nextBlockSize = 0;
    size_t nextBlockUsed = 0;
    std::atomic<bool> stop = false;

    std::function<void ()> doBlock = [&] ()
        {
            std::shared_ptr<const char> blockOut;

            //int64_t startOffset = byteOffset;
            int64_t startLine = doneLines;
            vector<size_t> lineOffsets = {0};
            bool lastBlock = false;
            size_t myChunkNumber = 0;

            if (stop.load(std::memory_order_relaxed))
                return;

            if (debug) cerr << "starting block at " << startLine << endl;

            try {
                //MLDB-1426
                if (mapped) {
                    std::streamsize pos = stream.tellg();

                    if (debug) cerr << "memory mapped for each line at pos " << pos << " of " << mappedSize << endl;

                    if (mappedSize == 0 || pos == EOF)
                        return;  // EOF, so nothing to read... probably empty
                    const char * start = mapped + pos;
                    const char * current = start;
                    const char * end = mapped + mappedSize;

                    while (current && current < end && (current - start) < defaultBlockSize
                           && (maxLines == -1 || doneLines < maxLines)) { //stop processing new line when we have enough)
                        std::tie(current, splitterState)
                            = splitter.nextBlock(current, end - current, nullptr, 0,
                                                 true /* no more data */, splitterState);
                        if (current) {
                            lineOffsets.push_back(current - start);
                            ++doneLines;
                            if (current == end)
                                break;
                            ++current;
                        }

                        if (stop.load(std::memory_order_relaxed))
                            return;
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
                else { // not memory mapped
                    if (debug) cerr << "block is not memory mapped" << endl;

                    // How far through our block are we?

                    std::shared_ptr<char> block = nextBlock;
                    size_t thisBlockSize = nextBlockSize;
                    size_t blockUsed = nextBlockUsed;

                    if (!block) {
                        block = std::shared_ptr<char>(new char[defaultBlockSize], [] (char * c) { delete[] c; });
                        blockUsed = 0;
                        thisBlockSize = defaultBlockSize;
                    }

                    blockOut = block;
                    size_t offset = blockUsed;

                    // First line starts at offset 0
                    if (debug) cerr << "starting block with istream: eof = " << stream.eof() << " offset = " << offset << " blockUsed " << blockUsed << " thisBlockSize " << thisBlockSize << endl;

                    while (stream && !stream.eof()
                           && (maxLines == -1 || doneLines < maxLines)  //stop processing new line when we have enough
                           && (offset < thisBlockSize)) {
                        
                        stream.read((char *)block.get() + offset,
                                    std::min<size_t>(READ_SIZE, thisBlockSize - offset));

                        // Check how many bytes we actually read
                        size_t bytesRead = stream.gcount();
                        
                        if (debug) cerr << "read " << bytesRead << " characters" << endl;

                        offset += bytesRead;

                        // Scan for the end of the block
                        const char * current = block.get() + lineOffsets.back();
                        const char * end = block.get() + offset;

                        while (current && current < end && (maxLines == -1 || doneLines < maxLines)) {
                            std::tie(current, splitterState)
                                = splitter.nextBlock(current, end - current, nullptr, 0,
                                                     stream.eof(), splitterState);
                            if (current && (current < end || stream.eof())) {
                                //cerr << "block is at " << current - block.get() << " of " << bytesRead << endl;
                                //ExcAssertEqual(*current, '\n');
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
#if 0
                        // If we are at the end of the stream
                        // make sure we include the last line
                        // if there was no newline
                        if (lineOffsets.back() != offset - 1) {
                            lineOffsets.push_back(offset);
                            ++doneLines;
                        }
#endif
                    }
                    else {
                        // Package up the leftover (truncated) block for the next
                        // block.
                        nextBlockUsed = offset - lineOffsets.back();
                        if (lineOffsets.size() == 1)
                            nextBlockSize = thisBlockSize + nextBlockUsed;
                        else nextBlockSize = defaultBlockSize + nextBlockUsed;

                        nextBlock = std::shared_ptr<char>(new char[nextBlockSize],
                                                          [] (char * c) { delete[] c; });
                        std::copy(block.get() + lineOffsets.back(), block.get() + offset,
                                  nextBlock.get());
                    }

                    bool emptyBlock = lineOffsets.size() == 1;
                    myChunkNumber = emptyBlock ? -1 : chunkNumber++;

                    // don't schedule a new block if we have enough lines
                    if (stream && !stream.eof() && (maxLines == -1 || doneLines < maxLines)) {
                        // Ready for another chunk
                        tp.add(doBlock);
                    } else if (stream.eof()) {
                        lastBlock = true;
                    }
                    
                    int64_t chunkLineNumber = startLine;
                    size_t lastLineOffset = lineOffsets[0];

                    size_t numLinesInBlock = lineOffsets.size() - 1;
                    if (maxLines != -1)
                        ExcAssertLessEqual(numLinesInBlock + startLine, maxLines);

                    if (!emptyBlock && startBlock && !startBlock(myChunkNumber, chunkLineNumber, lineOffsets.size() - 1)) {
                        stop = true;
                        return;
                    }

                    if (debug)
                        cerr << "passing on " << lineOffsets.size() - 1 << " lines in chunk " << myChunkNumber << endl;

                    size_t returnedLinesAtStart = returnedLines;
                    size_t returnedLinesFromThisBlock = 0;

                    for (unsigned i = 1;  i < lineOffsets.size() && (maxLines == -1 || returnedLines < maxLines);  ++i) {

                        if (debug && myChunkNumber == 88) {
                            cerr << "maxLines = " << maxLines << " returnedLines = " << returnedLines << " lastBlock = " << lastBlock << endl;
                        }

                        if (stop.load(std::memory_order_relaxed))
                            return;
                        const char * line = blockOut.get() + lastLineOffset;
                        size_t len = lineOffsets[i] - lastLineOffset;

                        auto fixedup = splitter.fixupBlock({line, len});

                        // if we are not at the last line
                        if (!lastBlock || fixedup.size() != 0 || i != lineOffsets.size() - 1) {
                            ++returnedLines;
                            ++returnedLinesFromThisBlock;
                            if (debug && myChunkNumber == 88) {
                                cerr << "line " << i << " of " << lineOffsets.size() << " in chunk " << myChunkNumber << endl;
                                //cerr << "  line is " << string(line, len) << endl;
                                //cerr << "  fixedup is " << string(fixedup.data(), fixedup.size()) << endl;
                            }
                            if (!onLine(fixedup.data(), fixedup.size(), myChunkNumber, chunkLineNumber++)) {
                                if (debug && myChunkNumber == 88)
                                    cerr << "  STOPPING" << endl;
                                stop = true;
                                return;
                            }
                            if (debug && myChunkNumber == 88) cerr << "  CONTINUING" << endl;

                        } else {
                            cerr << "*** skipped line " << returnedLines << " with lastBlock = " << lastBlock << " and fixedup size = " << fixedup.size() << endl;  
                        }
                    
                        lastLineOffset = lineOffsets[i];
                    }

                    //cerr << "returnedLines was " << returnedLinesAtStart << " now " << returnedLines << " from this block " << returnedLinesFromThisBlock << endl;
                    //cerr << "compared to " << returnedLines - returnedLinesAtStart << " from this block" << endl;
                    ExcAssertEqual(returnedLines, returnedLinesAtStart + returnedLinesFromThisBlock);

                    if (!emptyBlock && endBlock && !endBlock(myChunkNumber, chunkLineNumber, lineOffsets.size())) {
                        stop = true;
                        return;
                    }
                }
            } MLDB_CATCH_ALL {
                if (hasExc.fetch_add(1) == 0) {
                    exc = std::current_exception();
                    stop = true;
                }
            }
        };

    // Run the first block, which will enqueue the second before exiting, and so on
    doBlock();

    // Wait for all to be done
    tp.waitForAll();

    // If there was an exception, rethrow it rather than returning
    // cleanly
    if (hasExc) {
        std::rethrow_exception(exc);
    }

    return stop;
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
                                          uint64_t numLinesInBlock)> startBlock,
                      std::function<bool (int64_t blockNumber,
                                          int64_t lineNumber,
                                          uint64_t numLinesInBlock)> endBlock,
                      size_t blockSize,
                      const BlockSplitter & splitter)
{
    std::atomic<int> hasExc(false);
    std::exception_ptr exc;

    /// This is what we pass to the next block once we've finished scanning for
    /// line breaks.
    struct PassToNextBlock {
        FrozenMemoryRegion leftoverFromPreviousBlock;
        uint64_t doneLines = 0;
        bool bail = false;  ///< Should we bail out (stop) immediately?
        std::any splitterState;
    };

    constexpr bool debug = false;
    constexpr bool debug_lines = debug && false;

    // Set of promises, one per block, for the previous block to use to pass information
    // to the next block.  We use a map so that we can remove early entries. Each block gets
    // information from the promise blockNum and writes its output information to the
    // promise blockNum + 1. Each block is responsible for removing its input promise from
    // the map once it's done with it.

    Spinlock promisesMutex;
    std::map<size_t, std::promise<PassToNextBlock>> promises;
    
    auto getPromises = [&] (size_t blockNumber) -> std::pair<std::future<PassToNextBlock>, std::promise<PassToNextBlock> *>
    {
        if (debug) {
            cerr << "getting promises for block " << blockNumber << endl;
            cerr << "  current exists: " << promises.count(blockNumber) << endl;
            cerr << "  next exists: " << promises.count(blockNumber + 1) << endl;
        }
        std::unique_lock<Spinlock> guard(promisesMutex);
        //ExcAssert(promises.count(blockNumber));
        //ExcAssert(!promises.count(blockNumber + 1));
        auto future = promises[blockNumber].get_future();
        ExcAssert(future.valid());
        return { std::move(future), &promises[blockNumber + 1] };
    };
    
    auto doneBlock = [&] (size_t blockNumber)
    {
        std::unique_lock<Spinlock> guard(promisesMutex);
        promises.erase(blockNumber);
    };

    std::atomic<ssize_t> highestChunkNumber(-1);

    auto doBlock
        = [&hasExc,&exc,maxLines,&onLine,&startBlock,&endBlock,&getPromises, &splitter, &doneBlock, &highestChunkNumber]
        (int chunkNumber,
         uint64_t offset,
         FrozenMemoryRegion mem) -> bool
        {
            atomic_max(highestChunkNumber, chunkNumber);
            if (debug) {
                cerr << "processing chunk " << chunkNumber << " with " << mem.length() << " bytes" << endl;
                std::string data(mem.data(), mem.data() + mem.length());
                if (data.length() > 100) {
                    data.resize(100);
                    data += "...";
                }
                cerr << "    mem is " << data << endl;
            }
            
            // Contains the full first line of our block, which is made up
            // of whatever was leftover from the previous block plus
            // our current line
            FrozenMemoryRegion firstLine;

            // Contains the (partial) last line of our block
            FrozenMemoryRegion partialLastLine;
            
            // Offset in otherLines of line start characters
            vector<std::span<const char>> lines;
            
            // What we got from the last block
            PassToNextBlock fromPrev;

            auto [fromPrevFuture, toNextPromise] = getPromises(chunkNumber);

            if (debug) cerr << "  got promises" << endl;

            // Call this to tell the next block that it should bail out.  It's
            // safe to call at any time, including before the next block has
            // been launched and after the next block has already been told to
            // do something else.
            auto bailNextBlock = [&] ()
            {
                PassToNextBlock toNext;
                toNext.bail = true;
                toNextPromise->set_value(std::move(toNext));
            };

            if (splitter.isStateless()) {
#if 0  // can scan before previous block is finished (TODO: re-enable)                
                std::any splitterState;
                size_t length = mem.length();
                const char * start = mem.data();
                const char * current = start;
                if (length > 0) {
                    std::tie(current, splitterState) = splitter.nextBlock(start, length, nullptr, 0, splitterState);
                }
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

                        std::tie(current, splitterState) = splitter.nextBlock(current, end - current, nullptr, 0, splitterState);

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
                }
#endif
            }
                
            try {
                if (hasExc.load(std::memory_order_relaxed)) {
                    bailNextBlock();
                    return false;
                }

                if (debug) cerr << "    waiting for previous queue..." << endl;

                //fromPrevFuture.wait();
                fromPrev = fromPrevFuture.get();
                if (debug) cerr << "    done waiting for previous queue..." << endl;

                // We no longer need the entry in the promises map
                doneBlock(chunkNumber);

                // Do we bail out?  If our previous block says it has bailed,
                // then we should too.
                if (fromPrev.bail) {
                    bailNextBlock();
                    return false;
                }

                FrozenMemoryRegion leftoverFromPreviousBlock
                    = std::move(fromPrev.leftoverFromPreviousBlock);

                if (debug) {
                    cerr << "    had " << leftoverFromPreviousBlock.length() << " bytes leftover from previous block" << endl;
                    std::string leftoverStr(leftoverFromPreviousBlock.data(), leftoverFromPreviousBlock.length());
                    if (leftoverStr.length() > 100) {
                        leftoverStr.resize(100);
                        leftoverStr += "...";
                    }
                    cerr << "    " << leftoverStr << endl;
                }

                int64_t startLine = fromPrev.doneLines;
                std::any splitterState = std::move(fromPrev.splitterState);

                //cerr << "processing block with " << leftoverFromPreviousBlock.length()
                //     << " and " << mem.length() << " bytes and splitter " << type_name(splitter) << endl;

                const char * start1   = leftoverFromPreviousBlock.data();
                const char * end1     = start1 + leftoverFromPreviousBlock.length();
                const char * start2   = mem.data();
                const char * end2     = start2 + mem.length();
                const char * current = start1 == end1 ? start2 : start1;
                FrozenMemoryRegion leftover;
                FrozenMemoryRegion firstRecord;
                bool noMoreData = mem.length() == 0;  // We signal the last block by sending over a null block

                if (debug) cerr << "    noMoreData = " << noMoreData << endl;

                //cerr << "leftoverFromPreviousBlock.length() = " << leftoverFromPreviousBlock.length() << endl;
                //cerr << "mem.length() = " << mem.length() << endl;

                if (noMoreData && leftoverFromPreviousBlock.length() != 0) {
                    //cerr << "doing last line" << endl;
                    auto [newCurrent, newState] = splitter.nextBlock(start1, end1 - start1, nullptr, 0,
                                                                     true /* noMoreData */, splitterState);

                    //cerr << "newCurrent = " << (const void *)newCurrent << endl;
                    //cerr << "current = " << (const void *)current << endl;
                    //cerr << "start1 = " << (const void *)start1 << endl;
                    //cerr << "end1 = " << (const void *)end1 << endl;
                    //cerr << "start2 = " << (const void *)start2 << endl;
                    //cerr << "end2 = " << (const void *)end2 << endl;

                    //cerr << "newCurrent = " << (const void *)newCurrent << endl;
                    if (!newCurrent) {
                        newCurrent = end1;
                        // Pass on the partial record; the underlying logic can decide if it's valid or not
                        //throw MLDB::Exception("File is truncated in the middle of a record at last line");
                    }
                    ExcAssertEqual((const void *)newCurrent, (const void *)end1);
                    lines.emplace_back(start1, end1 - start1);
                    //cerr << "got last line " << string(firstRecord.data(), firstRecord.length()) << endl;
                    current = 0;
                }

                if (debug) cerr << "    lengths: prev block = " << end1 - start1 << ", this block = " << end2 - start2 << endl;

                // Scan the combined leftover and new blocks, normally it should only be once
                // to complete the partial last record.
                while (current && current >= start1 && current < end1) {
                    ExcAssert(start2 != 0);
                    //cerr << "doing current line" << endl;
                    auto [newCurrent, newState] = splitter.nextBlock(current, end1 - current, start2, end2 - start2, noMoreData, splitterState);
                    if (!newCurrent) {
                        if (debug) cerr << "    *** no break in the whole block, continuing" << endl;
                        // No break in the whole lot... it's all a partial record
                        leftover = FrozenMemoryRegion::combined(leftoverFromPreviousBlock, mem);
                        current = nullptr;
                        break;
                    }

                    if (newCurrent >= start1 && newCurrent < end1) {
                        if (debug_lines) cerr << "    *** got line in first block at position " << newCurrent - start1 << endl;
                        if (debug_lines) cerr << "    adding line " << string(current, newCurrent - start1) << endl;
                        lines.emplace_back(current, newCurrent - current);
                    }
                    else {
                        if (newCurrent < start2 || newCurrent > end2) {
                            cerr << "newCurrent = " << (const void *)newCurrent << endl;
                            cerr << "current = " << (const void *)current << endl;
                            cerr << "start1 = " << (const void *)start1 << endl;
                            cerr << "end1 = " << (const void *)end1 << endl;
                            cerr << "start2 = " << (const void *)start2 << endl;
                            cerr << "end2 = " << (const void *)end2 << endl;
                        }
                        ExcAssertGreaterEqual((const void *)newCurrent, (const void *)start2);
                        ExcAssertLessEqual((const void *)newCurrent, (const void *)end2);
                        //cerr << "doing combined" << endl;
                        if (debug_lines) cerr << "    *** got line in the second block at position " << newCurrent - start2 << endl;
                        if (debug_lines) {
                                cerr << "combining " << string(leftoverFromPreviousBlock.data(), 0, leftoverFromPreviousBlock.length())
                                     << " with " << string(current, end1)
                                     << " and " << string(start2, newCurrent) << endl;
                        }
                        firstRecord = FrozenMemoryRegion::combined(leftoverFromPreviousBlock, mem.rangeAtStart(newCurrent - start2));
                        lines.emplace_back(firstRecord.data(), firstRecord.length());
                        if (debug_lines) cerr << "    adding line " << string(firstRecord.data(), firstRecord.length()) << endl;
                    }
                    //cerr << "got first line " << string(lines.back().data(), lines.back().size()) << endl;
                    current = newCurrent;
                    splitterState = std::move(newState);
                }

                if (debug) cerr << "    got " << lines.size() << " lines" << endl;

                // Scan the current block
                while (current && current >= start2 && current < end2) {
                    if (startLine + lines.size() >= maxLines)
                        break;

                    auto [newCurrent, newState] = splitter.nextBlock(current, end2 - current, nullptr, 0, noMoreData, splitterState);
                    if (!newCurrent) {
                        // We've gotten all we can
                        leftover = mem.rangeAtEnd(end2 - current);
                        current = nullptr;
                        break;
                    }
                    lines.emplace_back(current, newCurrent - current);
                    //cerr << "got line " << string(current, newCurrent) << endl;
                    ExcAssertGreater((const void *)newCurrent, (const void *)current);
                    current = newCurrent;
                    splitterState = std::move(newState);
                }

                //cerr << "done " << lines.size() << " lines" << endl;
                //cerr << "leftover.length() = " << leftover.length() << endl;

                if (hasExc.load(std::memory_order_relaxed)) {
                    bailNextBlock();
                    return false;
                }

                uint64_t doneLines = startLine + lines.size();
                if (debug) cerr << "    doneLines = " << doneLines << endl;

                if (maxLines == -1 || doneLines < maxLines) {
                    // What we pass on to the next block
                    PassToNextBlock toNext;
                    toNext.leftoverFromPreviousBlock = std::move(leftover);
                    toNext.doneLines = doneLines;
                    toNext.splitterState = std::move(splitterState);
                    toNextPromise->set_value(std::move(toNext));
                }
                else {
                    bailNextBlock();
                }
                    
                int64_t chunkLineNumber = startLine;
                size_t numLines = lines.size();
                
                auto doLine = [&] (const char * line, size_t len)
                    {
                        //cerr << "doLine: " << string(line, len) << endl;
                        auto fixedup = splitter.fixupBlock({line, len});
                        //cerr << "fixedup: " << string(fixedup.data(), fixedup.size()) << endl;

                        return onLine(fixedup.data(), fixedup.size(), chunkNumber, chunkLineNumber++);
                    };
            
                if (startBlock)
                    if (!startBlock(chunkNumber, chunkLineNumber, numLines))
                        return false;

                auto returnedLines = startLine;
                
                for (unsigned i = 0;  i < lines.size() && (maxLines == -1 || returnedLines++ < maxLines); ++i) {
                    //cerr << "i = " << i << " maxLines = " << maxLines << " returnedLines = " << returnedLines << endl;
                    // Check for exception bailout every 256 lines
                    if (i % 256 == 0 && hasExc.load(std::memory_order_relaxed))
                        return false;

                    const char * line = lines[i].data();
                    size_t len = lines[i].size();

                    if (!doLine(line, len))
                        return false;
                }
                    
                if (endBlock)
                    if (!endBlock(chunkNumber, chunkLineNumber, numLines))
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
    pass.splitterState = splitter.newState();
    {
        std::unique_lock<Spinlock> guard(promisesMutex);
        promises[0].set_value(std::move(pass));
    }
    
    content->forEachBlockParallel(startOffset, blockSize, maxParallelism, doBlock);

    // last chunk with single last line is in the last queue entry
    if (!hasExc && !promises.empty()) {
        if (debug) cerr << "last one; promises.size() = " << promises.size() << endl;
        size_t chunkNumber = highestChunkNumber + 1;
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
