// This file is part of MLDB. Copyright 2015 Datacratic. All rights reserved.

/** for_each_line.cc
    Jeremy Barnes, 29 November 2013
    Copyright (c) 2013 Datacratic Inc.  All rights reserved.

*/

#include <atomic>
#include <exception>
#include <mutex>
#include "for_each_line.h"
#include "mldb/arch/threads.h"
#include <chrono>
#include <thread>
#include <cstring>
#include "mldb/jml/utils/ring_buffer.h"
#include "mldb/vfs/filter_streams.h"
#include "mldb/jml/utils/worker_task.h"
#include "mldb/base/exc_assert.h"
#include "mldb/types/date.h"


using namespace std;
using namespace ML;


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

    RingBufferSWMR<pair<int64_t, vector<string> > > decompressedLines;
};

}

namespace Datacratic {


/*****************************************************************************/
/* PARALLEL LINE PROCESSOR                                                   */
/*****************************************************************************/

static size_t
readStream(std::istream & stream,
           Processing & processing,
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
                processing.decompressedLines.push(std::move(current));
                current.first = done;
                current.second.clear();
                //current.clear();
                ExcAssertEqual(current.second.size(), 0);
                current.second.reserve(1000);
            }

            if (done % 1000000 == 0) {

                //cerr << "done " << done << " lines" << endl;
                Date now = Date::now();

                double elapsed = now.secondsSince(start);
                double instElapsed = now.secondsSince(lastCheck);
                cerr << ML::format("doing %.3fMlines/second total, %.3f instantaneous",
                                   done / elapsed / 1000000.0,
                                   1000000 / instElapsed / 1000000.0)
                     << endl;
                lastCheck = now;
            }
        }
    } catch (const std::exception & exc) {
        if (!ignoreStreamExceptions) {
            processing.takeLastException();
        }
        else {
            cerr << "stream threw ignored exception: " << exc.what() << endl;
        }
    }

    if (!current.second.empty() && !processing.hasException()) {
        processing.decompressedLines.push(std::move(current));
    }

    return done;
};

static void
parseLinesThreadStr(Processing & processing,
                    const std::function<void (const std::string &,
                                              int64_t lineNum)> & processLine)
{
    while (!processing.hasException()) {
        std::pair<int64_t, vector<string> > lines;
        if (!processing.decompressedLines.tryPop(lines)) {
            if (processing.shutdown) {
                break;
            }
            std::this_thread::sleep_for(std::chrono::milliseconds(1));
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
                cerr << "error dealing with line " << line
                     << ": " << exc.what() << endl;
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
            int numThreads,
            bool ignoreStreamExceptions,
            int64_t maxLines)
{
    auto onLineStr = [&] (const string & line, int64_t lineNum) {
        processLine(line.c_str(), line.size(), lineNum);
    };

    return forEachLineStr(stream, onLineStr, numThreads,
                          ignoreStreamExceptions, maxLines);
}

size_t
forEachLineStr(std::istream & stream,
               const std::function<void (const std::string &,
                                         int64_t)> & processLine,
               int numThreads,
               bool ignoreStreamExceptions,
               int64_t maxLines)
{
    Processing processing;

    std::vector<std::thread> threads;
    for (unsigned i = 0;  i < numThreads;  ++i)
        threads.emplace_back(std::bind(parseLinesThreadStr,
                                       std::ref(processing),
                                       std::ref(processLine)));
        
    size_t result = readStream(stream, processing,
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
            int numThreads,
            bool ignoreStreamExceptions,
            int64_t maxLines)
{
    ML::filter_istream stream(filename);
    return forEachLine(stream, processLine, numThreads,
                       ignoreStreamExceptions, maxLines);
}

size_t
forEachLineStr(const std::string & filename,
               const std::function<void (const std::string &, int64_t)> & processLine,
               int numThreads,
               bool ignoreStreamExceptions,
               int64_t maxLines)
{
    ML::filter_istream stream(filename);
    return forEachLineStr(stream, processLine, numThreads,
                          ignoreStreamExceptions, maxLines);
}


/*****************************************************************************/
/* FOR EACH LINE BLOCK                                                       */
/*****************************************************************************/

void forEachLineBlock(std::istream & stream,
                      std::function<bool (const char * line,
                                          size_t lineLength,
                                          int64_t blockNumber,
                                          int64_t lineNumber)> onLine,
                      int64_t lineOffset, // 0
                      int64_t maxLines)   // -1
{
    //static constexpr int64_t BLOCK_SIZE = 100000000;  // 100MB blocks
    static constexpr int64_t BLOCK_SIZE = 10000000;  // 10MB blocks
    static constexpr int64_t READ_SIZE = 200000;  // read&scan 200kb to fit in cache

    std::atomic<int64_t> doneLines(lineOffset);
    std::atomic<int64_t> byteOffset(0);
    std::atomic<int> chunkNumber(0);

    ML::Worker_Task & worker = ML::Worker_Task::instance();

    int group = worker.get_group(nullptr, "csv");

    // Memory map if possible
    const char * mapped = nullptr;
    size_t mappedSize = 0;

    ML::filter_istream * fistream = dynamic_cast<ML::filter_istream *>(&stream);

    if (fistream) {
        // Can we get a memory mapped version of our stream?  It
        // saves us having to copy data.  mapped will be set to
        // nullptr if it's not possible to memory map this stream.
        std::tie(mapped, mappedSize) = fistream->mapped();
    }

    std::function<void ()> doBlock = [&] ()
        {
            //cerr << "block starting at line " << doneLines << endl;

            std::shared_ptr<const char> blockOut;

            int64_t startOffset = byteOffset;
            int64_t startLine = doneLines;
            vector<size_t> lineOffsets = {0};

            if (mapped) {
                const char * start = mapped + stream.tellg();
                const char * current = start;
                const char * end = mapped + mappedSize;

                while (current && current < end && (current - start) < BLOCK_SIZE) {
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

                ++chunkNumber;

                if (current && current < end) {
                    // Ready for another chunk
                    worker.add(doBlock, "", group);
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
                       && (maxLines == -1 || doneLines < maxLines)
                       && (byteOffset - startOffset < BLOCK_SIZE)) {
                        
                    stream.read((char *)block.get() + offset,
                                std::min<size_t>(READ_SIZE, BLOCK_SIZE - offset));

                    // Check how many bytes we actually read
                    size_t bytesRead = stream.gcount();
                        
                    //cerr << "read " << bytesRead << " bytes" << endl;
                        
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
                                //cerr << "got line at offset " << lineOffsets.back() << endl;
                                ++doneLines;
                            }
                            ++current;
                        }
                    }

                    byteOffset += bytesRead;
                }

                // Get the last line, as we probably got just a partial
                // line in the last one
                std::string lastLine;
                getline(stream, lastLine);
                
                size_t cnt = stream.gcount();

                if (cnt != 0) {
                    // Check for overflow on the buffer size
                    if (offset + lastLine.size() + 1 > BLOCK_SIZE + EXTRA_SIZE) {
                        // reallocate and copy
                        std::shared_ptr<char> newBlock(new char[offset + lastLine.size() + 1],
                                                       [] (char * c) { delete[] c; });
                        std::copy(block.get(), block.get() + offset,
                                  newBlock.get());
                        block = newBlock;
                    }

                    std::copy(lastLine.data(), lastLine.data() + lastLine.length(),
                              block.get() + offset);
                    
                    lineOffsets.emplace_back(offset + lastLine.length());
                    ++doneLines;
                    offset += cnt;
                }                
                
                ++chunkNumber;

                if (stream) {
                    // Ready for another chunk
                    worker.add(doBlock, "", group);
                }
            }
                    
            //cerr << "processing block of " << lineOffsets.size() - 1
            //     << " lines starting at " << startLine << endl;

            int64_t chunkLineNumber = startLine;
            size_t lastLineOffset = lineOffsets[0];
            for (unsigned i = 1;  i < lineOffsets.size();  ++i) {
                const char * line = blockOut.get() + lastLineOffset;
                size_t len = lineOffsets[i] - lastLineOffset;

                // Skip \r for DOS line endings
                if (len > 0 && line[len - 1] == '\r')
                    --len;

                if (!onLine(line, len, chunkNumber, chunkLineNumber++))
                    return;
                lastLineOffset = lineOffsets[i] + 1;

                if (maxLines != -1 && i >= maxLines)
                    break;
            }
        };
            
    worker.add(doBlock, "start", group);
    worker.run_until_finished(group, true /* unlock */);
}

} // namespace Datacratic
