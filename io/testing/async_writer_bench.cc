// This file is part of MLDB. Copyright 2015 mldb.ai inc. All rights reserved.

/* async_writer_bench */

#include <fcntl.h>
#include <unistd.h>
#include <arpa/inet.h>
#include <sys/types.h>
#include <sys/socket.h>
#include <netinet/in.h>
#include <netinet/tcp.h>

#include <memory>

#include <chrono>
#include <thread>
#include "mldb/io/message_loop.h"
#include "mldb/io/async_writer_source.h"
#include "mldb/utils/testing/print_utils.h"
#include "mldb/arch/file_functions.h"

// fix SOCK_NONBLOCK for e.g. macOS
#ifndef SOCK_NONBLOCK
#include <fcntl.h>
#define SOCK_NONBLOCK O_NONBLOCK
#endif

using namespace std;
using namespace MLDB;

struct WriterSource : public AsyncWriterSource {
    WriterSource(int fd, size_t maxMessages)
        : AsyncWriterSource(nullptr, nullptr, nullptr, maxMessages, 0)
    {
        setFd(fd);
        enableQueue();
    }
};

struct ReaderSource : public AsyncWriterSource {
    ReaderSource(int fd, const OnReceivedData & onReaderData,
                 size_t readBufferSize)
        : AsyncWriterSource(nullptr, onReaderData, nullptr, 0, readBufferSize)
    {
        setFd(fd);
    }
};

/* order is : {writer, reader} */

pair<int, int> makePipePair()
{
    int fds[2];
#if defined(__linux__)
    if (pipe2(fds, O_NONBLOCK) == -1) {
        throw MLDB::Exception(errno, "pipe2");
    }
#else
    if (pipe(fds) == -1) {
        throw MLDB::Exception(errno, "pipe");
    }
    MLDB::set_file_flag(fds[0], O_NONBLOCK);
    MLDB::set_file_flag(fds[1], O_NONBLOCK);    
#endif

    return {fds[1], fds[0]};
}

pair<int, int> makeUnixSocketPair()
{
    int fds[2] = { -1, -1 };
    int res = socketpair(AF_UNIX, SOCK_STREAM, 0, fds);
    if (res == -1)
        throw MLDB::Exception(errno, "socketpair");
    MLDB::set_file_flag(fds[0], O_NONBLOCK);
    MLDB::set_file_flag(fds[1], O_NONBLOCK);    
    return {fds[0], fds[1]};
}

pair<int, int> makeTcpSocketPair()
{
    /* listener */
    struct sockaddr_in addr;
    socklen_t addrLen = sizeof(addr);
    addr.sin_port = htons(0);
    addr.sin_family = AF_INET;
    inet_aton("127.0.0.1", &addr.sin_addr);

    int listener = socket(AF_INET, SOCK_STREAM, 0);
    if (listener == -1) {
        throw MLDB::Exception(errno, "socket");
    }

    if (bind(listener, (const struct sockaddr *) &addr, addrLen) == -1) {
        throw MLDB::Exception(errno, "bind");
    }

    if (listen(listener, 666) == -1) {
        throw MLDB::Exception(errno, "listen");
    }

    if (getsockname(listener, (sockaddr *) &addr, &addrLen) == -1) {
        throw MLDB::Exception(errno, "getsockname");
    }

    /* writer */
    int writer = socket(AF_INET, SOCK_STREAM, 0);
    if (writer == -1) {
        throw MLDB::Exception(errno, "socket");
    }

#if 0
    {
        int flag = 1;
        if (setsockopt(writer,
                       IPPROTO_TCP, TCP_NODELAY,
                       (char *) &flag, sizeof(int)) == -1) {
            throw MLDB::Exception(errno, "setsockopt TCP_NODELAY");
        }
    }
#endif

    if (connect(writer, (const struct sockaddr *) &addr, addrLen) == -1) {
        throw MLDB::Exception(errno, "connect");
    }
    set_file_flag(writer, O_NONBLOCK);

    int reader = accept(listener, (struct sockaddr *) &addr, &addrLen);
    if (reader == -1) {
        throw MLDB::Exception(errno, "accept");
    }
    set_file_flag(reader, O_NONBLOCK);

    close(listener);

    return {writer, reader};
}

void doBench(const string & label,
             int writerFd, int readerFd,
             int numMessages, size_t msgSize)
{
    cerr << "writing " << numMessages << " of size " << msgSize << endl;
    
    string message = randomString(msgSize);
    MessageLoop writerLoop, readerLoop;

    /* writer setup */
    writerLoop.start();
    Date lastWrite;
    Date lastWriteResult;
    std::atomic<int> numWriteResults(0);
    int numWritten(0);
    int numMissed(0);
    auto onWriteResult = [&] (AsyncWriteResult result) {
        //cerr << format("got write result %d with error %d\n", numWriteResults.load(), result.error);
        if (result.error != 0) {
            throw MLDB::Exception("write error");
        }
        numWriteResults++;
        if (numWriteResults == numMessages) {
            lastWriteResult = Date::now();
            MLDB::wake_by_address(numWriteResults);
        }
    };

    auto writer = make_shared<WriterSource>(writerFd, 1000);
    writerLoop.addSource("writer", writer);

    /* reader setup */
    readerLoop.start();
    Date lastRead;
    size_t totalBytes = msgSize * numMessages;
    std::atomic<size_t> bytesRead(0);
    auto onReaderData = [&] (const char * data, size_t size) {
        bytesRead += size;
        //cerr << format("read %zd new total %zd items\n", size / msgSize, bytesRead / msgSize);
        if (bytesRead == totalBytes) {
            lastRead = Date::now();
        }
    };
    auto reader = make_shared<ReaderSource>(readerFd, onReaderData, 262144);
    readerLoop.addSource("reader", reader);

    cerr << "waiting for connection" << endl;
    writer->waitConnectionState(AsyncEventSource::CONNECTED);
    reader->waitConnectionState(AsyncEventSource::CONNECTED);
    cerr << "connected" << endl;

    Date start = Date::now();
    std::atomic_thread_fence(std::memory_order_release);
    for (numWritten = 0 ; numWritten < numMessages;) {
        if ((numWritten + 1) % 1000 == 0) {
            cerr << format("writing message %d with %d missed %zd read\n", numWritten, numMissed, bytesRead / msgSize);
        }
        if (writer->write(message, onWriteResult)) {
            numWritten++;
        }
        else {
            numMissed++;
            std::this_thread::sleep_for(std::chrono::milliseconds(1));
        }
    }
    std::atomic_thread_fence(std::memory_order_release);
    lastWrite = Date::now();

    while (numWriteResults < numMessages) {
        int old = numWriteResults;
        MLDB::wait_on_address(numWriteResults, old);
    }

    while (bytesRead < totalBytes) {
        //cerr << "bytesRead " << bytesRead << " totalBytes " << totalBytes << endl;
        std::this_thread::sleep_for(std::chrono::milliseconds(100));
    }

    double totalTime = lastRead - start;
    ::printf("%s,%d,%zu,%zu,%d,%f,%f,%f,%f,%f\n",
             label.c_str(),
             numMessages, msgSize, bytesRead.load(), numMissed,
             (lastWrite - start),
             (lastWriteResult - start),
             totalTime,
             (double(numMessages) / totalTime),
             (double(totalBytes) / totalTime));

    readerLoop.shutdown();
    writerLoop.shutdown();
}

void benchFunction(const string & label,
                   std::function<pair<int, int> ()> f)
{
    int multiplier(1);
    for (int i = 0; i < 4; i++) {
        multiplier *= 10;
        auto fds = f();
        doBench(label, fds.first, fds.second,
                100000 / multiplier, 50 * multiplier);
    }
}

int main()
{
    ::printf("label,msgs_count,msg_size,bytes_xfer,miss_count,"
             "delta_last_write,delta_last_written,delta_last_read,"
             "msg_rate,byte_rate\n");

    benchFunction("pipe", makePipePair);
    benchFunction("unix", makeUnixSocketPair);
    benchFunction("tcp4", makeTcpSocketPair);

    return 0;
}
