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

using namespace std;
using namespace MLDB;


namespace {

void setFileFlag(int fd, int newFlag)
{
    int oldFlags = fcntl(fd, F_GETFL, 0);
    fcntl(fd, F_SETFL, oldFlags | newFlag);
}

} // file scope


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
    if (pipe2(fds, O_NONBLOCK) == -1) {
        throw MLDB::Exception(errno, "pipe2");
    }

    return {fds[1], fds[0]};
}

pair<int, int> makeUnixSocketPair()
{
    int fds[2];
    socketpair(AF_UNIX, SOCK_STREAM | SOCK_NONBLOCK, 0, fds);
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
    setFileFlag(writer, O_NONBLOCK);

    int reader = accept(listener, (struct sockaddr *) &addr, &addrLen);
    if (reader == -1) {
        throw MLDB::Exception(errno, "accept");
    }
    setFileFlag(reader, O_NONBLOCK);

    close(listener);

    return {writer, reader};
}

void doBench(const string & label,
             int writerFd, int readerFd,
             int numMessages, size_t msgSize)
{
    string message = randomString(msgSize);
    MessageLoop writerLoop, readerLoop;

    /* writer setup */
    writerLoop.start();
    Date lastWrite;
    Date lastWriteResult;
    int numWriteResults(0);
    int numWritten(0);
    int numMissed(0);
    auto onWriteResult = [&] (AsyncWriteResult result) {
        if (result.error != 0) {
            throw MLDB::Exception("write error");
        }
        numWriteResults++;
        if (numWriteResults == numMessages) {
            lastWriteResult = Date::now();
            ML::futex_wake(numWriteResults);
        }
    };

    auto writer = make_shared<WriterSource>(writerFd, 1000);
    writerLoop.addSource("writer", writer);

    /* reader setup */
    readerLoop.start();
    Date lastRead;
    size_t totalBytes = msgSize * numMessages;
    size_t bytesRead(0);
    auto onReaderData = [&] (const char * data, size_t size) {
        bytesRead += size;
        if (bytesRead == totalBytes) {
            lastRead = Date::now();
        }
    };
    auto reader = make_shared<ReaderSource>(readerFd, onReaderData, 262144);
    readerLoop.addSource("reader", reader);

    writer->waitConnectionState(AsyncEventSource::CONNECTED);
    reader->waitConnectionState(AsyncEventSource::CONNECTED);

    Date start = Date::now();
    std::atomic_thread_fence(std::memory_order_release);
    for (numWritten = 0 ; numWritten < numMessages;) {
        if (writer->write(message, onWriteResult)) {
            numWritten++;
        }
        else {
            numMissed++;
        }
    }
    std::atomic_thread_fence(std::memory_order_release);
    lastWrite = Date::now();

    while (numWriteResults < numMessages) {
        int old = numWriteResults;
        ML::futex_wait(numWriteResults, old);
    }

    while (bytesRead < totalBytes) {
        std::this_thread::sleep_for(std::chrono::seconds(1));
    }

    double totalTime = lastRead - start;
    ::printf("%s,%d,%zu,%zu,%d,%f,%f,%f,%f,%f\n",
             label.c_str(),
             numMessages, msgSize, bytesRead, numMissed,
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
                10000000 / multiplier, 50 * multiplier);
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
