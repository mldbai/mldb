// This file is part of MLDB. Copyright 2015 Datacratic. All rights reserved.

/** s3_handlers.cc
    Jeremy Barnes, 3 July 2012
    Wolfgang Sourdeau, 7 July 2012
    Copyright (c) 2012, 2016 Datacratic.  All rights reserved.

    S3 VFS handlers
*/

#include <memory>
#include <exception>
#include <thread>
#include <chrono>
#include <boost/iostreams/stream_buffer.hpp>
#include "mldb/jml/utils/ring_buffer.h"
#include "mldb/jml/utils/string_functions.h"
#include "mldb/base/exc_assert.h"
#include "mldb/base/hash.h"
#include "mldb/types/url.h"
#include "mldb/vfs/filter_streams_registry.h"
#include "mldb/vfs/fs_utils.h"
#include "mldb/soa/service/s3.h"

using namespace std;
using namespace Datacratic;


namespace {

/* S3URLFSHANDLER */

struct S3UrlFsHandler : public UrlFsHandler {
    virtual FsObjectInfo getInfo(const Url & url) const
    {
        string bucket = url.host();
        auto api = getS3ApiForUri(url.toDecodedString());
        auto bucketPath = S3Api::parseUri(url.original);
        return api->getObjectInfo(bucket, bucketPath.second);
    }

    virtual FsObjectInfo tryGetInfo(const Url & url) const
    {
        string bucket = url.host();
        auto api = getS3ApiForUri(url.toDecodedString());
        auto bucketPath = S3Api::parseUri(url.original);
        return api->tryGetObjectInfo(bucket, bucketPath.second);
    }

    virtual void makeDirectory(const Url & url) const
    {
    }

    virtual bool erase(const Url & url, bool throwException) const
    {
        string bucket = url.host();
        auto api = getS3ApiForUri(url.toDecodedString());
        auto bucketPath = S3Api::parseUri(url.original);
        if (throwException) {
            api->eraseObject(bucket, "/" + bucketPath.second);
            return true;
        }
        else {
            return api->tryEraseObject(bucket, "/" + bucketPath.second);
        }
    }

    virtual bool forEach(const Url & prefix,
                         const OnUriObject & onObject,
                         const OnUriSubdir & onSubdir,
                         const std::string & delimiter,
                         const std::string & startAt) const
    {
        string bucket = prefix.host();
        auto api = getS3ApiForUri(prefix.toString());

        bool result = true;

        auto onObject2 = [&] (const std::string & prefix,
                              const std::string & objectName,
                              const S3Api::ObjectInfo & info,
                              int depth)
            {
                std::string filename = "s3://" + bucket + "/" + prefix + objectName;
                OpenUriObject open = [=] (const std::map<std::string, std::string> & options) -> UriHandler
                {
                    if (!options.empty())
                        throw ML::Exception("Options not accepted by S3");

                    std::shared_ptr<std::istream> result(new filter_istream(filename));
                    auto into = getInfo(Url(filename));

                    return UriHandler(result->rdbuf(), result, info);
                };

                return onObject(filename, info, open, depth);
            };

        auto onSubdir2 = [&] (const std::string & prefix,
                              const std::string & dirName,
                              int depth)
            {
                return onSubdir("s3://" + bucket + "/" + prefix + dirName,
                                depth);
            };

        // Get rid of leading / on prefix
        string prefix2 = string(prefix.path(), 1);

        api->forEachObject(bucket, prefix2, onObject2,
                           onSubdir ? onSubdir2 : S3Api::OnSubdir(),
                           delimiter, 1, startAt);

        return result;
    }
};

struct AtInit {
    AtInit() {
        registerUrlFsHandler("s3", new S3UrlFsHandler());
    }
} atInit;


size_t getTotalSystemMemory()
{
    long pages = sysconf(_SC_PHYS_PAGES);
    long page_size = sysconf(_SC_PAGE_SIZE);
    return pages * page_size;
}

struct StreamingDownloadSource {
    StreamingDownloadSource(const std::string & urlStr)
    {
        impl.reset(new Impl());
        impl->owner = getS3ApiForUri(urlStr);
        std::tie(impl->bucket, impl->object) = S3Api::parseUri(urlStr);
        impl->info = impl->owner->getObjectInfo(urlStr);
        impl->baseChunkSize = 1024 * 1024;  // start with 1MB and ramp up

        int numThreads = 1;
        if (impl->info.size > 1024 * 1024)
            numThreads = 2;
        if (impl->info.size > 16 * 1024 * 1024)
            numThreads = 3;
        if (impl->info.size > 256 * 1024 * 1024)
            numThreads = 5;

        impl->start(numThreads);
    }

    const FsObjectInfo & info()
    {
        ExcAssert(impl);
        return impl->info;
    }

    typedef char char_type;
    struct category
        : //input_seekable,
        boost::iostreams::input,
        boost::iostreams::device_tag,
        boost::iostreams::closable_tag
    { };

    struct Impl {
        Impl()
            : baseChunkSize(0)
        {
            reset();
        }

        ~Impl()
        {
            stop();
        }

        /* static variables, set during or right after construction */
        shared_ptr<S3Api> owner;
        std::string bucket;
        std::string object;
        S3Api::ObjectInfo info;
        size_t baseChunkSize;

        /* variables set during or after "start" has been called */
        size_t maxChunkSize;

        atomic<bool> shutdown;
        exception_ptr lastExc;

        /* read thread */
        uint64_t readOffset; /* number of bytes from the entire stream that
                              * have been returned to the caller */

        string readPart; /* data buffer for the part of the stream being
                          * transferred to the caller */
        ssize_t readPartOffset; /* number of bytes from "readPart" that have
                                 * been returned to the caller, or -1 when
                                 * awaiting a new part */
        int readPartDone; /* the number of the chunk representing "readPart" */

        /* http threads */
        typedef ML::RingBufferSRMW<string> ThreadData;

        int numThreads; /* number of http threads */
        vector<thread> threads; /* thread pool */
        vector<ThreadData> threadQueues; /* per-thread queue of chunk data */

        /* cleanup all the variables that are used during reading, the
           "static" ones are left untouched */
        void reset()
        {
            shutdown = false;

            readOffset = 0;

            readPart = "";
            readPartOffset = -1;
            readPartDone = 0;

            threadQueues.clear();
            threads.clear();
            numThreads = 0;
        }

        void start(int nThreads)
        {
            // Maximum chunk size is what we can do in 3 seconds
            maxChunkSize = (owner->bandwidthToServiceMbps
                            * 3.0 * 1000000);
            size_t sysMemory = getTotalSystemMemory();

            //cerr << "sysMemory = " << sysMemory << endl;
            // Limit each chunk to 1% of system memory
            maxChunkSize = std::min(maxChunkSize, sysMemory / 100);
            //cerr << "maxChunkSize = " << maxChunkSize << endl;
            numThreads = nThreads;

            for (int i = 0; i < numThreads; i++) {
                threadQueues.emplace_back(2);
            }

            /* ensure that the queues are ready before the threads are
               launched */
            std::atomic_thread_fence(std::memory_order_release);

            for (int i = 0; i < numThreads; i++) {
                auto threadFn = [&] (int threadNum) {
                    this->runThread(threadNum);
                };
                threads.emplace_back(threadFn, i);
            }
        }

        void stop()
        {
            shutdown = true;
            for (thread & th: threads) {
                th.join();
            }

            reset();
        }

        /* reader thread */
        std::streamsize read(char_type* s, std::streamsize n)
        {
            if (lastExc) {
                rethrow_exception(lastExc);
            }

            if (readOffset == info.size)
                return -1;

            if (readPartOffset == -1) {
                waitNextPart();
            }

            if (lastExc) {
                rethrow_exception(lastExc);
            }

            size_t toDo = min<size_t>(readPart.size() - readPartOffset,
                                      n);
            const char_type * start = readPart.c_str() + readPartOffset;
            std::copy(start, start + toDo, s);

            readPartOffset += toDo;
            if (readPartOffset == readPart.size()) {
                readPartOffset = -1;
            }

            readOffset += toDo;

            return toDo;
        }

        void waitNextPart()
        {
            int partThread = readPartDone % numThreads;
            ThreadData & threadQueue = threadQueues[partThread];

            /* We set a timeout to avoid dead locking when http threads have
             * exited after an exception. */
            while (!lastExc) {
                if (threadQueue.tryPop(readPart, 1.0)) {
                    break;
                }
            }

            readPartOffset = 0;
            readPartDone++;
        }

        /* download threads */
        void runThread(int threadNum)
        {
            ThreadData & threadQueue = threadQueues[threadNum];

            uint64_t start = 0;
            unsigned int prevChunkNbr = 0;

            try {
                for (int loop = 0;; loop++) {
                    /* number of the chunk that we need to process */
                    unsigned int chunkNbr = loop * numThreads + threadNum;

                    /* we adjust the offset by adding the chunk sizes of all
                       the chunks downloaded between our previous loop until
                       now */
                    for (unsigned int i = prevChunkNbr; i < chunkNbr; i++) {
                        start += getChunkSize(i);
                    }

                    if (start >= info.size) {
                        /* we are done */
                        return;
                    }
                    prevChunkNbr = chunkNbr;

                    size_t chunkSize = getChunkSize(chunkNbr);
                    uint64_t end = start + chunkSize;
                    if (end > info.size) {
                        end = info.size;
                        chunkSize = end - start;
                    }

                    auto partResult
                        = owner->get(bucket, "/" + object,
                                     S3Api::Range(start, chunkSize));

                    if (!(partResult.code_ == 200 || partResult.code_ == 206)) {
                        throw ML::Exception("http error "
                                            + to_string(partResult.code_)
                                            + " while getting part "
                                            + partResult.bodyXmlStr());
                    }
                    // it can sometimes happen that a file changes during download
                    // i.e it is being overwritten. Make sure we check for this condition
                    // and throw an appropriate exception
                    string chunkEtag = partResult.getHeader("etag") ;
                    if(chunkEtag != info.etag)
                        throw ML::Exception("chunk etag %s not equal to file etag %s: file <%s> has changed during download!!", chunkEtag.c_str(), info.etag.c_str(), object.c_str());
                    ExcAssert(partResult.body().size() == chunkSize);

                    while (true) {
                        if (shutdown || lastExc) {
                            return;
                        }
                        if (threadQueue.tryPush(partResult.body())) {
                            break;
                        }
                        else {
                            std::this_thread::sleep_for(std::chrono::milliseconds(100));
                        }
                    }
                }
            }
            catch (...) {
                lastExc = current_exception();
            }
        }

        size_t getChunkSize(unsigned int chunkNbr)
            const
        {
            size_t chunkSize = std::min(baseChunkSize * (1 << (chunkNbr / 2)),
                                        maxChunkSize);
            return chunkSize;
        }
    };

    std::shared_ptr<Impl> impl;

    std::streamsize read(char_type* s, std::streamsize n)
    {
        return impl->read(s, n);
    }

    bool is_open() const
    {
        return !!impl;
    }

    void close()
    {
        impl.reset();
    }
};

std::pair<std::unique_ptr<std::streambuf>, FsObjectInfo>
makeStreamingDownload(const std::string & uri)
{
    std::unique_ptr<std::streambuf> result;
    StreamingDownloadSource source(uri);
    result.reset(new boost::iostreams::stream_buffer<StreamingDownloadSource>
                 (source,131072));
    return make_pair(std::move(result), source.info());
}

std::pair<std::unique_ptr<std::streambuf>, FsObjectInfo>
makeStreamingDownload(const std::string & bucket,
                      const std::string & object)
{
    return makeStreamingDownload("s3://" + bucket + "/" + object);
}

struct StreamingUploadSource {

    StreamingUploadSource(const std::string & urlStr,
                          const OnUriHandlerException & excCallback,
                          const S3Api::ObjectMetadata & metadata)
    {
        impl.reset(new Impl());
        impl->owner = getS3ApiForUri(urlStr);
        std::tie(impl->bucket, impl->object) = S3Api::parseUri(urlStr);
        impl->metadata = metadata;
        impl->onException = excCallback;
        impl->chunkSize = 8 * 1024 * 1024;  // start with 8MB and ramp up

        impl->start();
    }

    typedef char char_type;
    struct category
        : public boost::iostreams::output,
          public boost::iostreams::device_tag,
          public boost::iostreams::closable_tag
    {
    };

    struct Impl {
        Impl()
            : offset(0), chunkIndex(0), shutdown(false),
              chunks(16)
        {
        }

        ~Impl()
        {
            //cerr << "destroying streaming upload at " << object << endl;
            stop();
        }

        shared_ptr<S3Api> owner;
        std::string bucket;
        std::string object;
        S3Api::ObjectMetadata metadata;
        std::string uploadId;
        size_t offset;
        size_t chunkSize;
        size_t chunkIndex;
        atomic<bool> shutdown;
        std::vector<std::thread> tg;

        Date startDate;

        struct Chunk {
            Chunk() : data(nullptr)
            {
            }

            Chunk(Chunk && other)
                noexcept
            {
                this->offset = other.offset;
                this->size = other.size;
                this->capacity = other.capacity;
                this->index = other.index;
                this->data = other.data;

                other.data = nullptr;
            }

            Chunk & operator = (Chunk && other)
                noexcept
            {
                this->offset = other.offset;
                this->size = other.size;
                this->capacity = other.capacity;
                this->index = other.index;
                this->data = other.data;
                other.data = nullptr;

                return *this;
            }

            ~Chunk()
                noexcept
            {
                if (this->data) {
                    delete[] this->data;
                }
            }

            void init(uint64_t offset, size_t capacity, int index)
            {
                this->offset = offset;
                this->size = 0;
                this->capacity = capacity;
                this->index = index;
                this->data = new char[capacity];
            }

            size_t append(const char * input, size_t n)
            {
                size_t todo = std::min(n, capacity - size);
                std::copy(input, input + todo, data + size);
                size += todo;
                return todo;
            }

            char * data;
            size_t size;
            size_t capacity;
            int index;
            uint64_t offset;

        private:
            Chunk(const Chunk & other) {}
            Chunk & operator = (const Chunk & other) { return *this; }
        };

        Chunk current;

        ML::RingBufferSWMR<Chunk> chunks;

        std::mutex etagsLock;
        std::vector<std::string> etags;
        std::exception_ptr exc;
        OnUriHandlerException onException;

        void start()
        {
            shutdown = false;
            S3Api::MultiPartUpload upload;
            try {
                upload = owner->obtainMultiPartUpload(bucket, "/" + object,
                                                      metadata,
                                                      S3Api::UR_EXCLUSIVE);
            }
            catch (...) {
                onException();
                throw;
            }

            uploadId = upload.id;
            //cerr << "uploadId = " << uploadId << " with " << metadata.numThreads
            //<< "threads!!! " << endl;

            startDate = Date::now();
            for (unsigned i = 0;  i < metadata.numRequests;  ++i)
                tg.emplace_back(std::bind<void>(&Impl::runThread, this));
            current.init(0, chunkSize, 0);
        }

        void stop()
        {
            if (!shutdown) {
                shutdown = true;
                for (auto & t: tg)
                    t.join();
            }
        }

        std::streamsize write(const char_type* s, std::streamsize n)
        {
            if (exc)
                std::rethrow_exception(exc);

            size_t done = current.append(s, n);
            offset += done;
            if (done < n) {
                flush();
                done += current.append(s + done, n - done);
            }

            //cerr << "writing " << n << " characters returned "
            //     << done << endl;

            if (exc)
                std::rethrow_exception(exc);

            return done;
        }

        void flush()
        {
            if (current.size == 0) return;
            chunks.push(std::move(current));
            ++chunkIndex;

            // Get bigger for bigger files
            if (chunkIndex % 5 == 0 && chunkSize < 64 * 1024 * 1024)
                chunkSize *= 2;

            current.init(offset, chunkSize, chunkIndex);
        }

        void finish()
        {
            if (exc)
                std::rethrow_exception(exc);
            // cerr << "pushing last chunk " << chunkIndex << endl;
            flush();

            if (!chunkIndex) {
                chunks.push(std::move(current));
                ++chunkIndex;
            }

            //cerr << "waiting for everything to stop" << endl;
            chunks.waitUntilEmpty();
            //cerr << "empty" << endl;
            stop();
            //cerr << "stopped" << endl;

            // Make sure that an exception in uploading the last chunk doesn't
            // lead to a corrupt (truncated) file
            if (exc)
                std::rethrow_exception(exc);

            string etag;
            try {
                etag = owner->finishMultiPartUpload(bucket, "/" + object,
                                                    uploadId,
                                                    etags);
            }
            catch (...) {
                onException();
                throw;
            }
            //cerr << "final etag is " << etag << endl;

            if (exc)
                std::rethrow_exception(exc);

            // double elapsed = Date::now().secondsSince(startDate);

            // cerr << "uploaded " << offset / 1024.0 / 1024.0
            //      << "MB in " << elapsed << "s at "
            //      << offset / 1024.0 / 1024.0 / elapsed
            //      << "MB/s" << " to " << etag << endl;
        }

        void runThread()
        {
            while (!shutdown) {
                Chunk chunk;
                if (chunks.tryPop(chunk, 0.01)) {
                    if (exc)
                        return;
                    try {
                        //cerr << "got chunk " << chunk.index
                        //     << " with " << chunk.size << " bytes at index "
                        //     << chunk.index << endl;

                        // Upload the data
                        HttpRequestContent body(string(chunk.data, chunk.size));
                        auto putResult = owner->put(bucket, "/" + object,
                                                    ML::format("partNumber=%d&uploadId=%s",
                                                               chunk.index + 1, uploadId),
                                                    {}, {},
                                                    body);
                        if (putResult.code_ != 200) {
                            cerr << putResult.bodyXmlStr() << endl;

                            throw ML::Exception("put didn't work: %d", (int)putResult.code_);
                        }
                        string etag = putResult.getHeader("etag");
                        // cerr << "successfully uploaded part " << chunk.index
                        //     << " with etag " << etag << endl;

                        std::unique_lock<std::mutex> guard(etagsLock);
                        while (etags.size() <= chunk.index)
                            etags.push_back("");
                        etags[chunk.index] = etag;
                    } catch (...) {
                        // Capture exception to be thrown later
                        exc = std::current_exception();
                        onException();
                    }
                }
                else {
                    std::this_thread::sleep_for(std::chrono::milliseconds(10));
                }
            }
        }
    };

    std::shared_ptr<Impl> impl;

    std::streamsize write(const char_type* s, std::streamsize n)
    {
        return impl->write(s, n);
    }

    bool is_open() const
    {
        return !!impl;
    }

    void close()
    {
        impl->finish();
        impl.reset();
    }
};

std::unique_ptr<std::streambuf>
makeStreamingUpload(const std::string & uri,
                    const OnUriHandlerException & onException,
                    const S3Api::ObjectMetadata & metadata)
{
    std::unique_ptr<std::streambuf> result;
    result.reset(new boost::iostreams::stream_buffer<StreamingUploadSource>
                 (StreamingUploadSource(uri, onException, metadata),
                  131072));
    return result;
}

std::unique_ptr<std::streambuf>
makeStreamingUpload(const std::string & bucket,
                    const std::string & object,
                    const OnUriHandlerException & onException,
                    const S3Api::ObjectMetadata & metadata)
{
    return makeStreamingUpload("s3://" + bucket + "/" + object,
                               onException, metadata);
}

/** Register S3 with the filter streams API so that a filter_stream can be
    used to treat an S3 object as a simple stream.
*/
struct RegisterS3Handler {
    static UriHandler
    getS3Handler(const std::string & scheme,
                 const std::string & resource,
                 std::ios_base::open_mode mode,
                 const std::map<std::string, std::string> & options,
                 const OnUriHandlerException & onException)
    {
        string::size_type pos = resource.find('/');
        if (pos == string::npos)
            throw ML::Exception("unable to find s3 bucket name in resource "
                                + resource);
        string bucket(resource, 0, pos);

        if (mode == ios::in) {
            std::unique_ptr<std::streambuf> source;
            FsObjectInfo info;
            auto dl = makeStreamingDownload("s3://" + resource);
            source = std::move(dl.first);
            info = std::move(dl.second);
            std::shared_ptr<std::streambuf> buf(source.release());
            return UriHandler(buf.get(), buf, info);
        }
        else if (mode == ios::out) {

            S3Api::ObjectMetadata md;
            for (auto & opt: options) {
                string name = opt.first;
                string value = opt.second;
                if (name == "redundancy" || name == "aws-redundancy") {
                    if (value == "STANDARD")
                        md.redundancy = S3Api::REDUNDANCY_STANDARD;
                    else if (value == "REDUCED")
                        md.redundancy = S3Api::REDUNDANCY_REDUCED;
                    else throw ML::Exception("unknown redundancy value " + value
                                             + " writing S3 object " + resource);
                }
                else if (name == "contentType" || name == "aws-contentType") {
                    md.contentType = value;
                }
                else if (name == "contentEncoding" || name == "aws-contentEncoding") {
                    md.contentEncoding = value;
                }
                else if (name == "acl" || name == "aws-acl") {
                    md.acl = value;
                }
                else if (name == "mode" || name == "compression"
                         || name == "compressionLevel") {
                    // do nothing
                }
                else if (name.find("aws-") == 0) {
                    throw ML::Exception("unknown aws option " + name + "=" + value
                                        + " opening S3 object " + resource);
                }
                else if(name == "num-threads")
                {
                    cerr << ("warning: use of obsolete 'num-threads' option"
                             " key\n");
                    md.numRequests = std::stoi(value);
                }
                else if(name == "num-requests")
                {
                    md.numRequests = std::stoi(value);
                }
                else {
                    cerr << "warning: skipping unknown S3 option "
                         << name << "=" << value << endl;
                }
            }

            std::shared_ptr<std::streambuf> buf
                (makeStreamingUpload("s3://" + resource, onException, md).release());
            return UriHandler(buf.get(), buf);
        }
        else throw ML::Exception("no way to create s3 handler for non in/out");
    }

    RegisterS3Handler()
    {
        registerUriHandler("s3", getS3Handler);
    }

} registerS3Handler;

} // file scope
