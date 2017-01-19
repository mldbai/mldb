/** s3_handlers.cc
    Jeremy Barnes, 3 July 2012
    Wolfgang Sourdeau, 7 July 2012
    This file is part of MLDB.
    Copyright (c) 2012, 2016 mldb.ai inc.  All rights reserved.

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
#include "mldb/vfs_handlers/exception_ptr.h"
#include "mldb/soa/service/s3.h"

using namespace std;
using namespace MLDB;


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
                        throw MLDB::Exception("Options not accepted by S3");

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


/****************************************************************************/
/* S3 DOWNLOADER                                                            */
/****************************************************************************/

size_t getTotalSystemMemory()
{
    long pages = sysconf(_SC_PHYS_PAGES);
    long page_size = sysconf(_SC_PAGE_SIZE);
    return pages * page_size;
}

struct S3Downloader {
    S3Downloader(const S3Api * api,
                 const string & bucket,
                 const string & resource, // starts with "/", unescaped (buggy)
                 ssize_t startOffset = 0, ssize_t endOffset = -1)
        : api(api),
          bucket(bucket), resource(resource),
          offset(startOffset),
          baseChunkSize(1024*1024), // start with 1MB and ramp up
          closed(false),
          readOffset(0),
          readPartOffset(-1),
          currentChunk(0),
          requestedBytes(0),
          currentRq(0),
          activeRqs(0)
    {
        fileInfo = api->getObjectInfo(bucket, resource.substr(1));
        if (!fileInfo) {
            throw MLDB::Exception("missing object: " + resource);
        }

        if (endOffset == -1 || endOffset > fileInfo.size) {
            endOffset = fileInfo.size;
        }
        downloadSize = endOffset - startOffset;

        /* Maximum chunk size is what we can do in 3 seconds, up to 1% of
           system memory. */
        maxChunkSize = api->bandwidthToServiceMbps * 3.0 * 1000000;
        size_t sysMemory = getTotalSystemMemory();
        maxChunkSize = std::min(maxChunkSize, sysMemory / 100);

        /* The maximum number of concurrent requests is set depending on
           the total size of the stream. */
        maxRqs = 1;
        if (fileInfo.size > 1024 * 1024)
            maxRqs = 5;
        if (fileInfo.size > 16 * 1024 * 1024)
            maxRqs = 15;
        if (fileInfo.size > 256 * 1024 * 1024)
            maxRqs = 30;
        chunks.resize(maxRqs);

        /* Kick start the requests */
        ensureRequests();
    }

    ~S3Downloader()
    {
        /* We ensure at runtime that "close" is called because it is mandatory
           for the proper cleanup of active requests. Because "close" can
           throw, we cannot however call it from the destructor. */
        if (!closed) {
            cerr << "destroying S3Downloader without invoking close()\n";
            abort();
        }
    }

    std::streamsize read(char * s, std::streamsize n)
    {
        if (closed) {
            throw MLDB::Exception("invoking read() on a closed download");
        }

        if (endOfDownload()) {
            return -1;
        }

        if (readPartOffset == -1) {
            waitNextPart();
        }
        ensureRequests();

        size_t toDo = min<size_t>(readPart.size() - readPartOffset,
                                  n);
        const char * start = readPart.c_str() + readPartOffset;
        std::copy(start, start + toDo, s);

        readPartOffset += toDo;
        if (readPartOffset == readPart.size()) {
            readPartOffset = -1;
        }

        readOffset += toDo;

        return toDo;
    }

    uint64_t getDownloadSize()
        const
    {
        return downloadSize;
    }

    bool endOfDownload()
        const
    {
        return (readOffset == downloadSize);
    }

    void close()
    {
        closed = true;
        while (activeRqs > 0) {
            ML::futex_wait(activeRqs, activeRqs);
        }
        excPtrHandler.rethrowIfSet();
    }

    const FsObjectInfo & info()
        const
    {
        return fileInfo;
    }

private:
    /* download Chunk */
    struct Chunk {
        enum State {
            IDLE,
            QUERY,
            RESPONSE
        };

        Chunk() noexcept
            : state(IDLE)
        {
        }

        Chunk(Chunk && other) noexcept
            : state(other.state.load()),
              data(std::move(other.data))
        {
        }

        void setQuerying()
        {
            ExcAssertEqual(state, IDLE);
            setState(QUERY);
        }

        void assign(string newData)
        {
            ExcAssertEqual(state, QUERY);
            data = move(newData);
            setState(RESPONSE);
            ML::futex_wake(state);
        }

        std::string retrieve()
        {
            ExcAssertEqual(state, RESPONSE);
            string chunkData = std::move(data);
            setState(IDLE);
            return chunkData;
        }

        void setState(int newState)
        {
            state = newState;
            ML::futex_wake(state);
        }

        bool isIdle()
            const
        {
            return (state == IDLE);
        }

        bool waitResponse(double timeout)
            const
        {
            if (timeout > 0.0) {
                int old = state;
                if (state != RESPONSE) {
                    ML::futex_wait(state, old, timeout);
                }
            }

            return (state == RESPONSE);
        }

    private:
        std::atomic<int> state;
        string data;
    };

    void waitNextPart()
    {
        unsigned int chunkNr(currentChunk % maxRqs);
        Chunk & chunk = chunks[chunkNr];
        while (!excPtrHandler.hasException() && !chunk.waitResponse(1.0));
        excPtrHandler.rethrowIfSet();
        readPart = chunk.retrieve();
        readPartOffset = 0;
        currentChunk++;
    }

    void ensureRequests()
    {
        while (true) {
            if (excPtrHandler.hasException()) {
                break;
            }
            if (activeRqs == maxRqs) {
                break;
            }
            ExcAssert(activeRqs < maxRqs);
            if (requestedBytes == downloadSize) {
                break;
            }
            ExcAssert(requestedBytes < downloadSize);

            Chunk & chunk = chunks[currentRq % maxRqs];
            if (!chunk.isIdle()) {
                break;
            }

            ensureRequest();
        }
    }

    void ensureRequest()
    {
        size_t chunkSize = getChunkSize(currentRq);
        uint64_t end = requestedBytes + chunkSize;
        if (end > fileInfo.size) {
            end = fileInfo.size;
            chunkSize = end - requestedBytes;
        }

        unsigned int chunkNr = currentRq % maxRqs;
        Chunk & chunk = chunks[chunkNr];
        activeRqs++;
        chunk.setQuerying();

        auto onResponse
            = [&, chunkNr, chunkSize] (S3Api::Response && response,
                                       std::exception_ptr excPtr) {
            this->handleResponse(chunkNr, chunkSize, std::move(response), excPtr);
        };
        S3Api::Range range(offset + requestedBytes, chunkSize);
        api->getAsync(onResponse, bucket, resource, range);
        ExcAssertLess(currentRq, UINT_MAX);
        currentRq++;
        requestedBytes += chunkSize;
    }

    void handleResponse(unsigned int chunkNr, size_t chunkSize,
                        S3Api::Response && response,
                        std::exception_ptr excPtr)
    {
        try {
            if (excPtr) {
                rethrow_exception(excPtr);
            }

            if (response.code_ != 200 && response.code_ != 206) {
                throw MLDB::Exception("http error "
                                    + to_string(response.code_)
                                    + " while getting chunk "
                                    + response.bodyXmlStr());
            }

            /* It can sometimes happen that a file changes during download i.e
               it is being overwritten. Make sure we check for this condition
               and throw an appropriate exception. */
            string chunkEtag = response.getHeader("etag");
            if (chunkEtag != fileInfo.etag) {
                throw MLDB::Exception("chunk etag '%s' differs from original"
                                    " etag '%s' of file '%s'",
                                    chunkEtag.c_str(), fileInfo.etag.c_str(),
                                    resource.c_str());
            }
            ExcAssertEqual(response.body().size(), chunkSize);
            Chunk & chunk = chunks[chunkNr];
            chunk.assign(std::move(response.body_));
        }
        catch (const std::exception & exc) {
            excPtrHandler.takeCurrentException();
        }
        activeRqs--;
        ML::futex_wake(activeRqs);
    }

    size_t getChunkSize(unsigned int chunkNbr)
        const
    {
        size_t chunkSize = std::min(baseChunkSize * (1 << (chunkNbr / 2)),
                                    maxChunkSize);
        return chunkSize;
    }

    /* static variables, set during or right after construction */
    const S3Api * api;
    std::string bucket;
    std::string resource;
    S3Api::ObjectInfo fileInfo;
    uint64_t offset; /* the lower position in the file from which the download
                      * is started */
    uint64_t downloadSize; /* total number of bytes to download */
    size_t baseChunkSize;
    size_t maxChunkSize;

    bool closed; /* whether close() was invoked */
    ExceptionPtrHandler excPtrHandler; /* TODO: use promise/future instead */

    /* read thread */
    uint64_t readOffset; /* number of bytes from the entire stream that
                          * have been returned to the caller */
    string readPart; /* data buffer for the part of the stream being
                      * transferred to the caller */
    ssize_t readPartOffset; /* number of bytes from "readPart" that have
                             * been returned to the caller, or -1 when
                             * awaiting a new part */
    unsigned int currentChunk; /* chunk being read */

    /* http requests */
    unsigned int maxRqs; /* maximum number of concurrent http requests */
    uint64_t requestedBytes; /* total number of bytes that have been
                              * requested, including the non-received ones */
    vector<Chunk> chunks; /* chunks */
    unsigned int currentRq;  /* number of done requests */
    atomic<unsigned int> activeRqs; /* number of pending http requests */
};


/****************************************************************************/
/* STREAMING DOWNLOAD SOURCE                                                */
/****************************************************************************/

struct StreamingDownloadSource {
    StreamingDownloadSource(const std::string & urlStr)
    {
        owner = getS3ApiForUri(urlStr);

        string bucket, resource;
        std::tie(bucket, resource) = S3Api::parseUri(urlStr);
        downloader.reset(new S3Downloader(owner.get(),
                                          bucket, "/" + resource));
    }

    const FsObjectInfo & info()
    {
        ExcAssert(downloader);
        return downloader->info();
    }

    typedef char char_type;
    struct category
        : //input_seekable,
        boost::iostreams::input,
        boost::iostreams::device_tag,
        boost::iostreams::closable_tag
    { };

    std::streamsize read(char_type * s, std::streamsize n)
    {
        return downloader->read(s, n);
    }

    bool is_open() const
    {
        return !!downloader;
    }

    void close()
    {
        downloader->close();
        downloader.reset();
    }

private:
    std::shared_ptr<S3Api> owner;
    std::shared_ptr<S3Downloader> downloader;
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


/****************************************************************************/
/* S3 UPLOADER                                                              */
/****************************************************************************/

/* The "touch" functions below enable to preload "mmapped" files into RAM
 * buffers, which significantly improves the performance of their upload. */
inline void touchByte(const char * c)
{
    __asm__(" # [in]":: [in] "r" (*c):);
}

inline void touch(const char * start, size_t size)
{
    const char * current = start - (intptr_t) start % 4096;
    if (current < start) {
        current += 4096;
    }
    const char * end = start + size;
    for (; current < end; current += 4096) {
        touchByte(current);
    }
}


struct S3Uploader {
    S3Uploader(const S3Api * api,
               const string & bucket,
               const string & resource, // starts with "/", unescaped (buggy)
               const OnUriHandlerException & excCallback,
               const S3Api::ObjectMetadata & objectMetadata)
        : api(api),
          bucket(bucket), resource(resource),
          metadata(objectMetadata),
          onException(excCallback),
          closed(false),
          chunkSize(8 * 1024 * 1024), // start with 8MB and ramp up
          currentRq(0),
          activeRqs(0)
    {
        /* Maximum chunk size is what we can do in 3 seconds, up to 1% of
           system memory. */
        maxChunkSize = api->bandwidthToServiceMbps * 3.0 * 1000000;
        size_t sysMemory = getTotalSystemMemory();
        maxChunkSize = std::min(maxChunkSize, sysMemory / 100);

        try {
            S3Api::MultiPartUpload upload
              = api->obtainMultiPartUpload(bucket, resource, metadata,
                                           S3Api::UR_EXCLUSIVE);
            uploadId = upload.id;
        }
        MLDB_CATCH_ALL {
            if (onException) {
                onException();
            }
            throw;
        }
    }

    ~S3Uploader()
    {
        /* We ensure at runtime that "close" is called because it is mandatory
           for the proper cleanup of active requests. Because "close" can
           throw, we cannot however call it from the destructor. */
        if (!closed) {
            cerr << "destroying S3Uploader without invoking close()\n";
            abort();
        }
    }

    std::streamsize write(const char * s, std::streamsize n)
    {
        std::streamsize done(0);

        touch(s, n);

        size_t remaining = chunkSize - current.size();
        while (n > 0) {
            if (excPtrHandler.hasException() && onException) {
                onException();
            }
            excPtrHandler.rethrowIfSet();
            size_t toDo = min(remaining, (size_t) n);
            if (toDo < n) {
                flush();
                remaining = chunkSize - current.size();
            }
            current.append(s, toDo);
            s += toDo;
            n -= toDo;
            done += toDo;
            remaining -= toDo;
        }

        return done;
    }

    void flush(bool force = false)
    {
        if (!force) {
            ExcAssert(current.size() > 0);
        }
        while (activeRqs == metadata.numRequests) {
            ML::futex_wait(activeRqs, activeRqs);
        }
        if (excPtrHandler.hasException() && onException) {
            onException();
        }
        excPtrHandler.rethrowIfSet();

        unsigned int rqNbr(currentRq);
        auto onResponse = [&, rqNbr] (S3Api::Response && response,
                                      std::exception_ptr excPtr) {
            this->handleResponse(rqNbr, std::move(response), excPtr);
        };

        unsigned int partNumber = currentRq + 1;
        if (etags.size() < partNumber) {
            etags.resize(partNumber);
        }

        activeRqs++;
        api->putAsync(onResponse, bucket, resource,
                      MLDB::format("partNumber=%d&uploadId=%s",
                                 partNumber, uploadId),
                      {}, {}, current);

        if (currentRq % 5 == 0 && chunkSize < maxChunkSize)
            chunkSize *= 2;

        current.clear();
        currentRq = partNumber;
    }

    void handleResponse(unsigned int rqNbr,
                        S3Api::Response && response,
                        std::exception_ptr excPtr)
    {
        try {
            if (excPtr) {
                rethrow_exception(excPtr);
            }

            if (response.code_ != 200) {
                cerr << response.bodyXmlStr() << endl;
                throw MLDB::Exception("put didn't work: %d", (int)response.code_);
            }

            string etag = response.getHeader("etag");
            ExcAssert(etag.size() > 0);
            etags[rqNbr] = etag;
        }
        catch (const std::exception & exc) {
            excPtrHandler.takeCurrentException();
        }
        activeRqs--;
        ML::futex_wake(activeRqs);
    }

    string close()
    {
        closed = true;
        if (current.size() > 0) {
            flush();
        }
        else if (currentRq == 0) {
            /* for empty files, force the creation of a single empty part */
            flush(true);
        }
        while (activeRqs > 0) {
            ML::futex_wait(activeRqs, activeRqs);
        }
        if (excPtrHandler.hasException() && onException) {
            onException();
        }
        excPtrHandler.rethrowIfSet();

        string finalEtag;
        try {
            finalEtag = api->finishMultiPartUpload(bucket, resource,
                                                   uploadId, etags);
        }
        MLDB_CATCH_ALL {
            if (onException) {
                onException();
            }
            throw;
        }

        return finalEtag;
    }

private:
    const S3Api * api;
    std::string bucket;
    std::string resource;
    S3Api::ObjectMetadata metadata;
    OnUriHandlerException onException;

    size_t maxChunkSize;
    std::string uploadId;

    /* state variables, used between "start" and "stop" */
    bool closed; /* whether close() was invoked */
    ExceptionPtrHandler excPtrHandler; /* TODO: use promise/future instead */

    string current; /* current chunk data */
    size_t chunkSize; /* current chunk size */
    std::vector<std::string> etags; /* etags of individual chunks */
    unsigned int currentRq;  /* number of done requests */
    atomic<unsigned int> activeRqs; /* number of pending http requests */
};


/****************************************************************************/
/* STREAMING UPLOAD SOURCE                                                  */
/****************************************************************************/

struct StreamingUploadSource {
    StreamingUploadSource(const std::string & urlStr,
                          const OnUriHandlerException & excCallback,
                          const S3Api::ObjectMetadata & metadata)
        : owner(getS3ApiForUri(urlStr))
    {
        string bucket, resource;
        std::tie(bucket, resource) = S3Api::parseUri(urlStr);
        uploader.reset(new S3Uploader(owner.get(), bucket, "/" + resource,
                                      excCallback, metadata));
    }

    typedef char char_type;
    struct category
        : public boost::iostreams::output,
          public boost::iostreams::device_tag,
          public boost::iostreams::closable_tag
    {
    };

    std::streamsize write(const char_type* s, std::streamsize n)
    {
        return uploader->write(s, n);
    }

    bool is_open() const
    {
        return !!uploader;
    }

    void close()
    {
        uploader->close();
        uploader.reset();
    }

private:
    std::shared_ptr<S3Api> owner;
    std::shared_ptr<S3Uploader> uploader;
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
            throw MLDB::Exception("unable to find s3 bucket name in resource "
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
                    else throw MLDB::Exception("unknown redundancy value " + value
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
                    throw MLDB::Exception("unknown aws option " + name + "=" + value
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
        else throw MLDB::Exception("no way to create s3 handler for non in/out");
    }

    RegisterS3Handler()
    {
        registerUriHandler("s3", getS3Handler);
    }

} registerS3Handler;

} // file scope
