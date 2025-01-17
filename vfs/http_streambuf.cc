/** http_streambuf.cc
    Jeremy Barnes, 26 November 2014
    Copyright (c) 2014 mldb.ai inc.  All rights reserved.

    This file is part of MLDB. Copyright 2015 mldb.ai inc. All rights reserved.
*/

#include <atomic>
#include "mldb/http/http_rest_proxy.h"
#include "mldb/vfs/filter_streams_registry.h"
#include "mldb/vfs/fs_utils.h"
#include "mldb/types/annotated_exception.h"
#include "mldb/types/basic_value_descriptions.h"
#include "mldb/vfs/exception_ptr.h"
#include <chrono>
#include <future>
#include "mldb/ext/concurrentqueue/blockingconcurrentqueue.h"
#include "mldb/base/iostream_adaptors.h"

using namespace std;
using moodycamel::BlockingConcurrentQueue;


namespace MLDB {

static FsObjectInfo
convertHeaderToInfo(const HttpHeader & header)
{
    FsObjectInfo result;

    if (header.responseCode() == 200) {
        result.exists = true;
        result.etag = header.tryGetHeader("etag");
        result.size = header.contentLength;
        result.contentType = header.contentType;
        string lastModified = header.tryGetHeader("last-modified");
        if (!lastModified.empty()) {
            static const char format[] = "%a, %d %b %Y %H:%M:%S %Z"; // rfc 1123
            struct tm tm;
            bzero(&tm, sizeof(tm));
            if (strptime(lastModified.c_str(), format, &tm)) {
                result.lastModified = Date(1900 + tm.tm_year, tm.tm_mon + 1, tm.tm_mday,
                                           tm.tm_hour, tm.tm_min, tm.tm_sec);
            }
        }
        else result.lastModified = Date::notADate();

        for (auto & h: header.headers) {
            result.objectMetadata[h.first] = h.second;
        }

        return result;
    }

    if (header.responseCode() >= 400 && header.responseCode() < 600) {
        result.exists = false;
        return result;
    }

    throw AnnotatedException(header.responseCode(),
                              "Unable to convert unknown header code "
                              + to_string(header.responseCode()) +
                              " to object info",
                              "code", header.responseCode());
}

struct HttpStreamingDownloadSource {

    /** Create a streaming download source.

        For options, the following is accepted:
        http-set-cookie: sets the given header in the request to the given value.
        httpArbitraryTooSlowAbort: Will abort if the connection speed is below
                                   10K/sec for 5 secs.
    */
    HttpStreamingDownloadSource(const Utf8String & urlStr,
                                const std::map<std::string, std::string> & options,
                                const OnUriHandlerException & onException)
    {
        impl.reset(new Impl(urlStr, options));
        impl->start();
        impl->onException = onException;
    }

    ~HttpStreamingDownloadSource()
    {
    }

    /** Wait for the HTTP header to be available from the connection, and
        return it.
    */
    HttpHeader getHeader() const
    {
        auto future = impl->headerPromise.get_future();
        return future.get();
    }

    typedef char char_type;
#if 0
    struct category
        :
        boost::iostreams::input_seekable,
        boost::iostreams::device_tag,
        boost::iostreams::closable_tag,
        boost::iostreams::multichar_tag
    { };
#endif

    using is_source = std::true_type;
    using is_seekable = std::true_type;

    struct Impl {
        Impl(const Utf8String & urlStr,
             const std::map<std::string, std::string> & options)
            : proxy(urlStr.extractAscii()), urlStr(urlStr), shutdown(false), dataQueue(100),
              eof(false), currentDone(0), headerSet(false),
              httpAbortOnSlowConnection(false)
        {
            for (auto & o: options) {
                if (o.first == "http-set-cookie")
                    proxy.setCookie(o.second);
                else if (o.first.find("http-") == 0)
                    throw AnnotatedException
                        (500,
                         "Unknown HTTP stream parameter '"
                         + o.first + " = " + o.second + "'");
                else if (o.first == "httpAbortOnSlowConnection" && o.second == "true") {
                    httpAbortOnSlowConnection = true;
                }
            }
            
            reset();
        }

        ~Impl()
        {
            stop();
        }

        HttpRestProxy proxy;
        Utf8String urlStr;

        atomic<bool> shutdown;
        exception_ptr lastExc;

        /* Data queue */
        BlockingConcurrentQueue<string> dataQueue;
        atomic<bool> eof;

        uint64_t currentStartOffset = 0;
        std::string current;
        size_t currentDone;

        vector<std::thread> threads; /* thread pool */

        std::atomic<bool> headerSet;
        std::promise<HttpHeader> headerPromise;

        bool httpAbortOnSlowConnection;

        ExceptionPtrHandler excPtrHandler;
        OnUriHandlerException onException;

        /* cleanup all the variables that are used during reading, the
           "static" ones are left untouched */
        void reset()
        {
            shutdown = false;
            current = "";
            currentDone = 0;
            currentStartOffset = 0;
            threads.clear();
        }

        void start()
        {
            threads.emplace_back(&Impl::runThread, this);
        }

        void stop()
        {
            shutdown = true;


            dataQueue.enqueue("");

            for (thread & th: threads) {
                th.join();
            }

            threads.clear();

            if (!headerSet) {
                if (lastExc)
                    headerPromise.set_exception(lastExc);
                else headerPromise.set_value(HttpHeader());
            }
        }

        /* reader thread */
        std::streamsize read(char_type* s, std::streamsize n)
        {
            if (lastExc) {
                rethrow_exception(lastExc);
            }

            if (eof)
                return -1;

            if (currentDone == current.size()) {
                currentStartOffset += currentDone;

                // Get some more data
                dataQueue.wait_dequeue(current);
                currentDone = 0;

                if (current.empty()) {
                    if (lastExc) rethrow_exception(lastExc);
                    eof = true;
                    return -1;  // shutdown or empty
                }
            }
            
            if (lastExc) {
                rethrow_exception(lastExc);
            }
            
            size_t toDo = min<size_t>(current.size() - currentDone, n);
            const char_type * start = current.c_str() + currentDone;
            std::copy(start, start + toDo, s);
            
            currentDone += toDo;

            return toDo;
        }

        std::streampos seek(std::streamsize where, std::ios_base::seekdir dir)
        {
            // If we seek to where we already are, return where we are
            if (dir == std::ios_base::cur && where == 0)
                return currentStartOffset + currentDone;
            throw Exception("http streambuf can't seek");
        }
        
        void runThread()
        {
            try {
                int errorCode =-1;
                std::string errorBody;
                bool error = false;
                auto onData = [&] (const std::string & data)
                    {
                        if (error) {
                            errorBody = data;
                            return true;
                        }
                        if (shutdown) {
                            return false;
                        }
                        while (!shutdown && !dataQueue.try_enqueue(data)) {
                            std::this_thread::sleep_for(std::chrono::milliseconds(1));
                        }
                        return !shutdown;
                    };

                auto isRedirect = [](const HttpHeader & header) 
                    {
                        return header.responseCode() >= 300
                        && header.responseCode() < 400;
                    };

                auto onHeader = [&] (const HttpHeader & header)
                    {
                        // Don't set the promise on a 3xx... it's a redirect
                        // and we will get the correct header later on
                        if (!isRedirect(header)) {
                            if (headerSet) {
                                throw std::logic_error("set header twice");
                            }
                            
                            if (!headerSet.exchange(true)) {
                                this->headerPromise.set_value(header);
                            }
                        }

                        if (shutdown) {
                            return false;
                        }

                        //cerr << "got header " << header << endl;
                        errorCode = header.responseCode();

                        if (header.responseCode() != 200 && !isRedirect(header)) {
                            error = true;
                        }

                        return !shutdown;
                    };

                auto resp = proxy.get("", {}, {}, -1 /* timeout */,
                                      false /* exceptions */,
                                      onData, onHeader,
                                      true /* follow redirect */,
                                      httpAbortOnSlowConnection);
                
                if (shutdown)
                    return;

                if (resp.code() != 200) {
                    if (resp.code() == 0) {
                        throw AnnotatedException
                            (400, "HTTP error reading " + urlStr + "\n\n"
                             + resp.errorMessage());
                    }
                    else {
                        throw AnnotatedException
                            (400, "HTTP code " + to_string(resp.code())
                             + " reading " + urlStr + "\n\n"
                             + string(errorBody, 0, 1024));
                    }
                }
                
                dataQueue.enqueue("");
                
            } catch (const std::exception & exc) {
                lastExc = std::current_exception();
                dataQueue.enqueue("");
                if (!headerSet.exchange(true)) {
                    headerPromise.set_exception(lastExc);
                }
                else {
                    excPtrHandler.takeCurrentException();
                }
            }
        }

    };

    std::shared_ptr<Impl> impl;

    std::streamsize read(char_type* s, std::streamsize n)
    {
        return impl->read(s, n);
    }

    std::streampos seek(std::streamsize where, std::ios_base::seekdir dir)
    {
        return impl->seek(where, dir);
    }        
    
    bool is_open() const
    {
        return !!impl;
    }

    void close()
    {
        if (impl->excPtrHandler.hasException() && impl->onException) {
            impl->onException(impl->excPtrHandler.getException());
        }
        impl->excPtrHandler.rethrowIfSet();
        impl.reset();
    }
};

std::pair<std::unique_ptr<std::streambuf>, FsObjectInfo>
makeHttpStreamingDownload(const Utf8String & uri,
                          const std::map<std::string, std::string> & options,
                          const OnUriHandlerException & onException)
{
    std::unique_ptr<std::streambuf> result;
    HttpStreamingDownloadSource source(uri, options, onException);
    const HttpHeader & header = source.getHeader();
    result.reset(new source_istreambuf<HttpStreamingDownloadSource>
                 (source, 131072));
    return { std::move(result), convertHeaderToInfo(header) };
}

struct HttpUrlFsHandler: UrlFsHandler {
    HttpRestProxy proxy;

    virtual FsObjectInfo getInfo(const Url & url) const
    {
        auto info = tryGetInfo(url);
        if (!info)
            throw MLDB::Exception("Couldn't get URI info for " + url.toString());
        return info;
    }

    virtual FsObjectInfo tryGetInfo(const Url & url) const
    {
        HttpHeader header;
        FsObjectInfo result;
        HttpRestProxy::Response resp;
        bool didGetHeader = false;

        for (unsigned attempt = 0;  attempt < 5;  ++attempt) {

            if (attempt != 0)
                std::this_thread::sleep_for(std::chrono::milliseconds(100 * attempt + random() % 100));
            didGetHeader = false;

            auto onHeader = [&] (const HttpHeader & gotHeader)
                {
                    header = gotHeader;
                    didGetHeader = true;

                    // Return false to make CURL stop after the header
                    //return false;
                    return true;
                };
        
            resp = proxy.perform("HEAD", url.toEncodedAsciiString(),
                                 HttpRestProxy::Content(),
                                 {}, {}, 1.0, false, nullptr, onHeader,
                                 true /* follow redirects */);
            
            if (!didGetHeader && resp.errorCode() != 0) {
                cerr << "error retrieving HEAD (retry) " << url.toString() << ": "
                     << resp.errorMessage() << endl;
                continue;  // didn't get header; retry
            }
        
#if 0
            cerr << "header = " << header << endl;
            cerr << "resp = " << resp << endl;
            cerr << "resp.responseCode = " << resp.code_ << endl;
            cerr << "resp.errorCode = " << resp.errorCode() << endl;
            cerr << "resp.errorMessage = " << resp.errorMessage() << endl;
            cerr << "header.responseCode() = " << header.responseCode() << endl;
#endif

            if (header.responseCode() >= 200 && header.responseCode() < 500) {
                return convertHeaderToInfo(header);
            }

            if (header.responseCode() >= 500 && header.responseCode() < 600) {
                continue;
            }

            cerr << "don't know what to do with response code "
                 << header.responseCode()
                 << " from HEAD" << endl;
        }

        throw MLDB::Exception("Couldn't reach server to determine HEAD of '"
                            + url.toString() + "': HTTP code "
                            + (didGetHeader ? to_string(header.responseCode()) : string("(unknown)"))
                            + " " + resp.errorMessage());

        //if (resp.hasHeader("content-type"))
        //    result.contentType = resp.getHeader("content-type");
        
        //cerr << "result = " << result.lastModified << endl;

        return result;
    }

    virtual size_t getSize(const Url & url) const
    {
        return getInfo(url).size;
    }

    virtual std::string getEtag(const Url & url) const
    {
        return getInfo(url).etag;
    }

    virtual void makeDirectory(const Url & url) const
    {
        // no-op
    }

    virtual bool erase(const Url & url, bool throwException) const
    {
        throw MLDB::Exception("Http URIs don't support DELETE");
    }

    /** For each object under the given prefix (object or subdirectory),
        call the given callback.
    */
    virtual bool forEach(const Url & prefix,
                         const OnUriObject & onObject,
                         const OnUriSubdir & onSubdir,
                         const std::string & delimiter,
                         const std::string & startAt) const
    {
        throw MLDB::Exception("Http URIs don't support listing");
    }
};

/** Register Http with the filter streams API so that a filter_stream can be
    used to treat an Http object as a simple stream.
*/
struct RegisterHttpHandler {
    static UriHandler
    getHttpHandler(const std::string & scheme,
                   const Utf8String & resource,
                   std::ios_base::openmode mode,
                   const std::map<std::string, std::string> & options,
                   const OnUriHandlerException & onException)
    {
        if (mode == ios::in) {
            std::pair<std::unique_ptr<std::streambuf>, FsObjectInfo> sb_info
                = makeHttpStreamingDownload(scheme+"://"+resource, options,
                                            onException);
            std::shared_ptr<std::streambuf> buf(sb_info.first.release());
            return UriHandler(buf.get(), buf, sb_info.second);
        }
        else if (mode == ios::out) {
            throw MLDB::Exception("Can't currently upload files via HTTP/HTTPs");
        }
        else throw MLDB::Exception("no way to create http handler for non in/out");
    }
    
    RegisterHttpHandler()
    {
        registerUriHandler("http", getHttpHandler);
        registerUriHandler("https", getHttpHandler);

        registerUrlFsHandler("http", new HttpUrlFsHandler());
        registerUrlFsHandler("https", new HttpUrlFsHandler());
    }

} registerHttpHandler;

} // namespace MLDB
