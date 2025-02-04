/* filter_streams.cc
   Jeremy Barnes, 17 March 2005
   Copyright (c) 2005 Jeremy Barnes.  All rights reserved.
   This file is part of MLDB. Copyright 2015 mldb.ai inc. All rights reserved.
   
   This file is part of "Jeremy's Machine Learning Library", copyright (c)
   1999-2015 Jeremy Barnes.
   
   Apache 2.0 license.

   ---
   
   Implementation of filter streams.
*/

#include "mldb/vfs/filter_streams.h"
#include "mldb/vfs/filter_streams_registry.h"
#include "compressor.h"
#include <fstream>
#include <mutex>
#include "mldb/arch/exception.h"
#include <errno.h>
#include <sstream>
#include <thread>
#include <unordered_map>
#include "fs_utils.h"
#include "mldb/base/exc_assert.h"
#include "mldb/base/scope.h"
#include "mldb/utils/lexical_cast.h"
#include "mldb/utils/split.h"
#include "mldb/arch/vm.h"
#include "mldb/base/iostream_adaptors.h"
#include <unistd.h>

using namespace std;

namespace MLDB {


/*****************************************************************************/
/* URI HANDLING                                                              */
/*****************************************************************************/

const UriHandlerFactory &
getUriHandler(const std::string & scheme);

std::pair<std::string, Utf8String>
getScheme(const Utf8String & uri)
{
    auto [scheme, resource, found] = split_on_first(uri, "://");
    if (!found)
        return make_pair("file", std::move(scheme));

    return make_pair(scheme.stealAsciiString(), resource);
}

UriHandler::
UriHandler(std::streambuf * buf,
           std::shared_ptr<void> bufOwnership,
           const FsObjectInfo & info,
           UriHandlerOptions options)
    : buf(buf),
      bufOwnership(std::move(bufOwnership)),
      options(std::move(options))
{
    this->info.reset(new FsObjectInfo(info));
}


/*****************************************************************************/
/* STREAM COMPRESSOR                                                         */
/*****************************************************************************/

struct StreamCompressor {
    using is_filter = std::true_type;

    StreamCompressor(Compressor * compressor, std::streambuf & buf)
        : compressor(compressor), buf(buf)
    {
        // Record the start position so that once we know the actual written
        // size we can go back and rewrite the header with the right size.
        if (compressor->canFixupLength()) {
            try {
                MLDB_TRACE_EXCEPTIONS(false);
                startPos = buf.pubseekoff(0, ios::cur, ios_base::in);
            } catch (std::ios_base::failure exc) {
                startPos = -1;
            }
        }
    }

    template<typename Sink>
    void writeAll(Sink& sink, const char* data, size_t size)
    {
        while (size > 0) {
            size_t written = write_stream(sink, data, size);
            if (!written) throw Exception("unable to write data");
            
            data += written;
            size -= written;
            bytesWritten += written;
        }
    }

    template<typename Sink>
    std::streamsize write(Sink& sink, const char* s, std::streamsize n)
    {
        auto onData = [&] (const char * data, size_t len) -> size_t
            {
                writeAll(sink, data, len);
                return len;
            };
        
        compressor->compress(s, n, onData);
        bytesRead += n;
        return n;
    }

    template<typename Sink>
    void close(Sink& sink)
    {
        if (alreadyClosed)
            return;

        auto onData = [&] (const char * data, size_t len) -> size_t
            {
                writeAll(sink, data, len);
                return len;
            };
        
        if (bytesRead == 0) {
            compressor->notifyInputSize(0);            
        }
        compressor->finish(onData);

        if (bytesRead != 0 && startPos != -1 && compressor->canFixupLength()) {
            std::string data = compressor->newHeaderForLength(bytesRead);
            {
                std::ostream stream(&buf);
                auto oldPos = stream.tellp();
                stream.seekp(startPos, std::ios::beg);
                stream.write(data.data(), data.size());
                stream.flush();
                stream.seekp(oldPos, std::ios::beg);
            }
        }

        alreadyClosed = true;
    }

    template<typename Sink>
    void flush(Sink& sink)
    {
        auto onData = [&] (const char * data, size_t len) -> size_t
            {
                writeAll(sink, data, len);
                return len;
            };
        
        compressor->flush(Compressor::FLUSH_AVAILABLE, onData);
    }

    std::shared_ptr<Compressor> compressor;
    std::streambuf & buf;
    uint64_t bytesWritten = 0;
    uint64_t bytesRead = 0;
    int64_t startPos = -1;
    bool alreadyClosed = false;
};


/*****************************************************************************/
/* STREAM DECOMPRESSOR                                                        */
/*****************************************************************************/

/** Adaptor to allow iostreams to be served by a decompressor object. */

struct StreamDecompressor {
    using is_filter = std::true_type;
    typedef char char_type;
    
    StreamDecompressor(Decompressor * decompressor)
        : decompressor(decompressor)
    {
        inbuf.resize(4096);
    }

    template<typename Source>
    std::streamsize read(Source& src, char* s, std::streamsize n)
    {
        auto sBefore = s;

        // First, return any buffered characters from outbuf
        auto numBuffered = std::min<std::streamsize>(n, outbuf.size() - outbufPos);
        std::copy(outbuf.data() + outbufPos,
                  outbuf.data() + outbufPos + numBuffered,
                  s);
        s += numBuffered;
        n -= numBuffered;
        streamPos += numBuffered;
        outbufPos += numBuffered;

        // Clear outbuf buffer once it's all used up
        if (outbufPos == outbuf.size()) {
            outbuf.resize(0);
            outbufPos = 0;
        }

        if (n == 0) {
            if (s == sBefore && eof)
                return EOF;
            return s - sBefore;
        }

        // Now, call the decompressor

        auto onData = [&] (const char * data, size_t dataLength)
            {
                // Put as much as can fit in the output
                auto numGenerated = std::min<std::streamsize>(dataLength, n);
                std::copy(data, data + numGenerated, s);
                s += numGenerated;
                n -= numGenerated;
                streamPos += numGenerated;

                // Everything else gets buffered for next time
                outbuf.append(data + numGenerated, dataLength - numGenerated);

                return dataLength;  // we always consume all of the characters
            };
        
        while (n > 0) {
            ssize_t numRead = read_stream(src, inbuf.data(), inbuf.size());
            if (numRead <= 0) {
                decompressor->finish(onData);
                eof = true;
                break;
            }
            else {
                decompressor->decompress(inbuf.data(), numRead, onData);
            }
        }

        return (s == sBefore && eof) ? EOF : s - sBefore;
    }
    
    template<typename Source>
    int get(Source & src)
    {
        char result;
        auto res = read(src, &result, 1);
        //cerr << "reading single char got " << res << " characters" << endl;
        if (res < 1)
            return -1;
        //cerr << "  returning " << (int)result << "'" << result << "' with pos now "
        //     << streamPos << endl;
        return result;
    }
    
    // Basic implementation of seek that will return the current position so
    // that gcount() and tellg() will work.
    template<typename Source>
    std::streampos seek(Source& src, std::streamsize where,
                        std::ios_base::seekdir dir)
    {
        if (dir == std::ios_base::cur && where == 0) {
            return streamPos;
        }
        throw Exception("decompressing streambuf can't seek");
    }
    
    std::vector<char> inbuf;  ///< Characters read from input but not yet passed to decompressor
    std::string outbuf; ///< Characters returned from decompressor but not yet written
    size_t outbufPos = 0;   ///< Position in outbuf
    uint64_t streamPos = 0;
    bool eof = false;

    std::shared_ptr<Decompressor> decompressor;
};



/*****************************************************************************/
/* FILTER_OSTREAM                                                            */
/*****************************************************************************/

filter_ostream::filter_ostream()
    : ostream(std::cout.rdbuf()), deferredFailure(false)
{
}

filter_ostream::
filter_ostream(filter_ostream && other) noexcept
    : ostream(other.rdbuf()),
    stream(std::move(other.stream)),
    sink(std::move(other.sink)),
    deferredFailure(other.deferredFailure.load()),
    deferredExcPtr(std::move(other.deferredExcPtr)),
    options(std::move(other.options))
{
    other.deferredFailure = false;
}

filter_ostream::
filter_ostream(const Utf8String & file, std::ios_base::openmode mode,
               const std::string & compression, int level)
    : ostream(std::cout.rdbuf()), deferredFailure(false)
{
    open(file, mode, compression, level);
}

filter_ostream::
filter_ostream(const Url & file, std::ios_base::openmode mode,
               const std::string & compression, int level)
    : ostream(std::cout.rdbuf()), deferredFailure(false)
{
    open(file, mode, compression, level);
}


filter_ostream::
filter_ostream(int fd, std::ios_base::openmode mode,
               const std::string & compression, int level)
    : ostream(std::cout.rdbuf()), deferredFailure(false)
{
    open(fd, mode, compression, level);
}

filter_ostream::
filter_ostream(int fd,
               const std::map<std::string, std::string> & options)
    : ostream(std::cout.rdbuf()), deferredFailure(false)
{
    open(fd, options);
}

filter_ostream::
filter_ostream(const Utf8String & uri,
               const std::map<std::string, std::string> & options)
    : ostream(std::cout.rdbuf()), deferredFailure(false)
{
    open(uri, options);
}

filter_ostream::
filter_ostream(const Url & uri,
               const std::map<std::string, std::string> & options)
    : ostream(std::cout.rdbuf()), deferredFailure(false)
{
    open(uri, options);
}

filter_ostream::
~filter_ostream()
{
    try {
        close();
    }
    catch (...) {
        cerr << "~filter_ostream: ignored exception\n" + MLDB::getExceptionString() + "\n";
    }
}

filter_ostream &
filter_ostream::
operator = (filter_ostream && other)
{
    exceptions(ios::goodbit);
    stream = std::move(other.stream);
    sink = std::move(other.sink);
    rdbuf(other.rdbuf());
    exceptions(other.exceptions());
    other.exceptions(ios::goodbit);
    other.rdbuf(0);
    deferredFailure = other.deferredFailure.load();
    other.deferredFailure = false;
    options = std::move(other.options);
    
    return *this;
}

namespace {

void addCompression(streambuf & buf,
                    filtering_ostream & stream,
                    const Utf8String & resource,
                    const std::string & compression,
                    int compressionLevel)
{
    if (compression == "none") {
        // nothing to do
    }
    else if (compression != "") {
        Compressor * compressor
            = Compressor::create(compression, compressionLevel);
        if (!compressor)
            throw MLDB::Exception("unknown filter compression " + compression);
        stream.push(StreamCompressor(compressor, buf));
    }
    else {
        std::string compressionFromFilename
            = Compressor::filenameToCompression(resource);
        if (compressionFromFilename != "") {
            Compressor * compressor
                = Compressor::create(compressionFromFilename, compressionLevel);
            if (!compressor)
                throw MLDB::Exception("unknown filter compression " + compression);
            stream.push(StreamCompressor(compressor, buf));
        }
    }
}

void addCompression(streambuf & buf,
                    filtering_ostream & stream,
                    const Utf8String & resource,
                    const std::map<std::string, std::string> & options)
{
    string compression;
    auto it = options.find("compression");
    if (it != options.end())
        compression = it->second;

    int compressionLevel = -1;
    it = options.find("compressionLevel");
    if (it != options.end())
        compressionLevel = MLDB::lexical_cast<int>(it->second);
    
    addCompression(buf, stream, resource, compression, compressionLevel);
}


/** Create an options map from a set of legacy options passed by value. */
std::map<std::string, std::string>
createOptions(std::ios_base::openmode mode,
              const std::string & compression,
              int compressionLevel)
{
    /* 
        app	(append) Set the stream's position indicator to the end of the stream before each output operation.
        ate	(at end) Set the stream's position indicator to the end of the stream on opening.
        binary	(binary) Consider stream as binary rather than text.
        in	(input) Allow input operations on the stream.
        out	(output) Allow output operations on the stream.
        trunc	(truncate) Any current content is discarded, assuming a length of zero on opening.
    */

    string modeStr;
    auto addMode = [&] (int mask, const char * name)
        {
            if ((mode & mask) == 0)
                return;
            if (!modeStr.empty())
                modeStr += ',';
            modeStr += name;
        };

    addMode(ios_base::app, "app");
    addMode(ios_base::ate, "ate");
    addMode(ios_base::binary, "binary");
    addMode(ios_base::in, "in");
    addMode(ios_base::out, "out");
    addMode(ios_base::trunc, "trunc");

    //cerr << "compression = " << compression << endl;

    std::map<std::string, std::string> result;

    if (!modeStr.empty())
        result["mode"] = modeStr;
    if (!compression.empty())
        result["compression"] = compression;
    if (compressionLevel != -1)
        result["compressionLevel"] = std::to_string(compressionLevel);

    return result;
}

std::ios_base::openmode getMode(const std::map<std::string, std::string> & options)
{
    std::ios_base::openmode result
        = std::ios_base::openmode(0);

    auto it = options.find("mode");
    if (it == options.end())
        return result;


    string::size_type lastPos = -1;

    for (auto pos = it->second.find(',', lastPos); true /* break in loop */;
         pos = lastPos) {
        string el(it->second, lastPos + 1, pos);
        if (el == "app")
            result |= ios_base::app;
        else if (el == "ate")
            result |= ios_base::ate;
        else if (el == "binary")
            result |= ios_base::binary;
        else if (el == "in")
            result |= ios_base::in;
        else if (el == "out")
            result |= ios_base::out;
        else if (el == "trunc")
            result |= ios_base::trunc;
        else throw MLDB::Exception("unknown filter_stream open mode " + el);

        if (pos == string::npos)
            break;
    }
    
    return result;
}

} // file scope

void
filter_ostream::
open(const Utf8String & uri, std::ios_base::openmode mode,
     const std::string & compression, int compressionLevel)
{
    //cerr << "uri = " << uri << " compression = " << compression << endl;

    open(uri, createOptions(mode, compression, compressionLevel));
}

void
filter_ostream::
open(const Url & uri, std::ios_base::openmode mode,
     const std::string & compression, int compressionLevel)
{
    open(uri.toDecodedString(), mode, compression, compressionLevel);
}

void
filter_ostream::
open(const Utf8String & uri,
     const std::map<std::string, std::string> & options)
{
    auto [scheme, resource] = getScheme(uri);

    std::ios_base::openmode mode = getMode(options);
    if (!mode)
        mode = std::ios_base::out;

    //cerr << "opening scheme " << scheme << " resource " << resource
    //     << endl;

    const auto & handler = getUriHandler(scheme);
    auto onException = [&](std::exception_ptr ptr) {
        if (!this->deferredFailure.exchange(true)) {
            this->deferredExcPtr = ptr;
        }
    };
    UriHandler res = handler(scheme, resource, mode, options, onException);
    
    return openFromHandler(res, resource, options);
}

void
filter_ostream::
open(const Url & uri,
     const std::map<std::string, std::string> & options)
{
    open(uri.toDecodedString(), options);
}

void
filter_ostream::
openFromStreambuf(std::streambuf * buf,
                  std::shared_ptr<void> bufOwnership,
                  const Utf8String & resource,
                  const std::string & compression,
                  int compressionLevel)
{
    openFromStreambuf(buf, bufOwnership, resource,
                      createOptions(std::ios_base::openmode(0),
                                    compression, compressionLevel));
}    

void
filter_ostream::
openFromStreambuf(std::streambuf * buf,
                  std::shared_ptr<void> bufOwnership,
                  const Utf8String & resource,
                  const std::map<std::string, std::string> & options)
{
    // TODO: exception safety for buf

    //cerr << "buf = " << (void *)buf << endl;
    //cerr << "weOwnBuf = " << weOwnBuf << endl;

    unique_ptr<filtering_ostream> new_stream
        (new filtering_ostream());

    addCompression(*buf, *new_stream, resource, options);

    if (!new_stream->empty()) {
        // We added something, so put the filters in place
        new_stream->push(*buf);
        this->stream = std::move(new_stream);
        rdbuf(this->stream->rdbuf());
    }
    else {
        // Don't need the filtering stream at all
        rdbuf(buf);
    }

    this->sink = std::move(bufOwnership);

    exceptions(ios::badbit | ios::failbit);
}

void
filter_ostream::
openFromHandler(const UriHandler & handler,
                const Utf8String & resource,
                const std::map<std::string, std::string> & options)
{
    openFromStreambuf(handler.buf, handler.bufOwnership, resource, options);
}

void filter_ostream::
open(int fd, std::ios_base::openmode mode,
     const std::string & compression, int compressionLevel)
{
    open(fd, createOptions(mode, compression, compressionLevel));
}

void filter_ostream::
open(int fd, const std::map<std::string, std::string> & options)
{
    unique_ptr<filtering_ostream> new_stream
        (new filtering_ostream());

    stringbuf headerbuf;
    addCompression(headerbuf, *new_stream, "", options);
    string header = headerbuf.str();
    if (!header.empty()) {
        ssize_t rc = ::write(fd, header.c_str(), header.size());
        if (rc < 0) {
            throw MLDB::Exception(errno, "open", "open");
        }
    }

    new_stream->push(file_descriptor_sink(fd, never_close_handle));

    stream.reset(new_stream.release());
    sink.reset();
    rdbuf(stream->rdbuf());

    exceptions(ios::badbit | ios::failbit);
}

void
filter_ostream::
close()
{
    if (rdbuf()) {
        flush_stream(*rdbuf());
        close_stream(*rdbuf());
    }
    stream.reset();
    exceptions(ios::goodbit);
    rdbuf(nullptr);
    sink.reset();
    options.clear();
    if (deferredExcPtr) {
        rethrow_exception(deferredExcPtr);
    }
    if (deferredFailure) {
        deferredFailure = false;
        exceptions(ios::badbit | ios::failbit);
        setstate(ios::badbit);
    }
}

std::string
filter_ostream::
status() const
{
    if (*this) return "good";
    else return string(fail() ? " fail" : "")
             + (bad() ? " bad" : "")
             + (eof() ? " eof" : "");
}


/*****************************************************************************/
/* FILTER_ISTREAM                                                            */
/*****************************************************************************/

filter_istream::filter_istream()
    : istream(std::cin.rdbuf()), deferredFailure(false)
{
}

filter_istream::
filter_istream(const Utf8String & file, std::ios_base::openmode mode,
               const std::string & compression)
    : istream(std::cin.rdbuf()),
      deferredFailure(false)
{
    open(file, mode, compression);
}

filter_istream::
filter_istream(const Url & file, std::ios_base::openmode mode,
               const string & compression)
    : istream(std::cin.rdbuf()),
      deferredFailure(false)
{
    open(file, mode, compression);
}


filter_istream::
filter_istream(const Utf8String & uri,
               const std::map<std::string, std::string> & options)
    : istream(std::cin.rdbuf()),
      deferredFailure(false)
{
    open(uri, options);
}

filter_istream::
filter_istream(const Url & uri,
               const std::map<std::string, std::string> & options)
    : istream(std::cin.rdbuf()),
      deferredFailure(false)
{
    open(uri, options);
}


filter_istream::
filter_istream(filter_istream && other) noexcept
    : istream(other.rdbuf()),
    stream(std::move(other.stream)),
    sink(std::move(other.sink)),
    deferredFailure(other.deferredFailure.load()),
    deferredExcPtr(std::move(other.deferredExcPtr)),
    info_(std::move(other.info_))
{
}

filter_istream::
filter_istream(const UriHandler & handler,
               const Utf8String & resource,
               const std::map<std::string, std::string> & options)
    : istream(std::cin.rdbuf()), deferredFailure(false)
{
    openFromHandler(handler, resource, options);
}

filter_istream::
~filter_istream()
{
    try {
        close();
    }
    catch (...) {
    }
}

filter_istream &
filter_istream::
operator = (filter_istream && other)
{
    exceptions(ios::goodbit);
    stream = std::move(other.stream);
    sink = std::move(other.sink);
    handlerOptions = std::move(other.handlerOptions);
    resource = std::move(other.resource);
    rdbuf(other.rdbuf());
    exceptions(other.exceptions());
    other.exceptions(ios::goodbit);
    other.rdbuf(0);
    info_ = std::move(other.info_);
    deferredFailure = other.deferredFailure.load();
    deferredExcPtr = std::move(other.deferredExcPtr);
    
    return *this;
}

void
filter_istream::
open(const Utf8String & uri,
     std::ios_base::openmode mode,
     const std::string & compression)
{
    exceptions(ios::badbit);

    auto [scheme, resource] = getScheme(uri);

    const auto & handlerFactory = getUriHandler(scheme);
    auto onException = [&](const exception_ptr & excPtr) {
        if (!this->deferredFailure.exchange(true)) {
            // only take first exception
            this->deferredExcPtr = excPtr;
        }
    };
    auto options = createOptions(mode, compression, -1);
    UriHandler handler = handlerFactory(scheme, resource, mode,
                                        options,
                                        onException);
    
    openFromHandler(handler, resource, options);
}

void
filter_istream::
open(const Url & uri,
     std::ios_base::openmode mode,
     const std::string & compression)
{
    open(uri.toDecodedString(), mode, compression);
}

void
filter_istream::
open(const Utf8String & uri,
     const std::map<std::string, std::string> & options)
{
    exceptions(ios::badbit);

    auto [scheme, resource] = getScheme(uri);

    const auto & handlerFactory = getUriHandler(scheme);
    auto onException = [&](const exception_ptr & excPtr) {
        if (!this->deferredFailure.exchange(true)) {
            // only take first exception
            this->deferredExcPtr = excPtr;
        }
    };
    UriHandler handler = handlerFactory(scheme, resource, ios::in, options, onException);
    openFromHandler(handler, resource, options);
}

void
filter_istream::
open(const Url & uri,
     const std::map<std::string, std::string> & options)
{
    open(uri.toDecodedString(), options);
}

void
filter_istream::
openFromHandler(const UriHandler & handler,
                const Utf8String & resource,
                const std::map<std::string, std::string> & options)
{
    string compression;
    auto cmpIt = options.find("compression");
    if (cmpIt != options.end())
        compression = cmpIt->second;
    
    this->handlerOptions = handler.options;
    this->info_ = handler.info;
    if (!this->info_)
        throw MLDB::Exception("Handler for resource '" + resource
                            + "' didn't set info");
    ExcAssert(this->info_);
    openFromStreambuf(handler.buf, handler.bufOwnership, resource, compression);
}

void
filter_istream::
openFromStreambuf(std::streambuf * buf,
                  std::shared_ptr<void> bufOwnership,
                  const Utf8String & resource,
                  const std::string & compression)
{
    // TODO: exception safety for buf

    unique_ptr<filtering_istream> new_stream
        (new filtering_istream());

    if (compression == "") {
        std::string compression = Compressor::filenameToCompression(resource);
        if (compression != "") {
            new_stream->push(StreamDecompressor(Decompressor::create(compression)));
        }
    } else if (compression == "none") {
        // no-op
    } else {
        new_stream->push(StreamDecompressor(Decompressor::create(compression)));
    }

    if (!new_stream->empty()) {
        new_stream->push(*buf);
        this->stream = std::move(new_stream);

        // MLDB-1140: if we add compression, we are no longer mappable, seekable,
        // etc so we reset the options here.  In a later iteration we could
        // potentially pre-decompress the file to a cache directory and map that,
        // but it requires extra support we don't have at the moment around file
        // caching, etc.
        this->handlerOptions = UriHandlerOptions();
        rdbuf(this->stream->rdbuf());
    }
    else {
        rdbuf(buf);
    }
    
    this->sink = std::move(bufOwnership);
    this->resource = resource;
}

void
filter_istream::
close()
{
    if (stream) {
        flush_stream(*stream);
        close_stream(*stream);
    }
    exceptions(ios::goodbit);
    rdbuf(0);
    stream.reset();
    sink.reset();
    if (deferredExcPtr) {
        rethrow_exception(deferredExcPtr);
    }
    if (deferredFailure) {
        deferredFailure = false;
        exceptions(ios::badbit | ios::failbit);
        setstate(ios::badbit);
    }
}

string
filter_istream::
readAll()
{
    std::ostringstream result;
    result << rdbuf();
    return result.str();
}

bool
filter_istream::
isForwardSeekable() const
{
    return handlerOptions.isForwardSeekable;
}

#if 0
    ssize_t ofs = -1, endofs = -1;
        

        stream.exceptions(ios::goodbit);
        ofs = stream.tellg();
        if (ofs != -1) {
            ssize_t backward = stream.rdbuf()->pubseekoff(-1, ios::cur);
            if (backward != -1) {
                cerr << "stream is backward seekable" << endl;
                endofs = stream.rdbuf()->pubseekoff(0, ios::end);
                stream.rdbuf()->pubseekoff(ofs, ios::beg);
            }
        }
        stream.exceptions(ios::failbit | ios::badbit);
#endif

bool
filter_istream::
isRandomSeekable() const
{
    return handlerOptions.isRandomSeekable;
}

bool
filter_istream::
isForkable() const
{
    return !!handlerOptions.fork;
}

filter_istream
filter_istream::
fork(const std::map<std::string, std::string> & options) const
{
    filter_istream result;
    result.openFromHandler(handlerOptions.fork(rdbuf(), sink),
                           this->resource, options);
    return result;
}

std::tuple<const char *, size_t, size_t>
filter_istream::
mapped() const
{
    return { handlerOptions.mapped, handlerOptions.mappedSize, handlerOptions.mappedCapacity };
}

FsObjectInfo
filter_istream::
info() const
{
    if (!info_)
        throw MLDB::Exception("Resource '" + resource + "' doesn't have info");
    return *info_;
}


/*****************************************************************************/
/* REGISTRATION                                                              */
/*****************************************************************************/

namespace {

std::mutex uriHandlersLock;
std::unordered_map<std::string, UriHandlerFactory> uriHandlers;

} // file scope

void registerUriHandler(const std::string & scheme,
                        const UriHandlerFactory & handler)
{
    if (!handler)
        throw MLDB::Exception("registerUriHandler: null handler passed");

    std::unique_lock<std::mutex> guard(uriHandlersLock);
    auto it = uriHandlers.find(scheme);
    if (it != uriHandlers.end())
        throw MLDB::Exception("already have a Uri handler registered for scheme "
                            + scheme);
    uriHandlers[scheme] = handler;
}

const UriHandlerFactory &
getUriHandler(const std::string & scheme)
{
    std::unique_lock<std::mutex> guard(uriHandlersLock);
    auto it = uriHandlers.find(scheme);
    if (it != uriHandlers.end())
        return it->second;

    auto pos = scheme.find('+');
    if (pos != string::npos) {
        string firstScheme(scheme, 0, pos);
        it = uriHandlers.find(firstScheme);
        if (it != uriHandlers.end())
            return it->second;
    }

    throw MLDB::Exception("Uri handler not found for scheme " + scheme);
}

struct RegisterFileHandler {
    static UriHandler
    getFileHandler(const std::string & scheme,
                   Utf8String resource,
                   std::ios_base::openmode mode,
                   const std::map<std::string, std::string> & options,
                   const OnUriHandlerException & onException)
    {
        if (resource == "")
            resource = "/dev/null";

        if (mode == ios::in) {
            if (resource == "-") {
                FsObjectInfo info;
                info.exists = true;
                info.lastModified = Date::now();
                return UriHandler(cin.rdbuf(), nullptr, info);
            }

            FsObjectInfo info
                = getUriObjectInfo(scheme +  "://" + resource);

            // MLDB-1303 mmap fails on empty files - force filebuf interface
            // on empty files despite the mapped option
            if (!options.count("mapped") || !info.size) {
                shared_ptr<std::filebuf> buf(new std::filebuf);
                buf->open(resource.rawString(), ios_base::openmode(mode));

                if (!buf->is_open())
                    throw MLDB::Exception("couldn't open file %s: %s",
                                        resource.c_str(), strerror(errno));

                return UriHandler(buf.get(), buf, info);
            } 
            else {
                try {
                    // Here is where we stick things we need to 
                    using KeepMeAround = std::vector<std::shared_ptr<const void>>;
                    auto keepMe = std::make_shared<KeepMeAround>();

                    mapped_file_params params(resource.rawString());
                    params.extra_capacity = MAPPING_EXTRA_CAPACITY;
                    mapped_file_source source(params);

                    auto buf = std::make_shared<source_istreambuf<mapped_file_source>>(source);
                
                    UriHandlerOptions options;
                    options.mapped = source.data();
                    options.mappedSize = buf->source().size();
                    options.mappedCapacity = buf->source().capacity();
                    ExcCheckGreaterEqual(buf->source().extra_capacity(), MAPPING_EXTRA_CAPACITY,
                                         "mapped file extra capacity not respected");

                    keepMe->emplace_back(buf);

                    return UriHandler(buf.get(), std::move(keepMe), info, options);
                } catch (const std::exception & exc) {
                    throw MLDB::Exception("Opening file " + resource + ": "
                                        + exc.what());
                }
            }
        }
        else if (mode & ios::out) {
            if (resource == "-")
                return UriHandler(cout.rdbuf(), nullptr);

            shared_ptr<std::filebuf> buf(new std::filebuf);
            buf->open(resource.rawString(), ios_base::openmode(mode));

            if (!buf->is_open())
                throw MLDB::Exception("couldn't open file %s: %s",
                                    resource.c_str(), strerror(errno));

            return UriHandler(buf.get(), buf);
        }
        else throw MLDB::Exception("no way to create file handler for non in/out");
    }

    RegisterFileHandler()
    {
        registerUriHandler("file", getFileHandler);
    }

} registerFileHandler;



/* "mem:" scheme for using memory as storage backend. Only useful for testing. */

namespace {

mutex memStringsLock;
map<Utf8String, string> memStrings;

} // "mem" scheme

string &
getMemStreamString(const Utf8String & name)
{
    unique_lock<mutex> guard(memStringsLock);

    return memStrings.at(name);    
}

void
setMemStreamString(const Utf8String & name,
                   const std::string & contents)
{
    unique_lock<mutex> guard(memStringsLock);

    memStrings.insert({name, contents});
}

void
deleteMemStreamString(const Utf8String & name)
{
    unique_lock<mutex> guard(memStringsLock);

    memStrings.erase(name);    
}

void
deleteAllMemStreamStrings()
{
    unique_lock<mutex> guard(memStringsLock);

    memStrings.clear();
}


struct MemStreamingInOut {
    MemStreamingInOut(string & targetString)
        : open_(true), pos_(0), targetString_(targetString)
    {
    }

    typedef char char_type;

    bool is_open() const
    {
        return open_;
    }

    void close()
    {
        open_ = false;
    }

    bool open_;
    size_t pos_;
    string & targetString_;
};

struct MemStreamingIn : public MemStreamingInOut {

    MemStreamingIn(string & targetString)
        : MemStreamingInOut(targetString)
    {
    }

    streamsize read(char * s, streamsize n)
    {
        streamsize res;
        unique_lock<mutex> guard(memStringsLock);

        size_t maxLen = std::min<size_t>(targetString_.size() - pos_,
                                         n);
        if (maxLen > 0) {
            const char * start = targetString_.c_str() + pos_;
            copy(start, start + maxLen, s);
            pos_ += maxLen;
            res = maxLen;
        }
        else {
            res = -1;
        }

        return res;
    }

    streamsize avail()
    {
        unique_lock<mutex> guard(memStringsLock);
        return targetString_.size() - pos_;
    }

    std::streampos seek(std::streamsize where, std::ios_base::seekdir dir)
    {
        unique_lock<mutex> guard(memStringsLock);
        int64_t newOffset;
            
        switch (dir) {
        case ios_base::beg:
            if (where < 0 || where > targetString_.size())
                return -1;
            newOffset = where;
            break;
        case ios_base::end:
            if (where < 0 || where > targetString_.size())
                return -1;
            newOffset = targetString_.size() - where;
            break;
        case ios_base::cur:
            newOffset = pos_ + where;
            if (newOffset < 0 || newOffset > targetString_.size())
                return -1;
            break;
        default:
            ExcAssert(false);
            break;
        }
        
        if (newOffset < 0 || newOffset > targetString_.size()) {
            return -1;  // seek error
        }
        
        return pos_ = newOffset;
    }
};

struct MemStreamingOut : public MemStreamingInOut {

    MemStreamingOut(string & targetString)
        : MemStreamingInOut(targetString)
    {
    }

    streamsize write(const char * s, streamsize n)
    {
        unique_lock<mutex> guard(memStringsLock);

        size_t nextLen = pos_ + n;
        targetString_.reserve(nextLen);
        targetString_.append(s, n);
        pos_ = nextLen;

        return n;
    }

    void flush() {}
};

struct RegisterMemHandler {
    static UriHandler
    getMemHandler(const string & scheme,
                  Utf8String resource,
                  ios_base::openmode mode,
                  const map<string, string> & options,
                  const OnUriHandlerException & onException)
    {
        if (scheme != "mem")
            throw MLDB::Exception("bad scheme name");
        if (resource == "")
            throw MLDB::Exception("bad resource name");

        unique_lock<mutex> guard(memStringsLock);

        // cerr << "string resource : " + resource + "\n";
        string & targetString = memStrings[resource];

        shared_ptr<std::streambuf> streamBuf;
        if (mode == ios::in) {
            streamBuf.reset(new source_istreambuf<MemStreamingIn>(MemStreamingIn(targetString), 4096));
        }
        else if (mode == ios::out) {
            streamBuf.reset(new sink_ostreambuf<MemStreamingOut>(MemStreamingOut(targetString), 4096));
        }
        else {
            throw MLDB::Exception("unable to create mem handler");
        }
        FsObjectInfo info;
        info.exists = true;
        info.size = targetString.size();
        return UriHandler(streamBuf.get(), streamBuf, info);
    }

    RegisterMemHandler()
    {
        registerUriHandler("mem", getMemHandler);
    }

} registerMemHandler;

} // namespace MLDB
