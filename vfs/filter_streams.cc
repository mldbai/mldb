// This file is part of MLDB. Copyright 2015 mldb.ai inc. All rights reserved.

/* filter_streams.cc
   Jeremy Barnes, 17 March 2005
   Copyright (c) 2005 Jeremy Barnes.  All rights reserved.
   
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
#include <boost/iostreams/filtering_stream.hpp>
#include <boost/iostreams/filter/bzip2.hpp>
#include <boost/iostreams/filter/gzip.hpp>
#include <boost/iostreams/device/file.hpp>
#include <boost/iostreams/device/file_descriptor.hpp>
#include <boost/iostreams/device/mapped_file.hpp>
#include <boost/version.hpp>
#include <boost/lexical_cast.hpp>
#include "mldb/arch/exception.h"
#include <errno.h>
#include <sstream>
#include <thread>
#include <unordered_map>
#include "ext/lzma/lzma.h"
#include "lz4_filter.h"
#include "fs_utils.h"


using namespace std;

namespace MLDB {

const UriHandlerFactory &
getUriHandler(const std::string & scheme);

std::pair<std::string, std::string>
getScheme(const std::string & uri)
{
    string::size_type pos = uri.find("://");
    if (pos == string::npos) {
        return make_pair("file", uri);
    }

    string scheme(uri, 0, pos);
    string resource(uri, pos + 3);

    return make_pair(scheme, resource);
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
/* BOOST COMPRESSOR                                                          */
/*****************************************************************************/

/** Adaptor to allow boost::iostreams to be served by a compressor object. */

struct BoostCompressor: public boost::iostreams::multichar_output_filter {

    BoostCompressor(Compressor * compressor)
        : compressor(compressor)
    {
    }

    template<typename Sink>
    void writeAll(Sink& sink, const char* data, size_t size)
    {
        while (size > 0) {
            size_t written = boost::iostreams::write(sink, data, size);
            if (!written) throw Exception("unable to write data");
            
            data += written;
            size -= written;
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
        return n;
    }

    template<typename Sink>
    void close(Sink& sink)
    {
        auto onData = [&] (const char * data, size_t len) -> size_t
            {
                writeAll(sink, data, len);
                return len;
            };
        
        compressor->finish(onData);
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
};


/*****************************************************************************/
/* BOOST DECOMPRESSOR                                                        */
/*****************************************************************************/

/** Adaptor to allow boost::iostreams to be served by a decompressor object. */

struct BoostDecompressor: public boost::iostreams::multichar_input_filter {

    BoostDecompressor(Decompressor * decompressor)
        : decompressor(decompressor)
    {
        inbuf.resize(4096);
    }

    template<typename Source>
    std::streamsize read(Source& src, char* s, std::streamsize n)
    {
        auto sBefore = s;

        size_t numWritten = 0;

        // First, return any buffered characters from outbuf
        auto numBuffered = std::min<std::streamsize>(n, outbuf.size() - outbufPos);
        std::copy(outbuf.data() + outbufPos,
                  outbuf.data() + outbufPos + numBuffered,
                  s);
        s += numBuffered;
        n -= numBuffered;
        numWritten += numBuffered;
        outbufPos += numBuffered;

        // Clear outbuf buffer once it's all used up
        if (outbufPos == outbuf.size()) {
            outbuf.resize(0);
            outbufPos = 0;
        }

        if (n == 0)
            return s - sBefore;

        // Now, call the decompressor

        auto onData = [&] (const char * data, size_t dataLength)
            {
                // Put as much as can fit in the output
                auto numGenerated = std::min<std::streamsize>(dataLength, n);
                std::copy(data, data + numGenerated, s);
                s += numGenerated;
                n -= numGenerated;
                numWritten += numGenerated;

                // Everything else gets buffered for next time
                ExcAssertEqual(outbuf.size(), 0);
                ExcAssertEqual(outbufPos, 0);
                outbuf.append(data + numGenerated, dataLength - numGenerated);

                return dataLength;  // we always consume all of the characters
            };
        
        while (n > 0) {
            ssize_t numRead = boost::iostreams::read(src, inbuf.data(), inbuf.size());
            if (numRead <= 0) {
                numWritten += decompressor->finish(onData);
                break;
            }
            else {
                numWritten += decompressor->decompress(inbuf.data(), numRead, onData);
            }
        }

        return s - sBefore;
    }

    std::vector<char> inbuf;  ///< Characters read from input but not yet passed to decompressor
    std::string outbuf; ///< Characters returned from decompressor but not yet written
    size_t outbufPos = 0;   ///< Position in outbuf

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
    deferredFailure(false)
{
}

filter_ostream::
filter_ostream(const std::string & file, std::ios_base::openmode mode,
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
filter_ostream(const std::string & uri,
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
        cerr << "~filter_ostream: ignored exception\n";
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

    return *this;
}

namespace {

bool ends_with(const std::string & str, const std::string & what)
{
    string::size_type result = str.rfind(what);
    return result != string::npos
        && result == str.size() - what.size();
}

void addCompression(streambuf & buf,
                    boost::iostreams::filtering_ostream & stream,
                    const std::string & resource,
                    const std::string & compression,
                    int compressionLevel)
{
    using namespace boost::iostreams;

    if (compression == "gz" || compression == "gzip"
        || (compression == ""
            && (ends_with(resource, ".gz") || ends_with(resource, ".gz~")))) {
        gzip_compressor compressor;
        if (compressionLevel != -1) {
            compressor = gzip_compressor(compressionLevel);
        }
        compressor.write(buf, "", 0);
        stream.push(compressor);
    }
    else if (compression == "bz2" || compression == "bzip2"
        || (compression == ""
            && (ends_with(resource, ".bz2") || ends_with(resource, ".bz2~")))) {
        if (compressionLevel == -1)
            stream.push(bzip2_compressor());
        else stream.push(bzip2_compressor(compressionLevel));
    }
    else if (compression == "lzma" || compression == "xz"
        || (compression == ""
            && (ends_with(resource, ".xz") || ends_with(resource, ".xz~")))) {
        if (compressionLevel == -1)
            stream.push(lzma_compressor());
        else stream.push(lzma_compressor(compressionLevel));
    }
    else if (compression == "lz4"
        || (compression == ""
            && (ends_with(resource, ".lz4") || ends_with(resource, ".lz4~")))) {
        stream.push(lz4_compressor(compressionLevel));
    }
    else if (compression == "none") {
        // nothing to do
    }
    else if (compression != "") {
        Compressor * compressor
            = Compressor::create(compression, compressionLevel);
        if (!compressor)
            throw MLDB::Exception("unknown filter compression " + compression);
        stream.push(BoostCompressor(compressor));
    }
    else {
        std::string compressionFromFilename
            = Compressor::filenameToCompression(resource);
        if (compressionFromFilename != "") {
            Compressor * compressor
                = Compressor::create(compressionFromFilename, compressionLevel);
            if (!compressor)
                throw MLDB::Exception("unknown filter compression " + compression);
            stream.push(BoostCompressor(compressor));
        }
    }
}

void addCompression(streambuf & buf,
                    boost::iostreams::filtering_ostream & stream,
                    const std::string & resource,
                    const std::map<std::string, std::string> & options)
{
    string compression;
    auto it = options.find("compression");
    if (it != options.end())
        compression = it->second;

    int compressionLevel = -1;
    it = options.find("compressionLevel");
    if (it != options.end())
        compressionLevel = boost::lexical_cast<int>(it->second);
    
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
open(const std::string & uri, std::ios_base::openmode mode,
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
open(const std::string & uri,
     const std::map<std::string, std::string> & options)
{
    using namespace boost::iostreams;

    string scheme, resource;
    std::tie(scheme, resource) = getScheme(uri);

    std::ios_base::openmode mode = getMode(options);
    if (!mode)
        mode = std::ios_base::out;

    //cerr << "opening scheme " << scheme << " resource " << resource
    //     << endl;

    const auto & handler = getUriHandler(scheme);
    auto onException = [&]() { this->deferredFailure = true; };
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
                  const std::string & resource,
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
                  const std::string & resource,
                  const std::map<std::string, std::string> & options)
{

    // TODO: exception safety for buf

    using namespace boost::iostreams;

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
                const std::string & resource,
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
    using namespace boost::iostreams;
    
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

#if (BOOST_VERSION < 104100)
    new_stream->push(file_descriptor_sink(fd));
#else
    new_stream->push(file_descriptor_sink(fd,
                                          boost::iostreams::never_close_handle));
#endif
    stream.reset(new_stream.release());
    sink.reset();
    rdbuf(stream->rdbuf());

    exceptions(ios::badbit | ios::failbit);
}

void
filter_ostream::
close()
{
    if (stream) {
        boost::iostreams::flush(*stream);
        boost::iostreams::close(*stream);
    }
    exceptions(ios::goodbit);
    rdbuf(0);
    stream.reset();
    sink.reset();
    options.clear();
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
filter_istream(const std::string & file, std::ios_base::openmode mode,
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
filter_istream(const std::string & uri,
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
    deferredFailure(false)
{
}

filter_istream::
filter_istream(const UriHandler & handler,
               const std::string & resource,
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
        cerr << "~filter_istream: ignored exception\n";
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

    return *this;
}

void
filter_istream::
open(const std::string & uri,
     std::ios_base::openmode mode,
     const std::string & compression)
{
    exceptions(ios::badbit);

    string scheme, resource;
    std::tie(scheme, resource) = getScheme(uri);

    const auto & handlerFactory = getUriHandler(scheme);
    auto onException = [&]() { this->deferredFailure = true; };
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
open(const std::string & uri,
     const std::map<std::string, std::string> & options)
{
    exceptions(ios::badbit);

    string scheme, resource;
    std::tie(scheme, resource) = getScheme(uri);

    const auto & handlerFactory = getUriHandler(scheme);
    auto onException = [&]() { this->deferredFailure = true; };
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
                const std::string & resource,
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
                  const std::string & resource,
                  const std::string & compression)
{
    // TODO: exception safety for buf

    using namespace boost::iostreams;

    unique_ptr<filtering_istream> new_stream
        (new filtering_istream());

    bool gzip = (compression == "gz" || compression == "gzip"
                 || (compression == ""
                     && (ends_with(resource, ".gz")
                         || ends_with(resource, ".gz~"))));
    bool bzip2 = (compression == "bz2" || compression == "bzip2"
                 || (compression == ""
                     && (ends_with(resource, ".bz2")
                         || ends_with(resource, ".bz2~"))));
    bool lzma = (compression == "xz" || compression == "lzma"
                 || (compression == ""
                     && (ends_with(resource, ".xz")
                         || ends_with(resource, ".xz~"))));

    bool lz4 = (compression == "lz4"
                 || (compression == ""
                     && (ends_with(resource, ".lz4")
                         || ends_with(resource, ".lz4~"))));

    if (gzip) new_stream->push(gzip_decompressor());
    else if (bzip2) new_stream->push(bzip2_decompressor());
    else if (lzma) new_stream->push(lzma_decompressor());
    else if (lz4) new_stream->push(lz4_decompressor());
    else if (compression == "") {
        std::string compression = Compressor::filenameToCompression(resource);
        if (compression != "") {
            new_stream->push(BoostDecompressor(Decompressor::create(compression)));
        }
    } else {
        new_stream->push(BoostDecompressor(Decompressor::create(compression)));
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
        boost::iostreams::flush(*stream);
        boost::iostreams::close(*stream);
    }
    exceptions(ios::goodbit);
    rdbuf(0);
    stream.reset();
    sink.reset();
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

std::pair<const char *, size_t>
filter_istream::
mapped() const
{
    return { handlerOptions.mapped, handlerOptions.mappedSize };
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
                   std::string resource,
                   std::ios_base::open_mode mode,
                   const std::map<std::string, std::string> & options,
                   const OnUriHandlerException & onException)
    {
        using namespace boost::iostreams;

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
                buf->open(resource, ios_base::openmode(mode));

                if (!buf->is_open())
                    throw MLDB::Exception("couldn't open file %s: %s",
                                        resource.c_str(), strerror(errno));

                return UriHandler(buf.get(), buf, info);
            } 
            else {
                try {
                    mapped_file_source source(resource);
                    shared_ptr<std::streambuf> buf(new stream_buffer<mapped_file_source>(source));
                
                    UriHandlerOptions options;
                    options.mapped = source.data();
                    options.mappedSize = source.size();
                    return UriHandler(buf.get(), buf, info, options);
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
            buf->open(resource, ios_base::openmode(mode));

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
map<string, string> memStrings;

} // "mem" scheme

string &
getMemStreamString(const string & name)
{
    unique_lock<mutex> guard(memStringsLock);

    return memStrings.at(name);    
}

void
setMemStreamString(const std::string & name,
                   const std::string & contents)
{
    unique_lock<mutex> guard(memStringsLock);

    memStrings.insert({name, contents});
}

void
deleteMemStreamString(const std::string & name)
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
    struct category
        : public boost::iostreams::input,
          public boost::iostreams::device_tag,
          public boost::iostreams::closable_tag
    {
    };

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
};

struct MemStreamingOut : public MemStreamingInOut {
    struct category
        : public boost::iostreams::output,
          public boost::iostreams::device_tag,
          public boost::iostreams::closable_tag
    {
    };

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
};

struct RegisterMemHandler {
    static UriHandler
    getMemHandler(const string & scheme,
                  string resource,
                  ios_base::open_mode mode,
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
            streamBuf.reset(new boost::iostreams::stream_buffer<MemStreamingIn>(MemStreamingIn(targetString),
                                                                                4096));
        }
        else if (mode == ios::out) {
            streamBuf.reset(new boost::iostreams::stream_buffer<MemStreamingOut>(MemStreamingOut(targetString),
                                                                                 4096));
        }
        else {
            throw MLDB::Exception("unable to create mem handler");
        }
        FsObjectInfo info;
        info.exists = true;
        return UriHandler(streamBuf.get(), streamBuf, info);
    }

    RegisterMemHandler()
    {
        registerUriHandler("mem", getMemHandler);
    }

} registerMemHandler;

} // namespace MLDB
