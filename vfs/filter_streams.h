/* filter_streams.h                                                -*- C++ -*-
   Jeremy Barnes, 12 March 2005
   Copyright (c) 2005 Jeremy Barnes.  All rights reserved.
   
   This file is part of MLDB. Copyright 2015 mldb.ai inc. All rights reserved.
   
   This file is part of "Jeremy's Machine Learning Library", copyright (c)
   1999-2015 Jeremy Barnes.
   
   Apache 2.0 license.
*/

#pragma once

#include <atomic>
#include <iostream>
#include <fstream>
#include <memory>
#include <map>
#include "types/url.h"

namespace MLDB {

struct FsObjectInfo;  // Structure for file system or URL metadata; in fs_utils.h


/*****************************************************************************/
/* BASE STRUCTURES                                                           */
/*****************************************************************************/

struct UriHandler;

/// Signature of a function used to fork a filter stream
typedef std::function<UriHandler (std::streambuf *, const std::shared_ptr<void> &)>
UriForkFunction;


/// Structure that provides extra options about a URI handler
struct UriHandlerOptions {
    UriHandlerOptions()
        : isForwardSeekable(false),
          isRandomSeekable(false),
          mapped(nullptr),
          mappedSize(0)
    {
    }

    bool isForwardSeekable;              ///< Can we seek forwards in the stream?
    bool isRandomSeekable;               ///< Can we seek at random in the stream?
    
    /// Function used to fork the handler.  If null, forking is not possible
    UriForkFunction fork;

    /// If it's a mapped stream, returns the location and size
    const char * mapped;
    size_t mappedSize;
};

struct UriHandler {
    UriHandler()
        : buf(nullptr)
    {
    }

    UriHandler(std::streambuf * buf,
               std::shared_ptr<void> bufOwnership,
               std::shared_ptr<FsObjectInfo> info = nullptr,
               UriHandlerOptions options = UriHandlerOptions())
        : buf(buf),
          bufOwnership(std::move(bufOwnership)),
          info(std::move(info)),
          options(std::move(options))
    {
    }
                     
    UriHandler(std::streambuf * buf,
               std::shared_ptr<void> bufOwnership,
               const FsObjectInfo & info,
               UriHandlerOptions options = UriHandlerOptions());
    
    std::streambuf * buf;                ///< Streambuf to operate on
    std::shared_ptr<void> bufOwnership;  ///< Ownership of the stream buffer
    std::shared_ptr<FsObjectInfo> info;  ///< Known information/metadata
    UriHandlerOptions options;
};    


/*****************************************************************************/
/* FILTER OSTREAM                                                            */
/*****************************************************************************/

/** Ostream class that has the following features:
    - It has move semantics so can be passed by reference
    - It can add filters to compress / decompress
    - It can hook into other filesystems (eg s3, ...) based upon an
      extensible API.
*/

class filter_ostream : public std::ostream {
public:
    filter_ostream();
    filter_ostream(const std::string & uri,
                   std::ios_base::openmode mode = std::ios_base::out,
                   const std::string & compression = "",
                   int compressionLevel = -1);
    filter_ostream(const Url & uri,
                   std::ios_base::openmode mode = std::ios_base::out,
                   const std::string & compression = "",
                   int compressionLevel = -1);
    filter_ostream(int fd,
                   std::ios_base::openmode mode = std::ios_base::out,
                   const std::string & compression = "",
                   int compressionLevel = -1);

    filter_ostream(int fd,
                   const std::map<std::string, std::string> & options);

    filter_ostream(const std::string & uri,
                   const std::map<std::string, std::string> & options);
    filter_ostream(const Url & uri,
                   const std::map<std::string, std::string> & options);

    filter_ostream(filter_ostream && other) noexcept;

    filter_ostream & operator = (filter_ostream && other);

    ~filter_ostream();

    void open(const std::string & uri,
              std::ios_base::openmode mode = std::ios_base::out,
              const std::string & compression = "",
              int level = -1);
    void open(const Url & uri,
              std::ios_base::openmode mode = std::ios_base::out,
              const std::string & compression = "",
              int level = -1);

    void open(int fd,
              std::ios_base::openmode mode = std::ios_base::out,
              const std::string & compression = "",
              int level = -1);
    

    void openFromStreambuf(std::streambuf * buf,
                           std::shared_ptr<void> bufOwnership,
                           const std::string & resource = "",
                           const std::string & compression = "",
                           int compressionLevel = -1);
                     
    void openFromHandler(const UriHandler & handler,
                         const std::string & resource,
                         const std::map<std::string, std::string> & options);
      
    /** Open with the given options.  Option keys are interpreted by plugins,
        but include:

        mode = comma separated list of out,append,create
        compression = string (gz, bz2, xz, ...)
        resource = string to be used in error messages
    */
    void open(const std::string & uri,
              const std::map<std::string, std::string> & options);
    void open(const Url & uri,
              const std::map<std::string, std::string> & options);

    void open(int fd,
              const std::map<std::string, std::string> & options);

    void openFromStreambuf(std::streambuf * buf,
                           std::shared_ptr<void> bufOwnership,
                           const std::string & resource,
                           const std::map<std::string, std::string> & options);
    
    void close();

    std::string status() const;

    /* notifies that an exception occurred in the streambuf */
    void notifyException()
    {
        deferredFailure = true;
    }

private:
    std::unique_ptr<std::ostream> stream;
    std::shared_ptr<void> sink;            ///< Ownership of streambuf
    std::atomic<bool> deferredFailure;
    std::map<std::string, std::string> options;
};


/*****************************************************************************/
/* FILTER ISTREAM                                                            */
/*****************************************************************************/

class filter_istream : public std::istream {
public:
    filter_istream();
    filter_istream(const std::string & uri,
                   std::ios_base::openmode mode = std::ios_base::in,
                   const std::string & compression = "");
    filter_istream(const Url & uri,
                   std::ios_base::openmode mode = std::ios_base::in,
                   const std::string & compression = "");
    /** Open with options.  The options available depend upon the scheme of
        the URI.

        Known options:

        - "mapped" (any value): if true, then the system will attempt to
          memory map the file.
        - "compression": if not set, it will detect.  If set to "none", it
          will not decompress no matter what it finds.  Otherwise, it can
          be set to a compression scheme to force that scheme to be used.
        - httpAbortOnSlowConnection: For http files, will timeout if the
          connexion is too slow. Refer to http_rest_proxy.cc for the
          specification of slow. (the parameter name is abortOnSlowConnection)
    */
    filter_istream(const std::string & uri,
                   const std::map<std::string, std::string> & options);
    filter_istream(const Url & uri,
                   const std::map<std::string, std::string> & options);
    
    filter_istream(filter_istream && other) noexcept;

    filter_istream & operator = (filter_istream && other);

    filter_istream(const UriHandler & handler,
                   const std::string & resource,
                   const std::map<std::string, std::string> & options);

    ~filter_istream();

    void open(const std::string & uri,
              std::ios_base::openmode mode = std::ios_base::in,
              const std::string & compression = "");
    void open(const Url & uri,
              std::ios_base::openmode mode = std::ios_base::in,
              const std::string & compression = "");

    /** Open.  See the documentation from the constructor with similar
        arguments.
    */
    void open(const std::string & uri,
              const std::map<std::string, std::string> & options);
    void open(const Url & uri,
              const std::map<std::string, std::string> & options);

    void openFromStreambuf(std::streambuf * buf,
                           std::shared_ptr<void> bufOwnership,
                           const std::string & resource = "",
                           const std::string & compression = "");

    void openFromHandler(const UriHandler & handler,
                         const std::string & resource,
                         const std::map<std::string, std::string> & options);
    
    void close();

    /* read the entire stream into a std::string */
    std::string readAll();

    /** Is this stream forward seekable?  This means that it can support
        skipping without having to read and discard data.

        When this is true, tellg() and seekg(positive num, ios::cur) will both
        work.
    */
    bool isForwardSeekable() const;

    /** Is this stream random seekable?  This means that it can support
        seeking around in the stream both forwards and backwards without
        needing to redo a lot of work, and it knows the length of whatever
        data is underneath.  Typically this is true for file and memory
        based streams.

        When this is true, tellg() and seekg() with any arguments will
        work.
    */
    bool isRandomSeekable() const;

    /** Is this stream forkable?  This means that another stream, with a
        separate streambuf (and read/write position) can be forked from
        this one, without needing to redo any expensive work.  If true,
        then streams can be split into multiple parts and processed in
        parallel.

        Note that to be random seekable, a stream must be forward seekable.
    */
    bool isForkable() const;

    /** Return a forked version of this stream: that is, a stream that
        points to the same data at the same position, that is created
        without performing any significant work (eg, re-downloading
        or re-decompressing).

        If supported, this should be thread-safe in that multiple
        concurrent calls to fork() should not interfere with each
        other.  It does not have to support reading from this stream
        at the same time as forking.  The read pointer of the returned
        fork should point at the same offset as the current stream.
    */
    filter_istream
    fork(const std::map<std::string, std::string> & options) const;

    /** Return the whole contents of the stream if it is mapped into
        the memory space.  Some algorithms can operate quicker if
        they have this available.

        If it's not mapped, it will return (nullptr, 0).
    */
    std::pair<const char *, size_t>
    mapped() const;

    /** Return the information and metadata about the underlying object,
        for example last modified date, etc.
    */
    FsObjectInfo info() const;
    
private:
    std::unique_ptr<std::istream> stream;
    UriHandlerOptions handlerOptions;

    std::shared_ptr<void> sink;            ///< Ownership of streambuf
    std::atomic<bool> deferredFailure;
    std::string resource;
    std::shared_ptr<FsObjectInfo> info_;
};

} // namespace MDLB
