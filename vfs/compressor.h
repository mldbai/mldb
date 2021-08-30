/* compressor.h                                                    -*- C++ -*-
   Jeremy Barnes, 19 September 2012
   Copyright (c) 2012 mldb.ai inc.  All rights reserved.

   Interface to a compressor object.

   We prefer this to other solutions as we have full control over when a
   stream is flushed and we can use this to minimise the potential for
   data loss.

   It would be nice to use boost::iostreams for this, but their flush() is
   buggy and there is no way to have precise control over flushing.
*/

#pragma once

#include <memory>
#include <functional>
#include <string>
#include <vector>

namespace MLDB {


/*****************************************************************************/
/* COMPRESSOR                                                                */
/*****************************************************************************/

struct Compressor {

    virtual ~Compressor();

    typedef std::function<size_t (const char * data, size_t len)> OnData;

    /** Flush levels. */
    enum FlushLevel {
        FLUSH_NONE,     ///< No flushing of compressor
        FLUSH_AVAILABLE,///< Flush all data would be available on decompression
        FLUSH_SYNC,     ///< Flush so that we can find our point in the file
        FLUSH_RESTART,  ///< Flush so we could restart the decompression here
    };

    /** Notify the implementation of the total size of the data that will be
        passed to compress.  Some of the compression formats can put this
        into their header to enable better decisions on allocation.

        This must be called either never or ponce before compress(), flush() or
        finish(), and behavior is undefined if the exact same amount of data
        is not then passed to compress().

        The default implementation does nothing.
    */
    virtual void notifyInputSize(uint64_t inputSize);
    
    /** Compress the given data block, and write the result into the
        given buffer.

        This will call onData zero or more times.
    */
    virtual void compress(const char * data, size_t len,
                          const OnData & onData) = 0;
    
    /** Flush the stream at the given flush level.  This will call onData
        zero or more times.
    */
    virtual void flush(FlushLevel flushLevel, const OnData & onData) = 0;

    /** Finish the stream... no more data can be written to it afterwards.
    */
    virtual void finish(const OnData & onData) = 0;

    /** Convert a filename to a compression scheme.  Returns the empty
        string if it isn't found.
    */
    static std::string filenameToCompression(const std::string & filename);

    /** Create a compressor with the given scheme.  Returns nullptr if
        the given compression scheme isn't found.
    */
    static Compressor * create(const std::string & compression,
                               int level);

    /** Describes a compressor. */
    struct Info {
        std::string name;
        std::vector<std::string> extensions;
        std::function<Compressor * (int level)> create;
    };

    /** Register a compressor.  Throws if the name is already used or the
        create function is null.
    */
    static std::shared_ptr<void>
    registerCompressor(const std::string & name,
                       const std::vector<std::string> & extensions,
                       std::function<Compressor * (int level)> create);

    /** Return information about the given compressor.  Throws an exception
        if the compression scheme is unknown.
    */
    static const Info &
    getCompressorInfo(const std::string & compressor);

    template<typename T>
    struct Register {
        Register(std::string name,
                 std::vector<std::string> extensions)
        {
            auto create = [] (int level) { return new T(level); };
            handle = registerCompressor(std::move(name),
                                        std::move(extensions),
                                        std::move(create));
        }

        std::shared_ptr<void> handle;
    };
};


/*****************************************************************************/
/* DECOMPRESSOR                                                              */
/*****************************************************************************/

struct Decompressor {

    virtual ~Decompressor();
    
    typedef std::function<size_t (const char * data, size_t len)> OnData;

    typedef std::function<void (std::shared_ptr<const char> data, size_t len)> OnSharedData;

    typedef std::function<std::shared_ptr<char> (size_t)> Allocate;
    
    /** Return the decompressed size, given a block containing the start
        of the data, a length of the block, and the total length of all
        of the comrpessed data (the block doesn't need to contain
        all of the data, in case it's not available).

        Will return

        - a length >= 0 if the length is known;
        - LENGTH_UNKNOWN if the length cannot be known by this decompressor
        - LENGTH_INSUFFICIENT_DATA if there is not enough data to know
          the length in the block, but with more data it would be known.
    */
    virtual int64_t decompressedSize(const char * block, size_t blockLen,
                                     int64_t totalLen) const = 0;

    static constexpr int64_t LENGTH_UNKNOWN = -1;
    static constexpr int64_t LENGTH_INSUFFICIENT_DATA = -2;
    
    /** Decompress the given data block, and write the result into the
        given buffer.

        This will call onData zero or more times.
    */
    virtual void decompress(const char * data, size_t len,
                            const OnData & onData) = 0;

    virtual void decompress(std::shared_ptr<const char> data, size_t len,
                            const OnSharedData & onData,
                            const Allocate & allocate);
    
    /** Finish decompressing the stream... no more data can be read from
        it afterwards.

        This may also do things like check the checksums, etc.
    */
    virtual void finish(const OnData & onData) = 0;

    virtual void finish(const OnSharedData & onData,
                        const Allocate & allocate);


    typedef std::function<bool (size_t blockNumber,
                                uint64_t blockOffset,
                                std::shared_ptr<const char> blockStart,
                                size_t blockLength)>
        ForEachBlockFunction;

    typedef std::function<std::pair<std::shared_ptr<const char>, size_t> (size_t)>
        GetDataFunction;
    
    virtual bool forEachBlockParallel(size_t requestedBlockSize,
                                      const GetDataFunction & getData,
                                      const ForEachBlockFunction & onBlock,
                                      const Allocate & allocate,
                                      int maxParallelism = -1);
    
    /** Create a compressor with the given scheme.  Returns nullptr if
        the given compression scheme isn't found.
    */
    static Decompressor * create(const std::string & compression);

    /** Describes a compressor. */
    struct Info {
        std::string name;
        std::vector<std::string> extensions;
        std::function<Decompressor * ()> create;
    };
    
    /** Register a decompressor.  Throws if the name is already used or the
        create function is null.
    */
    static std::shared_ptr<void>
    registerDecompressor(const std::string & name,
                         const std::vector<std::string> & extensions,
                         std::function<Decompressor * ()> create);
    
    /** Return information about the given compressor.  Throws an exception
        if the compression scheme is unknown.
    */
    static const Info &
    getDecompressorInfo(const std::string & decompressor);
    
    template<typename T>
    struct Register {
        Register(std::string name,
                 std::vector<std::string> extensions)
        {
            auto create = [] () { return new T(); };
            handle = registerDecompressor(std::move(name),
                                          std::move(extensions),
                                          std::move(create));
        }
        
        std::shared_ptr<void> handle;
    };
};

} // namespace MLDB
