/* content_descriptor.h                                            -*- C++ -*-
   Jeremy Barnes, 1 September 2018
   Copyright (c) 2018 Mldb.ai Inc.  All rights reserved.

   Structures to allow content to be obtained from a descriptor rather than
   simply by a URL.  Allows a much richer subsystem then simply "give me
   what is at this URL".
*/

#pragma once

#include <vector>
#include <map>
#include "mldb/types/url.h"
#include "mldb/types/string.h"
#include "mldb/types/any.h"
#include "mldb/types/path.h"
#include "mldb/types/value_description_fwd.h"
#include "mldb/block/memory_region.h"
#include "mldb/vfs/fs_utils.h"
#include "mldb/watch/watch.h"

namespace MLDB {

class filter_istream;
struct Url;


/*****************************************************************************/
/* ACCESS PATTERN                                                            */
/*****************************************************************************/

enum AccessPattern {
    ADV_UNKNOWN,     ///< Unknown access pattern
    ADV_SEQUENTIAL,  ///< Sequential or mostly sequential access to all data
    ADV_RANDOM       ///< Random access to all or part of the data
};


/*****************************************************************************/
/* ACCESS WHICH                                                              */
/*****************************************************************************/

enum AccessWhich {
    ADV_SOME,      ///< Some of the content will be accessed (unknown how)
    ADV_NONE,      ///< None of the content will be accessed
    ADV_ALL,       ///< All of the content will be accessed
    ADV_BEGINNING  ///< The beginning of the content will be accessed
};


/*****************************************************************************/
/* CONTENT HASH                                                              */
/*****************************************************************************/

struct ContentHash {
    Utf8String type;   ///< Type of the content hash; defines how it is used
    Utf8String value;  ///< Value of the content hash.  Typically a hex string
    Utf8String authority; ///< Authority which calculated the value if not std
};

DECLARE_STRUCTURE_DESCRIPTION(ContentHash);


/*****************************************************************************/
/* CONTENT HASHES                                                            */
/*****************************************************************************/

struct ContentHashes: public std::vector<ContentHash> {
    using std::vector<ContentHash>::vector;
};

PREDECLARE_VALUE_DESCRIPTION(ContentHashes);


/*****************************************************************************/
/* CONTENT DESCRIPTOR                                                        */
/*****************************************************************************/

struct ContentDescriptor {
    ContentHashes content;

    // Add a single (non-hashed) URL to enable it to be used in place of legacy URLs
    void addUrl(Utf8String url);

    Url getUrl() const;
    Utf8String getUrlString() const;
    std::u8string getUrlStringUtf8() const;
};

PREDECLARE_VALUE_DESCRIPTION(ContentDescriptor);


/*****************************************************************************/
/* BLOCK SEQUENCE HANDLER                                                    */
/*****************************************************************************/

/** Models something that produces data block by block, but does not know
    its length and cannot know its full information until the sequence is
    finished.

    This useful for modelling files of an unknown size, the output of
    compressors with no way to determine block length, etc.

    The only operations supported are to get the next block.
*/

struct BlockSequenceHandler {
    virtual ~BlockSequenceHandler() = default;

    virtual FrozenMemoryRegion getNext() = 0;
};


/*****************************************************************************/
/* INDEXED BLOCK SEQUENCE HANDLER                                            */
/*****************************************************************************/

/** Models a BlockSequenceHandler that can access individual blocks with
    the aid of an index, and thus perform some kind of random access
    over blocks (which may be expensive, but is less expensive than
    restarting the entire sequence to get to a given block).

    Blocks may only be randomly accessed once they have reached their
    index (they need to be discovered with getNext() first).

    This is useful for modelling compression schemes that have independently
    compressed blocks, like the lz4 container format.
*/

struct IndexedBlockSequenceHandler {
    virtual ~IndexedBlockSequenceHandler() = default;

    virtual std::pair<Path, FrozenMemoryRegion>
    getNext() = 0;

    virtual FrozenMemoryRegion getAtIndex(const Path & index) const = 0;
};


/*****************************************************************************/
/* CONTENT HANDLER                                                           */
/*****************************************************************************/

struct ContentHandler {
    virtual ~ContentHandler() = 0;

    virtual std::shared_ptr<const ContentHandler> getSharedThis() const = 0;

    virtual std::shared_ptr<ContentHandler> getSharedThis() = 0;
    
    virtual ContentDescriptor getDescriptor() const = 0;

    virtual ContentDescriptor getCanonicalDescriptor() const = 0;
    
    virtual FsObjectInfo getInfo() const = 0;

    virtual uint64_t getSize() const = 0;
    
    virtual Date getLastModified() const = 0;

    virtual AccessPattern getPattern() const = 0;

    virtual bool
    forEachBlockParallel(uint64_t startOffset,
                         uint64_t requestedBlockSize,
                         int maxParallelism,
                         std::function<bool (size_t blockNum, uint64_t blockOffset,
                                             FrozenMemoryRegion block)> fn)
        const;
    
    virtual FrozenMemoryRegion getRange(uint64_t offset = 0,
                                        int64_t length = -1) const;

    /** Gets the "natural" part of the range containing the given memory
        range.  The first return value is the actual offset of the first
        byte of the range; the second is the memory range itself.

        Using this interface allows an application to aligh itself to the
        natural blocks that are present in a content source.  This is
        important in situations where content is compressed or encrypted
        block-by-block as it allows blocks to be cached and re-used.

        This interface may be used to request data from a source which
        has an unknown length, without first calling getSize()
        which will need to scan the entire content to determine the length.

        In the case that the requested range is entirely beyond the end
        of the available content, the function should return
        { availableLength, FrozenMemoryRegion() } to indicate the end
        of the range.

        In the case that the requested range is partially beyond the end
        of the available content, a range containing the requested offset
        until the end of the available content should be returned.

        The calling function may not assume that length bytes are
        available unless it knows that the entire range is within the
        available content.
    */
    virtual std::pair<uint64_t, FrozenMemoryRegion>
    getRangeContaining(uint64_t offset, uint64_t length) const = 0;
    
    virtual filter_istream
    getStream(const std::map<Utf8String, Any> & options
                  = std::map<Utf8String, Any>()) const;

    virtual std::shared_ptr<ContentHandler>
    atOffset(uint64_t offset) const;
};


/*****************************************************************************/
/* UTILITY FUNCTIONS                                                         */
/*****************************************************************************/

std::shared_ptr<ContentHandler>
getContent(const ContentDescriptor & descriptor);


/** Return a content handler that decompresses the given input. */
std::shared_ptr<ContentHandler>
decompress(std::shared_ptr<ContentHandler> input,
           const std::string & compression);


std::shared_ptr<ContentHandler>
getDecompressedContent(const ContentDescriptor & descriptor,
                       size_t blockSize = 1024 * 1024);


#if 0
struct ContentBlockHandler {
    virtual ~ContentBlockHandler() = 0;
    virtual FsObjectInfo getInfo() const = 0;
    virtual FrozenMemoryRegion getBlock() const = 0;
};
#endif

#if 0
filter_istream getContentStream(const ContentDescriptor & descriptor,
                                const std::map<Utf8String, Any> & options
                                    = std::map<Utf8String, Any>());
#endif


/*****************************************************************************/
/* HASH SCHEME                                                               */
/*****************************************************************************/

#if 0

struct HashScheme {

    virtual ~HashScheme() = 0;

    
    
    static std::shared_ptr<void>
    registerHashScheme(const Utf8String & name,
                       std::function<HashScheme * ()> create);
    
    template<typename T>
    struct Register {
        Register(std::string name)
        {
            auto create = [] () { return new T(); };
            handle = registerHashScheme(std::move(name),
                                        std::move(create));
        }
        
        std::shared_ptr<void> handle;
    };
};

#endif

} // namespace MLDB

