/* content_descriptor.cc                                            -*- C++ -*-
   Jeremy Barnes, 1 September 2018
   Copyright (c) 2018 Mldb.ai Inc.  All rights reserved.

*/

#include "content_descriptor.h"
#include "file_serializer.h"
#include "mldb/types/structure_description.h"
#include "mldb/types/annotated_exception.h"
#include "mldb/vfs/filter_streams.h"
#include "mldb/types/any_impl.h"
#include <boost/iostreams/stream_buffer.hpp>
#include <mutex>
#include "mldb/vfs/compressor.h"
#include "mldb/watch/watch_impl.h"
#include "mldb/base/thread_pool.h"


using namespace std;

namespace MLDB {


/*****************************************************************************/
/* CONTENT HASH                                                              */
/*****************************************************************************/

IMPLEMENT_STRUCTURE_DESCRIPTION(ContentHash)
{
    addField("type", &ContentHash::type,
             "Type of hash (indexes into registered hash functions)");
    addField("value", &ContentHash::value,
             "Value of hash");
}


/*****************************************************************************/
/* CONTENT HASHES                                                            */
/*****************************************************************************/

struct ContentHashesDescription
    : public ValueDescriptionT<ContentHashes> {

    virtual void parseJsonTyped(ContentHashes * content,
                                JsonParsingContext & context) const
    {
        // For backwards compatibility, just a string is assumed to be the URL.
        if (context.isString()) {
            Utf8String url = context.expectStringUtf8();
            ContentHash hash{"url", std::move(url)};
            content->emplace_back(std::move(hash));
            return;
        }

        auto onMember = [&] ()
            {
                Utf8String key = context.fieldName();

                ContentHash hash;
                hash.type = std::move(key);
                if (context.isString()) {
                    Utf8String val = context.expectStringUtf8();
                    hash.value = std::move(val);
                }
                else if (context.isObject()) {
                    auto onMember = [&] ()
                    {
                        if (context.fieldName() == "value") {
                            hash.value = context.expectStringUtf8();
                        }
                        else if (context.fieldName() == "authority") {
                            hash.authority = context.expectStringUtf8();
                        }
                        else {
                            context.exception
                            ("expected 'value' or 'authority' as keys "
                             "in ContentHash, not '" + context.fieldName()
                             + "'");
                        }
                    };

                    context.forEachMember(onMember);
                }
                else {
                    context.exception("unexpected type for ContentHash value; "
                                      "expected string or object");
                }
                content->emplace_back(std::move(hash));
            };
        
        context.forEachMember(onMember);
    }
    
    virtual void printJsonTyped(const ContentHashes * val,
                                JsonPrintingContext & context) const
    {
        if (val->size() == 1 && (*val)[0].type == "url") {
            context.writeStringUtf8((*val)[0].value);
            return;
        }

        context.startObject();

        for (auto & v: *val) {
            context.startMember(v.type);
            context.writeStringUtf8(v.value);
        }
        
        context.endObject();
    }
    
    virtual bool isDefaultTyped(const ContentHashes * val) const
    {
        return val->empty();
    }
};

DEFINE_VALUE_DESCRIPTION_NS(ContentHashes, ContentHashesDescription);


/*****************************************************************************/
/* CONTENT DESCRIPTOR                                                        */
/*****************************************************************************/

Url
ContentDescriptor::
getUrl() const
{
    for (auto & h: content) {
        if (h.type == "url") {
            return Url(h.value.rawString());
        }
    }

    throw AnnotatedException(400, "Content description has no URL");
}

Utf8String
ContentDescriptor::
getUrlString() const
{
    return getUrl().toDecodedString();
}

std::string
ContentDescriptor::
getUrlStringUtf8() const
{
    return getUrl().toDecodedString();
}

static ContentHashesDescription contentHashesDescription;
    
struct ContentDescriptorDescription
    : public ValueDescriptionT<ContentDescriptor> {

    virtual void parseJsonTyped(ContentDescriptor * val,
                                JsonParsingContext & context) const
    {
        contentHashesDescription.parseJsonTyped(&val->content, context);
    }
    
    virtual void printJsonTyped(const ContentDescriptor * val,
                                JsonPrintingContext & context) const
    {
        contentHashesDescription.printJsonTyped(&val->content, context);
    }
    
    virtual bool isDefaultTyped(const ContentDescriptor * val) const
    {
        return contentHashesDescription.isDefaultTyped(&val->content);
    }
};

DEFINE_VALUE_DESCRIPTION_NS(ContentDescriptor, ContentDescriptorDescription);


/*****************************************************************************/
/* AT OFFSET CONTENT HANDLER                                                 */
/*****************************************************************************/

#if 0

struct AtOffsetContentHandler: public ContentHandler {
    virtual ~AtOffsetContentHandler()
    {
    }

    virtual FsObjectInfo
    getInfo() const
    {
        FsObjectInfo prev = underlying->getInfo();
        FsObjectInfo result;
        if (prev.size != -1)
            result.size = prev.size - offset;
        result.lastModified = prev.lastModified;
        return result;
    }
        
    virtual FrozenMemoryRegion getRange(uint64_t offset = 0,
                                        int64_t length = -1) const
    {
    }

    virtual filter_istream getStream() const
    {
        
    }

    virtual Date getLastModified() const
    {
        return underlying->lastModified();
    }

    virtual std::shared_ptr<ContentHandler>
    atOffset(uint64_t offset) const override
    {
        return underlying->atOffset(offset + this->offset);
    }

    std::shared_ptr<ContentHandler> underlying;
    uint64_t offset = 0;
};

#endif

/*****************************************************************************/
/* CONTENT INPUT SEEKABLE STREAMBUF                                          */
/*****************************************************************************/

struct ContentInputSeekableStreambuf {

    ContentInputSeekableStreambuf(std::shared_ptr<const ContentHandler> handler)
    {
        impl.reset(new Impl(std::move(handler)));
    }

    typedef char char_type;
    
    static_assert(sizeof(char_type) == 1,
                  "content streams for single-char bytes only");

    struct category
        : public boost::iostreams::input_seekable,
          public boost::iostreams::device_tag,
          public boost::iostreams::closable_tag {
    };
    
    struct Impl {
        Impl(std::shared_ptr<const ContentHandler> handler__)
            : handler(std::move(handler__))
        {
        }

        ~Impl()
        {
        }

        std::shared_ptr<const ContentHandler> handler;
        FrozenMemoryRegion mem;
        size_t regionStartOffset = 0;
        size_t offset = 0;

        static constexpr size_t blockSize = 1024 * 1024;
        
        std::streamsize read(char_type* s, std::streamsize n)
        {
            if (offset == handler->getSize()) {
                // end of stream
                return -1;
            }

            if (!mem
                || offset < regionStartOffset
                || offset >= regionStartOffset + mem.length()) {
                // We need to get a different block, as we're outside of
                // the current one

                size_t length
                    = std::min<size_t>(handler->getSize() - offset,
                                       blockSize);
                
                std::tie(regionStartOffset, mem)
                    = handler->getRangeContaining(offset, length);

                ExcAssertGreaterEqual(offset, regionStartOffset);
            }
            
            uint64_t regionOffset = offset - regionStartOffset;
            uint64_t charsLeft = mem.length() - regionOffset;
            uint64_t numChars = std::min<size_t>(n, charsLeft);
            ExcAssertGreater(numChars, 0);
            std::memcpy(s, mem.data() + offset, numChars);
            offset += numChars;
            return numChars;
        }

        std::streampos seek(std::streamsize where, std::ios_base::seekdir dir)
        {
            //cerr << "seek " << where << " " << dir << endl;
            int64_t newOffset;
            
            switch (dir) {
            case ios_base::beg:
                if (where < 0 || where > handler->getSize())
                    return -1;
                newOffset = where;
                break;
            case ios_base::end:
                if (where < 0 || where > handler->getSize())
                    return -1;
                newOffset = handler->getSize() - where;
                break;
            case ios_base::cur:
                newOffset = offset + where;
                if (newOffset < 0 || newOffset > handler->getSize())
                    return -1;
                break;
            default:
                ExcAssert(false);
                break;
            }

            if (newOffset < 0 || newOffset > handler->getSize()) {
                return -1;  // seek error
            }

            return offset = newOffset;
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
        impl.reset();
    }
};


/*****************************************************************************/
/* CONTENT HANDLER                                                           */
/*****************************************************************************/

ContentHandler::
~ContentHandler()
{
}

bool
ContentHandler::
forEachBlockParallel(uint64_t startOffset,
                     uint64_t requestedBlockSize,
                     int maxParallelism,
                     std::function<bool (size_t, uint64_t, FrozenMemoryRegion)> fn) const
{
    size_t offset = startOffset;

    std::atomic<bool> finished(false);
    
    ThreadWorkGroup tp(maxParallelism);

    size_t blockNumber = 0;
    
    while (!finished && !tp.hasException()) {
        uint64_t startOffset;
        FrozenMemoryRegion region;

        std::tie(startOffset, region)
            = getRangeContaining(offset, requestedBlockSize);

        if (!region)
            break;
        
        uint64_t toSkip = startOffset - offset;
        ExcAssertEqual(toSkip, 0);
        size_t myBlockNumber = blockNumber++;
        
        auto processBlock
            = [myBlockNumber, startOffset, region, &fn, &finished] ()
            {
                if (!fn(myBlockNumber, startOffset, region))
                    finished = true;
            };

        if (maxParallelism == 1)
            processBlock();
        else
            tp.add(std::move(processBlock));

        offset = startOffset + region.length();
    }

    tp.waitForAll();

    return !finished;
}

FrozenMemoryRegion
ContentHandler::
getRange(uint64_t offset, int64_t length) const
{
    uint64_t startOffset = 0;
    FrozenMemoryRegion region;

    std::tie(startOffset, region) = getRangeContaining(offset, length);

    if (!region) {
        throw Exception("Attempt to get out-of-range offset");
    }
    
    uint64_t skipBytes = offset - startOffset;

    region = region.range(skipBytes, region.length());

    if (length != -1 && region.length() != length) {
        throw Exception("Attempt to get out-of-range length");
    }

    return region;
}

filter_istream
ContentHandler::
getStream(const std::map<Utf8String, Any> & options) const
{
    bool isMapped = false;
    
    std::map<std::string, std::string> options2;
    for (auto & opt: options) {
        if (opt.first == "mapped") {
            isMapped = opt.second.asJson().asBool();
        }
        else {
            options2[opt.first.rawString()] = opt.second.asJson().toString();
        }
    }

    auto descriptor = getDescriptor();
    
    std::string compression
        = Compressor::filenameToCompression(descriptor.getUrlStringUtf8());
    
    cerr << "url = " << descriptor.getUrlStringUtf8() << " compression = "
         << compression << endl;
    
    while (isMapped) {  // actually an if, but allows break
        // Just get one single big block
        auto contentHandler = getContent(descriptor);

        struct Vals {
            FsObjectInfo info;
            FrozenMemoryRegion mem;

#if 0
            ~Vals()
            {
                cerr << endl << endl << endl;
                cerr << "NO MORE MAPPING VALS" << endl;
                cerr << endl << endl << endl;
            }
#endif
        };

        auto vals = std::make_shared<Vals>();
        vals->info = contentHandler->getInfo();
        vals->mem = contentHandler->getRange();

        // Verify the hashes that were asked for
        for (auto & hash: descriptor.content) {
            if (hash.type == "url")
                continue;
            // ...  need to verify ...
            cerr << "warning: not verifying hash " << jsonEncodeStr(hash)
                 << endl;
        }
        
        std::shared_ptr<Decompressor> decompressor;
        if (compression != "") {
            // Get the decompressor and decompress the block with it
            decompressor.reset(Decompressor::create(compression));

            int64_t outputSize = decompressor
                ->decompressedSize(vals->mem.data(),
                                   vals->mem.length(),
                                   vals->mem.length());

            if (outputSize < 0) {
                break;
            }

            static MemorySerializer serializer;
            
            // Create a writeable memory block to hold the decompressed
            // version
            auto output
                = serializer.allocateWritable(outputSize, 4096 /* alignment */);

            size_t pos = 0;
            auto onData = [&] (const char * data, size_t len)
                {
                    ssize_t remaining = output.length() - pos;
                    if (remaining < len) {
                        throw AnnotatedException
                            (400, "Compressor length was wrong",
                             "url", descriptor.getUrlString());
                    }
                    std::memcpy(output.data() + pos, data, len);
                    pos += len;
                    return len;
                };
            
            decompressor->decompress(vals->mem.data(), vals->mem.length(),
                                     onData);

            vals->mem = output.freeze();
            vals->info.size = outputSize;
        }
        
        UriHandlerOptions uriOptions;
        uriOptions.isForwardSeekable = true;
        uriOptions.isRandomSeekable = true;
        uriOptions.mapped = vals->mem.data();
        uriOptions.mappedSize = vals->mem.length();

        //cerr << "returning " << vals->mem.length() << " bytes mapped at "
        //     << (void *)vals->mem.data() << endl;

        ContentInputSeekableStreambuf bufImpl(getSharedThis());
        std::streambuf * buf
            = new boost::iostreams::stream_buffer<ContentInputSeekableStreambuf>
            (std::move(bufImpl), 1024 * 1024);
        
        UriHandler handler(buf /* streambuf */,
                           vals /* ownership */,
                           std::shared_ptr<FsObjectInfo>(vals, &vals->info) /* info */,
                           uriOptions);
        
        filter_istream stream(handler, descriptor.getUrlStringUtf8(), options2);
        return stream;
    }

    // Not mapped.  We go block by block.
    filter_istream result(descriptor.getUrlStringUtf8(), options2);
    return result;
}

std::shared_ptr<ContentHandler>
ContentHandler::
atOffset(uint64_t offset) const
{
    throw Exception("atOffset");
}


/*****************************************************************************/
/* URL CONTENT HANDLER                                                       */
/*****************************************************************************/

struct UrlContentHandler
    : public ContentHandler,
      public std::enable_shared_from_this<UrlContentHandler> {
    UrlContentHandler(const ContentDescriptor & descriptor)
        : descriptor(descriptor),
          stream(descriptor.getUrlStringUtf8(),
                 { { "mapped", "true" }, { "compression", "none" } })
    {
    }

    virtual ~UrlContentHandler()
    {
    }
    
    virtual std::shared_ptr<const ContentHandler> getSharedThis() const override
    {
        return shared_from_this();
    }

    virtual std::shared_ptr<ContentHandler> getSharedThis() override
    {
        return shared_from_this();
    }
    
    virtual ContentDescriptor getDescriptor() const
    {
        return descriptor;
    }

    virtual ContentDescriptor getCanonicalDescriptor() const
    {
        return descriptor;  // TODO: collect from info, etc
    }
    
    virtual FsObjectInfo getInfo() const override
    {
        return stream.info();
    }

    virtual uint64_t getSize() const
    {
        auto size = stream.info().size;
        if (size == -1) {
            // Size isn't known.  We need to ask for the whole thing to get
            // the size out of it.
            // TODO: do better than this!

            // First attempt to seek
            if (stream.isRandomSeekable()) {
                
            }
            
            auto total = getRangeContaining(0, -1).second.length();
            return total;
        }
        return stream.info().size;
    }

    virtual Date getLastModified() const override
    {
        return stream.info().lastModified;
    }

    virtual AccessPattern getPattern() const override
    {
        return ADV_UNKNOWN;
    }
    
    virtual FrozenMemoryRegion
    getRange(uint64_t offset, int64_t length) const override
    {
        const char * data;
        size_t len;

        std::tie(data, len) = stream.mapped();
        if (data && length) {
            if (length == -1)
                length = len - offset;
            return FrozenMemoryRegion(shared_from_this(),
                                      data + offset, length);
        }

        std::unique_lock<std::mutex> guard(mutex);
        
        // We do it by seeking if we can
        if (content.empty()) {
            content = stream.readAll();
            //cerr << "read all returned " << content.size() << endl;
        }

        //cerr << "offset " << offset << " length " << length << endl;
        
        if (length == -1)
            length = content.length() - offset;

        ExcAssertLessEqual(offset, content.size());
        ExcAssertGreaterEqual(length, 0);
        ExcAssertLessEqual(offset + length, content.size());

        //cerr << "returning from " << offset << " to " << length << endl;

        
        return FrozenMemoryRegion(shared_from_this(),
                                  content.data() + offset, length);
    }

    virtual std::pair<uint64_t, FrozenMemoryRegion>
    getRangeContaining(uint64_t offset, uint64_t length) const
    {
        // TODO: later, look for natural chunks in the data and use
        // them.

        //cerr << "getRangeContaining " << offset << " " << length
        //     << " " << getSize() << endl;
        
        if (offset >= getSize()) {
            return { getSize(), FrozenMemoryRegion() };
        }
        if (offset + length > getSize()) {
            length = getSize() - offset;
        }
        
        return { offset, getRange(offset, length) };
#if 0
        const char * data;
        size_t len;

        std::tie(data, len) = stream.mapped();
        if (data && length) {
            if (length == -1)
                length = len - offset;
            return FrozenMemoryRegion(shared_from_this(),
                                      data, len);
        }
#endif        
#if 0        
        if (stream.isRandomSeekable()) {
            stream.seekg(offset, ios_base::beg);
        }
#endif
    }
    
    ContentDescriptor descriptor;
    mutable filter_istream stream;
    mutable std::string content;
    mutable std::mutex mutex;
};

std::shared_ptr<ContentHandler>
getContent(const ContentDescriptor & descriptor)
{
    return std::make_shared<UrlContentHandler>(descriptor);
}


/*****************************************************************************/
/* CONTENT DECOMPRESSOR                                                      */
/*****************************************************************************/

struct ContentDecompressor
    : public ContentHandler,
      public std::enable_shared_from_this<ContentDecompressor> {

    ContentDecompressor(std::shared_ptr<const ContentHandler> source__,
                        std::string compression__,
                        size_t blockSize)
        : serializer("tmp", "content-decompressor"),
          source(std::move(source__)),
          compression(std::move(compression__)),
          blockSize(blockSize)
    {
        auto inputSize = source->getSize();

        uint64_t firstOffset = 0;
        FrozenMemoryRegion firstBlock;

        std::tie(firstOffset, firstBlock)
            = source->getRangeContaining(0, 4096);

        ExcAssertEqual(firstOffset, 0);
        
        decompressor.reset(Decompressor::create(compression));

        auto res = decompressor->decompressedSize(firstBlock.data(),
                                                  firstBlock.length(),
                                                  inputSize);

        if (res >= 0) {
            knownContentSize = res;
        }
        else knownContentSize = -1;
    }
    
    virtual ~ContentDecompressor()
    {
    }

    virtual std::shared_ptr<const ContentHandler> getSharedThis() const override
    {
        return shared_from_this();
    }

    virtual std::shared_ptr<ContentHandler> getSharedThis() override
    {
        return shared_from_this();
    }
    
    virtual ContentDescriptor getDescriptor() const override
    {
        // TODO: not right; need to add in the decompressor
        return source->getDescriptor();
    }

    virtual ContentDescriptor getCanonicalDescriptor() const override
    {
        // TODO: not right; need to add in the decompressor
        return source->getDescriptor();
    }
    
    virtual FsObjectInfo getInfo() const override
    {
        return source->getInfo();
        // todo: size, checksums are different
    }

    virtual uint64_t getSize() const override
    {
        if (knownContentSize >= 0)
            return knownContentSize;

        // Otherwise, we need to scan to the end to get the known size 
        std::unique_lock<std::mutex> guard(mutex);
        
        while (!finished) {
            getNewRegion();
        }

        return knownContentSize = doneOutputOffset;
    }
    
    virtual Date getLastModified() const override
    {
        return source->getLastModified();
    }

    virtual AccessPattern getPattern() const override
    {
        return pattern;
    }
    
    virtual bool
    forEachBlockParallel(uint64_t startOffset,
                         uint64_t requestedBlockSize,
                         int maxParallelism,
                         std::function<bool (size_t, uint64_t, FrozenMemoryRegion)> fn) const
    {
        //maxParallelism = 1;

        if (!decompressor)
            decompressor.reset(Decompressor::create(compression));

        uint64_t currentOffset = 0 /* startOffset */;
        auto getData = [&] (size_t numBytes)
            -> std::pair<std::shared_ptr<const char>, size_t>
            {
                //cerr << "getData for " << numBytes << " bytes" << " at " << currentOffset
                //     << endl;

                uint64_t startOffset;
                FrozenMemoryRegion region;

                std::tie(startOffset, region)
                    = source->getRangeContaining(currentOffset, numBytes);
                

                ssize_t toSkip = currentOffset - startOffset;

                //cerr << "toSkip = " << toSkip << endl;
                
                if (toSkip > 0) {
                    region = region.rangeAtEnd(region.length() - toSkip);
                }

                currentOffset += region.length();
                
                auto onFree = [region] (const char *) {};

                return { std::shared_ptr<const char>(region.data(), std::move(onFree)),
                         region.length() };
            };

        auto onBlock = [&] (size_t blockNumber,
                            uint64_t blockStartOffset,
                            std::shared_ptr<const char> mem,
                            size_t length) -> bool
            {
                if (blockStartOffset + length < startOffset)
                    return true;

                if (blockStartOffset < startOffset) {
                    size_t toSkip = startOffset - blockStartOffset;
                    mem = std::shared_ptr<const char>(mem, mem.get() + toSkip);
                    length -= toSkip;
                    blockStartOffset += toSkip;
                }

                FrozenMemoryRegion region(mem, mem.get(), length);
                return fn(blockNumber, blockStartOffset, std::move(region));
            };

        auto allocate = [&] (size_t len) -> std::shared_ptr<char>
            {
                return std::shared_ptr<char>(new char[len], [] (char * p) { delete[] p; });
            };
        
        return decompressor->forEachBlockParallel(requestedBlockSize, getData, onBlock,
                                                  allocate, maxParallelism);
    }
    
    virtual std::pair<uint64_t, FrozenMemoryRegion>
    getRangeContaining(uint64_t offset, uint64_t length) const override
    {
        std::unique_lock<std::mutex> guard(mutex);

        if (pattern == ADV_UNKNOWN) {
            if (offset < lastStartOffset) {
                pattern = ADV_RANDOM;
            }
            else {
                pattern = ADV_SEQUENTIAL;
            }
        }

        // If it looks like we're just scanning linearly, then attempt
        // to service without doing anything special.
        if (pattern == ADV_SEQUENTIAL) {
            // If we're in the same block as last time, or just after the
            // end, then we're still linear
            if (offset < lastStartOffset) {
                cerr << "not sequential; restart" << endl;
                // We need to restart to move to random
                pattern = ADV_RANDOM;
                finished = false;
                decompressor.reset();
                regions.clear();
                // Fall through to random
            }
            else {
                if (finished) {
                    return { doneOutputOffset, FrozenMemoryRegion() };
                }
        
                //cerr << "offset = " << offset << " last = " << lastStartOffset
                //     << "-" << lastEndOffset << " pattern = " << pattern << endl;
                //cerr << "last returned from " << lastBlockOffset << " to "
                //     << lastBlockOffset + lastBlock.length() << endl;

                if (lastBlock
                    && offset >= lastBlockOffset
                    && offset + length <= lastBlockOffset + lastBlock.length()) {

                    // Can service from the last read block
                    //cerr << "can service from last read block" << endl;

                    lastStartOffset = offset;
                    lastEndOffset = offset + length;

                    return { lastBlockOffset, lastBlock };
                }

                static MemorySerializer memSerializer;
                
                //cerr << "sequential but can't service" << endl;
                if (offset < lastBlockOffset + lastBlock.length()) {
                    // We're overlapping with the end of the last block
                    // We need to take part of the last and also get some
                    // new data

                    size_t overlappingAtEnd
                        = lastBlockOffset + lastBlock.length() - offset;
                    
                    //cerr << "overlapping with "
                    //     << overlappingAtEnd
                    //     << " bytes of last block" << endl;

                    regions[offset]
                        = lastBlock.rangeAtEnd(overlappingAtEnd);
                }

                uint64_t minToRead
                    = offset + length
                    - lastBlockOffset - lastBlock.length();

                decompressNewBlock(memSerializer, minToRead);

                if (regions.empty()) {
                    ExcAssert(finished);
                    return { doneOutputOffset, FrozenMemoryRegion() };
                }
                
                std::vector<FrozenMemoryRegion> toCombine;
                for (auto & r: regions) {
                    toCombine.emplace_back(std::move(r.second));
                }

                lastBlockOffset = regions.begin()->first;
                lastBlock = FrozenMemoryRegion::combined(toCombine);

                //cerr << "got " << regions.size() << " blocks from "
                //     << lastBlockOffset << " to "
                //     << lastBlockOffset + lastBlock.length()
                //     << endl;

                regions.clear();

                lastStartOffset = offset;
                lastEndOffset = offset + length;

                return { lastBlockOffset, lastBlock };
            }
        }

        if (!decompressor) {
            decompressor.reset();
            doneInputOffset = 0;
            doneOutputOffset = 0;
            finished = false;

            if (knownContentSize != 0) {
                serializer.reserve(knownContentSize);
            }
        }

        while (!finished && doneOutputOffset < offset + length)
            getNewRegion();

        return serializer.getRangeContaining(offset, length);
    }

    // lock must be held
    // Create a new decompressed block and add it to the map of blocks
    void decompressNewBlock(MappedSerializer & serializer,
                            size_t minLength)
        const
    {
        if (!decompressor)
            decompressor.reset(Decompressor::create(compression));

        auto startOffset = doneOutputOffset;
        
        while (!finished && doneOutputOffset < startOffset + minLength) {
            size_t maxInput = blockSize;
            uint64_t blockOffset;
            FrozenMemoryRegion input;

            std::tie(blockOffset, input)
                = source->getRangeContaining(doneInputOffset, maxInput);

            //cerr << "asking for input from " << doneInputOffset << " for "
            //     << maxInput << "characters" << endl;
            //cerr << "got input from " << blockOffset
            //     << " for " << input.length() << " characters" << endl;

            if (!input) {
                finished = true;
                break;
            }
            
            // Feed the whole block to the decompressor
            size_t startOffset = doneInputOffset - blockOffset;

            auto onData = [&] (const char * data, size_t length) -> size_t
                {
                    MutableMemoryRegion region
                        = serializer.allocateWritable(length, 1 /* align */);
                    std::memcpy(region.data(), data, length);

                    regions[doneOutputOffset] = region.freeze();

                    doneOutputOffset += length;
                    
                    //cerr << "decompressed " << length << " bytes at "
                    //     << doneOutputOffset << endl;

                    return length;
                };
            
            decompressor->decompress(input.data() + startOffset,
                                     input.length() - startOffset,
                                     onData);

            doneInputOffset += input.length() - startOffset;
        }
    }
    
    // lock must be held...
    void getNewRegion() const
    {
        decompressNewBlock(serializer, blockSize /* min length */);
    }

    mutable std::mutex mutex;
    mutable TemporaryFileSerializer serializer;
    mutable AccessPattern pattern = ADV_UNKNOWN;
    std::shared_ptr<const ContentHandler> source;
    std::string compression;
    size_t blockSize;
    mutable int64_t knownContentSize = -1;
    mutable uint64_t doneInputOffset = 0;
    mutable uint64_t doneOutputOffset = 0;
    mutable bool finished = false;
    mutable std::shared_ptr<Decompressor> decompressor;
    mutable std::map<uint64_t, FrozenMemoryRegion> regions;

    // Offsets of the last access, used to detect and respond to linear or
    // nearly linear access patterns
    mutable uint64_t lastStartOffset = 0;
    mutable uint64_t lastEndOffset = 0;
    mutable uint64_t lastBlockOffset = 0;
    mutable FrozenMemoryRegion lastBlock;
};

std::shared_ptr<ContentHandler>
decompress(std::shared_ptr<ContentHandler> source,
           const std::string & compression,
           size_t blockSize)
{
    if (compression == "" || compression == "none" || compression == "null") {
        return source;
    }

    return std::make_shared<ContentDecompressor>(source, compression, blockSize);
}

std::shared_ptr<ContentHandler>
getDecompressedContent(const ContentDescriptor & descriptor,
                       size_t blockSize)
{
    return decompress(getContent(descriptor),
                      Compressor::filenameToCompression
                          (descriptor.getUrlStringUtf8()),
                      blockSize);
}

#if 0
struct FilterStreamContentHandler
    : public ContentHandler,
      public std::enable_shared_from_this<FilterStreamContentHandler> {
    FilterStreamContentHandler(const ContentDescriptor & descriptor)
        : stream(descriptor.getUrlStringUtf8(), { { "mapped", "true" }, { "compression", "none" } })
    {
    }

    virtual ~FilterStreamContentHandler()
    {
    }
    
    virtual FsObjectInfo getInfo() const
    {
        return stream.info();
    }

    virtual FrozenMemoryRegion
    getRange(uint64_t offset, int64_t length) const
    {
        const char * data;
        size_t len;

        std::tie(data, len) = stream.mapped();
        if (data && length) {
            if (length == -1)
                length = len - offset;
            return FrozenMemoryRegion(shared_from_this(),
                                      data, len);
        }

        std::unique_lock<std::mutex> guard(mutex);
        
        // We do it by seeking if we can
        if (stream.isRandomSeekable() && false) {
            stream.seekg(offset, ios_base::beg);
            std::string buf;
            throw MLDB::Exception("not implemented");
        }
        else {
            if (content.empty()) {
                content = stream.readAll();
            }

            if (length == -1)
                length = content.length() - offset;
            return FrozenMemoryRegion(shared_from_this(),
                                      content.data() + offset, length);
        }
    }

    mutable filter_istream stream;
    mutable std::string content;
    mutable std::mutex mutex;
};

std::shared_ptr<ContentHandler>
getContent(const ContentDescriptor & descriptor)
{
    return std::make_shared<FilterStreamContentHandler>(descriptor);
}
#endif

} // namespace MLDB

