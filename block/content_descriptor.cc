/* content_descriptor.cc                                            -*- C++ -*-
   Jeremy Barnes, 1 September 2018
   Copyright (c) 2018 Mldb.ai Inc.  All rights reserved.

*/

#include "content_descriptor.h"
#include "mldb/types/structure_description.h"
#include "mldb/types/annotated_exception.h"
#include "mldb/vfs/filter_streams.h"
#include "mldb/types/any_impl.h"
#include <boost/iostreams/stream_buffer.hpp>
#include <mutex>
#include "mldb/vfs/compressor.h"

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
                Utf8String val = context.expectStringUtf8();
                ContentHash hash{std::move(key), std::move(val)};
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
/* CONTENT HANDLER                                                           */
/*****************************************************************************/

ContentHandler::
~ContentHandler()
{
}


/*****************************************************************************/
/* UTILITY FUNCTIONS                                                         */
/*****************************************************************************/

struct ContentInputStreambuf {

    ContentInputStreambuf(FrozenMemoryRegion mem)
    {
        impl.reset(new Impl(std::move(mem)));
    }

    typedef char char_type;
    struct category
        : public boost::iostreams::input_seekable,
          public boost::iostreams::device_tag,
          public boost::iostreams::closable_tag
    { };
    
    struct Impl {
        Impl(FrozenMemoryRegion mem)
            : mem(std::move(mem))
        {
        }

        ~Impl()
        {
        }

        FrozenMemoryRegion mem;
        size_t offset = 0;

        Date startDate;

        std::streamsize read(char_type* s, std::streamsize n)
        {
            static_assert(sizeof(char_type) == 1, "content streams for single-char bytes only");

            size_t charsLeft = mem.length() - offset;
            size_t numChars = std::min<size_t>(n, charsLeft);
            if (numChars == 0)
                return -1;
            std::memcpy(s, mem.data() + offset, numChars);
            offset += numChars;
            return numChars;
        }

        std::streampos seek(std::streamsize where, std::ios_base::seekdir dir)
        {
            switch (dir) {
            case ios_base::beg:
                offset = where;
                break;
            case ios_base::end:
                offset = mem.length() - where;
                break;
            case ios_base::cur:
                offset += where;
                break;
            default:
                ExcAssert(false);
                break;
            }

            return offset;
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

filter_istream getContentStream(const ContentDescriptor & descriptor,
                                const std::map<Utf8String, Any> & options)
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
        };

        auto vals = std::make_shared<Vals>();
        vals->info = contentHandler->getInfo();
        vals->mem = contentHandler->getRange();

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

        cerr << "returning " << vals->mem.length() << " bytes mapped at "
             << (void *)vals->mem.data() << endl;

        std::streambuf * buf
            = new boost::iostreams::stream_buffer<ContentInputStreambuf>
            (ContentInputStreambuf(vals->mem), 131072);
        
        UriHandler handler(buf /* streambuf */,
                           vals /* ownership */,
                           std::shared_ptr<FsObjectInfo>(vals, &vals->info) /* info */,
                           uriOptions);
                           
        filter_istream stream(handler, descriptor.getUrlStringUtf8(), options2);
        return stream;
    }

    filter_istream result(descriptor.getUrlStringUtf8(), options2);
    return result;
}

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

} // namespace MLDB

