/* content_descriptor.cc                                            -*- C++ -*-
   Jeremy Barnes, 1 September 2018
   Copyright (c) 2018 Mldb.ai Inc.  All rights reserved.

*/

#include "content_descriptor.h"
#include "mldb/types/structure_description.h"
#include "mldb/types/annotated_exception.h"
#include "filter_streams.h"
#include "mldb/types/any_impl.h"


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
/* UTILITY FUNCTIONS                                                         */
/*****************************************************************************/

filter_istream getContentStream(const ContentDescriptor & descriptor,
                                const std::map<Utf8String, Any> & options)
{
    std::map<std::string, std::string> options2;
    for (auto & opt: options) {
        options2[opt.first.rawString()] = opt.second.asJson().toString();
    }

    filter_istream result(descriptor.getUrlStringUtf8(), options2);
    return result;
}

} // namespace MLDB

