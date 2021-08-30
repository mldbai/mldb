/** http_streambuf.cc
    Jeremy Barnes, 26 November 2014
    Copyright (c) 2014 mldb.ai inc.  All rights reserved.

    This file is part of MLDB. Copyright 2015 mldb.ai inc. All rights reserved.
*/

#include <atomic>
#include <boost/iostreams/stream_buffer.hpp>
#include "mldb/http/http_rest_proxy.h"
#include "mldb/vfs/filter_streams_registry.h"
#include "mldb/vfs/fs_utils.h"
#include "mldb/types/annotated_exception.h"
#include "mldb/types/basic_value_descriptions.h"
#include <chrono>
#include <future>


using namespace std;


namespace MLDB {

struct ContentUrlFsHandler: UrlFsHandler {
    virtual FsObjectInfo getInfo(const Url & url) const
    {
        auto info = tryGetInfo(url);
        if (!info)
            throw MLDB::Exception("Couldn't get URI info for " + url.toString());
        return info;
    }

    virtual FsObjectInfo tryGetInfo(const Url & url) const
    {
        throw AnnotatedException(500, "content tryGetInfo(): not implemented");
    }

    virtual size_t getSize(const Url & url) const
    {
        throw AnnotatedException(500, "content getSize(): not implemented");
    }

    virtual std::string getEtag(const Url & url) const
    {
        throw AnnotatedException(500, "content getEtag(): not implemented");
    }

    virtual void makeDirectory(const Url & url) const
    {
        throw AnnotatedException(500, "content makeDirectory(): not implemented");
    }

    virtual bool erase(const Url & url, bool throwException) const
    {
        throw AnnotatedException(500, "content erase(): not implemented");
    }

    virtual bool forEach(const Url & prefix,
                         const OnUriObject & onObject,
                         const OnUriSubdir & onSubdir,
                         const std::string & delimiter,
                         const std::string & startAt) const
    {
        throw AnnotatedException(500, "content forEach(): not implemented");
    }
};

namespace {

static struct RegisterContentHandler {
    static UriHandler
    getContentHandler(const std::string & scheme,
                      const std::string & resource,
                      std::ios_base::openmode mode,
                      const std::map<std::string, std::string> & options,
                      const OnUriHandlerException & onException)
    {
        string::size_type pos = resource.find('/');
        if (pos == string::npos)
            throw MLDB::Exception("unable to find content bucket name in resource "
                                + resource);
        string bucket(resource, 0, pos);

        if (mode == ios::in) {
            std::pair<std::unique_ptr<std::streambuf>, FsObjectInfo> sb_info;
            throw AnnotatedException(500, "content getContentHandler(): not implemented");
            std::shared_ptr<std::streambuf> buf(sb_info.first.release());
            return UriHandler(buf.get(), buf, sb_info.second);
        }
        else if (mode == ios::out) {
            throw MLDB::Exception("Can't currently upload files via CONTENT/CONTENTs");
        }
        else throw MLDB::Exception("no way to create content handler for non in/out");
    }
    
    RegisterContentHandler()
    {
        registerUriHandler("content", getContentHandler);
        registerUrlFsHandler("content", new ContentUrlFsHandler());
    }

} registerContentHandler;

} // file scope

} // namespace MLDB
