/** gcs_handler.cc
    Mathieu Marquis Bolduc
    This file is part of MLDB.
    Copyright (c) 2017 mldb.ai inc.  All rights reserved.
    Google Cloud Storage VFS handlers
*/

#include <memory>
#include <exception>
#include <thread>
#include <chrono>
#include <boost/iostreams/stream_buffer.hpp>
#include "mldb/jml/utils/ring_buffer.h"
#include "mldb/jml/utils/string_functions.h"
#include "mldb/base/exc_assert.h"
#include "mldb/base/hash.h"
#include "mldb/types/url.h"
#include "mldb/vfs/filter_streams_registry.h"
#include "mldb/vfs/fs_utils.h"
#include "mldb/vfs_handlers/exception_ptr.h"
#include "mldb/http/http_rest_proxy.h"

using namespace std;
using namespace MLDB;

namespace {

struct GCSUrlFsHandler : public UrlFsHandler {
    virtual FsObjectInfo getInfo(const Url & url) const
    {
        throw MLDB::Exception("GCSUrlFsHandler::getInfo not implemented");
    }

    virtual FsObjectInfo tryGetInfo(const Url & url) const
    {
        throw MLDB::Exception("GCSUrlFsHandler::tryGetInfo not implemented");
    }

    virtual void makeDirectory(const Url & url) const
    {
        throw MLDB::Exception("GCSUrlFsHandler::makeDirectory not implemented");
    }

    virtual bool erase(const Url & url, bool throwException) const
    {
        throw MLDB::Exception("GCSUrlFsHandler::erase not implemented");
    }

    virtual bool forEach(const Url & prefix,
                         const OnUriObject & onObject,
                         const OnUriSubdir & onSubdir,
                         const std::string & delimiter,
                         const std::string & startAt) const
    {
        throw MLDB::Exception("GCSUrlFsHandler::forEach not implemented");
    }
};

/****************************************************************************/
/* STREAMING DOWNLOAD SOURCE                                                */
/****************************************************************************/

struct StreamingDownloadSource {
    StreamingDownloadSource(const std::string & urlStr)
    {
        auto ressource = parseurl(urlStr);
        Json::Value requestPayload;
        std::string getUrl = "https://www.googleapis.com/storage/v1/b/" + ressource.first + "/o/" + ressource.second;
        HttpRestProxy proxy;
        auto resp = proxy.get(getUrl);

        cerr << resp.code() << endl;
        cerr << resp.body() << endl;

        if (resp.code() == 200) {
            Json::Value jsonResp = resp.jsonBody();
            size_t fileSize = std::atol(jsonResp["size"].asString().c_str());
            //Set the info
            //TODO: Metadata
            info_.lastModified = Date::parseIso8601DateTime(jsonResp["updated"].asString());
            info_.size = fileSize;
            info_.exists = true;
            info_.etag = jsonResp["etag"].asString();
            info_.storageClass = jsonResp["storageClass"].asString();
            if (fileSize < 5242880) {
                //try direct download
                std::string mediaUrl = getUrl + "?alt=media";
                auto mediaResp = proxy.get(mediaUrl);
                bufferedValue = make_shared<std::string>(std::move(mediaResp.body()));
                buffered_pos = 0;
            }
            else {
                //download through HTTP
                httpstream = std::make_shared<filter_istream>(jsonResp["mediaLink"].asString()/*, { { "mapped", "true" } }*/);
            }
        }       
    }

    static std::pair<std::string, std::string> parseurl(std::string url) {
        size_t bucket_delimiter_pos = url.find_first_of('/');
        //TODO: check for failure
        std::string bucketStr = url.substr(0, bucket_delimiter_pos);
        std::string objectStr = url.substr(bucket_delimiter_pos+1);

        return {std::move(bucketStr), std::move(objectStr)};
    }

    const FsObjectInfo & info()
    {
        return info_;
    }

    //Todo: verify this
    typedef char char_type;
    struct category
        : //input_seekable,
        boost::iostreams::input,
        boost::iostreams::device_tag,
        boost::iostreams::closable_tag
    { };

    std::streamsize read(char_type * s, std::streamsize n)
    {        
        if (bufferedValue) {
             size_t readSize = std::min(size_t(n), bufferedValue->length() - buffered_pos);
             if (readSize > 0) {
                memcpy(s, bufferedValue->c_str() + buffered_pos, readSize);
                buffered_pos += readSize;
                return readSize;
             }
        }
        else if (httpstream) {
            httpstream->read(s, n);
            return httpstream->gcount();
        }
       
        return 0;
    }

    bool is_open() const
    {        
        return (bool)bufferedValue || (bool)httpstream;
    }

    void close()
    {
        bufferedValue.reset();
        httpstream.reset();
    }

private:

    FsObjectInfo info_;
    std::shared_ptr<std::string> bufferedValue;
    size_t buffered_pos;
    std::shared_ptr<filter_istream> httpstream;    
};


std::pair<std::unique_ptr<std::streambuf>, FsObjectInfo>
makeStreamingDownload(const std::string & uri)
{
    std::unique_ptr<std::streambuf> result;
    StreamingDownloadSource source(uri);
    result.reset(new boost::iostreams::stream_buffer<StreamingDownloadSource>
                 (source,131072));
    return make_pair(std::move(result), source.info());
}


struct AtInit {
    AtInit() {
        registerUrlFsHandler("gcs", new GCSUrlFsHandler());
    }
} atInit;

struct RegisterGCSHandler {
    static UriHandler
    getGCSHandler(const std::string & scheme,
                 const std::string & resource,
                 std::ios_base::open_mode mode,
                 const std::map<std::string, std::string> & options,
                 const OnUriHandlerException & onException)
    {
        string::size_type pos = resource.find('/');
        if (pos == string::npos)
            throw MLDB::Exception("unable to find s3 bucket name in resource "
                                + resource);
        string bucket(resource, 0, pos);

        if (mode == ios::in) {
            std::unique_ptr<std::streambuf> source;
            FsObjectInfo info;
            auto dl = makeStreamingDownload(resource);
            source = std::move(dl.first);
            info = std::move(dl.second);
            std::shared_ptr<std::streambuf> buf(source.release());
            return UriHandler(buf.get(), buf, info);
        }
        else if (mode == ios::out) {
            throw MLDB::Exception("GCS Handler out mode not implemented");
        }
        else throw MLDB::Exception("no way to create GCS handler for non in/out");
    }

    RegisterGCSHandler()
    {
        registerUriHandler("gcs", getGCSHandler);
    }

} registerGCSHandler;

} // namespace