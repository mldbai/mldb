/**                                                                 -*- C++ -*-
 * azure_blob_storage.cc
 * Mich, 2017-02-15
 * This file is part of MLDB. Copyright 2017 mldb.ai inc. All rights reserved.
 **/

#include <mutex>
#include <boost/iostreams/stream_buffer.hpp>
#include <fstream>
#include <thread>
#include <unordered_map>

#include "mldb/arch/exception.h"
#include "mldb/arch/format.h"
#include "mldb/base/exc_assert.h"
#include "mldb/jml/utils/string_functions.h"
#include "mldb/soa/credentials/credential_provider.h"
#include "mldb/types/date.h"
#include "mldb/vfs/filter_streams.h"
#include "mldb/vfs/filter_streams_registry.h"
#include "mldb/vfs/fs_utils.h"
#include "azure_blob_storage.h"
#include "mldb/ext/azure-storage-cpp/Microsoft.WindowsAzure.Storage/includes/was/storage_account.h"
#include "mldb/ext/azure-storage-cpp/Microsoft.WindowsAzure.Storage/includes/was/common.h"
#include "mldb/ext/azure-storage-cpp/Microsoft.WindowsAzure.Storage/includes/was/blob.h"
#include "mldb/ext/casablanca/Release/include/cpprest/asyncrt_utils.h"


using namespace std;
using namespace azure::storage;

namespace {
MLDB::FsObjectInfo
getObjectInfoFromCloudBlobProperties(const cloud_blob_properties & p)
{
    MLDB::FsObjectInfo info;
    info.size = p.size();
    info.exists = true;
    auto last = p.last_modified().to_string(utility::datetime::ISO_8601);
    info.lastModified = MLDB::Date::parseIso8601(last);
    info.etag = p.etag();
    //info.ownerId N/A;
    //info.ownerName N/A;

    info.objectMetadata["cacheControl"] = p.cache_control();
    info.objectMetadata["contentDisposition"] = p.content_disposition();
    info.objectMetadata["contentEncoding"] = p.content_encoding();
    info.objectMetadata["contentLanguage"] = p.content_language();
    info.objectMetadata["contentMd5"] = p.content_md5();
    info.objectMetadata["contentType"] = p.content_type();

    {
        string type;
        switch (p.type()) {
            case blob_type::unspecified:
                type = "unspecified";
                break;
            case blob_type::page_blob:
                type = "PageBlob";
                break;
            case blob_type::block_blob:
                type = "BlockBlob";
                break;
            case blob_type::append_blob:
                type = "AppendBlob";
                break;
            default:
                ExcAssert(false);
            info.objectMetadata["type"] = type;
        }
    }

    {
        string leaseStatus;
        switch (p.lease_status()) {
            case lease_status::unspecified:
                leaseStatus = "unspecified";
                break;
            case lease_status::locked:
                leaseStatus = "locked";
                break;
            case lease_status::unlocked:
                leaseStatus = "unlocked";
                break;
            default:
                ExcAssert(false);
        }
        info.objectMetadata["leaseStatus"] = leaseStatus;
    }

    {
        string leaseDuration;
        switch (p.lease_duration()) {
            case lease_duration::unspecified:
                leaseDuration = "unspecified";
                break;
            case lease_duration::fixed:
                leaseDuration = "fixed";
                break;
            case lease_duration::infinite:
                leaseDuration = "infinite";
                break;
            default:
                ExcAssert(false);
        }
        info.objectMetadata["leaseDuration"] = leaseDuration;
    }

    {
        string leaseState;
        switch (p.lease_state()) {
            case lease_state::unspecified:
                leaseState = "unspecified";
                break;
            case lease_state::leased:
                leaseState = "leased";
                break;
            case lease_state::breaking:
                leaseState = "breaking";
                break;
            case lease_state::available:
                leaseState = "available";
                break;
            case lease_state::broken:
                leaseState = "broken";
                break;
            case lease_state::expired:
                leaseState = "expired";
                break;
            default:
                ExcAssert(false);

        }
        info.objectMetadata["leaseState"] = leaseState;
    }

    info.objectMetadata["appendBlobCommittedBlockCount"] =
        p.append_blob_committed_block_count();

    info.objectMetadata["serverEncrypted"] = p.server_encrypted();

    return info;
}
} // anonymous namespace

namespace MLDB {

struct AzureBlobStorageDownloadSource {

    AzureBlobStorageDownloadSource(cloud_blob & blob, FsObjectInfo info)
    {
        impl.reset(new Impl());
        impl->blob = blob;
        impl->start();
        impl->info = info;
    }

    typedef char char_type;
    struct category
        : public boost::iostreams::input /*_seekable*/,
          public boost::iostreams::device_tag,
          public boost::iostreams::closable_tag
    { };
    
    struct Impl {
        Impl() : offset(0)
        {
        }

        ~Impl()
        {
            stop();
        }

        cloud_blob blob;

        Date startDate;
        string data;
        uint64_t offset;
        FsObjectInfo info;

        void start()
        {
            startDate = Date::now();
        }

        void stop()
        {
        }

        std::streamsize read(char_type* s, std::streamsize n)
        {
            BOOST_STATIC_ASSERT(sizeof(char_type) == 1);
            concurrency::streams::container_buffer<std::vector<uint8_t>> buffer;
            concurrency::streams::ostream outputStream(buffer);
            uint64_t toRead = offset + n <= info.size ? n : info.size - offset;
            if (toRead == 0) {
                return 0;
            }
            blob.download_range_to_stream(outputStream, offset, toRead);
            auto rawData = buffer.collection();
            string data = string(rawData.cbegin(), rawData.cend());
            offset += data.size();
            strncpy(s, data.c_str(), toRead);
            return toRead;
        }
    };

    std::shared_ptr<Impl> impl;

    std::streamsize read(char_type* s, std::streamsize n)
    {
        return impl->read(s, n);
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

struct AzureBlobStorageUploadSource {
    AzureBlobStorageUploadSource(cloud_append_blob & blob)
    {
        impl.reset(new Impl());
        impl->blob = blob;
        impl->blob.create_or_replace();
        impl->start();
    }

    typedef char char_type;
    struct category
        : public boost::iostreams::output,
          public boost::iostreams::device_tag,
          public boost::iostreams::closable_tag
    {
    };

    struct Impl {
        Impl()
        {
        }

        ~Impl()
        {
            stop();
        }

        OnUriHandlerException onException;
        
        size_t offset;

        Date startDate;
        cloud_append_blob blob;

        void start()
        {
            startDate = Date::now();
        }
        
        void stop()
        {
        }

        std::streamsize write(const char_type* s, std::streamsize n)
        {
            string str(s, n);
            concurrency::streams::istream append_input_stream =
                concurrency::streams::bytestream::open_istream(
                        utility::conversions::to_utf8string(_XPLATSTR(str)));
            blob.append_block(append_input_stream, utility::string_t());
            append_input_stream.close().wait();

            Date now = Date::now();
            offset += n;

            return n;
        }

        void flush()
        {
        }

        void finish()
        {
            stop();

            double elapsed = Date::now().secondsSince(startDate);

            if (false) {
                // if false so that it still compiles
                cerr << "uploaded " << offset / 1024.0 / 1024.0
                     << "MB in " << elapsed << "s at "
                     << offset / 1024.0 / 1024.0 / elapsed
                     << "MB/s" << endl;
            }
        }
    };

    std::shared_ptr<Impl> impl;

    std::streamsize write(const char_type* s, std::streamsize n)
    {
        return impl->write(s, n);
    }

    bool is_open() const
    {
        return !!impl;
    }

    void close()
    {
        impl->finish();
        impl.reset();
    }
};

namespace {

struct AzureStorageAccountInfo {
    string accountName;
    shared_ptr<cloud_blob_client> client;
   
    //connStr = "DefaultEndpointsProtocol=<>;AccountName=<>;AccountKey=<>;"
    AzureStorageAccountInfo(const string & connStr) {
        // azure connection string provide unfriendly "invalid argument"
        // exception if it is invalid. Here we provide a friendly error.
        const string msg =
            "Invalid azure storage connection string. The expected format is: "
            "DefaultEndpointsProtocol=<>;AccountName=<>;AccountKey=<>;";
        bool keyFound = false;
        bool defaultEndpointsProtocolFound = false;

        auto parts = ML::split(connStr, ';');
        if (parts.size() != 4 || parts[3] != "") {
            throw MLDB::Exception(msg);
        }
        parts.pop_back();
        for (const auto & p: parts) {
            const auto subparts = ML::split(p, '=', 2);
            if (subparts.size() != 2 || subparts[1].size() == 0) {
                throw MLDB::Exception(msg + " - Failed on: " + p);
            }
            const auto & key = subparts[0];
            if (key == "DefaultEndpointsProtocol") {
                defaultEndpointsProtocolFound = true;
                continue;
            }
            if (key == "AccountName") {
                accountName = subparts[1];
                continue;
            }
            if (key == "AccountKey") {
                keyFound = true;
                continue;
            }
            throw MLDB::Exception(msg + " - Failed on unknown component : " + p);
        }

        //cloud_storage_account
        auto storageAccount = cloud_storage_account::parse(connStr);
        client = make_shared<cloud_blob_client>(
            storageAccount.create_cloud_blob_client());
    }
};

mutex azureStorageAccountLock;
unordered_map<string, AzureStorageAccountInfo> azureStorageAccounts;

} // file scope

void
registerAzureStorageAccount(const std::string & connStr)
{
    unique_lock<std::mutex> guard(azureStorageAccountLock);
    AzureStorageAccountInfo info(connStr);
    if (azureStorageAccounts.find(info.accountName) != azureStorageAccounts.end()) {
        throw AzureAccountAlreadyRegistered(info.accountName);
    }
    azureStorageAccounts.insert(make_pair(info.accountName, std::move(info)));
}

struct AzureBlobInfo {
    string accountName;
    string containerName;
    string filename;

    AzureBlobInfo(const string & accountName, const string & containerName,
                  const string & filename)
        : accountName(accountName), containerName(containerName),
          filename(filename)
    {
    }

    static AzureBlobInfo fromPath(const string & path) {
        ExcAssert(path[0] != '/');
        auto parts = ML::split(path, '/', 3);
        ExcAssert(parts.size() == 3);
        return AzureBlobInfo(parts[0], parts[1], parts[2]);
    }

    static AzureBlobInfo fromUri(const string & uri) {
        const string prefix = "azureblob://";
        ExcAssert(uri.find(prefix) == 0);
        return AzureBlobInfo::fromPath(uri.substr(prefix.size()));
    }
};

cloud_blob
getAzureBlobReference(const AzureBlobInfo & blobInfo)
{
    unique_lock<std::mutex> guard(azureStorageAccountLock);
    auto res = azureStorageAccounts.find(blobInfo.accountName);
    if (res == azureStorageAccounts.end()) {
        throw MLDB::Exception("No azure storage client found for name: "
                              + blobInfo.containerName);
    }

    auto containerRef =
        res->second.client->get_container_reference(
            _XPLATSTR(blobInfo.containerName));
    return containerRef.get_blob_reference(_XPLATSTR(blobInfo.filename));
}

cloud_blob_container
getAzureBlobContainer(const AzureBlobInfo & blobInfo)
{
    unique_lock<std::mutex> guard(azureStorageAccountLock);
    auto res = azureStorageAccounts.find(blobInfo.accountName);
    if (res == azureStorageAccounts.end()) {
        throw MLDB::Exception("No azure storage client found for name: "
                              + blobInfo.containerName);
    }

    return res->second.client->get_container_reference(
        _XPLATSTR(blobInfo.containerName));
}

struct RegisterAzbsHandler {

    static UriHandler
    getAzbsHandler(const std::string & scheme,
                   const std::string & resource,
                   std::ios_base::open_mode mode,
                   const std::map<std::string, std::string> & options,
                   const OnUriHandlerException & onException)
    {
        auto blobInfo = AzureBlobInfo::fromPath(resource);

        if (mode == ios::in) {
            auto blob = getAzureBlobReference(blobInfo);
            concurrency::streams::container_buffer<std::vector<uint8_t>> buffer;
            concurrency::streams::ostream outputStream(buffer);
            blob.download_to_stream(outputStream);

            auto info = make_shared<FsObjectInfo>(
                    getObjectInfoFromCloudBlobProperties(blob.properties()));

            std::unique_ptr<std::streambuf> result;
            result.reset(new boost::iostreams::stream_buffer<AzureBlobStorageDownloadSource>
                         (AzureBlobStorageDownloadSource(blob, *info.get()), 131072));
            std::shared_ptr<std::streambuf> buf(result.release());

            return UriHandler(buf.get(), buf, info);
        }
        if (mode == ios::out) {
            cloud_append_blob blob(getAzureBlobReference(blobInfo));
            std::unique_ptr<std::streambuf> result;
            result.reset(new boost::iostreams::stream_buffer<AzureBlobStorageUploadSource>
                         (AzureBlobStorageUploadSource(blob), 131072));
            std::shared_ptr<std::streambuf> buf(result.release());
            return UriHandler(buf.get(), buf);
        }
        throw MLDB::Exception("no way to create azureblob handler for non in/out");
    }

    RegisterAzbsHandler()
    {
        registerUriHandler("azureblob", getAzbsHandler);
    }

} registerAzbsHandler;


namespace {

struct AzbsUrlFsHandler : public UrlFsHandler {

    UriHandler getUriHandler(const Url & url) const
    {
        string urlStr = url.toDecodedString();
        const string prefix = "azureblob://";
        ExcAssert(urlStr.find(prefix) == 0);
        const auto fooFct = [](){};
        const std::map<std::string, std::string> options;
        return RegisterAzbsHandler::getAzbsHandler(
            "", urlStr.substr(prefix.size()), ios::in, options, fooFct);
    }

    FsObjectInfo getInfo(const Url & url) const override
    {
        const auto handler = getUriHandler(url);
        return std::move(*(handler.info.get()));
    }

    FsObjectInfo tryGetInfo(const Url & url) const override
    {
        try {
            const auto handler = getUriHandler(url);
            return std::move(*(handler.info.get()));
        }
        catch (const azure::storage::storage_exception & exc) {
        }
        return FsObjectInfo();
    }

    void makeDirectory(const Url & url) const override
    {
    }

    bool erase(const Url & url, bool throwException) const override
    {
        auto urlStr = url.toDecodedString();
        auto blobInfo = AzureBlobInfo::fromUri(urlStr);
        auto blob = getAzureBlobReference(blobInfo);
        blob.delete_blob();
        return true;
    }

    bool forEach_(const string & prefix,
                  const OnUriObject & onObject,
                  const OnUriSubdir & onSubdir,
                  int depth) const
    {
        ExcAssert(prefix.size() == 0 || prefix[prefix.size() - 1] == '/');
        auto blobInfo = AzureBlobInfo::fromUri(prefix);
        string curDir = "/" + blobInfo.containerName + "/" + blobInfo.filename;
        auto container = getAzureBlobContainer(blobInfo);
        auto result = container.list_blobs(
            blobInfo.filename, false, blob_listing_details::none, 0,
            blob_request_options(), operation_context());
        for (const auto & item: result) {
            if (item.is_blob()) {
                string uri = "azureblob://" + blobInfo.accountName
                    + item.as_blob().uri().path();

                OpenUriObject open = [=] (
                    const map<string, string> & options) -> UriHandler
                {
                    if (!options.empty()) {
                        throw MLDB::Exception(
                            "Options not accepted by azureblob");
                    }
                    shared_ptr<std::istream> result(
                        new filter_istream(uri));
                    auto info = getInfo(Url(uri));
                    return UriHandler(result->rdbuf(), result, info);
                };

                if (!onObject(uri, getInfo(Url(uri)), open, depth)) {
                    return false;
                }
            }
            else {
                string path = item.as_directory().uri().path();
                string dirname =
                    path.substr(curDir.size(), path.size() - curDir.size());
                if (onSubdir(dirname, depth)) {
                    if (!forEach_(prefix + dirname, onObject, onSubdir, depth + 1)) {
                        return false;
                    }
                }
            }
        }

        return true;
    }

    bool forEach(const Url & prefix,
                 const OnUriObject & onObject,
                 const OnUriSubdir & onSubdir,
                 const std::string & delimiter,
                 const std::string & startAt) const override
    {
        ExcAssert(delimiter == "/");
        ExcAssert(startAt == "");
        return forEach_(prefix.toDecodedString(), onObject, onSubdir, 1);
    }
};

struct AtInit {
    AtInit() {
        registerUrlFsHandler("azureblob", new AzbsUrlFsHandler());
    }
} atInit;

} // namespace nameless
} // namespace MLDB
