/**                                                                 -*- C++ -*-
 * azure_blob_storage.cc
 * Mich, 2017-02-15
 * This file is part of MLDB. Copyright 2017 mldb.ai inc. All rights reserved.
 **/

#include <mutex>
#include <boost/iostreams/stream_buffer.hpp>
#include "mldb/vfs_handlers/sftp.h"
#include <sys/types.h>
#include <sys/socket.h>
#include <netdb.h>
#include <stdio.h>
#include <stdlib.h>
#include <unistd.h>
#include <string.h>
#include "mldb/arch/exception.h"
#include "mldb/arch/format.h"
#include "mldb/types/date.h"
#include <fstream>
#include "mldb/vfs/filter_streams.h"
#include "mldb/vfs/filter_streams_registry.h"
#include "mldb/vfs/fs_utils.h"
#include "mldb/base/exc_assert.h"
#include "mldb/soa/credentials/credential_provider.h"
#include <thread>
#include <unordered_map>
#include "jml/utils/string_functions.h"
#include "azure_blob_storage.h"
#include "mldb/ext/azure-storage-cpp/Microsoft.WindowsAzure.Storage/includes/was/storage_account.h"
#include "mldb/ext/azure-storage-cpp/Microsoft.WindowsAzure.Storage/includes/was/common.h"
#include "mldb/ext/azure-storage-cpp/Microsoft.WindowsAzure.Storage/includes/was/blob.h"


using namespace std;
using namespace azure::storage;


namespace MLDB {

struct AzureBlobStorageDownloadSource {

    AzureBlobStorageDownloadSource(cloud_block_blob & blob)
    {
        impl.reset(new Impl());
        impl->blob = blob;
        impl->start();
    }

    typedef char char_type;
    struct category
        : public boost::iostreams::input /*_seekable*/,
          public boost::iostreams::device_tag,
          public boost::iostreams::closable_tag
    { };
    
    struct Impl {
        Impl()
        {
        }

        ~Impl()
        {
            stop();
        }

        cloud_block_blob blob;

        Date startDate;
        string data;

        void start()
        {
            startDate = Date::now();
            concurrency::streams::container_buffer<std::vector<uint8_t>> buffer;
            concurrency::streams::ostream outputStream(buffer);
            blob.download_to_stream(outputStream);
            auto rawData = buffer.collection();
            data = string(rawData.cbegin(), rawData.cend());
        }

        void stop()
        {
        }

        std::streamsize read(char_type* s, std::streamsize n)
        {
            BOOST_STATIC_ASSERT(sizeof(char_type) == 1);

            strncpy(s, data.c_str(), n - 1);
            streamsize copiedSize = data.size() < n ? data.size() : n - 1;
            if (data.size() < n) {
                data.clear();
            }
            else {
                data = data.substr(n - 1);
            }
            return copiedSize;
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
        size_t lastPrint;
        Date lastTime;

        Date startDate;
        cloud_append_blob blob;
        //concurrency::streams::istream source;

        void start()
        {
            startDate = Date::now();
        }
        
        void stop()
        {
        }

        std::streamsize write(const char_type* s, std::streamsize n)
        {
            concurrency::streams::istream append_input_stream =
                concurrency::streams::bytestream::open_istream(
                        utility::conversions::to_utf8string(_XPLATSTR(s)));
            blob.append_block(append_input_stream, utility::string_t());
            append_input_stream.close().wait();

            Date now = Date::now();
            //lastPrint = offset; TODO?
            lastTime = now;

            return n;
        }

        void flush()
        {
        }

        void finish()
        {
            stop();

            double elapsed = Date::now().secondsSince(startDate);

            cerr << "uploaded " << offset / 1024.0 / 1024.0
                 << "MB in " << elapsed << "s at "
                 << offset / 1024.0 / 1024.0 / elapsed
                 << "MB/s" << endl;
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
    //connStr = "DefaultEndpointsProtocol=<>;AccountName=<>;AccountKey=<>;"
    unique_lock<std::mutex> guard(azureStorageAccountLock);
    AzureStorageAccountInfo info(connStr);
    if (azureStorageAccounts.find(info.accountName) != azureStorageAccounts.end()) {
        throw AzureAccountAlreadyRegistered(info.accountName);
    }
    azureStorageAccounts.insert(make_pair(info.accountName, std::move(info)));
}

shared_ptr<cloud_blob_client>
getAzureStorageClientFromName(const string & name)
{
    unique_lock<std::mutex> guard(azureStorageAccountLock);
    auto res = azureStorageAccounts.find(name);
    if (res == azureStorageAccounts.end()) {
        throw MLDB::Exception("No azure storage client found for name: " + name);
    }
    return res->second.client;
}


struct RegisterAzbsHandler {

    static UriHandler
    getAzbsHandler(const std::string & scheme,
                   const std::string & resource,
                   std::ios_base::open_mode mode,
                   const std::map<std::string, std::string> & options,
                   const OnUriHandlerException & onException)
    {
        auto parts = ML::split(resource, '/', 3);
        if (parts.size() != 3) {
            throw MLDB::Exception("Invalid azureblob:// uri");
        }
        const auto & account = parts[0];
        const auto & containerName = parts[1];
        const auto & filename = parts[2];

        auto client = getAzureStorageClientFromName(account);
        auto container =
            client->get_container_reference(_XPLATSTR(containerName));

        //TODO
        auto info = std::make_shared<FsObjectInfo>();

        if (mode == ios::in) {
            auto blob =
                container.get_block_blob_reference(_XPLATSTR(filename));
            concurrency::streams::container_buffer<std::vector<uint8_t>> buffer;
            concurrency::streams::ostream outputStream(buffer);
            blob.download_to_stream(outputStream);

            std::unique_ptr<std::streambuf> result;
            result.reset(new boost::iostreams::stream_buffer<AzureBlobStorageDownloadSource>
                         (AzureBlobStorageDownloadSource(blob), 131072));
            std::shared_ptr<std::streambuf> buf(result.release());

            return UriHandler(buf.get(), buf, info);
        }
        if (mode == ios::out) {
            azure::storage::cloud_append_blob blob = container.get_append_blob_reference(_XPLATSTR(filename));
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
        ExcAssert(urlStr.find("azureblob://") == 0);
        const auto fooFct = [](){};
        const std::map<std::string, std::string> options;
        return RegisterAzbsHandler::getAzbsHandler(
            "", urlStr.substr(7), ios::in, options, fooFct);
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
        catch (const MLDB::Exception & exc) {
        }
        return FsObjectInfo();
    }

    void makeDirectory(const Url & url) const override
    {
    }

    bool erase(const Url & url, bool throwException) const override
    {
        //TODO
        return true;
    }

    bool forEach(const Url & prefix,
                 const OnUriObject & onObject,
                 const OnUriSubdir & onSubdir,
                 const std::string & delimiter,
                 const std::string & startAt) const override
    {
        return true;
    }
};

struct AtInit {
    AtInit() {
        registerUrlFsHandler("azureblob", new AzbsUrlFsHandler());
    }
} atInit;

} // namespace nameless
} // namespace MLDB
