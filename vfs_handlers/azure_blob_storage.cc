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

#if 0
SftpConnection::Directory::
Directory(const std::string & path,
          LIBSSH2_SFTP_HANDLE * handle,
          const SftpConnection * owner)
    : path(path), handle(handle), owner(owner)
{
}

SftpConnection::Directory::
~Directory()
{
    libssh2_sftp_close(handle);
}

void
SftpConnection::Directory::
ls() const
{
    do {
        char mem[512];
        char longentry[512];
        LIBSSH2_SFTP_ATTRIBUTES attrs;
 
        /* loop until we fail */ 
        int rc = libssh2_sftp_readdir_ex(handle, mem, sizeof(mem),

                                         longentry, sizeof(longentry),
                                         &attrs);
        if(rc > 0) {
            /* rc is the length of the file name in the mem
               buffer */ 
 
            if (longentry[0] != '\0') {
                printf("%s\n", longentry);
            } else {
                if(attrs.flags & LIBSSH2_SFTP_ATTR_PERMISSIONS) {
                    /* this should check what permissions it
                       is and print the output accordingly */ 
                    printf("--fix----- ");
                }
                else {
                    printf("---------- ");
                }
 
                if(attrs.flags & LIBSSH2_SFTP_ATTR_UIDGID) {
                    printf("%4ld %4ld ", attrs.uid, attrs.gid);
                }
                else {
                    printf("   -    - ");
                }
 
                if(attrs.flags & LIBSSH2_SFTP_ATTR_SIZE) {
                    printf("%8lld ", (unsigned long long)attrs.filesize);
                }
                    
                printf("%s\n", mem);
            }
        }
        else
            break;
 
    } while (1);
}

void
SftpConnection::Directory::
forEachFile(const OnFile & onFile) const
{
    do {
        char mem[512];
        char longentry[512];
        Attributes attrs;
 
        /* loop until we fail */ 
        int rc = libssh2_sftp_readdir_ex(handle,
                                         mem, sizeof(mem),
                                         longentry, sizeof(longentry),
                                         &attrs);

        if(rc > 0) {
            /* rc is the length of the file name in the mem
               buffer */ 
            string filename(mem, mem + rc);
            onFile(filename, attrs);
        }
        else
            break;
 
    } while (1);
}


/*****************************************************************************/
/* FILE                                                                      */
/*****************************************************************************/

SftpConnection::File::
File(const std::string & path,
     LIBSSH2_SFTP_HANDLE * handle,
     SftpConnection * owner)
    : path(path), handle(handle), owner(owner)
{
}

SftpConnection::File::
~File()
{
    libssh2_sftp_close(handle);
}

SftpConnection::Attributes
SftpConnection::File::
getAttr() const
{
    Attributes result;
    int res = libssh2_sftp_fstat_ex(handle, &result, 0);
    if (res == -1)
        throw MLDB::Exception("getAttr(): " + owner->lastError());
    return result;
}

uint64_t
SftpConnection::File::
size() const
{
    return getAttr().filesize;
}

void
SftpConnection::File::
downloadTo(const std::string & filename) const
{
    uint64_t bytesToRead = size();

    uint64_t done = 0;
    std::ofstream stream(filename.c_str());

    size_t bufSize = 1024 * 1024;

    char * buf = new char[bufSize];
            
    Date start = Date::now();

    for (;;) {
        ssize_t numRead = libssh2_sftp_read(handle, buf, bufSize);
        //cerr << "read " << numRead << " bytes" << endl;
        if (numRead < 0) {
            throw MLDB::Exception("read(): " + owner->lastError());
        }
        if (numRead == 0) break;

        stream.write(buf, numRead);
        uint64_t doneBefore = done;
        done += numRead;

        if (doneBefore / 10000000 != done / 10000000) {
            double elapsed = Date::now().secondsSince(start);
            double rate = done / elapsed;
            cerr << "done " << done << " of "
                 << bytesToRead << " at "
                 << rate / 1024.0
                 << "k/sec window " << numRead
                 << " time left "
                 << (bytesToRead - done) / rate
                 << "s" << endl;
        }
    }

    delete[] buf;
}

/*****************************************************************************/
/* SFTP CONNECTION                                                           */
/*****************************************************************************/

SftpConnection::
SftpConnection()
    : sftp_session(0)
{
}

SftpConnection::
~SftpConnection()
{
    close();
}

void
SftpConnection::
connectPasswordAuth(const std::string & hostname,
                    const std::string & username,
                    const std::string & password,
                    const std::string & port)
{
    SshConnection::connect(hostname, port);
    SshConnection::passwordAuth(username, password);

    sftp_session = libssh2_sftp_init(session);
 
    if (!sftp_session) {
        throw MLDB::Exception("can't initialize SFTP session: "
                            + lastError());
    }

}

void
SftpConnection::
connectPublicKeyAuth(const std::string & hostname,
                              const std::string & username,
                              const std::string & publicKeyFile,
                              const std::string & privateKeyFile,
                              const std::string & port)
{
    SshConnection::connect(hostname, port);
    SshConnection::publicKeyAuth(username, publicKeyFile, privateKeyFile);

    sftp_session = libssh2_sftp_init(session);
 
    if (!sftp_session) {
        throw MLDB::Exception("can't initialize SFTP session: "
                            + lastError());
    }

}

SftpConnection::Directory
SftpConnection::
getDirectory(const std::string & path) const
{
    LIBSSH2_SFTP_HANDLE * handle
        = libssh2_sftp_opendir(sftp_session, path.c_str());
        
    if (!handle) {
        throw MLDB::Exception("couldn't open path: " + lastError());
    }

    return Directory(path, handle, this);
}

SftpConnection::File
SftpConnection::
openFile(const std::string & path)
{
    LIBSSH2_SFTP_HANDLE * handle
        = libssh2_sftp_open_ex(sftp_session, path.c_str(),
                               path.length(), LIBSSH2_FXF_READ, 0,
                               LIBSSH2_SFTP_OPENFILE);
        
    if (!handle) {
        throw MLDB::Exception("couldn't open path: " + lastError());
    }

    return File(path, handle, this);
}

bool
SftpConnection::
getAttributes(const std::string & path, Attributes & attrs)
    const
{
    int res = libssh2_sftp_stat_ex(sftp_session,
                                   path.c_str(), path.length(), LIBSSH2_SFTP_STAT,
                                   &attrs);
    return (res != -1);
}
    
void
SftpConnection::
close()
{
    if (sftp_session) {
        libssh2_sftp_shutdown(sftp_session);
        sftp_session = 0;
    }

    SshConnection::close();
}

void
SftpConnection::
uploadFile(const char * start,
           size_t size,
           const std::string & path)
{
    /* Request a file via SFTP */ 
    LIBSSH2_SFTP_HANDLE * handle =
        libssh2_sftp_open(sftp_session, path.c_str(),
                          LIBSSH2_FXF_WRITE|LIBSSH2_FXF_CREAT|LIBSSH2_FXF_TRUNC,
                          LIBSSH2_SFTP_S_IRUSR|LIBSSH2_SFTP_S_IWUSR|
                          LIBSSH2_SFTP_S_IRGRP|LIBSSH2_SFTP_S_IROTH);
    
    if (!handle) {
        throw MLDB::Exception("couldn't open path: " + lastError());
    }

    Date started = Date::now();

    uint64_t offset = 0;
    uint64_t lastPrint = 0;
    Date lastTime = started;

    for (; offset < size; ) {
        /* write data in a loop until we block */ 
        size_t toSend = std::min<size_t>(size - offset,
                                         1024 * 1024);

        ssize_t rc = libssh2_sftp_write(handle,
                                        start + offset,
                                        toSend);
        
        if (rc == -1)
            throw MLDB::Exception("couldn't upload file: " + lastError());

        offset += rc;
        
        if (offset > lastPrint + 5 * 1024 * 1024 || offset == size) {
            Date now = Date::now();

            double mb = 1024 * 1024;

            double doneMb = offset / mb;
            double totalMb = size / mb;
            double elapsedOverall = now.secondsSince(started);
            double mbSecOverall = doneMb / elapsedOverall;
            double elapsedSince = now.secondsSince(lastTime);
            double mbSecInst = (offset - lastPrint) / mb / elapsedSince;

            cerr << MLDB::format("done %.2fMB of %.2fMB (%.2f%%) at %.2fMB/sec inst and %.2fMB/sec overall",
                               doneMb, totalMb,
                               100.0 * doneMb / totalMb,
                               mbSecInst,
                               mbSecOverall)
                 << endl;
                               

            lastPrint = offset;
            lastTime = now;
        }
        //cerr << "at " << offset / 1024.0 / 1024.0
        //     << " of " << size << endl;
    }
 
    libssh2_sftp_close(handle);
}

bool
SftpConnection::
isAlive() const
{
    LIBSSH2_CHANNEL * channel = libssh2_sftp_get_channel(sftp_session);
    int res = libssh2_channel_setenv_ex(channel,
                                        "MLDB_PING", // var name
                                        9,           // length of var name
                                        "1",         // value
                                        1);          // length of value
    return res != LIBSSH2_ERROR_SOCKET_RECV
        && res != LIBSSH2_ERROR_SOCKET_DISCONNECT;
}
#endif
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
        std::string path;
        //size_t offset;

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
#if 0



struct SftpStreamingUploadSource {

    SftpStreamingUploadSource(const SftpConnection * owner,
                              const std::string & path,
                              const OnUriHandlerException & excCallback)
    {
        impl.reset(new Impl());
        impl->owner = owner;
        impl->path = path;
        impl->onException = excCallback;
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
            : owner(0), handle(0), offset(0), lastPrint(0)
        {
        }

        ~Impl()
        {
            stop();
        }

        const SftpConnection * owner;
        LIBSSH2_SFTP_HANDLE * handle;
        std::string path;
        OnUriHandlerException onException;
        
        size_t offset;
        size_t lastPrint;
        Date lastTime;

        Date startDate;

        void start()
        {
            /* Request a file via SFTP */ 
            handle =
                libssh2_sftp_open(owner->sftp_session, path.c_str(),
                                  LIBSSH2_FXF_WRITE|LIBSSH2_FXF_CREAT|LIBSSH2_FXF_TRUNC,
                                  LIBSSH2_SFTP_S_IRUSR|LIBSSH2_SFTP_S_IWUSR|
                                  LIBSSH2_SFTP_S_IRGRP|LIBSSH2_SFTP_S_IROTH);
            
            if (!handle) {
                onException();
                throw MLDB::Exception("couldn't open path: " + owner->lastError());
            }

            startDate = Date::now();
        }
        
        void stop()
        {
            if (handle) libssh2_sftp_close(handle);
        }

        std::streamsize write(const char_type* s, std::streamsize n)
        {
            ssize_t done = 0;

            while (done < n) {

                ssize_t rc = libssh2_sftp_write(handle, s + done, n - done);
            
                if (rc == -1) {
                    onException();
                    throw MLDB::Exception("couldn't upload file: " + owner->lastError());
                }
            
                offset += rc;
                done += rc;

                if (offset > lastPrint + 5 * 1024 * 1024) {
                    Date now = Date::now();
                
                    double mb = 1024 * 1024;
                
                    double doneMb = offset / mb;
                    double elapsedOverall = now.secondsSince(startDate);
                    double mbSecOverall = doneMb / elapsedOverall;
                    double elapsedSince = now.secondsSince(lastTime);
                    double mbSecInst = (offset - lastPrint) / mb / elapsedSince;
                
                    cerr << MLDB::format("done %.2fMB at %.2fMB/sec inst and %.2fMB/sec overall",
                                       doneMb, 
                                       mbSecInst,
                                       mbSecOverall)
                         << endl;
                
                
                    lastPrint = offset;
                    lastTime = now;
                }
            }

            return done;
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

filter_ostream
SftpConnection::
streamingUpload(const std::string & path) const
{
    filter_ostream result;
    auto onException = [&] { result.notifyException(); };
    std::shared_ptr<std::streambuf> buf(streamingUploadStreambuf(path, onException).release());
    result.openFromStreambuf(buf.get(), buf, path);
    
    return result;
}

std::unique_ptr<std::streambuf>
SftpConnection::
streamingUploadStreambuf(const std::string & path,
                         const OnUriHandlerException & onException) const
{
    std::unique_ptr<std::streambuf> result;
    result.reset(new boost::iostreams::stream_buffer<SftpStreamingUploadSource>
                 (SftpStreamingUploadSource(this, path, onException),
                  131072));
    return result;
}

filter_istream
SftpConnection::
streamingDownload(const std::string & path) const
{
    filter_istream result;
    std::shared_ptr<std::streambuf> buf(streamingDownloadStreambuf(path).release());
    result.openFromStreambuf(buf.get(), buf, path);

    return result;
}

std::unique_ptr<std::streambuf>
SftpConnection::
streamingDownloadStreambuf(const std::string & path) const
{
    std::unique_ptr<std::streambuf> result;
    result.reset(new boost::iostreams::stream_buffer<SftpStreamingDownloadSource>
                 (SftpStreamingDownloadSource(this, path),
                  131072));
    return result;
}

int
SftpConnection::
unlink(const string & path) const {
    return libssh2_sftp_unlink(sftp_session, path.c_str());
}

int
SftpConnection::
mkdir(const string & path) const {
    return libssh2_sftp_mkdir(sftp_session, path.c_str(),
                              LIBSSH2_SFTP_S_IRWXU | LIBSSH2_SFTP_S_IRWXG |
                              LIBSSH2_SFTP_S_IRWXO);
}
#endif
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

        //cloud_block_blob
        auto blob =
            container.get_block_blob_reference(_XPLATSTR(filename));


        concurrency::streams::container_buffer<std::vector<uint8_t>> buffer;
        concurrency::streams::ostream outputStream(buffer);

        blob.download_to_stream(outputStream);
        //TODO
        auto info = std::make_shared<FsObjectInfo>();

        if (mode == ios::in) {
            std::unique_ptr<std::streambuf> result;
            result.reset(new boost::iostreams::stream_buffer<AzureBlobStorageDownloadSource>
                         (AzureBlobStorageDownloadSource(blob), 131072));
            std::shared_ptr<std::streambuf> buf(result.release());

            return UriHandler(buf.get(), buf, info);
        }
#if 0
        //ucout << _XPLATSTR("Stream: ") << to_string(buffer.collection()) << std::endl;
        const auto & connection = getSftpConnectionFromConnStr(connStr);
        string path = resource.substr(connStr.size());
        if (mode == ios::in) {
            std::shared_ptr<std::streambuf> buf
                (connection.streamingDownloadStreambuf(path).release());

            SftpConnection::Attributes attr;
            if (!connection.getAttributes(path, attr)) {
                throw MLDB::Exception("Couldn't read attributes for sftp "
                                    "resource");
            }

            auto info = std::make_shared<FsObjectInfo>();
            info->exists = true;
            info->size = attr.filesize;
            info->ownerId = std::to_string(attr.uid);
            info->lastModified = Date::fromSecondsSinceEpoch(attr.mtime);

            return UriHandler(buf.get(), buf, info);
        }
        if (mode == ios::out) {
            std::shared_ptr<std::streambuf> buf
                (connection.streamingUploadStreambuf(path, onException)
                 .release());
            return UriHandler(buf.get(), buf);
        }
        throw MLDB::Exception("no way to create sftp handler for non in/out");
#endif
        throw MLDB::Exception("no way to create sftp handler for non in/out");
    }

    RegisterAzbsHandler()
    {
        registerUriHandler("azureblob", getAzbsHandler);
    }

} registerAzbsHandler;

#if 0
const SftpConnection & getSftpConnectionFromConnStr(const std::string & connStr)
{
    std::unique_lock<std::mutex> guard(sftpHostsLock);
    auto it = sftpHosts.find(connStr);
    if (it != sftpHosts.end() && it->second.connection.get()->isAlive()) {
        return *it->second.connection.get();
    }
    auto creds = getCredential("sftp", "sftp://" + connStr);

    const auto pos = connStr.find(":");
    string host;
    string port;
    if (pos == string::npos) {
        host = connStr;
        port = "ssh";
    }
    else {
        host = connStr.substr(0, pos);
        port = connStr.substr(pos + 1);
    }


    SftpHostInfo info;
    info.sftpHost = host;
    info.connection = std::make_shared<SftpConnection>();
    info.connection->connectPasswordAuth(host, creds.id, creds.secret, port);
    sftpHosts[connStr] = info;
    return *info.connection.get();
}
#endif

namespace {

// string connStrFromUri(const string & uri) {
//     ExcAssert(uri.find("sftp://") == 0);
//     const auto pos = uri.find("/", 7);
//     if (pos == string::npos) {
//         throw MLDB::Exception("Couldn't find sftp hostname in %s", uri.c_str());
//     }
//     return uri.substr(7, pos - 7);
// };

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
