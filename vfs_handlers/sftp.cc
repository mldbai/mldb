/* sftp.cc
   Jeremy Barnes, 21 June 2012
   Copyright (c) 2012 mldb.ai inc.  All rights reserved.

   This file is part of MLDB. Copyright 2015 mldb.ai inc. All rights reserved.

   sftp connection.
*/

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
#include "mldb/credentials/credential_provider.h"
#include "mldb/utils/starts_with.h"
#include "mldb/utils/split.h"
#include <thread>
#include <unordered_map>
#include <errno.h>


using namespace std;


namespace MLDB {

namespace {

// libssh2 doesn't know about the encoding of what is on the other end,
// which is a potential security risk if the other encoding isn't
// utf-8. To avoid this, we refuse to do anything that's not ASCII in
// things that are passed to libssh2 for now.

const char * ssh2CharPointer(const std::string & str)
{
    for (size_t i = 0;  i < str.size();  ++i) {
        if (str[i] & 0x80 || str[0] < 32)
            throw MLDB::Exception("non-ASCII character in path name");
    }
    return str.c_str();
}

const char * ssh2CharPointer(const Utf8String & str)
{
    return ssh2CharPointer(str.rawString());
}

} // file scope

/*****************************************************************************/
/* SOCKET CONNECTION                                                         */
/*****************************************************************************/

SocketConnection::
SocketConnection()
    : sock(-1)
{
}

SocketConnection::
~SocketConnection()
{
    close();
}

void
SocketConnection::
connect(const Utf8String & hostname,
        const std::string & port)
{
    struct addrinfo hints;
    struct addrinfo *result, *rp;

    /* Obtain address(es) matching host/port */
    memset(&hints, 0, sizeof(struct addrinfo));
    hints.ai_family = AF_UNSPEC;     /* Allow IPv4 or IPv6 */
    hints.ai_socktype = SOCK_STREAM; /* Datagram socket */
    hints.ai_flags = AI_CANONNAME;
    hints.ai_protocol = 0;           /* Any protocol */

    int res = getaddrinfo(hostname.c_str(), port.c_str(), &hints, &result);
    if (res != 0)
        throw MLDB::Exception("getaddrinfo: %s", gai_strerror(res));

    cerr << "res = " << res << endl;
    cerr << "result = " << result << endl;

    /* getaddrinfo() returns a list of address structures.
       Try each address until we successfully connect(2).
       If socket(2) (or connect(2)) fails, we (close the socket
       and) try the next address. */

    for (rp = result; rp; rp = rp->ai_next) {
        if (rp->ai_canonname)
            cerr << "trying " << rp->ai_canonname << endl;
        else cerr << "trying null" << endl;

        sock = socket(rp->ai_family, rp->ai_socktype,
                      rp->ai_protocol);
        if (sock == -1) {
            cerr << "couldn't create connection socket: "
                 << strerror(errno) << endl;
            continue;
        }
            
        if (::connect(sock, rp->ai_addr, rp->ai_addrlen) != -1) {
            cerr << "connected" << endl;
            break;                  /* Success */
        }            

        cerr << "couldn't connect: " << strerror(errno) << endl;

        ::close(sock);
    }
        
    if (!rp)
        throw MLDB::Exception("couldn't connect anywhere");
        
    freeaddrinfo(result);           /* No longer needed */
}

void
SocketConnection::
close()
{
    ::close(sock);
}


/*****************************************************************************/
/* SSH CONNECTION                                                            */
/*****************************************************************************/

SshConnection::
SshConnection()
    : session(0)
{
}

SshConnection::
~SshConnection()
{
    close();
}

void
SshConnection::
connect(const Utf8String & hostname,
        const std::string & port)
{
    SocketConnection::connect(hostname, port);

    /* Create a session instance
     */ 
    session = libssh2_session_init();

    if(!session)
        throw MLDB::Exception("couldn't get libssh2 session");
 
    /* ... start it up. This will trade welcome banners, exchange keys,
     * and setup crypto, compression, and MAC layers
     */ 
    int rc = libssh2_session_handshake(session, sock);

    if(rc) {
        throw MLDB::Exception("error establishing session");
    }
 
    /* At this point we havn't yet authenticated.  The first thing to do
     * is check the hostkey's fingerprint against our known hosts Your app
     * may have it hard coded, may go to a file, may present it to the
     * user, that's your call
     */ 
    const char * fingerprint
        = libssh2_hostkey_hash(session, LIBSSH2_HOSTKEY_HASH_SHA1);

    printf("Fingerprint: ");
    for(int i = 0; i < 20; i++) {
        printf("%02X ", (unsigned char)fingerprint[i]);
    }
    printf("\n");
}

void
SshConnection::
passwordAuth(const Utf8String & username,
             const Utf8String & password)
{
    /* We could authenticate via password */ 
    if (libssh2_userauth_password(session,
                                  ssh2CharPointer(username),
                                  ssh2CharPointer(password))) {

        throw MLDB::Exception("password authentication failed: " + lastError());
    }
}

void
SshConnection::
publicKeyAuth(const Utf8String & username,
              const Utf8String & publicKeyFile,
              const Utf8String & privateKeyFile)
{
/* Or by public key */ 
    if (libssh2_userauth_publickey_fromfile(session, ssh2CharPointer(username),
                                            ssh2CharPointer(publicKeyFile),
                                            ssh2CharPointer(privateKeyFile),
                                            "")) {
        throw MLDB::Exception("public key authentication failed: " + lastError());
    }
}
 
void
SshConnection::
setBlocking()
{
    /* Since we have not set non-blocking, tell libssh2 we are blocking */ 
    libssh2_session_set_blocking(session, 1);
}

Utf8String
SshConnection::
lastError() const
{
    char * errmsg = 0;
    int res = libssh2_session_last_error(session, &errmsg, 0, 0);
    if (res)
        cerr << "error getting error: " << res << endl;
    return errmsg;
}

void
SshConnection::
close()
{
    if (session) {
        libssh2_session_disconnect(session, "Normal Shutdown");
        libssh2_session_free(session);
    }
    session = 0;

    SocketConnection::close();
}


/*****************************************************************************/
/* ATTRIBUTES                                                                */
/*****************************************************************************/



/*****************************************************************************/
/* DIRECTORY                                                                 */
/*****************************************************************************/

SftpConnection::Directory::
Directory(const Utf8String & path,
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
File(const Utf8String & path,
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
connectPasswordAuth(const Utf8String & hostname,
                    const Utf8String & username,
                    const Utf8String & password,
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
connectPublicKeyAuth(const Utf8String & hostname,
                     const Utf8String & username,
                     const Utf8String & publicKeyFile,
                     const Utf8String & privateKeyFile,
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
getDirectory(const Utf8String & path) const
{
    LIBSSH2_SFTP_HANDLE * handle
        = libssh2_sftp_opendir(sftp_session, ssh2CharPointer(path));
        
    if (!handle) {
        throw MLDB::Exception("couldn't open path: " + lastError());
    }

    return Directory(path, handle, this);
}

SftpConnection::File
SftpConnection::
openFile(const Utf8String & path)
{
    LIBSSH2_SFTP_HANDLE * handle
        = libssh2_sftp_open_ex(sftp_session, ssh2CharPointer(path),
                               path.length(), LIBSSH2_FXF_READ, 0,
                               LIBSSH2_SFTP_OPENFILE);
        
    if (!handle) {
        throw MLDB::Exception("couldn't open path: " + lastError());
    }

    return File(path, handle, this);
}

bool
SftpConnection::
getAttributes(const Utf8String & path, Attributes & attrs)
    const
{
    int res = libssh2_sftp_stat_ex(sftp_session,
                                   ssh2CharPointer(path), path.length(), LIBSSH2_SFTP_STAT,
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
           const Utf8String & path)
{
    /* Request a file via SFTP */ 
    LIBSSH2_SFTP_HANDLE * handle =
        libssh2_sftp_open(sftp_session, ssh2CharPointer(path),
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

struct SftpStreamingDownloadSource {

    SftpStreamingDownloadSource(const SftpConnection * owner,
                                Utf8String path)
    {
        impl.reset(new Impl());
        impl->owner = owner;
        impl->path = path;
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
            : owner(0), offset(0), handle(0)
        {
        }

        ~Impl()
        {
            stop();
        }

        const SftpConnection * owner;
        Utf8String path;
        size_t offset;
        LIBSSH2_SFTP_HANDLE * handle;

        Date startDate;

        void start()
        {
            handle
                = libssh2_sftp_open_ex(owner->sftp_session, ssh2CharPointer(path),
                                       path.length(), LIBSSH2_FXF_READ, 0,
                                       LIBSSH2_SFTP_OPENFILE);
            
            if (!handle) {
                throw MLDB::Exception("couldn't open path: "
                                    + owner->lastError());
            }
        }

        void stop()
        {
            if (handle) libssh2_sftp_close(handle);
        }

        std::streamsize read(char_type* s, std::streamsize n)
        {
            BOOST_STATIC_ASSERT(sizeof(char_type) == 1);

            ssize_t numRead = libssh2_sftp_read(handle, s, n);
            if (numRead < 0) {
                throw MLDB::Exception("read(): " + owner->lastError());
            }
            
            return numRead;
        }
    };

    std::shared_ptr<Impl> impl;

    std::streamsize read(char_type* s, std::streamsize n)
    {
        return impl->read(s, n);
    }

#if 0
    void seek(std::streamsize where, std::ios_base::seekdir dir)
    {
    }
#endif

    bool is_open() const
    {
        return !!impl;
    }

    void close()
    {
        impl.reset();
    }
};



struct SftpStreamingUploadSource {

    SftpStreamingUploadSource(const SftpConnection * owner,
                              const Utf8String & path,
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
        Utf8String path;
        OnUriHandlerException onException;
        
        size_t offset;
        size_t lastPrint;
        Date lastTime;

        Date startDate;

        void start()
        {
            /* Request a file via SFTP */ 
            handle =
                libssh2_sftp_open(owner->sftp_session, ssh2CharPointer(path),
                                  LIBSSH2_FXF_WRITE|LIBSSH2_FXF_CREAT|LIBSSH2_FXF_TRUNC,
                                  LIBSSH2_SFTP_S_IRUSR|LIBSSH2_SFTP_S_IWUSR|
                                  LIBSSH2_SFTP_S_IRGRP|LIBSSH2_SFTP_S_IROTH);
            
            if (!handle) {
                auto excPtr = make_exception_ptr(
                    MLDB::Exception("couldn't open path: " + owner->lastError()));
                onException(excPtr);
                throw excPtr;
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
                    auto excPtr = make_exception_ptr(
                        MLDB::Exception("couldn't upload file: " + owner->lastError()));
                    onException(excPtr);
                    throw excPtr;
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
streamingUpload(const Utf8String & path) const
{
    filter_ostream result;
    auto onException = [&](const exception_ptr & excPtr) { result.notifyException(); };
    std::shared_ptr<std::streambuf> buf(streamingUploadStreambuf(path, onException).release());
    result.openFromStreambuf(buf.get(), buf, path);
    
    return result;
}

std::unique_ptr<std::streambuf>
SftpConnection::
streamingUploadStreambuf(const Utf8String & path,
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
streamingDownload(const Utf8String & path) const
{
    filter_istream result;
    std::shared_ptr<std::streambuf> buf(streamingDownloadStreambuf(path).release());
    result.openFromStreambuf(buf.get(), buf, path);

    return result;
}

std::unique_ptr<std::streambuf>
SftpConnection::
streamingDownloadStreambuf(const Utf8String & path) const
{
    std::unique_ptr<std::streambuf> result;
    result.reset(new boost::iostreams::stream_buffer<SftpStreamingDownloadSource>
                 (SftpStreamingDownloadSource(this, path),
                  131072));
    return result;
}

int
SftpConnection::
unlink(const Utf8String & path) const {
    return libssh2_sftp_unlink(sftp_session, ssh2CharPointer(path));
}

int
SftpConnection::
mkdir(const Utf8String & path) const {
    return libssh2_sftp_mkdir(sftp_session, ssh2CharPointer(path),
                              LIBSSH2_SFTP_S_IRWXU | LIBSSH2_SFTP_S_IRWXG |
                              LIBSSH2_SFTP_S_IRWXO);
}

namespace {

struct SftpHostInfo {
    Utf8String sftpHost;
    std::shared_ptr<SftpConnection> connection;  //< Used to access this uri
};

std::mutex sftpHostsLock;
std::unordered_map<Utf8String, SftpHostInfo> sftpHosts;

} // file scope

/** Sftp support for filter_ostream opens.  Register the host name here, and
    you can open it directly from sftp.
*/

void registerSftpHostPassword(const Utf8String & hostname,
                              const Utf8String & username,
                              const Utf8String & password,
                              const std::string & port)
{
    std::unique_lock<std::mutex> guard(sftpHostsLock);
    if (sftpHosts.count(hostname)){
        throw HostAlreadyRegistered(hostname);
    }

    SftpHostInfo info;
    info.sftpHost = hostname;
    info.connection = std::make_shared<SftpConnection>();
    info.connection->connectPasswordAuth(hostname.rawString(), username.rawString(), password.rawString(), port);
    
    sftpHosts[hostname] = info;
}

void registerSftpHostPublicKey(const Utf8String & hostname,
                               const Utf8String & username,
                               const Utf8String & publicKeyFile,
                               const Utf8String & privateKeyFile,
                               const std::string & port)
{
    std::unique_lock<std::mutex> guard(sftpHostsLock);
    if (sftpHosts.count(hostname)){
        throw HostAlreadyRegistered(hostname);
    }

    SftpHostInfo info;
    info.sftpHost = hostname;
    info.connection = std::make_shared<SftpConnection>();
    info.connection->connectPublicKeyAuth(hostname.rawString(), username.rawString(),
                                          publicKeyFile.rawString(),
                                          privateKeyFile.rawString(),
                                          port);
    sftpHosts[hostname] = info;
}

struct RegisterSftpHandler {

    static UriHandler
    getSftpHandler(const std::string & scheme,
                   const Utf8String & resource,
                   std::ios_base::openmode mode,
                   const std::map<std::string, std::string> & options,
                   const OnUriHandlerException & onException)
    {
        auto [connStr, path, found] = split_on_first(resource, "/");
        if (!found) {
            throw MLDB::Exception("unable to find sftp host name in resource "
                                + resource);
        }
        const auto & connection = getSftpConnectionFromConnStr(connStr);

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
    }

    RegisterSftpHandler()
    {
        registerUriHandler("sftp", getSftpHandler);
    }

} registerSftpHandler;

const SftpConnection & getSftpConnectionFromConnStr(const Utf8String & connStr)
{
    std::unique_lock<std::mutex> guard(sftpHostsLock);
    auto it = sftpHosts.find(connStr);
    if (it != sftpHosts.end() && it->second.connection.get()->isAlive()) {
        return *it->second.connection.get();
    }
    auto creds = getCredential("sftp", "sftp://" + connStr);

    auto [host, port, found] = split_on_first(connStr, ":");
    if (!found) {
        port = "ssh";
    }

    SftpHostInfo info;
    info.sftpHost = host;
    info.connection = std::make_shared<SftpConnection>();
    info.connection->connectPasswordAuth(host, creds.id, creds.secret, port.extractAscii());
    auto [it2, inserted] = sftpHosts.insert({connStr, std::move(info)});
    return *it2->second.connection.get();
}

namespace {

string connStrFromUri(const Utf8String & uri) {
    return must_remove_prefix(uri, "sftp://").extractAscii();
};

struct SftpUrlFsHandler : public UrlFsHandler {

    UriHandler getUriHandler(const Url & url) const
    {
        auto urlStr = url.toDecodedString();
        urlStr = must_remove_prefix(urlStr, "sftp://");
        const std::map<std::string, std::string> options;
        return RegisterSftpHandler::getSftpHandler(
            "", urlStr, ios::in, options, nullptr);
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
        auto urlStr = url.toDecodedString();
        string connStr = connStrFromUri(urlStr);
        const auto & conn = getSftpConnectionFromConnStr(connStr);
        conn.mkdir(must_remove_prefix(urlStr, "sftp://" + connStr));
    }

    bool erase(const Url & url, bool throwException) const override
    {
        auto urlStr = url.toDecodedString();
        auto connStr = connStrFromUri(urlStr);
        const auto & conn = getSftpConnectionFromConnStr(connStr);
        int res = 0;
        try {
            res = conn.unlink(must_remove_prefix(urlStr, "sftp://" + connStr));
        }
        catch (const Exception & e) {
            if (throwException) {
                throw;
            }
            res = -1;
        }
        return res == 0;
    }

    bool forEach(const Url & prefix,
                 const OnUriObject & onObject,
                 const OnUriSubdir & onSubdir,
                 const std::string & delimiter,
                 const std::string & startAt) const override
    {
        ExcAssert(delimiter == "/");
        auto url = prefix.toString();
        const string connStr = connStrFromUri(url);
        const auto & conn = getSftpConnectionFromConnStr(connStr);

        function<void(Utf8String, int)> processPath = [&] (Utf8String path, int depth) {
            auto dir = conn.getDirectory(path);
            dir.forEachFile([&] (Utf8String name, SftpConnection::Attributes attr) {
                // For help with masks see
                // https://github.com/libssh2/libssh2/blob/master/docs/libssh2_sftp_fstat_ex.3
                Utf8String currUri = "sftp://" + connStr + path + "/" + name;
                if (LIBSSH2_SFTP_S_ISREG (attr.permissions)) {
                    OpenUriObject open = [=,this] (const std::map<std::string, std::string> & options) -> UriHandler
                    {
                        if (!options.empty()) {
                            throw MLDB::Exception("Options not accepted by S3");
                        }

                        std::shared_ptr<std::istream> result(
                            new filter_istream(currUri));
                        auto info = getInfo(Url(currUri));

                        return UriHandler(result->rdbuf(), result, info);
                    };
                    onObject(currUri, getInfo(Url(currUri)), open, 1);
                    return;
                }
                if (LIBSSH2_SFTP_S_ISDIR (attr.permissions)) {
                    if (name == ".." || name == ".") {
                        return;
                    }
                    if (onSubdir && onSubdir(currUri, depth)) {
                        processPath(path + "/" + name, depth + 1);
                    }
                    return;
                }

            });
        };

        Utf8String path(url.begin() + 7 + connStr.size(), url.end());
        processPath(path, 0);

        return true;
    }
};

struct AtInit {
    AtInit() {
        registerUrlFsHandler("sftp", new SftpUrlFsHandler());
    }
} atInit;

} // namespace nameless
} // namespace MLDB
