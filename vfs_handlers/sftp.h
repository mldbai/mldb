/* sftp.h                                                          -*- C++ -*-
   Jeremy Barnes, 21 June 2012
   Copyright (c) 2012 mldb.ai inc.  All rights reserved.

   This file is part of MLDB. Copyright 2015 mldb.ai inc. All rights reserved.

   Sftp functionality.
*/

#pragma once

#include <string>
#include <libssh2.h>
#include <libssh2_sftp.h>
#include <functional>
#include <memory>
#include "mldb/vfs/filter_streams.h"
#include "mldb/vfs/filter_streams_registry.h"
#include "mldb/arch/exception.h"

namespace MLDB {


/*****************************************************************************/
/* SOCKET CONNECTION                                                         */
/*****************************************************************************/

/** Deals with a single socket for an ssh connection.  The libssh2 library
    leaves socket management to the user.
*/

struct SocketConnection {
    int sock;

    SocketConnection();

    ~SocketConnection();

    void connect(const Utf8String & hostname,
                 const std::string & port);

    void close();
};


/*****************************************************************************/
/* SSH CONNECTION                                                            */
/*****************************************************************************/

/** Deals with a single ssh connection.  This is used to multiplex various
    other kinds of protocols, once authentication is done.
*/

struct SshConnection : public SocketConnection {
    LIBSSH2_SESSION *session;

    SshConnection();

    ~SshConnection();

    void connect(const Utf8String & hostname,
                 const std::string & port);

    void passwordAuth(const Utf8String & username,
                      const Utf8String & password);

    void publicKeyAuth(const Utf8String & username,
                       const Utf8String & publicKeyFile,
                       const Utf8String & privateKeyFile);

    void setBlocking();

    Utf8String lastError() const;

    void close();
};


/*****************************************************************************/
/* SFTP CONNECTION                                                           */
/*****************************************************************************/

/** An SFTP connection, built on top of the ssh connection. */

struct SftpConnection : public SshConnection {
    LIBSSH2_SFTP *sftp_session;

    SftpConnection();
    SftpConnection(const SftpConnection & other) = delete;

    ~SftpConnection();

    void connectPasswordAuth(const Utf8String & hostname,
                             const Utf8String & username,
                             const Utf8String & password,
                             const std::string & port = "ssh");

    void connectPublicKeyAuth(const Utf8String & hostname,
                              const Utf8String & username,
                              const Utf8String & publicKeyFile,
                              const Utf8String & privateKeyFile,
                              const std::string & port = "ssh");

    struct Attributes : public LIBSSH2_SFTP_ATTRIBUTES {
    };

    struct File {
        Utf8String path;
        LIBSSH2_SFTP_HANDLE *handle;
        SftpConnection * owner;

        File(const Utf8String & path,
             LIBSSH2_SFTP_HANDLE * handle,
             SftpConnection * owner);

        ~File();

        Attributes getAttr() const;

        uint64_t size() const;

        void downloadTo(const std::string & filename) const;
    };

    struct Directory {
        Utf8String path;
        LIBSSH2_SFTP_HANDLE *handle;
        const SftpConnection * owner;
        
        Directory(const Utf8String & path,
                  LIBSSH2_SFTP_HANDLE * handle,
                  const SftpConnection * owner);

        ~Directory();

        void ls() const;

        typedef std::function<void (std::string, Attributes)> OnFile;

        void forEachFile(const OnFile & onFile) const;
    };

    Directory getDirectory(const Utf8String & path) const;

    File openFile(const Utf8String & path);

    void uploadFile(const char * start,
                    size_t size,
                    const Utf8String & path);

    bool getAttributes(const Utf8String & path, Attributes & attrs) const;
    
    std::unique_ptr<std::streambuf>
    streamingUploadStreambuf(const Utf8String & path,
                             const OnUriHandlerException & onException) const;

    std::unique_ptr<std::streambuf>
    streamingDownloadStreambuf(const Utf8String & path) const;

    filter_ostream streamingUpload(const Utf8String & path) const;
    filter_istream streamingDownload(const Utf8String & path) const;

    int unlink(const Utf8String & path) const;
    int mkdir(const Utf8String & path) const;

    void close();

    SftpConnection operator = (const SftpConnection & other) = delete;

    bool isAlive() const;
};


/** Sftp support for filter_ostream opens.  Register the sftp host here,
    and you can open it directly from s3 using the sftp:// syntax.
*/

class HostAlreadyRegistered : public MLDB::Exception {
public:
    HostAlreadyRegistered(const Utf8String & bucketName) : 
        MLDB::Exception("sftp host %s already registered",
                      bucketName.c_str())
    {
    }
};

void registerSftpHostPassword(const Utf8String & hostname,
                              const Utf8String & username,
                              const Utf8String & password,
                              const std::string & port = "ssh");

void registerSftpHostPublicKey(const Utf8String & hostname,
                               const Utf8String & username,
                               const Utf8String & publicKeyFile,
                               const Utf8String & privateKeyFile,
                               const std::string & port = "ssh");

const SftpConnection & getSftpConnectionFromConnStr(const Utf8String & connStr);

} // namespace MLDB
