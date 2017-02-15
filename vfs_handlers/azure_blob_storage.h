/**                                                                 -*- C++ -*-
 * azure_blob_storage.h
 * Mich, 2017-02-15
 * This file is part of MLDB. Copyright 2017 mldb.ai inc. All rights reserved.
 **/

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


/** Sftp support for filter_ostream opens.  Register the sftp host here,
    and you can open it directly from s3 using the sftp:// syntax.
*/

class AzureAccountAlreadyRegistered : public MLDB::Exception {
public:
    AzureAccountAlreadyRegistered(const std::string & account) :
        MLDB::Exception("Azure account %s already registered", account.c_str())
    {
    }
};

/**
 * There doesn't seem to be a distinction at this level for the storage type.
 * connStr = "DefaultEndpointsProtocol=<>;AccountName=<>;AccountKey=<>;"
 **/
void registerAzureStorageAccount(const std::string & connStr);

//const SftpConnection & getSftpConnectionFromConnStr(const std::string & connStr);

} // namespace MLDB

