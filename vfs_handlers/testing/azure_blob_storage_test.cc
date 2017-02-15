/**                                                                 -*- C++ -*-
 * azure_blob_storage_test.cc
 * Mich, 2017-02-15
 * This file is part of MLDB. Copyright 2017 mldb.ai inc. All rights reserved.
 **/

#define BOOST_TEST_MAIN
#define BOOST_TEST_DYN_LINK

#include <sys/types.h>
#include <sys/stat.h>
#include <unistd.h>

#include <string>
#include <boost/test/unit_test.hpp>

#include "mldb/utils/testing/fixtures.h"
#include "mldb/vfs/filter_streams.h"

#include "mldb/vfs/fs_utils.h"
#include "mldb/vfs_handlers/azure_blob_storage.h"


using namespace std;
using namespace MLDB;

BOOST_AUTO_TEST_CASE( test_azure_storage_invalid_registration_string )
{
    MLDB_TRACE_EXCEPTIONS(false);
    BOOST_REQUIRE_THROW(registerAzureStorageAccount(""), MLDB::Exception);
    BOOST_REQUIRE_THROW(registerAzureStorageAccount("patate"), MLDB::Exception);
    BOOST_REQUIRE_THROW(registerAzureStorageAccount("a;b;c"), MLDB::Exception);
    BOOST_REQUIRE_THROW(
            registerAzureStorageAccount(
                "DefaultEndpointsProtocol=proto;AccountName=;AccountKey=key;"),
            MLDB::Exception);
}

BOOST_AUTO_TEST_CASE( test_azure_storage_double_registration )
{
    MLDB_TRACE_EXCEPTIONS(false);
    BOOST_REQUIRE_THROW(registerAzureStorageAccount(""), MLDB::Exception);
    string connStr =
        "DefaultEndpointsProtocol=proto;AccountName=name;AccountKey=key;";
    registerAzureStorageAccount(connStr);
    BOOST_REQUIRE_THROW(registerAzureStorageAccount(connStr), MLDB::Exception);
}
