/**                                                                 -*- C++ -*-
 * azure_blob_storage_test.cc
 * Mich, 2017-02-15
 * This file is part of MLDB. Copyright 2017 mldb.ai inc. All rights reserved.
 **/

#include <sys/types.h>
#include <sys/stat.h>
#include <unistd.h>

#include <string>
#include <boost/test/unit_test.hpp>

#include "mldb/jml/utils/file_functions.h"
#include "mldb/utils/testing/fixtures.h"
#include "mldb/vfs/filter_streams.h"

#include "mldb/vfs/fs_utils.h"
#include "mldb/vfs_handlers/azure_blob_storage.h"
#include <boost/test/included/unit_test.hpp>

using namespace boost::unit_test;
using namespace std;
using namespace MLDB;

struct AzureRegistration {
    AzureRegistration() : registered(false)
    {
        const char * home = getenv("HOME");
        if (home == NULL) {
            return;
        }
        auto filename = home + string("/.azure_cloud_credentials");
        if (ML::fileExists(filename)) {
            std::ifstream stream(filename.c_str());
            int lineNum = 1;
            for (; stream;  ++lineNum) {
                string line;
                getline(stream, line);
                if (line.empty()) {
                    continue;
                }
                try {
                registerAzureStorageAccount(line);
                }
                catch (const MLDB::Exception & exc) {
                    cerr << "Failed at " << filename << ":" << lineNum << endl;
                    throw;
                }
                registered = true;
            }
        }
    }

    bool isRegistered()
    {
        return registered;
    }

    private:
        bool registered;

} azureRegistration;

void test_azure_storage_invalid_registration_string()
{
    MLDB_TRACE_EXCEPTIONS(false);
    BOOST_REQUIRE_THROW(registerAzureStorageAccount(""), MLDB::Exception);
    BOOST_REQUIRE_THROW(registerAzureStorageAccount("patate"), MLDB::Exception);
    BOOST_REQUIRE_THROW(registerAzureStorageAccount("a;b;c"), MLDB::Exception);
    BOOST_REQUIRE_THROW(
            registerAzureStorageAccount(
                "DefaultEndpointsProtocol=proto;AccountName=;AccountKey=key1;"),
            MLDB::Exception);
}

void test_azure_storage_double_registration()
{
    MLDB_TRACE_EXCEPTIONS(false);
    string connStr =
        "DefaultEndpointsProtocol=proto;AccountName=name;AccountKey=key1234=;";
    registerAzureStorageAccount(connStr);
    BOOST_REQUIRE_THROW(registerAzureStorageAccount(connStr), MLDB::Exception);
}

void test_azure_storage_read()
{
    filter_istream read("azureblob://publicelementai/private/a_propos.txt");
    string res = read.readAll();
    cerr << res << endl;
    BOOST_REQUIRE_EQUAL(res.find("# Ne pas supprimer"), 0);
}

void test_azure_storage_write_and_erase()
{
    auto now = Date::now();
    string outputUri =
        "azureblob://publicelementai/private/subdirectory/ecriture"
        + to_string(now.secondsSinceEpoch()) + ".txt";
    filter_ostream w(outputUri);
    string outputStr = "writing to azure from filter_ostream" + now.printIso8601();
    w << outputStr;
    w.close();

    auto objectInfo = tryGetUriObjectInfo(outputUri);
    BOOST_REQUIRE(objectInfo.exists);

    filter_istream read(outputUri);
    auto res = read.readAll();
    cerr << "READ: " << res << endl;
    cerr << "EXPECTED: " << outputStr << endl;
    BOOST_REQUIRE_EQUAL(res, outputStr);

    // Erase
    eraseUriObject(outputUri);
    objectInfo = tryGetUriObjectInfo(outputUri);
    BOOST_REQUIRE(!objectInfo.exists);
}

void test_azure_file_crawler()
{
    // for each uri
    auto onObject = [&] (const string & uri,
                         const FsObjectInfo & info,
                         const OpenUriObject & open,
                         int depth) -> bool
    {
        cerr << "on object: " << uri << " - " << info.size << endl;
        return true;
    };
    auto onSubDir = [&] (const string & dirName, int depth) -> bool
    {
        return true;
    };
    forEachUriObject("azureblob://publicelementai/private/", onObject, onSubDir);
}

test_suite*
init_unit_test_suite( int argc, char* argv[] )
{
    framework::master_test_suite().add(
        BOOST_TEST_CASE(&test_azure_storage_invalid_registration_string));
    framework::master_test_suite().add(
        BOOST_TEST_CASE(&test_azure_storage_double_registration));

    auto azureTests = {&test_azure_storage_read,
                       &test_azure_storage_write_and_erase,
                       &test_azure_file_crawler};
    if (azureRegistration.isRegistered()) {
        for (const auto & test: azureTests) {
            framework::master_test_suite().add(
                BOOST_TEST_CASE(test));
        }
    }
    else {
        cerr << "No valid azure connection string found, skipping "
             << azureTests.size() << " related tests" << endl;
    }
    return 0;
}
