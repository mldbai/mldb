/**                                                                 -*- C++ -*-
 * azure_blob_storage_public_test.cc
 * Mich, 2017-02-23
 * This file is part of MLDB. Copyright 2017 mldb.ai inc. All rights reserved.
 **/

#define BOOST_TEST_MAIN
#define BOOST_TEST_DYN_LINK

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

BOOST_AUTO_TEST_CASE( test_azure_read_public )
{
    filter_istream read("azureblob://publicelementai/public/a_propos.txt");
    auto res = read.readAll();
    BOOST_REQUIRE_EQUAL(res.find("# Ne pas supprimer"), 0);
}

BOOST_AUTO_TEST_CASE( test_azure_read_private )
{
    BOOST_REQUIRE_THROW(
        filter_istream("azureblob://publicelementai/private/a_propos.txt"),
        std::exception);
}
