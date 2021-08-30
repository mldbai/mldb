/* filter_streams_test.cc
   Jeremy Barnes, 29 June 2011
   Copyright (c) 2011 mldb.ai inc.
   Copyright (c) 2011 Jeremy Barnes.

   This file is part of MLDB. Copyright 2015 mldb.ai inc. All rights reserved.
*/

#define BOOST_TEST_MAIN
#define BOOST_TEST_DYN_LINK

#include <string.h>

#include "mldb/vfs/filter_streams.h"
#include "mldb/vfs/fs_utils.h"
#include "mldb/vfs/compressor.h"
#include "mldb/arch/exception.h"
#include "mldb/arch/exception_handler.h"

#include "mldb/compiler/filesystem.h"
#include <boost/test/unit_test.hpp>
#include <thread>
#include <vector>
#include <stdint.h>
#include <iostream>
#include <fcntl.h>
#include <exception>

#include "mldb/base/scope.h"
#include "mldb/arch/exception_handler.h"
#include "mldb/arch/demangle.h"

using namespace std;
namespace fs = std::filesystem;
using namespace MLDB;

using boost::unit_test::test_suite;

struct FileCleanup {
    FileCleanup(const string & filename)
        : filename_(filename)
    {
    }

    ~FileCleanup()
    {
        ::unlink(filename_.c_str());
    }

    string filename_;
};

fs::path binDir = std::string(getenv("BIN"));
fs::path tmpDir = std::string(getenv("TMP"));

void system(const std::string & command)
{
    int res = ::system(command.c_str());
    if (res == -1)
        throw MLDB::Exception(errno, "system(): system");
    if (res != 0)
        throw MLDB::Exception("command %s returned code %d",
                            command.c_str(), res);
}

BOOST_AUTO_TEST_CASE( test_compress_decompress_lz4_content_size )
{
    string input_file = "mldb/vfs/testing/filter_streams_test.cc";
    string output_file = "tmp/compressor_test.lz4";
    string lz4_cmd = binDir / string("lz4cli --content-size " + input_file + " " + output_file);

    Scope_Exit(::unlink(output_file.c_str()));
    system(lz4_cmd);

    std::ifstream stream(output_file.c_str());

    char buf[20];
    stream.read(buf, 20);

    int numRead = stream.gcount();

    auto decomp = Decompressor::create("lz4");
    
    auto size = decomp->decompressedSize(buf, numRead, -1 /* total len unknown */);

    cerr << "decompressed size = " << size << endl;

    filter_istream stream2(output_file);

    BOOST_CHECK_EQUAL(size, stream2.readAll().length());
}
