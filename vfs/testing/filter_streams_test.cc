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
#include "mldb/vfs/filter_streams_registry.h"
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
#include "mldb/base/exc_assert.h"
#include "mldb/arch/demangle.h"
#include "mldb/base/hex_dump.h"
#include "mldb/utils/environment.h"
#include "mldb/base/iostream_adaptors.h"

using namespace std;
namespace fs = std::filesystem;
using namespace MLDB;

using boost::unit_test::test_suite;

#pragma clang diagnostic ignored "-Wunknown-warning-option"
#pragma clang diagnostic ignored "-Wvla-cxx-extension"

EnvOption<fs::path> binDir("BIN", "./bin");
EnvOption<fs::path> tmpDir("TMP", "./tmp");

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


void system(const std::string & command)
{
    int res = ::system(command.c_str());
    if (res == -1)
        throw MLDB::Exception(errno, "system(): system");
    if (res != 0)
        throw MLDB::Exception("command %s returned code %d",
                            command.c_str(), res);
}

void compress_using_tool(const std::string & input_file,
                         const std::string & output_file,
                         const std::string & command)
{
    system(command + " " + input_file + " > " + output_file);
}

void decompress_using_tool(const std::string & input_file,
                           const std::string & output_file,
                           const std::string & command)
{
    try {
        //system("hexdump -C " + input_file + " | head -n 10");
        system("cat " + input_file + " | " + command + " > " + output_file);
    } catch (...) {
        std::ifstream stream(input_file);
        constexpr size_t BUF_SIZE = 1024;
        char buf[BUF_SIZE];
        size_t n = stream.readsome(buf, BUF_SIZE);
        hex_dump(buf, n);
        throw;
    }
}

void compress_using_stream(const std::string & input_file,
                           const std::string & output_file)
{
    ifstream in(input_file.c_str());

    filter_ostream out(output_file);

    constexpr size_t BUF_SIZE = 16384;
    char buf[BUF_SIZE];

    while (in) {
        in.read(buf, BUF_SIZE);
        auto n = in.gcount();
        
        out.write(buf, n);
    }
}

void decompress_using_stream(const std::string & input_file,
                             const std::string & output_file)
{
    filter_istream in(input_file);
    ofstream out(output_file.c_str());

    char buf[16386];

    while (in) {
        in.read(buf, 16384);
        int n = in.gcount();
        
        out.write(buf, n);
    }
}

void assert_files_identical(const std::string & input_file,
                            const std::string & output_file)
{
    system("diff " + input_file + " " + output_file);
}

void test_compress_decompress(const std::string & input_file,
                              const std::string & extension,
                              const std::string & zip_command,
                              const std::string & unzip_command)
{
    string base = "tmp/filter_streams_test-" + extension;
    string cmp1 = base + ".1." + extension;
    string cmp2 = base + ".2." + extension;
    string dec1 = base + ".1";
    string dec2 = base + ".2";
    string dec3 = base + ".3";
    string dec4 = base + ".4";


    // Test 1: compress using filter stream
    Scope_Exit(::unlink(cmp1.c_str()));
    compress_using_stream(input_file, cmp1);

    // Test 2: compress using tool
    Scope_Exit(::unlink(cmp2.c_str()));
    compress_using_tool(input_file, cmp2, zip_command);

    // Test 3: decompress stream file using tool (sanity check)
    Scope_Exit(::unlink(dec1.c_str()));
    decompress_using_tool(cmp1, dec1, unzip_command);
    assert_files_identical(input_file, dec1);

    // Test 4: decompress tool file using stream
    Scope_Exit(::unlink(dec2.c_str()));
    decompress_using_stream(cmp2, dec2);
    assert_files_identical(input_file, dec2);
    
    // Test 5: decompress stream file using stream
    Scope_Exit(::unlink(dec3.c_str()));
    decompress_using_stream(cmp1, dec3);
    assert_files_identical(input_file, dec3);
    
    // Test 6: decompress tool file using tool (sanity)
    Scope_Exit(::unlink(dec4.c_str()));
    decompress_using_tool(cmp2, dec4, unzip_command);
    assert_files_identical(input_file, dec4);
}

BOOST_AUTO_TEST_CASE( test_compress_decompress_gz )
{
    string input_file = "mldb/vfs/testing/filter_streams_test.cc";
    test_compress_decompress(input_file, "gz", "gzip -c", "gzip -d");
}

BOOST_AUTO_TEST_CASE( test_compress_decompress_bzip2 )
{
    string input_file = "mldb/vfs/testing/filter_streams_test.cc";
    test_compress_decompress(input_file, "bz2", "bzip2 -c", "bzip2 -d");
}

BOOST_AUTO_TEST_CASE( test_compress_decompress_xz )
{
    string input_file = "mldb/vfs/testing/filter_streams_test.cc";
    test_compress_decompress(input_file, "xz", "xz -c", "xz -d");
}

BOOST_AUTO_TEST_CASE( test_compress_decompress_lz4 )
{
    string input_file = "mldb/vfs/testing/filter_streams_test.cc";
    string lz4_cmd = binDir.get() / "lz4cli";
    test_compress_decompress(input_file, "lz4", lz4_cmd, lz4_cmd + " -d");
}

BOOST_AUTO_TEST_CASE( test_compress_decompress_lz4_content_size )
{
    string input_file = "mldb/vfs/testing/filter_streams_test.cc";
    string lz4_cmd = binDir.get() / "lz4cli -B7 -BX --content-size";
    test_compress_decompress(input_file, "csize.lz4", lz4_cmd, lz4_cmd + " -d");
}

BOOST_AUTO_TEST_CASE( test_compress_decompress_zstandard )
{
    string input_file = "mldb/vfs/testing/filter_streams_test.cc";
    string zstd_cmd = binDir.get() / "zstd";
    test_compress_decompress(input_file, "zst", zstd_cmd + " -c", zstd_cmd + " -d");
}

BOOST_AUTO_TEST_CASE( test_open_failure )
{
    filter_ostream stream;
    {
        MLDB_TRACE_EXCEPTIONS(false);
        BOOST_CHECK_THROW(stream.open("/no/file/is/here"), std::exception);
        BOOST_CHECK_THROW(stream.open("/no/file/is/here.gz"), std::exception);
        BOOST_CHECK_THROW(stream.open("/no/file/is/here.gz"), std::exception);
    }
}

BOOST_AUTO_TEST_CASE( test_write_failure )
{
    MLDB_TRACE_EXCEPTIONS(false);
    int fd = open("/dev/null", O_RDWR, 0);

    cerr << "fd = " << fd << endl;

    filter_ostream stream(fd);
    BOOST_CHECK(!stream.eof());
    BOOST_CHECK(!stream.fail());
    BOOST_CHECK(!stream.bad());
    BOOST_CHECK_EQUAL(stream.rdstate(), std::ios::goodbit);

    BOOST_CHECK(stream.good());

    stream << "hello";
    stream << std::endl;

    {
        MLDB_TRACE_EXCEPTIONS(false);
        close(fd);
    }

    cerr <<" done close" << endl;

    {
        MLDB_TRACE_EXCEPTIONS(false);
        BOOST_CHECK_THROW(stream << "hello again" << std::endl, std::exception);
    }

}

/* ensures that empty gz/bzip2/xz streams have a valid header */
BOOST_AUTO_TEST_CASE( test_empty_gzip )
{
    fs::create_directories(tmpDir.get());

    string fileprefix(tmpDir.get() / "empty.");
    vector<string> exts = { "gz", "bz2", "xz", "lz4", "zst" };
    map<string, string> compressions = {
        { "gz", "gzip" },
        { "bz2", "bzip2" },
        { "xz", "lzma" },
        { "lz4", "lz4" },
        { "zst", "zstd" } };

    for (const auto & ext: exts) {
        cerr << "testing extension " << ext << endl;
        string filename = fileprefix + ext;

        /* stream from filename */
        {
            FileCleanup cleanup(filename);

            {
                filter_ostream filestream(filename);
            }

            BOOST_CHECK(getUriSize(filename) > 2);
            {
                filter_istream filestream(filename);
                string line;
                filestream >> line;
                BOOST_CHECK_EQUAL(line.size(), 0);
            }
        }

        /* stream from file descriptor */
        {
            FileCleanup cleanup(filename);

            int fd = open(filename.c_str(), O_RDWR| O_CREAT, S_IRWXU);
            {
                filter_ostream filestream;
                filestream.open(fd, ios::out, compressions[ext]);
            }
            close(fd);

            BOOST_CHECK(getUriSize(filename) > 2);
            {
                filter_istream filestream(filename);
                string line;
                filestream >> line;
                BOOST_CHECK_EQUAL(line.size(), 0);
            }
        }
    }
}

BOOST_AUTO_TEST_CASE( test_large_blocks )
{
    size_t blockSize = 64 * 1024 * 1024;
    char * block = new char [blockSize];
    for (unsigned i = 0;  i < blockSize;  ++i) {
        block[i] = i ^ (i << 8) ^ (i << 16) ^ (1 << 24);
    }
    
    string filename = tmpDir.get() / "badfile.zstd";

    FileCleanup cleanup(filename);
        
    {
        filter_ostream stream(filename);
        stream.write(block, blockSize);
    }

    {
        filter_istream stream(filename);
        char * block2 = new char[blockSize];
        stream.read(block2, blockSize);
        BOOST_CHECK_EQUAL(stream.gcount(), blockSize);

        //BOOST_CHECK(stream.eof());

        BOOST_CHECK_EQUAL_COLLECTIONS(block, block + blockSize,
                                      block2, block2 + blockSize);
    }
}

/* ensures that writing a 8M bytes text works */
BOOST_AUTO_TEST_CASE( test_mem_scheme_out )
{
    Scope_Exit(deleteAllMemStreamStrings());

    string text("");
    {
        string pattern("AbCdEfGh");
        text.reserve(pattern.size() * 1000000);
        for (int i = 0; i < 1000000; i++) {
            text += pattern;
        }
    }

    {
        filter_ostream outS("mem://out_file.txt");
        outS << text;
    }

    string result = getMemStreamString("out_file.txt");
    BOOST_CHECK_EQUAL(text, result);
}

/* ensures that writing a 8M bytes text to a gz target invokes the gz
 * filter */
BOOST_AUTO_TEST_CASE( test_mem_scheme_out_gz )
{
    Scope_Exit(deleteAllMemStreamStrings());

    string text("");
    {
        string pattern("AbCdEfGh");
        text.reserve(pattern.size() * 1000000);
        for (int i = 0; i < 1000000; i++) {
            text += pattern;
        }
    }

    {
        filter_ostream outS("mem://out_file.gz");
        outS << text;
    }

    /* A very poor check, indeed... Ultimatly, a real decompression should
    occur, but since we know that the gz filter works, and that unfiltered
    output works, logically we can deduce that the gz filter was invoked
    correctly if the strings are not equal. */
    string result = getMemStreamString("out_file.gz");
    BOOST_CHECK_NE(text, result);
}

/* ensures that reading a 8M bytes text works well too */
BOOST_AUTO_TEST_CASE( test_mem_scheme_in )
{
    Scope_Exit(deleteAllMemStreamStrings());

    string text("");
    {
        string pattern("AbCdEfGh");
        text.reserve(pattern.size() * 1000000);
        for (int i = 0; i < 1000000; i++) {
            text += pattern;
        }
        setMemStreamString("in_file.txt", text);
    }

    string result;
    filter_istream inS("mem://in_file.txt");
    while (inS) {
        char buf[16384];
        inS.read(buf, 16384);
        result.append(buf, inS.gcount());
    }

    BOOST_CHECK_EQUAL(text, result);
}

/* Testing the behaviour of filter_stream when exceptions occur during read,
 * write, close or destruction */
struct ExceptionSource {
    enum ThrowType {
        ThrowOnWrite,
        ThrowOnRead,
        ThrowOnClose
    };

    ExceptionSource(const OnUriHandlerException & onException,
                    ThrowType throwType)
        : onException_(onException), throwType_(throwType)
    {
    }

    typedef char char_type;
    using is_source = std::true_type;

    bool is_open() const
    {
        return true;
    }

    std::streamsize write(const char_type* s, std::streamsize n)
    {
        if (throwType_ == ThrowType::ThrowOnWrite) {
            throw MLDB::Exception("throwing when writing");
        }
        return n;
    }

    std::streamsize read(char_type* s, std::streamsize n)
    {
        if (throwType_ == ThrowType::ThrowOnRead) {
            throw MLDB::Exception("throwing when reading");
        }
        char randomdata[n];
        ::memcpy(s, randomdata, n);
        return n;
    }

    void close()
    {
        if (throwType_ == ThrowType::ThrowOnClose) {
            onException_(exception_ptr());
        }
    }

    OnUriHandlerException onException_;
    ThrowType throwType_;
};

struct RegisterExcHandlers {
    static UriHandler
    getExceptionSource(const OnUriHandlerException & onException,
                       ExceptionSource::ThrowType throwType)
    {
        shared_ptr<streambuf> handler;

        handler.reset(new source_istreambuf<ExceptionSource>(ExceptionSource(onException, throwType), 1));
        FsObjectInfo info;
        info.exists = true;
        return UriHandler(handler.get(), handler, info);
    }

    static UriHandler
    getExceptionSink(const OnUriHandlerException & onException,
                     ExceptionSource::ThrowType throwType)
    {
        shared_ptr<streambuf> handler;

        handler.reset(new sink_ostreambuf<ExceptionSource>(ExceptionSource(onException, throwType), 1));
        FsObjectInfo info;
        info.exists = true;
        return UriHandler(handler.get(), handler, info);
    }

    static UriHandler
    getExcOnReadHandler(const std::string & scheme,
                        const Utf8String & resource,
                        std::ios_base::openmode mode,
                        const std::map<std::string, std::string> & options,
                        const OnUriHandlerException & onException)
    {
        return getExceptionSource(onException, ExceptionSource::ThrowOnRead);
    }

    static UriHandler
    getExcOnWriteHandler(const std::string & scheme,
                         const Utf8String & resource,
                         std::ios_base::openmode mode,
                         const std::map<std::string, std::string> & options,
                         const OnUriHandlerException & onException)
    {
        return getExceptionSink(onException, ExceptionSource::ThrowOnWrite);
    }

    static UriHandler
    getExcOnCloseHandler(const std::string & scheme,
                         const Utf8String & resource,
                         std::ios_base::openmode mode,
                         const std::map<std::string, std::string> & options,
                         const OnUriHandlerException & onException)
    {
        return getExceptionSink(onException, ExceptionSource::ThrowOnClose);
    }

    void registerBuckets()
    {
    }

    RegisterExcHandlers()
    {
        registerUriHandler("throw-on-read", getExcOnReadHandler);
        registerUriHandler("throw-on-write", getExcOnWriteHandler);
        registerUriHandler("throw-on-close", getExcOnCloseHandler);
    }
} registerExcHandlers;

BOOST_AUTO_TEST_CASE(test_filter_stream_exceptions_read)
{
    filter_istream stream("throw-on-read://exception-zone");

    string data;
    auto action = [&]() {
        MLDB_TRACE_EXCEPTIONS(false);
        stream >> data;
    };

    BOOST_CHECK_THROW(action(), MLDB::Exception);
}

BOOST_AUTO_TEST_CASE(test_filter_stream_exceptions_write)
{
    MLDB_TRACE_EXCEPTIONS(false);
    filter_ostream stream("throw-on-write://exception-zone");

    auto action = [&]() {
        string sample("abcdef0123456789");
        /* we loop enough times to saturate the stream internal buffer
         * and cause "write" to be called */
        for (int i = 0; i < 1000000; i++) {
            stream << sample;
        }
    };

    BOOST_CHECK_THROW(action(), MLDB::Exception);
}

BOOST_AUTO_TEST_CASE(test_filter_stream_exceptions_close)
{
    filter_ostream stream("throw-on-close://exception-zone");

    auto action = [&]() {
        MLDB_TRACE_EXCEPTIONS(false);
        stream.close();
    };

    BOOST_CHECK_THROW(action(), std::ios_base::failure);
}

BOOST_AUTO_TEST_CASE(test_filter_stream_exceptions_destruction_ostream)
{
    unique_ptr<filter_ostream> stream;
    stream.reset(new filter_ostream("throw-on-close://exception-zone"));

    auto action = [&]() {
        MLDB_TRACE_EXCEPTIONS(false);
        stream.reset();
    };

    action();
}

BOOST_AUTO_TEST_CASE(test_filter_stream_exceptions_destruction_istream)
{
    unique_ptr<filter_istream> stream;
    stream.reset(new filter_istream("throw-on-close://exception-zone"));

    auto action = [&]() {
        MLDB_TRACE_EXCEPTIONS(false);
        stream.reset();
    };

    action();
}

BOOST_AUTO_TEST_CASE(test_filter_stream_mapping)
{
    filter_istream stream1("file://mldb/utils/testing/fixtures/hello.txt",
                           { { "mapped", "true" } });

    BOOST_CHECK(!stream1.eof());
    BOOST_CHECK(!stream1.fail());
    BOOST_CHECK(!stream1.bad());
    BOOST_CHECK_EQUAL(stream1.rdstate(), std::ios::goodbit);

    const char * addr;
    size_t size;
    size_t capacity;

    std::tie(addr, size, capacity) = stream1.mapped();
    cerr << "addr = " << (void *)addr << endl;
    cerr << "size = " << size << endl;
    cerr << "capacity = " << capacity << endl;

    BOOST_REQUIRE(addr != nullptr);
    BOOST_CHECK_EQUAL(strncmp(addr, "hello", 5), 0);
    BOOST_CHECK_GE(strncmp(std::get<0>(stream1.mapped()), "hello", 5), 0);

    std::string str;
    BOOST_CHECK(stream1.good());
    getline(stream1, str);
    BOOST_CHECK(stream1.good());

    int c MLDB_UNUSED = stream1.get();
    BOOST_CHECK(!stream1.bad());
    BOOST_CHECK(stream1.fail());
    BOOST_CHECK(stream1.eof());


    BOOST_CHECK_EQUAL(str, "hello");
    stream1.close();

    filter_istream stream2("file://mldb/utils/testing/fixtures/hello.txt.gz",
                           { { "mapped", "true" } });

    getline(stream2, str);

    BOOST_CHECK_EQUAL(str, "hello");
    BOOST_CHECK(!stream2.fail());
    BOOST_CHECK(!stream2.bad());

    getline(stream2, str);

    BOOST_CHECK_EQUAL(str, "");
    BOOST_CHECK(stream2.eof());
    BOOST_CHECK(stream2.fail());
    BOOST_CHECK(!stream2.bad());
    stream2.close();

    // Verify that the mapped stream has extra space beyond the end even when it's
    // a multiple of the page size (16kb is chosen instead of 4k because that's the
    // page size of arm64 based macs, which have the largest pages of supported
    // hosts for MLDB).
    filter_istream stream3("file://mldb/utils/testing/fixtures/16kbofones.txt",
                           { { "mapped", "true" } });
    
    std::tie(addr, size, capacity) = stream3.mapped();
    cerr << "addr = " << (const void *)addr << endl;
    cerr << "size = " << size << endl;
    cerr << "capacity = " << capacity << endl;

    BOOST_REQUIRE(addr != nullptr);
    BOOST_REQUIRE_EQUAL(size, 16384);
    BOOST_REQUIRE_GE(capacity, 16384 + MAPPING_EXTRA_CAPACITY);

    // Verify the contents of the mapped file
    for (size_t i = 0;  i < 16384;  ++i) {
        BOOST_CHECK_EQUAL(addr[i], '1');
    }

    // Verify that we can read beyond the end and that it returns zeros (anything
    // else indicates uninitialized memory which is a security risk and will trigger
    // undefined behaviour / address sanitizer / valgrind issues).
    for (size_t i = size;  i < capacity;  ++i) {
        BOOST_CHECK_EQUAL(addr[i], 0);
    }
}

BOOST_AUTO_TEST_CASE(test_empty_filter_stream_mapped)
{
    filter_istream stream;
    stream.open("file://mldb/utils/testing/fixtures/empty.txt",
                { { "mapped", "true" } });
    // we cannot map an empty file
    auto [addr, size, capacity] = stream.mapped();
    BOOST_CHECK_EQUAL(addr, (char *)nullptr);
    BOOST_CHECK_EQUAL(size, 0);
    // but we can read it without failing
    BOOST_CHECK_EQUAL(stream.readAll(), "");
}

BOOST_AUTO_TEST_CASE(test_file_stream_tellg)
{
    {
        filter_istream stream;
        stream.open("file://mldb/utils/testing/fixtures/hello.txt");
        std::string str;
        getline(stream, str);
        BOOST_CHECK_EQUAL(str, "hello");
        BOOST_CHECK_EQUAL(stream.tellg(), 6);
        stream.close();
        BOOST_CHECK_EQUAL(stream.tellg(), -1);
    }

    {
        filter_istream stream;
        stream.open("file://mldb/utils/testing/fixtures/hello.txt.gz");
        std::string str;
        getline(stream, str);
        BOOST_CHECK_EQUAL(str, "hello");
        BOOST_CHECK_EQUAL(stream.tellg(), 6);
        stream.close();
        BOOST_CHECK_EQUAL(stream.tellg(), -1);
    }

    {
        filter_istream stream;
        stream.open("https://raw.githubusercontent.com/mldbai/mldb/master/utils/testing/fixtures/hello.txt");
        std::string str;
        getline(stream, str);
        BOOST_CHECK_EQUAL(str, "hello");
        BOOST_CHECK_EQUAL(stream.tellg(), 6);
        stream.close();
        BOOST_CHECK_EQUAL(stream.tellg(), -1);
    }

    {
        filter_istream stream;
        stream.open("https://raw.githubusercontent.com/mldbai/mldb/master/utils/testing/fixtures/hello.txt.gz");
        std::string str;
        getline(stream, str);
        BOOST_CHECK_EQUAL(str, "hello");
        BOOST_CHECK_EQUAL(stream.tellg(), 6);
        stream.close();
        BOOST_CHECK_EQUAL(stream.tellg(), -1);
    }

    {
        {
            filter_ostream stream("mem://hello.txt");
            stream << "hello" << endl;
            stream.close();
        }

        filter_istream stream;
        stream.open("mem://hello.txt");
        std::string str;
        getline(stream, str);
        BOOST_CHECK_EQUAL(str, "hello");
        BOOST_CHECK_EQUAL(stream.tellg(), 6);
        BOOST_CHECK_EQUAL(stream.info().size, 6);
        stream.close();
        BOOST_CHECK_EQUAL(stream.tellg(), -1);
    }

    {
        filter_istream stream;
        stream.open("mldb/vfs/testing/fixtures/minimal.csv", {{ "mapped", "true" }});
        BOOST_CHECK_EQUAL(stream.tellg(), 0);
        std::string line;
        getline(stream, line);
        BOOST_CHECK_EQUAL(line, "a,b,c");
        BOOST_CHECK_EQUAL(stream.tellg(), 6);
        getline(stream, line);
        BOOST_CHECK_EQUAL(line, "d,e,f");
        BOOST_CHECK_EQUAL(stream.tellg(), 12);
    }
}
