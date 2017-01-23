/* MLDBFB-239-s3-test.cc
   Jeremy Barnes, 2 July 2012
   This file is part of MLDB.
   Copyright (c) 2012 mldb.ai inc.  All rights reserved.

   Test of s3.
*/

#define BOOST_TEST_MAIN
#define BOOST_TEST_DYN_LINK

#include <set>
#include <boost/test/unit_test.hpp>
#include "mldb/base/exc_assert.h"
#include "mldb/jml/utils/guard.h"
#include "mldb/arch/exception.h"
#include "mldb/soa/service/aws.h"
#include "mldb/soa/service/s3.h"
#include "mldb/utils/testing/print_utils.h"
#include "mldb/vfs/filter_streams.h"


using namespace std;
using namespace MLDB;


#define ENABLE_LONG_TESTS 1
string bucket = "private-mldb-ai";


/* ensures that reduced redundancy is properly set when specified for S3 objects
   and that we can properly set up a publicly readable ACL, via the
   filter_istream interface.
*/
BOOST_AUTO_TEST_CASE( test_s3_attributes )
{
    vector<string> filesToClean;

    ML::Call_Guard guard([&] () { for (auto & f: filesToClean) tryEraseUriObject(f); });

    string filename = MLDB::format("s3://%s/attributes"
                                 "/pid-%d-reduced-redundancy",
                                 bucket.c_str(), getpid());
    filesToClean.push_back(filename);

    {
        filter_ostream stream(filename, { { "aws-redundancy", "REDUCED" } });
    
        stream << "HELLO" << endl;
    }

    auto s3Api = getS3ApiForUri(filename);

    auto info = s3Api->getObjectInfo(filename, S3ObjectInfoTypes::FULL_INFO);
    ExcAssert(info);

    BOOST_CHECK_EQUAL(info.storageClass, "REDUCED_REDUNDANCY");

    // test we can make a file public
    {
        filter_ostream stream(filename, { { "aws-acl", "public-read" } });
    
        stream << "HELLO" << endl;
    }
    
    string publicName = S3Api::getPublicUri(filename, "http");

    cerr << "public name = " << publicName << endl;

    // Get it via HTTP and check that it's OK
    HttpRestProxy proxy;
    auto resp = proxy.get(publicName);

    cerr << "resp = " << resp << endl;

    BOOST_CHECK_EQUAL(resp.code(), 200);
    BOOST_CHECK_EQUAL(resp.body(), "HELLO\n");
}

#if 1
/* Test s3 uploads with small payloads */
BOOST_AUTO_TEST_CASE( test_s3_uploads )
{
    MLDB_TRACE_EXCEPTIONS(false);

    vector<string> cleanupUrls;
    auto cleanup = [&] () {
        for (const string & fullName: cleanupUrls) {
            tryEraseUriObject(fullName);
        }
    };
    ML::Call_Guard guard(cleanup);

    auto writeFile = [&] (const string & filename, const string & contents) {
        FileCommiter commiter(filename);
        filter_ostream outs(filename);
        outs << contents;
        outs.close();
        commiter.commit();
    };
    
    string basename = "/s3api-upload-" + randomString(16);
    string fileUrl = "s3://" + bucket + basename;
    cleanupUrls.push_back(fileUrl);
    
    /* Test writing of empty file */
    writeFile(fileUrl, "");
    auto info = tryGetUriObjectInfo(fileUrl);
    BOOST_CHECK_EQUAL(bool(info), true);
    BOOST_CHECK_NE(info.etag, string());
    BOOST_CHECK_EQUAL(info.size, 0);

    /* Test writing of non-empty file */
    string contents = randomString(1024);
    writeFile(fileUrl, contents);
    info = tryGetUriObjectInfo(fileUrl);
    string etag1 = info.etag;
    BOOST_CHECK_EQUAL(info.size, contents.size());

    /* Ensure that a double upload result in the same etag. In practice upload
     * should be a no-op in this case due to "check" being CM_SIZE, but there
     * is currently no way of knowing if this worked. */
    writeFile(fileUrl, contents);
    info = tryGetUriObjectInfo(fileUrl);
    string etag2 = info.etag;
    BOOST_CHECK_EQUAL(etag1, etag2);
}
#endif


#if ENABLE_LONG_TESTS
/* Test three aspects of large (100MB) uploads and downloads:
   - ensure that the multiple upload and download requests result in
     correctly ordered transfers
   - with a slower connection than those on ec2, it also tests the timeout
     recovery code
   - ensure that the code behaves the same way for streams as with the
     S3Api methods
 */
BOOST_AUTO_TEST_CASE( test_s3_large_files )
{
    MLDB_TRACE_EXCEPTIONS(false);

    string contents = randomString(100 * 1024 * 1024);

    vector<string> cleanupUrls;
    auto cleanup = [&] () {
        for (const string & fullName: cleanupUrls) {
            tryEraseUriObject(fullName);
        }
    };
    ML::Call_Guard guard(cleanup);

    string basename = "/s3-large-files-" + randomString(16);
    string fileUrl = "s3://" + bucket + basename;
    cleanupUrls.push_back(fileUrl);

    auto s3Api = getS3ApiForUri(fileUrl);

    /* upload with filter_ostream (StreamingUploadSource, S3Uploader) */
    {
        FileCommiter commiter(fileUrl);
        filter_ostream stream(fileUrl);
        stream << contents;
        stream.close();
        commiter.commit();

        auto info = tryGetUriObjectInfo(fileUrl);
        BOOST_CHECK_EQUAL(info.size, contents.size());
    }

    /* download with S3Api::get */
    {
        auto info = tryGetUriObjectInfo(fileUrl);
        BOOST_CHECK_EQUAL(info.size, contents.size());
        auto resp = s3Api->get(bucket, basename, info.size);
        ExcAssertEqual(resp.body_, contents);
    }

    /* download with filter_istream (S3DownloadSource, S3Downloader) */
    {
        char buffer[65536];

        filter_istream stream(fileUrl);
        size_t offset(0);
        const char * src = contents.c_str();
        while (!stream.eof()) {
            stream.read(buffer, sizeof(buffer));
            int res = ::memcmp(buffer, src + offset, stream.gcount());
            ExcAssertEqual(res, 0);
            offset += stream.gcount();
        }
        stream.close();
    }
}
#endif


/* ensures that operations on s3 resources with space are performed properly */
BOOST_AUTO_TEST_CASE( test_s3_objects_with_spaces )
{
    string object("s3_test/filename with spaces");
    string escapedObject("s3_test/filename%20with%20spaces");
    string resource("/" + object);
    string escapedResource("/" + escapedObject);
    string unescapedUrl("s3://" + bucket + resource);
    string escapedUrl("s3://" + bucket + escapedResource);

    auto s3Api = getS3ApiForUri(escapedUrl);

    auto cleanupFn = [&] () {
        cerr << "erasing test file... ";
        s3Api->erase(bucket, resource);
        cerr << " done\n";
    };

    size_t bufferSize(1024*10);
    string randomData = randomString(bufferSize);

    s3Api->erase(bucket, resource);
    s3Api->erase(bucket, escapedResource);

    /* streaming upload */
    ML::Call_Guard cleanup(cleanupFn);

    cerr << "streaming upload with unescaped url\n";
    filter_ostream stream(unescapedUrl);
    stream << randomData;
    stream.close();

    /* test getObjectInfo, which indirectly tests HEAD */
    auto info = s3Api->getObjectInfo(bucket, object);
    BOOST_CHECK_EQUAL(info.size, bufferSize);

    /* corresponding download */
    filter_istream inputStream(unescapedUrl);
    string body = inputStream.readAll();
    BOOST_CHECK_EQUAL(body, randomData);

    /* test get */
    filter_istream download(unescapedUrl);
    string contents = download.readAll();
    BOOST_CHECK_EQUAL(contents, randomData);
}


BOOST_AUTO_TEST_CASE( test_s3_foreach_bucket )
{
    set<string> buckets;
    auto onBucket = [&] (const string & name) {
        if (buckets.count(name)) {
            throw MLDB::Exception("bucket already reported");
        }
        buckets.insert(name);
        return true;
    };
    auto s3Api = getS3ApiForUri("s3://" + bucket);
    bool res = s3Api->forEachBucket(onBucket);

    BOOST_CHECK_EQUAL(buckets.count(bucket), 1);
    BOOST_CHECK_EQUAL(res, true);
}


BOOST_AUTO_TEST_CASE( test_s3_foreach_object )
{
    string prefix = "s3-test-foreach-" + randomString(6);
    string url = "s3://" + bucket + "/" + prefix;
    vector<string> cleanupUrls;

    /* expected values: url prefix + '|' basename + '|' + depth */
    vector<string> objectUrls{url + "/|filename1|2",
                              url + "/subdir1/|filename1.txt|3",
                              url + "/subdir1/|filename2.txt|3",
                              url + "/subdir1/subdir2/|filename1.txt|4",
                              url + "/subdir3/|filename1.txt|3"};
    vector<string> dirUrls{url + "||1", url + "/|subdir1|2", url + "/subdir1/|subdir2|3",
                           url + "/|subdir3|2"};

    {
        vector<string> filenames{"filename1",
                                 "subdir1/filename1.txt",
                                 "subdir1/filename2.txt",
                                 "subdir1/subdir2/filename1.txt",
                                 "subdir3/filename1.txt"};
        for (const string & filename: filenames) {
            string fullName = url + "/" + filename;
            cleanupUrls.emplace_back(fullName);
            cerr << "creating object: "  + fullName + "\n";
            filter_ostream output(fullName);
            output << "dummy content";
        }
    }

    auto cleanup = [&] () {
        for (const string & fullName: cleanupUrls) {
            cerr << "erasing: " + fullName + "\n";
            tryEraseUriObject(fullName);
        }
    };
    ML::Call_Guard guard(cleanup);


    auto s3Api = getS3ApiForUri(url);


    /* version with bucket and prefix */
    {
        int urlCounter(0), dirUrlCounter(0);

        auto onObject = [&] (const string & cbPrefix,
                             const string & cbObjectName,
                             const S3Api::ObjectInfo & info,
                             int depth) {
            string url("s3://" + bucket
                       + "/" + cbPrefix
                       + "|" + cbObjectName
                       + "|" + to_string(depth));
            BOOST_CHECK_EQUAL(url, objectUrls[urlCounter]);
            urlCounter++;
            return true;
        };
        auto onSubdir = [&] (const std::string & cbPrefix,
                             const std::string & cbDirName,
                             int depth) {
            string url("s3://" + bucket
                       + "/" + cbPrefix
                       + "|" + cbDirName
                       + "|" + to_string(depth));
            // cerr << "dir url: " + url + "\n";
            BOOST_CHECK_EQUAL(url, dirUrls[dirUrlCounter]);
            dirUrlCounter++;
            // cerr << "  depth: " + to_string(depth) + "\n";
            return true;
        };

        s3Api->forEachObject(bucket, prefix, onObject, onSubdir);
    }

    /* version with prefix url */
    {
        int urlCounter(0), dirUrlCounter(0);

        auto onObject = [&] (const string & cbUrl,
                             const S3Api::ObjectInfo & info,
                             int depth) {
            BOOST_CHECK_EQUAL(cbUrl, cleanupUrls[urlCounter]);
            urlCounter++;
            return true;
        };
        auto onSubdir = [&] (const std::string & cbPrefix,
                             const std::string & cbDirName,
                             int depth) {
            string url("s3://" + bucket
                       + "/" + cbPrefix
                       + "|" + cbDirName
                       + "|" + to_string(depth));
            BOOST_CHECK_EQUAL(url, dirUrls[dirUrlCounter]);
            dirUrlCounter++;
            // cerr << "onSubdir: cbPrefix: " + cbPrefix;
            // cerr << "  cbObjectName: " + cbDirName;
            // cerr << "  depth: " + to_string(depth) + "\n";
            return true;
        };

        s3Api->forEachObject(url, onObject, onSubdir);
    }
}


/* Test the different versions of getObjectInfo, with and without default
 * parameters. */
BOOST_AUTO_TEST_CASE( test_s3_getObjectInfo )
{
    MLDB_TRACE_EXCEPTIONS(false);

    string filename = "s3-test-getObjectInfo-" + randomString(6);
    string url = "s3://" + bucket + "/" + filename;

    auto cleanup = [&] () {
        tryEraseUriObject(url);
    };
    ML::Call_Guard guard(cleanup);


    auto s3Api = getS3ApiForUri(url);

    {
        filter_ostream stream(url);
        stream << "Some random contents";
    }

    auto checkInfo = [&] (const S3Api::ObjectInfo & info,
                          bool isShort) {
        BOOST_CHECK_EQUAL(info.exists, true);
        BOOST_CHECK_NE(info.lastModified.secondsSinceEpoch(), 0);
        BOOST_CHECK_EQUAL(info.size, 20);
        BOOST_CHECK_NE(info.etag, "");
        if (isShort) {


            BOOST_CHECK_EQUAL(info.storageClass, "");
            BOOST_CHECK_EQUAL(info.ownerId, "");
            BOOST_CHECK_EQUAL(info.ownerName, "");
        }
        else {
            BOOST_CHECK_NE(info.storageClass, "");
            BOOST_CHECK_NE(info.ownerId, "");
            BOOST_CHECK_NE(info.ownerName, "");
        }
    };

    /* version with bucket and prefix */
    {
        /* implicit short */
        auto info = s3Api->getObjectInfo(bucket, filename);
        checkInfo(info, true);

        /* explicit short */
        info = s3Api->getObjectInfo(bucket, filename,
                                    S3ObjectInfoTypes::SHORT_INFO);
        checkInfo(info, true);

        /* explicit long */
        info = s3Api->getObjectInfo(bucket, filename,
                                    S3ObjectInfoTypes::FULL_INFO);
        checkInfo(info, false);

        /* unexisting object */
        BOOST_CHECK_THROW(s3Api->getObjectInfo(bucket, filename + "-missing"),
                          MLDB::Exception);
        BOOST_CHECK_THROW(s3Api->getObjectInfo(bucket, filename + "-missing",
                                               S3ObjectInfoTypes::FULL_INFO),
                          MLDB::Exception);

        /* "try" versions on unexisting */
        info = s3Api->tryGetObjectInfo(bucket, filename + "-missing");
        BOOST_CHECK_EQUAL(info.exists, false);
        BOOST_CHECK_EQUAL((bool) info, false);
        info = s3Api->tryGetObjectInfo(bucket, filename + "-missing",
                                       S3ObjectInfoTypes::SHORT_INFO);
        BOOST_CHECK_EQUAL(info.exists, false);
        BOOST_CHECK_EQUAL((bool) info, false);
        info = s3Api->tryGetObjectInfo(bucket, filename + "-missing",
                                       S3ObjectInfoTypes::FULL_INFO);
        BOOST_CHECK_EQUAL(info.exists, false);
        BOOST_CHECK_EQUAL((bool) info, false);
    }

    /* version with url */
    {
        /* implicit short */
        auto info = s3Api->getObjectInfo(url);
        checkInfo(info, true);

        /* explicit short */
        info = s3Api->getObjectInfo(url, S3ObjectInfoTypes::SHORT_INFO);
        checkInfo(info, true);

        /* explicit long */
        info = s3Api->getObjectInfo(url, S3ObjectInfoTypes::FULL_INFO);
        checkInfo(info, false);

        /* unexisting object */
        BOOST_CHECK_THROW(s3Api->getObjectInfo(url + "-missing"),
                          MLDB::Exception);
        BOOST_CHECK_THROW(s3Api->getObjectInfo(url + "-missing",
                                               S3ObjectInfoTypes::FULL_INFO),
                          MLDB::Exception);

        /* "try" versions on unexisting */
        info = s3Api->tryGetObjectInfo(url + "-missing");
        BOOST_CHECK_EQUAL(info.exists, false);
        BOOST_CHECK_EQUAL((bool) info, false);
        info = s3Api->tryGetObjectInfo(url + "-missing",
                                       S3ObjectInfoTypes::SHORT_INFO);
        BOOST_CHECK_EQUAL(info.exists, false);
        BOOST_CHECK_EQUAL((bool) info, false);
        info = s3Api->tryGetObjectInfo(url + "-missing",
                                       S3ObjectInfoTypes::FULL_INFO);
        BOOST_CHECK_EQUAL(info.exists, false);
        BOOST_CHECK_EQUAL((bool) info, false);
    }
}

BOOST_AUTO_TEST_CASE( test_s3_url_credentials )
{
    MLDB_TRACE_EXCEPTIONS(false);

    string filename = "s3-test-getObjectInfo-" + randomString(6);
    string anonymousUrl = "s3://" + bucket + "/" + filename;

    auto cleanup = [&] () {
        tryEraseUriObject(anonymousUrl);
    };
    ML::Call_Guard guard(cleanup);

    string testContents("Some random contents");

    {
        filter_ostream stream(anonymousUrl);
        stream << "Some random contents";
        stream.close();
    }

    auto readUrl = [&] (const string & url) {
        string contents;

        filter_istream stream(url);
        while (stream) {
            char buffer[16384];
            stream.read(buffer, 16384);
            contents.append(buffer, stream.gcount());
        }
        stream.close();

        return contents;
    };

    /* test without creds */
    {
        string contents = readUrl(anonymousUrl);
        BOOST_CHECK_EQUAL(contents, testContents);
    }
}
