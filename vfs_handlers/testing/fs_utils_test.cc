// This file is part of MLDB. Copyright 2015 mldb.ai inc. All rights reserved.

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


using namespace std;
using namespace MLDB;

/* S3 backend credentials not provided, ~/.cloud_credentials expected, ask your friendly @ops */

/* local fs backend */
/* ensures tryGetUriObjectInfo and derivatives works for local files */
BOOST_AUTO_TEST_CASE( test_local_tryGetUriObjectInfo )
{
    /* non-existing files */
    {
        FsObjectInfo infos = tryGetUriObjectInfo("file:///does/not/exist");
        BOOST_CHECK_EQUAL(infos.exists, false);
        BOOST_CHECK_EQUAL(infos.lastModified, Date());
        BOOST_CHECK_EQUAL(infos.size, -1);
        BOOST_CHECK_EQUAL(infos.etag, "");
        BOOST_CHECK_EQUAL(infos.storageClass, "");
    }
    {
        FsObjectInfo infos = tryGetUriObjectInfo("/does/not/exist");
        BOOST_CHECK_EQUAL(infos.exists, false);
        BOOST_CHECK_EQUAL(infos.lastModified, Date());
        BOOST_CHECK_EQUAL(infos.size, -1);
        BOOST_CHECK_EQUAL(infos.etag, "");
        BOOST_CHECK_EQUAL(infos.storageClass, "");
    }

    /* existing files */
    TestFolderFixture fixtures("fs_utils_test");
    char cCurDir[1024];
    string testDir(getcwd(cCurDir, sizeof(cCurDir)));

    int filecount(0);
    /*  absolute filenames */
    {
        string filename = (testDir + "/" + fixtures.uniqueName()
                           + to_string(filecount));
        filecount++;

        {
            filter_ostream stream(filename);
            stream << "this file exists";
        }

        /* with file:// scheme */
        {
            FsObjectInfo infos = tryGetUriObjectInfo("file://" + filename);
            BOOST_CHECK_EQUAL(infos.exists, true);
            BOOST_CHECK_NE(infos.lastModified, Date());
            BOOST_CHECK_EQUAL(infos.size, 16);
            BOOST_CHECK_EQUAL(infos.etag, "");
            BOOST_CHECK_EQUAL(infos.storageClass, "");

            size_t fileSize = getUriSize("file://" + filename);
            BOOST_CHECK_EQUAL(fileSize, 16);

            string etag = getUriEtag("file://" + filename);
            BOOST_CHECK_EQUAL(etag, "");
        }

        /* without scheme, defaults to file:// */
        {
            FsObjectInfo infos = tryGetUriObjectInfo(filename);
            BOOST_CHECK_EQUAL(infos.exists, true);
            BOOST_CHECK_NE(infos.lastModified, Date());
            BOOST_CHECK_EQUAL(infos.size, 16);
            BOOST_CHECK_EQUAL(infos.etag, "");
            BOOST_CHECK_EQUAL(infos.storageClass, "");

            size_t fileSize = getUriSize(filename);
            BOOST_CHECK_EQUAL(fileSize, 16);

            string etag = getUriEtag(filename);
            BOOST_CHECK_EQUAL(etag, "");
        }
    }

    /* relative filenames, without schemes (!) */
    {
        string filename = fixtures.uniqueName() + to_string(filecount);
        filecount++;

        {
            filter_ostream stream(filename);
            stream << "this file exists";
        }
        {
            FsObjectInfo infos = tryGetUriObjectInfo(filename);
            BOOST_CHECK_EQUAL(infos.exists, true);
            BOOST_CHECK_NE(infos.lastModified, Date());
            BOOST_CHECK_EQUAL(infos.size, 16);
            BOOST_CHECK_EQUAL(infos.etag, "");
            BOOST_CHECK_EQUAL(infos.storageClass, "");

            size_t fileSize = getUriSize(filename);
            BOOST_CHECK_EQUAL(fileSize, 16);

            string etag = getUriEtag(filename);
            BOOST_CHECK_EQUAL(etag, "");
        }
    }
}

BOOST_AUTO_TEST_CASE( test_local_makeUriDirectory )
{
    TestFolderFixture fixtures("fs_utils_test");
    char cCurDir[1024];
    string testDir(getcwd(cCurDir, sizeof(cCurDir)));
    testDir += "/" + fixtures.uniqueName() + "/subdir";

    string filename = testDir + "/" + "some-file";

    struct stat dirStats;
    int res = ::stat(testDir.c_str(), &dirStats);
    BOOST_CHECK_EQUAL(res, -1);
    BOOST_CHECK_EQUAL(errno, ENOENT);

    makeUriDirectory(filename);

    /* file must not exist, as we want to create its directory */
    res = ::stat(filename.c_str(), &dirStats);
    BOOST_CHECK_EQUAL(res, -1);

    res = ::stat(testDir.c_str(), &dirStats);
    BOOST_CHECK_EQUAL(res, 0);
    BOOST_CHECK_EQUAL((dirStats.st_mode & S_IFMT), S_IFDIR);
}

BOOST_AUTO_TEST_CASE( test_local_tryEraseUriObject )
{
    TestFolderFixture fixtures("fs_utils_test");
    char cCurDir[1024];
    string testFile(getcwd(cCurDir, sizeof(cCurDir)));
    testFile += "/" + fixtures.uniqueName();

    /* file does not exist yet */
    bool res = tryEraseUriObject("file://" + testFile);
    BOOST_CHECK_EQUAL(res, false); // does not exist
    res = tryEraseUriObject(testFile);
    BOOST_CHECK_EQUAL(res, false); // still does not exist

    /* file now exists */
    filter_ostream stream(testFile);
    stream << "file exists";
    stream.close();

    struct stat fileStats;
    int result = ::stat(testFile.c_str(), &fileStats);
    BOOST_CHECK_EQUAL(result, 0);
    result = tryEraseUriObject("file://" + testFile);
    BOOST_CHECK_EQUAL(result, true); // file did exist
    result = ::stat(testFile.c_str(), &fileStats);
    BOOST_CHECK_EQUAL(result, -1);
    BOOST_CHECK_EQUAL(errno, ENOENT);
}

/* S3 backend */
/* ensures tryGetUriObjectInfo and derivatives works for s3 files */
BOOST_AUTO_TEST_CASE( test_s3_tryGetUriObjectInfo )
{
    /* non-existing file */
    {
        FsObjectInfo infos = tryGetUriObjectInfo("s3://tests.datacratic.com/does/not/exist");
        BOOST_CHECK_EQUAL(infos.exists, false);
        BOOST_CHECK_EQUAL(infos.lastModified, Date());
        BOOST_CHECK_EQUAL(infos.size, -1);
        BOOST_CHECK_EQUAL(infos.etag, "");
        BOOST_CHECK_EQUAL(infos.storageClass, "");
    }

    /* existing file */
    {
        string fileUrl("s3://tests.datacratic.com/s3_test/fs_utils_test.txt");

        filter_ostream stream(fileUrl);
        stream << "this file exists on s3";
        stream.close();

        FsObjectInfo infos = tryGetUriObjectInfo(fileUrl);
        BOOST_CHECK_EQUAL(infos.exists, true);
        BOOST_CHECK_NE(infos.lastModified, Date());
        BOOST_CHECK_EQUAL(infos.size, 22);
        string fileEtag = infos.etag;
        BOOST_CHECK_NE(fileEtag, "");
        BOOST_CHECK_EQUAL(infos.storageClass, "STANDARD");

        size_t fileSize = getUriSize(fileUrl);
        BOOST_CHECK_EQUAL(fileSize, 22);

        string etag = getUriEtag(fileUrl);
        BOOST_CHECK_EQUAL(etag, fileEtag);
    }
}

BOOST_AUTO_TEST_CASE( test_s3_makeUriDirectory )
{
    string dirname = "s3://tests.datacratic.com/s3-prefix/directory";

    makeUriDirectory(dirname);

    /* no object with that name exists, since makeUriDirectory is a
     * no op */
    {
        FsObjectInfo infos = tryGetUriObjectInfo(dirname);
        BOOST_CHECK_EQUAL(infos.exists, false);
        BOOST_CHECK_EQUAL(infos.lastModified, Date());
        BOOST_CHECK_EQUAL(infos.size, -1);
        BOOST_CHECK_EQUAL(infos.etag, "");
        BOOST_CHECK_EQUAL(infos.storageClass, "");
    }
}

BOOST_AUTO_TEST_CASE( test_s3_tryEraseUriObject )
{
    string filename = "s3://tests.datacratic.com/this/file/will/be/erased";

    /* we create the file */
    filter_ostream ostream(filename);
    ostream << "exists";
    ostream.close();

    /* we ensure it exists */
    FsObjectInfo infos = tryGetUriObjectInfo(filename);
    BOOST_CHECK_EQUAL(infos.exists, true);

    /* we delete it */
    tryEraseUriObject(filename);

    /* we ensure it no longer exists */
    infos = tryGetUriObjectInfo(filename);
    BOOST_CHECK_EQUAL(infos.exists, false);
}
