// This file is part of MLDB. Copyright 2015 mldb.ai inc. All rights reserved.

/*
  root_behavior_manager_test.cc
*/ 

#define BOOST_TEST_MAIN
#define BOOST_TEST_DYN_LINK

#include <stdio.h>
#include <unistd.h>
#include <sys/mount.h>
#include <sys/stat.h>
#include <sys/types.h>
#include <sys/vfs.h>

#include <boost/test/unit_test.hpp>

#include "mldb/arch/timers.h"
#include "mldb/jml/utils/guard.h"
#include "mldb/vfs/fs_utils.h"
#include "mldb/vfs/filter_streams.h"
#include "mldb/utils/runner.h"
#include "mldb/utils/testing/print_utils.h"
#include "mldb/plugins/behavior/behavior_manager.h"

using namespace std;
using namespace MLDB;

namespace {

uint64_t freeDiskSpace(const string & directory)
{
    struct statfs fsstats;
    int res = ::statfs(directory.c_str(), &fsstats);
    if (res == -1) {
        throw MLDB::Exception(errno, "statfs");
    }

    return fsstats.f_bavail * fsstats.f_bsize;
}

/****************************************************************************/
/* TEMPORARY MOUNT                                                          */
/****************************************************************************/

/* TemporaryMount is a class that performs the creation and mounting of
 * temporary filesystem volumes. The programs instantiating it must be run as
 * root. */

struct TemporaryMount
{
    TemporaryMount(size_t volSize);
    ~TemporaryMount() noexcept;

    const std::string & mountPoint() const
    {
        return mountPoint_;
    }

private:
    void createVolume();
    void mountVolume();

    void attachVolume();

    void cleanup() noexcept;
    void detachVolume() noexcept;
    void unmountVolume() noexcept;

    size_t volSize_;
    std::string volume_;
    bool volumeExists_;

    std::string loopDevice_;
    bool attached_;

    std::string mountPoint_;
    bool mountPointExists_;
    bool mounted_;
};

const string FSTYPE("ext2");
const string MKFS("/sbin/mkfs." + FSTYPE);
const string LOSETUP("/sbin/losetup");

TemporaryMount::
TemporaryMount(size_t volSize)
    : volSize_(volSize),
      volumeExists_(false), attached_(false),
      mountPointExists_(false), mounted_(false)
{
    // if (getuid() != 0) {
    //     throw MLDB::Exception("You are not root");
    // }

    bool ok(false);

    ML::Call_Guard guard([&] {
        if (!ok) {
            cleanup();
        }
    });

    /* we create a sparse file of size "volSize" */
    createVolume();

    /* initialise the filesystem with blocks of 1024 KBytes */
    auto runRes = execute({MKFS,
                           "-b", "1024",
                           "-i", "1024",
                           "-m", "0",
                           volume_});
    if (runRes.processStatus() != 0) {
        throw MLDB::Exception("failure initialising filesystem");
    }

    /* attach the volume file to the loop device */
    attachVolume();

    /* create the mount directory */
    mountPoint_ = "/tmp/" + randomString(16);
    int res = ::mkdir(mountPoint_.c_str(), 0700);
    if (res == -1) {
        throw MLDB::Exception(errno, "mkdir");
    }
    mountPointExists_ = true;

    /* mount the filesystem */
    mountVolume();

    ok = true;
}

void
TemporaryMount::
cleanup()
    noexcept
{
    if (mounted_) {
        unmountVolume();
    }
    if (mountPointExists_) {
        int res = ::rmdir(mountPoint_.c_str());
        if (res == -1) {
            perror("TemporaryMount::cleanup rmdir");
        }
        mountPointExists_ = false;
    }
    if (attached_) {
        detachVolume();
    }
    if (volumeExists_) {
        int res = ::unlink(volume_.c_str());
        if (res == -1) {
            perror("TemporaryMount::cleanup unlink");
        }
        volumeExists_ = false;
    }
}

void
TemporaryMount::
createVolume()
{
    volume_ = "/tmp/" + randomString(16);

    FILE * f = ::fopen(volume_.c_str(), "w+");
    if (!f) {
        throw MLDB::Exception(errno, "fopen");
    }
    volumeExists_ = true;
    ML::Call_Guard guard([&] {
        ::fclose(f);
    });
    int res = ::fseek(f, volSize_ - 1, SEEK_SET);
    if (res == -1) {
        throw MLDB::Exception(errno, "fseek");
    }
    res = ::fwrite("a", 1, 1, f);
    if (res == -1) {
        throw MLDB::Exception(errno, "fwrite");
    }
}

void
TemporaryMount::
mountVolume()
{
    ExcAssert(!mounted_);
    ExcAssert(attached_);
    ExcAssert(!loopDevice_.empty());
    ExcAssert(!mountPoint_.empty());
    int res = ::mount(loopDevice_.c_str(), mountPoint_.c_str(),
                      FSTYPE.c_str(), 0, 0);
    if (res == -1) {
        throw MLDB::Exception(errno, "mount");
    }
    mounted_ = true;
}

void
TemporaryMount::
unmountVolume()
    noexcept
{
    try {
        ExcAssert(mounted_);
        ExcAssert(!volume_.empty());
        ExcAssert(!mountPoint_.empty());
    } catch (...) {
        std::terminate();
    }
    int res = umount(mountPoint_.c_str());
    if (res == -1) {
        perror("TemporaryMount::unmountVolume");
    }
    mounted_ = false;
}

void
TemporaryMount::
attachVolume()
{
    auto onStdout = [&] (std::string && data) {
        loopDevice_ += data;
    };
    auto sink = make_shared<CallbackInputSink>(onStdout);
    auto runRes = execute({LOSETUP, "--find", "--show", volume_},
                          sink, nullptr);
    if (runRes.processStatus() != 0) {
        throw MLDB::Exception("failure attaching loop device");
    }
    attached_ = true;
    ExcAssert(!loopDevice_.empty());
    loopDevice_.resize(loopDevice_.size() - 1);
    cerr << "loop device: " + loopDevice_ + "\n";
}

void
TemporaryMount::
detachVolume()
    noexcept
{
    try {
        ExcAssert(attached_);
        ExcAssert(!loopDevice_.empty());
    } catch (...) {
        std::terminate();
    }
    auto runRes = execute({LOSETUP, "--detach", loopDevice_});
    if (runRes.processStatus() != 0) {
        perror("detaching loop device");
    }
    attached_ = false;
    loopDevice_.clear();
}

TemporaryMount::
~TemporaryMount()
    noexcept
{
    cleanup();
}

void populateCache(const string & directory, uint64_t space)
{
    uint64_t used(0);
    size_t nFiles(0);

    /* compute the new atime */
    struct timeval newATime;
    int res = ::gettimeofday(&newATime, nullptr);
    if (res == -1) {
        throw MLDB::Exception("gettimeofday");
    }

    while (true) {
        uint64_t fileSize = nFiles * 6543;
        if (used + fileSize > space) {
            break;
        }
        string filename = directory + "/file" + to_string(nFiles);
        filter_ostream stream(filename);
        stream << randomString(fileSize);
        stream.close();

        /* apply the new atime */
        newATime.tv_sec -= 60;
        struct timeval times[] = {newATime, newATime};
        res = ::utimes(filename.c_str(), times);
        if (res == -1) {
            throw MLDB::Exception("utimes");
        }

        used += fileSize;
        nFiles++;
    }
    cerr << ("populated cache with " + to_string(nFiles)
             + " files of " + to_string(used) + " bytes in total"
             + " on " + to_string(space)
             + "\n");
}

}

BOOST_AUTO_TEST_CASE( test_beh_manager_parse_specs )
{
    /* setup and mount a temporary volume */
    size_t volumeSize(20*1024*1024);
    TemporaryMount volume(volumeSize);
    const string & mountPoint = volume.mountPoint();

    BehaviorManager behMgr;
    behMgr.setRemoteCacheDir(mountPoint);

    uint64_t initialSpace = freeDiskSpace(mountPoint);
    populateCache(mountPoint, initialSpace - 10000);

    uint64_t freeSpace(0), newFreeSpace(0);

    /* populating the cache created a file named "file73" that is going to be
     * deleted later */
    auto info = tryGetUriObjectInfo(mountPoint + "/file73");
    BOOST_CHECK_EQUAL(bool(info), true);

    /* test with a file larger than volume size: cleanup refused */
    bool res = behMgr.prepareRemoteCache(volumeSize + 100);
    BOOST_CHECK_EQUAL(res, false);

    /* test with a file smaller then free size: no cleanup required */
    freeSpace = freeDiskSpace(mountPoint);
    res = behMgr.prepareRemoteCache(100);
    BOOST_CHECK_EQUAL(res, true);
    newFreeSpace = freeDiskSpace(mountPoint);
    BOOST_CHECK_EQUAL(newFreeSpace, freeSpace);

    /* test with a file smaller than the sum of the sizes of the cleanable
       files: the cleanup must have succeeded and file73 must have been
       deleted */
    res = behMgr.prepareRemoteCache(freeSpace + 10);
    BOOST_CHECK_EQUAL(res, true);
    newFreeSpace = freeDiskSpace(mountPoint);
    BOOST_CHECK_GT(newFreeSpace, freeSpace);
    freeSpace = newFreeSpace;
    info = tryGetUriObjectInfo(mountPoint + "/file73");
    BOOST_CHECK_EQUAL(bool(info), false);

    /* Our algorithm for dealing with race conditions requires us to be able
     * to remove all the files that takes up to the required disk space,
     * notwithstanding the initial free space. Thus, requesting a file smaller
     * than the disk space but larger than the used space must fail and all
     * the files present on disk must stay untouched. */
    res = behMgr.prepareRemoteCache(initialSpace - 10000);
    BOOST_CHECK_EQUAL(res, false);
    newFreeSpace = freeDiskSpace(mountPoint);
    BOOST_CHECK_EQUAL(newFreeSpace, freeSpace);
}
