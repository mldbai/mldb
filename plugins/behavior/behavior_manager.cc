// This file is part of MLDB. Copyright 2015 mldb.ai inc. All rights reserved.

/* behavior_manager.cc
   Jeremy Barnes, 7 August 2012
   Copyright (c) 2012 mldb.ai inc.  All rights reserved.
   
*/

#define __STDC_FORMAT_MACROS 1
#include "inttypes.h"
#include <stdio.h>
#include <math.h>
#include <unistd.h>
#include <utime.h>
#include <sys/mman.h>
#include <sys/stat.h>
#include <sys/syscall.h>
#include <sys/time.h>
#include <sys/types.h>
#include <sys/vfs.h>
#include <ftw.h>

#include <boost/algorithm/string/split.hpp>
#include <boost/filesystem.hpp>

#include "mldb/arch/fslock.h"
#include "mldb/arch/timers.h"
#include "mldb/arch/vm.h"
#include "mldb/jml/utils/file_functions.h"
#include "mldb/jml/utils/string_functions.h"
#include "mldb/vfs/filter_streams.h"
#include "mldb/jml/utils/guard.h"
#include "mldb/base/parallel.h"
#include "mldb/types/date.h"
#include "mldb/vfs/fs_utils.h"
#include "mldb/base/scope.h"

#include "mapped_behavior_domain.h"
#include "merged_behavior_domain.h"
#include "mutable_behavior_domain.h"
#include "behavior_svd.h"

#include "behavior_manager.h"

using namespace std;
namespace fs = boost::filesystem;
using namespace ML;
using namespace MLDB;


namespace {

string
getErrorString(int errNum)
{
    char buffer[128]; /* error messages are < 80 chars most of the time */

    char * msg = strerror_r(errNum, buffer, sizeof(buffer));
    if (msg == nullptr) {
        throw MLDB::Exception(errno, "strerror_r");
    }

    return string(msg);
}

/* This function returns a cache filename for the corresponding remote entry. It
 * takes its etag, strips the double-quotes from it and appends the ".beh"
 * suffix. */
string
getRemoteCacheName(const FsObjectInfo & info)
{
    string etag = info.etag;

    // Remove the quotes which makes scripting harder.
    if (!etag.empty() && etag[0] == '"' && etag[etag.size()-1] == '"')
        etag = etag.substr(1, etag.size() - 2);

    return etag + ".beh";
}

/* This function updates the "last access time" field of the given file
 * metadata. It is used to ensure that this field is properly updated even
 * when the "noatime" flag is set as parameter for mounting the underlying
 * file system. */
void
touchATime(const string & filename)
{
    /* compute the new atime */
    struct timeval newATime;
    int res = ::gettimeofday(&newATime, nullptr);
    if (res == -1) {
        throw MLDB::Exception("gettimeofday");
    }

    /* fetch the mtime */
    struct timeval oldMTime;
    struct stat filestat;
    res = ::stat(filename.c_str(), &filestat);
    if (res == -1) {
        throw MLDB::Exception("stat");
    }
    oldMTime.tv_sec = filestat.st_mtim.tv_sec;
    oldMTime.tv_usec = filestat.st_mtim.tv_nsec / 1000;

    /* apply the new values */
    struct timeval times[] = {newATime, oldMTime};
    res = ::utimes(filename.c_str(), times);
    if (res == -1) {
        throw MLDB::Exception("utimes");
    }
}

} // file scope


namespace MLDB {

/*****************************************************************************/
/* BEHAVIOR MANAGER                                                         */
/*****************************************************************************/

BehaviorManager::
BehaviorManager()
    : cacheLoadedFiles(false), touchPages(true)
{
}

BehaviorManager::
~BehaviorManager()
{
}

std::shared_ptr<BehaviorDomain>
BehaviorManager::
get(const std::vector<std::string> & inputFiles,
    bool preIndex,
    CacheAction cacheAction,
    bool skipBadFiles,
    std::function<bool (Json::Value)> onProgress)
{
    std::shared_ptr<BehaviorDomain> behs;
    if (inputFiles.empty())
        return std::make_shared<MutableBehaviorDomain>();
    else if (inputFiles.size() == 1) {
        behs = get(inputFiles[0], cacheAction, onProgress);
    }
    else {
        vector<std::shared_ptr<BehaviorDomain> > toMerge(inputFiles.size());

        auto loadFile = [&] (int i)
            {
                string filename = inputFiles[i];
                try {
                    toMerge[i] = get(filename, cacheAction);
                } catch (const std::exception & exc) {
                    if (!skipBadFiles)
                        throw;
                    else cerr << "beh mgr: skipping bad file " << filename << endl;
                }
            };

        parallelMap(0, inputFiles.size(), loadFile);

        int numBadFiles = 0, numGoodFiles = 0;
        
        vector<std::shared_ptr<BehaviorDomain> > filtered;
        for (auto f: toMerge) {
            if (f) {
                filtered.push_back(f);
                ++numGoodFiles;
            }
            else {
                ++numBadFiles;
            }
        }

        if (numBadFiles > 0)
            cerr << ("beh mgr: skipping " + to_string(numBadFiles)
                     + " of " + to_string(toMerge.size()) + " bad files\n");

        behs = std::make_shared<MergedBehaviorDomain>(filtered, preIndex);
    }

    return behs;
}

std::shared_ptr<BehaviorDomain>
BehaviorManager::
get(const std::string & filename, CacheAction cacheAction,
    std::function<bool (Json::Value)> onProgress)
{
    {
        std::unique_lock<std::mutex> guard(lock);
        if (cache.count(filename))
            return cache[filename];
    }

    std::shared_ptr<BehaviorDomain> result;

    try {
        string scheme = getUriScheme(filename);

        if (scheme == "file") {
            result = getFile(getUriPath(filename), onProgress);
        }
        else {
            result = getRemote(filename, onProgress);
        }
    } catch (const std::exception & exc) {
        throw MLDB::Exception("exception loading beh file " + filename + ": " + exc.what());
    } catch (...) {
        throw MLDB::Exception("exception loading beh file " + filename);
    }

    if (cacheAction == CACHE_ALWAYS
        || (cacheAction == CACHE_CONFIG && cacheLoadedFiles)) {
        std::unique_lock<std::mutex> guard(lock);
        if (cache.count(filename))
            return cache[filename];
        cache[filename] = result;
    }

    return result;
}

void touchByte(const char * c)
{
    __asm__
        (" # [in]"
         : 
         : [in] "r" (*c)
         :
         );
}

/** Touch the given range of memory, to ensure that its contents will be
    available to the application without page faults.

    The progress function that can be passed in allows a calling process to
    know how it is progressing and cancel it (by returning false from the
    lambda).  A null function means don't report progress.
*/
bool touch(const char * start, size_t size,
           const std::function<bool (size_t) > & progress)
{
    int res;

    if (progress)
        if (!progress(0))
            return false;

    /* This madvise call is harmless and should be there.  It tells the kernel that
       we're about to use this memory sequentially and so it can enable read-ahead
       on the disk file underneath.  This may allow it to be loaded more efficiently
       from disk.
    */
    res = madvise((void *)start, size,
                  MADV_WILLNEED | MADV_SEQUENTIAL);
    if (res == -1 && errno != EBADF)
        throw MLDB::Exception(errno, "madvise");
    
    /* Block size at which we report progress.  */
    enum {
        BLOCKSIZE = 4096 * 4096
    };

    if (progress)
        if (!progress(1))
            return false;

    /* Iterate through our file a "block" at at time, and for each of those
       blocks touch each page exactly once.  This forces the entire set of
       page tables to be populated and the memory to be paged in from disk
       sequentially, which means that further access will be low latency.
    */
    for (size_t i = 0;  i < size;  i += BLOCKSIZE) {
        for (size_t j = i;  j < size && j < i + BLOCKSIZE;  j += page_size) {
            touchByte(start + j);
        }
        if (progress)
            if (!progress(i))
                return false;
    }

    if (progress)
        return progress(size);

    else return true;
}

bool touchWithProgress(const std::string & filename,
                       const ML::File_Read_Buffer & file,
                       const std::function<bool (Json::Value)> & onProgress)
{
    Date start = Date::now();

    auto onProgress2 = [&] (size_t bytesDone)
        {
            if (!onProgress)
                return true;

            double seconds = Date::now().secondsSince(start);
                    
            Json::Value val;
            val["process"] = "memory mapping file";
            val["file"] = filename;
            val["mappedMB"] = bytesDone / 1000000.0;
            val["totalBytes"] = file.size() / 1000000.0;
            val["percentDone"] = 100.0 * bytesDone / file.size();
            val["elapsedSeconds"] = seconds;
            val["rateMbPerSecond"] = bytesDone / 1000000.0 / seconds;
                    
            return onProgress(val);
        };
    
    return touch(file.start(), file.size(), onProgress2);
}

void
BehaviorManager::
preloadRemoteCache(const set<string> & filenames)
    const
{
    cerr << "WARNING: BehaviorManager::preloadRemoteCache is a no-op for now\n";
#if 0
    map<string, pair<string, string>> files;

    // Date start = Date::now();

    if (s3CacheDir.empty()) {
        throw MLDB::Exception("no s3 cache dir set");
    }

    for (const string & filename: filenames) {
        auto s3UrlParts = S3Api::parseUri(filename);
        const string & s3ObjectName = s3UrlParts.second;
        auto s3 = getS3ApiForUri(filename);
        auto objectInfo = s3->getObjectInfo(filename);

        if (canCacheS3File(filename, objectInfo.size)) {
            string fileCacheDir = s3CacheDir + "/" + s3ObjectName;
            fs::create_directories(fileCacheDir);

            string basename = getS3CacheName(objectInfo);
            files[filename] = make_pair(fileCacheDir, basename);
        }
    }

    while (!files.empty()) {
        bool doSleep(true);
        vector<string> toRemove;

        for (const auto & entry: files) {
            const string & fileCacheDir = entry.second.first;
            const string & basename = entry.second.second;
            string fullname = fileCacheDir + "/" + basename;

            ML::GuardedFsLock fsLock(fullname);
            if (fsLock.tryLock()) {
                if (fs::exists(fullname)) {
                    toRemove.push_back(entry.first);
                }
                else {
                    /* we need to download and save it */
                    cerr << ("beh mgr: [" + to_string(getpid()) + "]"
                             + " preloading '" + entry.first + "'\n");
                    cacheS3File(entry.first, objectInfo.size,
                                nullptr, fullname);
                }
                doSleep = false;
                fsLock.unlock();
            }
        }

        for (const string & filename: toRemove) {
            files.erase(filename);
        }

        if (doSleep) {
            ML::sleep(0.2);
        }
    }


    // Date end = Date::now();
    // double delta = end.secondsSinceEpoch() - start.secondsSinceEpoch();
    // ::fprintf(stderr, "%d: preloaded %ld files in %f secs\n", ::getpid(),
    // filenames.size(), delta);
#endif
}

/* load entire file contents in a std::string, using an filter_istream */
string
BehaviorManager::
readFile(const string & filename,
         std::function<bool (uint64_t)> onProgress)
    const
{
    string contents;
    char readBuf[1024*1024];

    uint64_t totalRead = 0;

    if (onProgress)
        if (!onProgress(totalRead))
            return "";

    filter_istream stream(filename);
    while (stream) {
        stream.read(readBuf, sizeof(readBuf));
        ssize_t read = stream.gcount();
        contents.append(readBuf, read);
        totalRead += read;
        if (onProgress)
            if (!onProgress(totalRead))
                return "";
    }

    if (onProgress)
        if (!onProgress(totalRead))
            return "";

    return contents;
}

bool
BehaviorManager::
prepareRemoteCache(uint64_t objectSize)
    const
{
    // Check whether there is enough empty space there...
    struct statfs fsstats;
    int res = ::statfs(remoteCacheDir.c_str(), &fsstats);
    if (res == -1) {
        cerr << ("beh mgr: statfs on remoteCacheDir: "
                 + getErrorString(errno) + "\n");
        return false;
    }

    uint64_t totalSize = fsstats.f_blocks * fsstats.f_bsize;
    if (totalSize < objectSize) {
        cerr << ("beh mgr: disk space in '" + remoteCacheDir
                 + "' too small to hold a " + to_string(objectSize)
                 + " bytes files\n");
        return false;
    }

    uint64_t freeSpace = fsstats.f_bavail * fsstats.f_bsize;

    /* An alternative method here is to deduce the freeSpace from the
       objectSize, so that only the required disk space would be recovered.
       However, in that case, a race condition between two processes could
       lead to the freed space being reused right away by a concurrent
       instance. It is thus better to free a bit more space than required. */
    if (freeSpace < objectSize && !recoverRemoteCacheDiskSpace(objectSize)) {
        cerr << ("beh mgr: not enough disk space could be recovered"
                 " to store a " + to_string(objectSize) + " bytes files\n");
        return false;
    }

    return true;
}

namespace {

enum FileAction {
    FA_CONTINUE = FTW_CONTINUE,
    FA_SKIP_SIBLINGS = FTW_SKIP_SIBLINGS,
    FA_SKIP_SUBTREE = FTW_SKIP_SUBTREE,
    FA_STOP = FTW_STOP
};

enum FileType {
    FT_FILE = FTW_F,
    FT_DIR  = FTW_D,
    FT_DIR_INACCESSIBLE = FTW_DNR,
    FT_FILE_INACCESSIBLE = FTW_NS
};

std::string print(FileType type);

std::ostream & operator << (std::ostream & stream, const FileType & type);

typedef std::function<FileAction (std::string dir,
                                    std::string basename,
                                    const struct stat & stats,
                                    FileType type,
                                    int depth)>
    OnFileFound;

std::string print(FileType type)
{
    switch (type) {
    case FT_FILE: return "FILE";
    case FT_DIR:  return "DIR";
    case FT_DIR_INACCESSIBLE: return "DIR_INACCESSIBLE";
    case FT_FILE_INACCESSIBLE: return "FILE_INACCESSIBLE";
    default:
        return MLDB::format("FileType(%d)", type);
    }
}

struct ScanFilesData;

static __thread ScanFilesData * scanFilesThreadData = 0;

struct ScanFilesData {
    ScanFilesData(const OnFileFound & onFileFound,
                  int maxDepth)
        : onFileFound(onFileFound),
          maxDepth(maxDepth),
          isThrown(false)
    {
    }
    
    OnFileFound onFileFound;
    int maxDepth;
    std::exception_ptr thrown;
    bool isThrown;

    static int onFile (const char *fpath, const struct stat *sb,
                       int typeflag, struct FTW *ftwbuf)
    {
        ScanFilesData * d = scanFilesThreadData;
        ExcAssert(d);
        try {
            if (d->maxDepth != -1 && ftwbuf->level > d->maxDepth)
                return FTW_SKIP_SIBLINGS;
            string dir(fpath, fpath + ftwbuf->base);
            string basename(fpath + ftwbuf->base);

            FileAction action = d->onFileFound(dir, basename, *sb,
                                               (FileType)typeflag,
                                               ftwbuf->level);
            return action;
        } MLDB_CATCH_ALL {
            d->thrown = std::current_exception();
            d->isThrown = true;
            return FTW_STOP;
        }
    }
}; 

static void scanFiles(const std::string & path,
                      OnFileFound onFileFound,
                      int maxDepth = -1)
{
    ScanFilesData data(onFileFound, maxDepth);

    auto * oldData = scanFilesThreadData;
    Scope_Exit(scanFilesThreadData = oldData);
    scanFilesThreadData = &data;

    int res = nftw(path.c_str(),
                   &ScanFilesData::onFile, 
                   maxDepth == -1 ? 100 : maxDepth + 1,
                   FTW_ACTIONRETVAL);

    if (data.isThrown) {
        auto exc = data.thrown;
        rethrow_exception(exc);
    }

    if (res == -1) {
        if (errno == ENOENT)
            return;
        throw MLDB::Exception(errno, "ftw");
    }
}

} // file scope

vector<BehaviorManager::RemoteCacheEntry>
BehaviorManager::
getRemoteCacheEntries()
    const
{
    ExcAssert(!remoteCacheDir.empty());
    vector<RemoteCacheEntry> cacheFiles;

    auto onFileFound = [&] (std::string dir,
                            std::string basename,
                            const struct stat & stats,
                            FileType type,
                            int depth) {
        if (S_ISREG(stats.st_mode)) {
            RemoteCacheEntry newFile{dir + basename,
                                Date::fromTimespec(stats.st_atim),
                                stats.st_size};
            cacheFiles.emplace_back(move(newFile));
        }
        return FA_CONTINUE;
    };

    scanFiles(remoteCacheDir, onFileFound, -1);

    return cacheFiles;
}

vector<BehaviorManager::RemoteCacheEntry>
BehaviorManager::
getRemoteRecoverableEntries(uint64_t objectSize)
    const
{
    vector<RemoteCacheEntry> recoverableEntries;

    /* retrieve and sort the entries by access time in descending order */
    vector<RemoteCacheEntry> entries = getRemoteCacheEntries();
    auto sortByDate = [&] (const RemoteCacheEntry & left,
                           const RemoteCacheEntry & right) -> int {
        return ((right.accessTime > left.accessTime)
                ? 1 : 0);
    };
    sort(entries.begin(), entries.end(), sortByDate);

    /* build a list of files that needs to be deleted in order to recover
       "objectSize" bytes */
    uint64_t recoverable(0);
    for (const RemoteCacheEntry & file: entries) {
        if (recoverable >= objectSize) {
            break;
        }
        recoverableEntries.push_back(file);
        recoverable += file.size;
    }

    if (recoverable < objectSize) {
        cerr << ("beh mgr: not enough recoverable entries for a "
                 + to_string(objectSize) + " bytes file\n");
        recoverableEntries.clear();
    }

    return recoverableEntries;
}

bool
BehaviorManager::
recoverRemoteCacheDiskSpace(uint64_t objectSize)
    const
{
    vector<RemoteCacheEntry> entries = getRemoteRecoverableEntries(objectSize);
    if (entries.size() == 0) {
        return false;
    }

    uint64_t recovered(0);
    for (const RemoteCacheEntry & file: entries) {
        if (tryEraseUriObject(file.name)) {
            recovered += file.size;
        }
        else {
            cerr << ("beh mgr: file '" + file.name
                     + "' could not be deleted\n");
        }
    }

    if (recovered < objectSize) {
        cerr << ("beh mgr: requested: " + to_string(objectSize) + " bytes;"
                 " recovered: " + to_string(recovered) + "\n");
    }

    return (recovered >= objectSize);
}

/* Concurrency scenarios:
   1) process 1 and process 2 request the cleanup of a distinct list of files,
      in which case enough disk space can be recovered for both files
   2) process 1 has enough disk space to store a file and has not begun to
      store it yet, process 2 initially has enough disk space too. Both
      processes start downloading and storing their file, leading to ENOSPC.
      This case is handled by calling the disk space recovery functions as
      many times as pwrite fails with ENOSPC, with a maximum of 10 attempts
      for the same call to pwrite
   3) process 1 and process 2 request disk space but clean up a list of files
      which have one or more entries in common: the total disk space actually
      recovered will be less than the requested disk space. Again, this is
      handled by calling the disk space recovery functions as many times are
      pwrite fails with ENOSPC
 */
bool
BehaviorManager::
cacheRemoteFile(const string & filename, uint64_t fileSize,
            std::function<bool (uint64_t)> onProgress,
            const string & cacheFile)
    const
{
    char buffer[1024*1024];
    uint64_t totalRead = 0;

    ExcAssert(remoteCacheDir.size() > 0);
    ExcAssert(remoteCacheDir.size() < (1024*1024 - 14));

    ::sprintf(buffer, "%s/remote-download-XXXXXX", remoteCacheDir.c_str());
    int fd = mkstemp(buffer);
    /* we put the filename in "tmp" so that it can be remembered while the
       buffer is used for reading file data */
    string tmp(buffer, ::strlen(buffer));

    if (fd == -1) {
        throw MLDB::Exception(errno, "creating temporary file: " + tmp);
    }

    bool ok = false;

    ML::Call_Guard guard([&] () {
        if(!ok) {
            ::unlink(tmp.c_str());
        }

        ::close(fd);
    });

    Date start = Date::now();

    if (onProgress)
        if (!onProgress(totalRead))
            return false;

    uint64_t ofs = 0;
    filter_istream stream(filename);
    while (stream) {
        stream.read(buffer, sizeof(buffer));
        ssize_t read = stream.gcount();

        /* If the write operation fails due to lack of disk space, we attempt
           to recover the disk space equivalent to the size of the original remote
           file. In spite of the compression, using that quantity as reference
           enables to reduce the number of recovery operations, which is
           presumably quite costly. Ideally, we would approximate the
           remaining size by comparing the number of read bytes from both the
           compressed and the uncompressed streams, but there is unfortunately
           no standard way of doing that. */
        int failedAttempts(0);
        while (true) {
            ssize_t res = ::pwrite(fd, buffer, read, ofs);
            if (res > -1) {
                break;
            }

            if (errno == EINTR) {
                continue;
            }

            failedAttempts++;
            if (errno == ENOSPC
                && failedAttempts < 11
                && recoverRemoteCacheDiskSpace(fileSize)) {
                continue;
            }

            cerr << ("beh mgr: error writing cache file: "
                     + getErrorString(errno) + "\n");
            return false;
        }

        ofs += read;

        totalRead += read;
        if (onProgress)
            if (!onProgress(totalRead))
                return false;
    }

    if (onProgress)
        if (!onProgress(totalRead))
            return false;

    int res = ::rename(tmp.c_str(), cacheFile.c_str());
    if (res == -1) {
        throw MLDB::Exception(errno, "error renaming cache file " + tmp);
    }

    ok = true;

    return true;
}

std::shared_ptr<BehaviorDomain>
BehaviorManager::
getRemote(const std::string & filename,
      std::function<bool (Json::Value)> onProgress)
{
    ExcAssert(!filename.empty());

    auto objectInfo = getUriObjectInfo(filename);
    string remoteObjectName = getUriPath(filename);
    string cacheFile;

    bool useCache = (remoteCacheDir != "" && fs::exists(remoteCacheDir));
    bool cacheFileExists = false;

    if (useCache && fs::exists(remoteCacheDir)) {
        string fileCacheDir = remoteCacheDir + "/" + remoteObjectName;

        string basename = getRemoteCacheName(objectInfo);
        cacheFile = remoteCacheDir + "/" + basename; //fileCacheDir + "/" + basename;

        cacheFileExists = fs::exists(cacheFile);

        /* The objectInfo.size value is a bad reference value when storing
           compressed files, since it will be always smaller than the real
           decompressed size. However, the cache cleanup will occur later
           again from "cacheRemoteFile" in the case where write operation fails
           due to lack of disk space. */
        if (!prepareRemoteCache(objectInfo.size)) {
            cacheStats_.cacheBytesUnsaved += objectInfo.size;
            cacheStats_.cacheFilesUnsaved++;
            useCache = false;
            cerr << ("beh mgr: skipped cache due to unability to cache"
                     " requested file\n");
        }
    }

    if (useCache && cacheFileExists) {
        try {
            ML::File_Read_Buffer file(cacheFile);
            touchATime(cacheFile);

            if(touchPages) {
                if (!touchWithProgress(cacheFile, file, onProgress))
                    return nullptr;
            }

            cacheStats_.cacheBytesRead += file.size();
            cacheStats_.cacheFilesRead++;

            return std::make_shared<MappedBehaviorDomain>(file);
        }
        catch (const std::exception & exc) {
            cerr << ("beh mgr: error loading local cache file "
                     + cacheFile + ": " + exc.what() + "\n");
        }
    }

    Date start = Date::now();

    bool cancelled = false;
    auto onFileProgress = [&] (uint64_t bytesRead)
        {
            if (onProgress) {
                double seconds = Date::now().secondsSince(start);

                Json::Value val;
                val["process"] = "downloading";
                val["file"] = filename;
                val["readMB"] = bytesRead / 1000000.0;
                val["elapsedSeconds"] = seconds;
                val["rateMbPerSecond"] = bytesRead / 1000000.0 / seconds;

                cancelled = !onProgress(val);
                return !cancelled;
            }

            return true;  // continue
        };

    if (useCache) {
        if (cacheRemoteFile(filename, objectInfo.size,
                        onFileProgress, cacheFile)) {
            if (cancelled)
                return nullptr;

            ML::File_Read_Buffer file(cacheFile);
            touchATime(cacheFile);

            if(touchPages) {
                if (!touchWithProgress(cacheFile, file, onProgress))
                    return nullptr;
            }

            return std::make_shared<MappedBehaviorDomain>(file);
        }
    }

    auto contents = std::make_shared<string>();
    *contents = move(readFile(filename, onFileProgress));
    if (cancelled)
        return nullptr;

    auto onDone = [contents] () {
        shared_ptr<string> localPtr = contents;
        localPtr.reset();
    };

    ML::File_Read_Buffer file(contents->c_str(), contents->size(),
                              filename, onDone);
    auto result = std::make_shared<MappedBehaviorDomain>(file);
    return result;
}

std::shared_ptr<BehaviorDomain>
BehaviorManager::
getFile(const std::string & filename,
        std::function<bool (Json::Value)> onProgress)
{
    ExcAssert(!filename.empty());

    if (ML::endsWith(filename, ".gz") || ML::endsWith(filename, ".lz4")) {
        auto contents = std::make_shared<string>();

        bool cancelled = false;
        auto onFileProgress = [&] (uint64_t bytesRead)
            {
                if (onProgress) {
                    Json::Value val;
                    val["file"] = filename;
                    val["readMB"] = bytesRead / 1000000.0;
                    cancelled = !onProgress(val);
                    return !cancelled;
                }

                return true;  // continue
            };

        *contents = move(readFile(filename, onFileProgress));
        if (cancelled)
            return nullptr;

        auto onDone = [contents] () {
            shared_ptr<string> localPtr = contents;
            localPtr.reset();
        };
        ML::File_Read_Buffer file(contents->c_str(), contents->size(),
                                  filename, onDone);
        return std::make_shared<MappedBehaviorDomain>(file);
    }

    File_Read_Buffer file(filename);

    if(touchPages) {
        if (!touchWithProgress(filename, file, onProgress))
            return nullptr;
    }
    
    return std::make_shared<MappedBehaviorDomain>(file);
}

void
BehaviorManager::
save(BehaviorDomain & behs,
     const std::string & filename)
    const
{
    if (filename == "") return;

    filter_ostream stream(filename);
    behs.saveToStream(stream);
}

void
BehaviorManager::
saveSvd(const BehaviorSvd & svd,
        const std::string & filename)
{
    filter_ostream stream(filename);
    DB::Store_Writer store(stream);
    svd.serialize(store);
}

std::shared_ptr<BehaviorSvd>
BehaviorManager::
getSvd(const std::string & filename)
{
    filter_istream stream(filename);
    DB::Store_Reader store(stream);
    auto result = std::make_shared<BehaviorSvd>();
    result->reconstitute(store);
    return result;
}

void
BehaviorManager::
setRemoteCacheDir(const std::string & dir)
{
    remoteCacheDir = dir;
}

std::map<std::string, std::string>
BehaviorManager::
parseSpecs(const std::string & specs) const
{
    vector<string> splitStr;
    boost::split(splitStr, specs, boost::is_any_of(" "));
    map<std::string, std::string> result;
    for (auto k_vStr : splitStr) {
        vector<string> key_value;
        boost::split(key_value, k_vStr, boost::is_any_of(":"));
        if (key_value.size() != 2)
            throw MLDB::Exception("invalid key " + k_vStr + " in specs");
        result[key_value[0]] = key_value[1];
    }
    return result;
}



} // namespace MLDB
