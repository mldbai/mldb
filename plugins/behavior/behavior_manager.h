/* behavior_manager.h                                             -*- C++ -*-
   Jeremy Barnes, 7 August 2012
   Copyright (c) 2012 mldb.ai inc.  All rights reserved.

   This file is part of MLDB. Copyright 2015 mldb.ai inc. All rights reserved.
   
   Behavior manager that can get them from local files or from remote URIs.
*/

#pragma once

#include <atomic>
#include <map>
#include <mutex>
#include <set>
#include <string>
#include <thread>
#include <vector>
#include <boost/algorithm/string.hpp>
#include <boost/lexical_cast.hpp>

#include "mldb/types/date.h"
#include "behavior_domain.h"
#include "mapped_behavior_domain.h"

namespace MLDB {


class BehaviorSvd;


/****************************************************************************/
/* Remote CACHE STATS                                                           */
/****************************************************************************/

struct RemoteCacheStats {
    RemoteCacheStats()
        : cacheBytesRead(0),
          cacheFilesRead(0),
          cacheBytesUnsaved(0),
          cacheFilesUnsaved(0),
          remoteBytesRead(0),
          remoteBytesReadDecompressed(0),
          remoteFilesRead(0)
    {
    }

    std::atomic<uint64_t> cacheBytesRead;           // number of bytes read from the cache
    std::atomic<int> cacheFilesRead;                // number of files read from the cache

    std::atomic<uint64_t> cacheBytesUnsaved;        // number of bytes that could not be
                                                    // saved to the cache
    std::atomic<int> cacheFilesUnsaved;             // number of files that could not be
                                                    // saved to the cache

    std::atomic<uint64_t> remoteBytesRead;              // number of bytes read from remote, before decompression
    std::atomic<uint64_t> remoteBytesReadDecompressed;  // number of bytes read from remote, after
                                                    // decompression
    std::atomic<int> remoteFilesRead;                   // number of files read from remote
};


/*****************************************************************************/
/* BEHAVIOR MANAGER                                                         */
/*****************************************************************************/

struct BehaviorManager {

    BehaviorManager();

    virtual ~BehaviorManager();

    enum CacheAction {
        CACHE_ALWAYS,  ///< Always cache the file
        CACHE_NEVER,   ///< Never cache the file
        CACHE_CONFIG   ///< Based upon configuration
    };

    std::shared_ptr<BehaviorDomain>
    get(const std::vector<std::string> & inputFiles,
        bool preIndex = true,
        CacheAction cacheAction = CACHE_CONFIG,
        bool skipBadFiles = false,
        std::function<bool (Json::Value)> onProgress = nullptr);

    virtual std::shared_ptr<BehaviorDomain>
    get(const std::string & inputFile,
        CacheAction cacheAction = CACHE_CONFIG,
        std::function<bool (Json::Value)> onProgress = nullptr);

    virtual std::shared_ptr<BehaviorDomain>
    getRemote(const std::string & inputFile,
          std::function<bool (Json::Value)> onProgress);

    virtual std::shared_ptr<BehaviorDomain>
    getFile(const std::string & inputFile,
            std::function<bool (Json::Value)> onProgress);

    /* When using a cache, ensure that the specified files are present or
     * being downloaded by other threads. This mechanisms ensure that, unlike
     * get(), the loading of one file by another process will not block this
     * process from downloading the other files it requires in the
     * meantime. */
    void preloadRemoteCache(const std::set<std::string> & filenames) const;

    /*
     * Returns the appropriate behavior domain from json specs
     */
    std::shared_ptr<BehaviorDomain>
    virtual getFromSpecs(const Json::Value & specs)
    { throw MLDB::Exception("implement in subclass"); }


    /** Cache the given file after parsing the specs.  The force flag will
        cause a rebuild of the cache even if there is already a cached
        file there.
    */
    virtual void cacheFromSpecs(const Json::Value & specs, bool force)
    { throw MLDB::Exception("implement in subclass"); }

    void cacheFromSpecs(const std::string & specs,
                                bool force)
    { cacheFromSpecs(Json::parse(specs), force); }

    /** Set whether or not we cache input files (files we merge from) */
    virtual void setCacheInputFiles(bool cacheThem)
    {
        if (!cacheThem)
            setRemoteCacheDir("");
    }

    // Parse a specs string (for getFromSpecs) into a key value map
    // TODO : deprecate this
    std::map<std::string, std::string> parseSpecs(const std::string & specs) const;

    // Split a spec value (e.g. "a,b,c" -> ["a","b","c"] and "1,3,5-8" -> ["1","3","5","6","7"])
    // TODO : deprecate this
    template <typename T=std::string>
    std::vector<T> splitSpec(const std::string & spec) const;

    // TODO : deprecate this
    std::vector<std::string> splitSpec(const std::string & spec) const
    { return splitSpec<std::string>(spec); }

    void save(BehaviorDomain & behs,
              const std::string & filename) const;
    
    void saveSvd(const BehaviorSvd & file,
                 const std::string & filename);

    std::shared_ptr<BehaviorSvd>
    getSvd(const std::string & filename);

    /** Set a directory in which files downloaded from Remote will be cached.
        If these are there and paged in on a subsequent call they will be
        used in preference to going back to Remote.
    */
    void setRemoteCacheDir(const std::string & dir);

    /** Compatibility shim for old name. */
    void setS3CacheDir(const std::string & dir)
    {
        setRemoteCacheDir(dir);
    }


    /** Retrieve statistics about the cache usage covering the lifetime of the
     * current instance of the BehaviorManager. */
    const RemoteCacheStats & getCacheStats()
        const
    {
        return cacheStats_;
    }

    struct RemoteCacheEntry {
        std::string name;
        Date accessTime;
        int64_t size;
    };
    /** Load stats for all regular files found under the cache directory */
    std::vector<RemoteCacheEntry> getRemoteCacheEntries() const;

    std::string remoteCacheDir;

    /** If this is true, files that are loaded will be cached in memory
        so that multiple calls to get() the same file will return the
        same object.  This is off by default as it means that we never
        free any of the BehaviorDomain objects.
    */
    bool cacheLoadedFiles;
    bool touchPages;

    mutable std::mutex lock;
    int shutdown;
    std::list<std::pair<ML::File_Read_Buffer, std::string> > writebackQueue;
    std::unique_ptr<std::thread> writebackThread;

    std::map<std::string, std::shared_ptr<BehaviorDomain> > cache;

    /* Test whether the cache directory exists, check that the total disk
       space is large enough to hold the file and perform the cleanup if
       necessary. Returns false if any of these operations failed. Note that
       the disk cache code can lead to race conditions that only the use of a
       daemon can avoid. */
    bool prepareRemoteCache(uint64_t objectSize) const;

private:
    std::vector<RemoteCacheEntry> getRemoteRecoverableEntries(uint64_t objectSize) const;
    bool recoverRemoteCacheDiskSpace(uint64_t objectSize) const;

    std::string readFile(const std::string & filename,
                         std::function<bool (uint64_t)> onProgress) const;

    /* Performs the download of the given file from a remote URI into the given
       "cacheFile". Returns "false" when an error has occurred or when
       "onProgress" has itself returned "false", and "true" otherwise. */
    bool cacheRemoteFile(const std::string & filename,
                     uint64_t fileSize,
                     std::function<bool (uint64_t)> onProgress,
                     const std::string & cacheFile) const;

    RemoteCacheStats cacheStats_;
};


template <typename T>
std::vector<T>
BehaviorManager::
splitSpec(const std::string & spec) const
{
    using namespace std;
    vector<T> result;
    vector<string> commaSplit;
    boost::split(commaSplit, spec, boost::is_any_of(","));

    for (string x : commaSplit) {
        auto dash = x.find("-");
        if (dash == string::npos)
            result.push_back(boost::lexical_cast<T>(x));
        else {
            int begin = boost::lexical_cast<int>(x.substr(0,dash));
            int end = boost::lexical_cast<int>(x.substr(dash+1));
            for (int i=begin; i < end; ++i)
                result.push_back(boost::lexical_cast<T>(i));
        }
    }
    return result;
}

} // namespace MLDB
