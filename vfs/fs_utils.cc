// This file is part of MLDB. Copyright 2015 mldb.ai inc. All rights reserved.

/* fs_utils.cc
   Wolfgang Sourdeau, February 2014
   Copyright (c) 2014 mldb.ai inc.  All rights reserved.

   A set of file-system abstraction functions intended to support common
   operations among different fs types or alikes.
*/

#include <libgen.h>

#include <memory>
#include <map>
#include <mutex>

#include "mldb/arch/arch.h"
#include "mldb/compiler/filesystem.h"
#include "mldb/ext/googleurl/src/url_util.h"
#include "mldb/types/structure_description.h"
#include "mldb/types/map_description.h"

#include "mldb/vfs/fs_utils.h"
#include "mldb/base/scope.h"
#include "mldb/vfs/filter_streams_registry.h"
#include "mldb/utils/split.h"
#include "mldb/utils/starts_with.h"

#include <sys/types.h>
#include <sys/stat.h>
#include <unistd.h>
#include <errno.h>


using namespace std;
using namespace MLDB;

namespace fs = std::filesystem;

namespace {

/* registry */

struct Registry 
{
    std::mutex mutex;
    map<string, std::unique_ptr<const UrlFsHandler> > handlers;
};

Registry& getRegistry()
{
    static Registry* registry = new Registry;
    return *registry;
}

} // file scope

namespace MLDB {

DEFINE_STRUCTURE_DESCRIPTION(FsObjectInfo);

FsObjectInfoDescription::
FsObjectInfoDescription()
{
    addField("exists", &FsObjectInfo::exists,
             "Does the object exist?");
    addField("lastModified", &FsObjectInfo::lastModified,
             "Date the object was last modified");
    addAuto ("lastAccessed", &FsObjectInfo::lastAccessed,
             "Date the object was last modified");
    addField("size", &FsObjectInfo::size,
             "Size in bytes of the object");
    addField("etag", &FsObjectInfo::etag,
             "Entity tag (unique hash) for object");
    addField("storageClass", &FsObjectInfo::storageClass,
             "Storage class of object if applicable");
    addField("ownerId", &FsObjectInfo::ownerId,
             "ID of owner");
    addField("ownerName", &FsObjectInfo::ownerName,
             "Name of owner");
    addField("contentType", &FsObjectInfo::contentType,
             "Content type of object");
    addField("objectMetadata", &FsObjectInfo::objectMetadata,
             "Metadata about the object");
    addField("userMetadata", &FsObjectInfo::userMetadata,
             "Metadata placed there by the user");
}

static bool acceptUrisWithoutScheme = true;

/// Set a GLOBAL flag that URIs without a scheme will not be accepted
void setGlobalAcceptUrisWithoutScheme(bool accept)
{
    acceptUrisWithoutScheme = accept;
}

/* ensures that local filenames are represented as urls */
Url makeUrl(const Utf8String & urlStr)
{
    if (urlStr.empty())
        throw MLDB::Exception("can't makeUrl on empty url");

    auto [scheme, rest, found] = split_on_first(urlStr, "://");
    if (found)
        return Url(urlStr);

    if (!acceptUrisWithoutScheme)
        throw MLDB::Exception("Cannot accept URI without scheme (if you want a file, add file://): " + urlStr);
    return Url("file://" + urlStr);

    /* absolute local filenames */
    if (urlStr.startsWith("/")) {
        return Url("file://" + urlStr);
    }
    /* relative filenames */
    else {
        return Url(Utf8String("file://" + fs::current_path().u8string()) + "/" + urlStr);
    }
}

// Return the scheme for the URI
std::string getUriScheme(const Utf8String & uri)
{
    return makeUrl(uri).scheme();
}

// Return the path (everything after the scheme) for the URI
std::string getUriPath(const Utf8String & uri)
{
    return makeUrl(uri).asciiPath();
}

static FsObjectInfo extractInfo(const fs::path & path, const fs::file_status & stats_)
{
    FsObjectInfo objectInfo;

#if MLDB_INTEL_ISA
#else
#  define stat64 stat
#endif
    struct stat64 stats;
    int res = ::stat64(path.c_str(), &stats);
    if (res == -1)
        throw MLDB::Exception(errno, "stat64");

    objectInfo.exists = true;
#if __APPLE__
    objectInfo.lastModified = Date::fromTimespec(stats.st_mtimespec);
    objectInfo.lastAccessed = Date::fromTimespec(stats.st_atimespec);
#else
    objectInfo.lastModified = Date::fromTimespec(stats.st_mtim);
    objectInfo.lastAccessed = Date::fromTimespec(stats.st_atim);
#endif
    objectInfo.size = stats.st_size;

    return objectInfo;
}

static FsObjectInfo extractInfo(const Utf8String & path_, const fs::directory_entry & entry)
{
    Utf8String path = must_remove_prefix(path_, "file://");

#if MLDB_INTEL_ISA
#else
#  define stat64 stat
#endif
    struct stat64 stats;
    int res = ::stat64(path.c_str(), &stats);
    if (res == -1)
        throw MLDB::Exception(errno, "stat64");

    FsObjectInfo objectInfo;

    objectInfo.exists = true;
#if __APPLE__
    objectInfo.lastModified = Date::fromTimespec(stats.st_mtimespec);
    objectInfo.lastAccessed = Date::fromTimespec(stats.st_atimespec);
#else
    objectInfo.lastModified = Date::fromTimespec(stats.st_mtim);
    objectInfo.lastAccessed = Date::fromTimespec(stats.st_atim);
#endif
    objectInfo.size = stats.st_size;

    return objectInfo;
}

/* LOCALURLFSHANDLER */

struct LocalUrlFsHandler : public UrlFsHandler {

    virtual FsObjectInfo getInfo(const Url & url) const
    {
        fs::path path = url.path().rawString();
        std::error_code ec;
        auto status = fs::status(path, ec);
        if (ec) {
            if (ec == std::errc::no_such_file_or_directory)
                return FsObjectInfo();
            throw MLDB::Exception(ec.message());
        }

        // TODO: owner ID (uid) and name (uname)

        return extractInfo(path, status);
    }

    virtual FsObjectInfo tryGetInfo(const Url & url) const
    {
        fs::path path = url.path().rawString();
        std::error_code ec;
        auto status = fs::status(path, ec);
        if (ec) {
            return FsObjectInfo();
        }

        return extractInfo(path, status);
    }
    
    virtual void makeDirectory(const Url & url) const
    {
        std::error_code ec;
        string path = url.path().rawString();

        // Ignore return code; it tells us about the work done, not
        // the postcondition.  We check for success in the error
        // code.
        fs::create_directories(path, ec);
        if (ec) {
            throw MLDB::Exception(ec.message());
        }
    }

    virtual bool erase(const Url & url, bool throwException) const
    {
        std::error_code ec;
        string path = url.path().rawString();
        fs::remove(path, ec);
        if (ec) {
            if (throwException) {
                throw MLDB::Exception(ec.message());
            }
            else return false;
        }
        return true;
    }

    virtual bool forEach(const Url & prefix,
                         const OnUriObject & onObject,
                         const OnUriSubdir & onSubdir,
                         const std::string & delimiter,
                         const std::string & startAt) const
    {
        if (startAt != "")
            throw MLDB::Exception("not implemented: startAt for local files");
        if (delimiter != "/")
            throw MLDB::Exception("not implemented: delimiters other than '/' "
                                "for local files");

        auto path = prefix.path();

        for (auto it = fs::recursive_directory_iterator(path.rawString()), end = fs::recursive_directory_iterator();
             it != end;  ++it) {
            const auto & entry = *it;
            Utf8String filename = "file://" + entry.path().string();

            if (entry.is_regular_file()) {

                OpenUriObject open = [=,this] (const std::map<std::string, std::string> & options)
                -> UriHandler
                {
                    if (!options.empty())
                        throw MLDB::Exception("Options not accepted by file://");

                    std::shared_ptr<std::istream> result(new filter_istream(filename.rawString(), options));
                    return UriHandler(result->rdbuf(), std::move(result),
                                            getInfo(Url(filename)));
                };
                
                if (!onObject(filename.rawString(), extractInfo(filename.rawString(), entry), open, it.depth()))
                    return false;
            }
            else if (entry.is_regular_file()) {
                if (!onSubdir || it.depth() == 0 || onSubdir(filename.rawString(), it.depth()))
                    continue;
                else {
                    it.disable_recursion_pending();
                }
            }
            // else skip it
        }

        return true;
    }
};


const UrlFsHandler * findFsHandler(const string & scheme)
{
    auto& registry = getRegistry();

    std::unique_lock<std::mutex> guard(registry.mutex);
    auto handler = registry.handlers.find(scheme);
    if (handler != registry.handlers.end())
        return handler->second.get();

    // Look for a prefix scheme
    auto pos = scheme.find("+");
    if (pos != string::npos) {
        string firstScheme(scheme, 0, pos);

        //cerr << "firstScheme = " << firstScheme << endl;

        handler = registry.handlers.find(firstScheme);
    }
    if (handler != registry.handlers.end())
        return handler->second.get();
    
    throw MLDB::Exception("no handler found for scheme: " + scheme);
}


namespace {

struct AtInit {
    AtInit() {
        registerUrlFsHandler("file", new LocalUrlFsHandler());
    }
} atInit;

} // file scope

/* URLFSHANDLER */

size_t
UrlFsHandler::
getSize(const Url & url) const
{
    return getInfo(url).size;
}

string
UrlFsHandler::
getEtag(const Url & url) const
{
    return getInfo(url).etag;
}


/* registry */

void registerUrlFsHandler(const std::string & scheme,
                          UrlFsHandler * handler)
{
    auto& registry = getRegistry();

    if (registry.handlers.find(scheme) != registry.handlers.end()) {
        throw MLDB::Exception("fs handler already registered");
    }

    /* this enables googleuri to parse our urls properly */
    url_util::AddStandardScheme(scheme.c_str());

    registry.handlers[scheme].reset(handler);
}

FsObjectInfo
tryGetUriObjectInfo(const Utf8String & url)
{
    Url realUrl = makeUrl(url);
    return findFsHandler(realUrl.scheme())->tryGetInfo(realUrl);
}

FsObjectInfo
getUriObjectInfo(const Utf8String & url)
{
    Url realUrl = makeUrl(url);
    return findFsHandler(realUrl.scheme())->getInfo(realUrl);
}
 
size_t
getUriSize(const Utf8String & url)
{
    Url realUrl = makeUrl(url);
    return findFsHandler(realUrl.scheme())->getSize(realUrl);
}

std::string
getUriEtag(const Utf8String & url)
{
    Url realUrl = makeUrl(url);
    return findFsHandler(realUrl.scheme())->getEtag(realUrl);
}

void
makeUriDirectory(const Utf8String & url)
{
    auto [dirPart, filePart, found] = split_on_last(url, "/");
    if (!found) {
        throw MLDB::Exception("makeUriDirectory cannot work on filenames: instead of " + url + " you should probably write file://" + url);
    }

    Url realUrl = makeUrl(dirPart);
    findFsHandler(realUrl.scheme())->makeDirectory(realUrl);
}

bool
eraseUriObject(const Utf8String & url, bool throwException)
{
    Url realUrl = makeUrl(url);
    return findFsHandler(realUrl.scheme())->erase(realUrl, throwException);
}

bool
tryEraseUriObject(const Utf8String & uri)
{
    return eraseUriObject(uri, false);
}

bool forEachUriObject(const Utf8String & urlPrefix,
                      const OnUriObject & onObject,
                      const OnUriSubdir & onSubdir,
                      const std::string & delimiter,
                      const std::string & startAt)
{
    Url realUrl = makeUrl(urlPrefix);
    return findFsHandler(realUrl.scheme())
        ->forEach(realUrl, onObject, onSubdir, delimiter, startAt);
}

string
baseName(const std::string & filename)
{
    char *fnCopy = ::strdup(filename.c_str());
    Scope_Exit(::free(fnCopy));
    char *dirNameC = ::basename(fnCopy);
    string dirname(dirNameC);

    return dirname;
}

string
dirName(const std::string & filename)
{
    char *fnCopy = ::strdup(filename.c_str());
    Scope_Exit(::free(fnCopy));
    char *dirNameC = ::dirname(fnCopy);
    string dirname(dirNameC);

    return dirname;
}

/****************************************************************************/
/* UX FUNCTIONS                                                             */
/****************************************************************************/

void
checkWritability(const Utf8String & url, const std::string & parameterName)
{
    // try to create output folder and write open a writer to make sure 
    // we have permissions before
    try {
        makeUriDirectory(url);
    } catch ( std::exception const& ex) {
        throw MLDB::Exception(MLDB::format("Error when trying to create folder specified "
                "in parameter '%s'. Value: '%s'. Exception: %s",
                parameterName, url, ex.what()));
    }

    try {
        filter_ostream writer(url);
    } catch (std::exception const& ex) {
        throw MLDB::Exception(MLDB::format("Error when trying to write to file specified "
                "in parameter '%s'. Value: '%s'. Exception: %s",
                parameterName, url, ex.what()));
    }

    // remove empty file
    tryEraseUriObject(url);
}


/****************************************************************************/
/* FILE COMMITER                                                            */
/****************************************************************************/

FileCommiter::
~FileCommiter()
{
    if (!commited_) {
        tryEraseUriObject(fileUrl_);
    }
}

} // namespace MLDB
