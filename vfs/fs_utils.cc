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

#include "boost/filesystem.hpp"
#include "mldb/ext/googleurl/src/url_util.h"
#include "mldb/types/structure_description.h"
#include "mldb/types/map_description.h"

#include "mldb/vfs/fs_utils.h"
#include "mldb/base/scope.h"
#include "mldb/vfs/filter_streams_registry.h"

#include <sys/types.h>
#include <sys/stat.h>
#include <unistd.h>
#include <ftw.h>


using namespace std;
using namespace MLDB;

namespace fs = boost::filesystem;

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
Url makeUrl(const string & urlStr)
{
    if (urlStr.empty())
        throw MLDB::Exception("can't makeUrl on empty url");

    /* scheme is specified */
    if (urlStr.find("://") != string::npos) {
        return Url(urlStr);
    }
    else if (!acceptUrisWithoutScheme) {
        throw MLDB::Exception("Cannot accept URI without scheme (if you want a file, add file://): " + urlStr);
    }
    /* absolute local filenames */
    else if (urlStr[0] == '/') {
        return Url("file://" + urlStr);
    }
    /* relative filenames */
    else {
        char cCurDir[65536]; // http://insanecoding.blogspot.ca/2007/11/pathmax-simply-isnt.html
        string filename(getcwd(cCurDir, sizeof(cCurDir)));
        filename += "/" + urlStr;

        return Url("file://" + filename);
    }
}

// Return the scheme for the URI
std::string getUriScheme(const std::string & uri)
{
    return makeUrl(uri).scheme();
}

// Return the path (everything after the scheme) for the URI
std::string getUriPath(const std::string & uri)
{
    return makeUrl(uri).path();
}

static FsObjectInfo extractInfo(const struct stat & stats)
{
    FsObjectInfo objectInfo;

    objectInfo.exists = true;
    objectInfo.lastModified = Date::fromTimespec(stats.st_mtim);
    objectInfo.size = stats.st_size;

    return objectInfo;
}

/* LOCALURLFSHANDLER */

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

std::ostream &
operator << (std::ostream & stream, const FileType & type)
{
    return stream << print(type);
}

namespace {

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

struct LocalUrlFsHandler : public UrlFsHandler {

    virtual FsObjectInfo getInfo(const Url & url) const
    {
        struct stat stats;
        string path = url.path();

        // cerr << "fs info on path: " + path + "\n";
        int res = ::stat(path.c_str(), &stats);
        if (res == -1) {
            if (errno == ENOENT) {
                return FsObjectInfo();
            }
            throw MLDB::Exception(errno, "stat");
        }

        // TODO: owner ID (uid) and name (uname)

        return extractInfo(stats);
    }

    virtual FsObjectInfo tryGetInfo(const Url & url) const
    {
        struct stat stats;
        string path = url.path();

        // cerr << "fs info on path: " + path + "\n";
        int res = ::stat(path.c_str(), &stats);
        if (res == -1) {
            return FsObjectInfo();
        }

        return extractInfo(stats);
    }
    
    virtual void makeDirectory(const Url & url) const
    {
        boost::system::error_code ec;
        string path = url.path();

        // Ignore return code; it tells us about the work done, not
        // the poscondition.  We check for success in the error
        // code.
        fs::create_directories(path, ec);
        if (ec.value() != boost::system::errc::success) {
            throw MLDB::Exception(ec.message());
        }
    }

    virtual bool erase(const Url & url, bool throwException) const
    {
        string path = url.path();
        int res = ::unlink(path.c_str());
        if (res == -1) {
            if (throwException) {
                throw MLDB::Exception(errno, "unlink");
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
        

        bool result = true;
        auto onFileFound = [&] (const std::string & dir,
                                const std::string & basename,
                                const struct stat & stats,
                                FileType type,
                                int depth) -> FileAction
            {
                if (type == FT_FILE) {
                    std::string filename = "file://" + dir + basename;

                    OpenUriObject open = [=] (const std::map<std::string, std::string> & options)
                    -> UriHandler
                    {
                        if (!options.empty())
                            throw MLDB::Exception("Options not accepted by S3");

                        std::shared_ptr<std::istream> result(new filter_istream(filename, options));
                        return UriHandler(result->rdbuf(), std::move(result),
                                              getInfo(Url(filename)));
                    };
                    
                    result = onObject(filename,
                                      extractInfo(stats),
                                      open,
                                      depth);
                    if (!result)
                        return FA_STOP;
                    else return FA_CONTINUE;
                }
                else if (type == FT_DIR) {
                    if (!onSubdir || depth == 0)
                        return FA_CONTINUE;
                    else if (onSubdir("file://" + dir + basename,
                                      depth))
                        return FA_CONTINUE;
                    else return FA_SKIP_SUBTREE;
                }
                else return FA_CONTINUE;
            };

        scanFiles(prefix.path(), onFileFound, -1);

        return result;
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
tryGetUriObjectInfo(const std::string & url)
{
    Url realUrl = makeUrl(url);
    return findFsHandler(realUrl.scheme())->tryGetInfo(realUrl);
}

FsObjectInfo
getUriObjectInfo(const std::string & url)
{
    Url realUrl = makeUrl(url);
    return findFsHandler(realUrl.scheme())->getInfo(realUrl);
}
 
size_t
getUriSize(const std::string & url)
{
    Url realUrl = makeUrl(url);
    return findFsHandler(realUrl.scheme())->getSize(realUrl);
}

std::string
getUriEtag(const std::string & url)
{
    Url realUrl = makeUrl(url);
    return findFsHandler(realUrl.scheme())->getEtag(realUrl);
}

void
makeUriDirectory(const std::string & url)
{
    string dirUrl(url);
    size_t slashIdx = dirUrl.rfind('/');
    if (slashIdx == string::npos) {
        throw MLDB::Exception("makeUriDirectory cannot work on filenames: instead of " + url + " you should probably write file://" + url);
    }
    dirUrl.resize(slashIdx);

    // cerr << "url: " + url + "/dirUrl: " + dirUrl + "\n";

    Url realUrl = makeUrl(dirUrl);
    findFsHandler(realUrl.scheme())->makeDirectory(realUrl);
}

bool
eraseUriObject(const std::string & url, bool throwException)
{
    Url realUrl = makeUrl(url);
    return findFsHandler(realUrl.scheme())->erase(realUrl, throwException);
}

bool
tryEraseUriObject(const std::string & uri)
{
    return eraseUriObject(uri, false);
}

bool forEachUriObject(const std::string & urlPrefix,
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
checkWritability(const std::string & url, const std::string & parameterName)
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
