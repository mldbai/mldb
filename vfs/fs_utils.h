/* fs_utils.h                                                       -*- C++ -*-
   Wolfgang Sourdeau, February 2014
   Copyright (c) 2014 mldb.ai Inc.  All rights reserved.
   This file is part of MLDB. Copyright 2015 mldb.ai inc. All rights reserved.
   
   A set of file-system abstraction functions intended to support common
   operations among different fs types or alikes.
*/

#pragma once

#include <string>
#include <functional>
#include <map>
#include <exception>

#include "mldb/ext/jsoncpp/value.h"
#include "mldb/types/date.h"
#include "mldb/types/url.h"
#include "mldb/types/value_description_fwd.h"

namespace MLDB {

struct UriHandler;


/*****************************************************************************/
/* FS OBJECT INFO                                                            */
/*****************************************************************************/

/** This class contains information about an object in some kind of generalized
    file system. Made to be value copyable, do not extend.
*/

struct FsObjectInfo {
    FsObjectInfo()
        : exists(false), size(-1)
    {}

    bool exists;                ///< If false, the object doesn't exist
    Date lastModified;          ///< Date last modified
    int64_t size;               ///< Size in bytes
    std::string etag;           ///< Element tag (content hash) when supported
    std::string storageClass;   ///< Storage class of object (S3 only)
    std::string ownerId;        ///< ID of the owner (uid or identifier)
    std::string ownerName;      ///< Name of owner

    std::map<std::string, Json::Value> userMetadata;  ///< Metadata from user
    std::map<std::string, Json::Value> objectMetadata;  ///< Metadata from obj

    operator bool () const { return exists; }
};

DECLARE_STRUCTURE_DESCRIPTION(FsObjectInfo);


/*****************************************************************************/
/* CALLBACK TYPES                                                            */
/*****************************************************************************/

/// Type of a callback when we find a subdirectory in a directory traversal
/// If it returns false, then the subdirectory will not be traversed into

typedef std::function<bool (const std::string & dirName,
                            int depth)>
OnUriSubdir;

/// Type of a callback to call when we want to open an element of an
/// archive or directory we're iterating through.

typedef std::function<UriHandler (const std::map<std::string, std::string> & options)>
OpenUriObject;

/// Type of a callback when we find an object in a directory traversal
/// If it returns false, then the iteration will be terminated.
///
/// The open method can be used to obtain a stream pointing to the object,
/// which will normally be more efficient than opening the URI directly.

typedef std::function<bool (const std::string & uri,
                            const FsObjectInfo & info,
                            const OpenUriObject & open,
                            int depth)>
OnUriObject;



/*****************************************************************************/
/* URL FS HANDLER                                                            */
/*****************************************************************************/

/** Handles dealing with objects in a generalized file system. */

struct UrlFsHandler {
    virtual FsObjectInfo getInfo(const Url & url) const = 0;
    virtual FsObjectInfo tryGetInfo(const Url & url) const = 0;

    virtual size_t getSize(const Url & url) const;
    virtual std::string getEtag(const Url & url) const;

    virtual void makeDirectory(const Url & url) const = 0;
    virtual bool erase(const Url & url, bool throwException) const = 0;

    /** For each object under the given prefix (object or subdirectory),
        call the given callback.
    */
    virtual bool forEach(const Url & prefix,
                         const OnUriObject & onObject,
                         const OnUriSubdir & onSubdir,
                         const std::string & delimiter,
                         const std::string & startAt) const = 0;
};

/** Register a new handler for handling URIs of the given scheme. */
void registerUrlFsHandler(const std::string & scheme,
                          UrlFsHandler * handler);


/*****************************************************************************/
/* FREE FUNCTIONS                                                            */
/*****************************************************************************/

// Parse a string into an URL
Url makeUrl(const std::string & urlStr);

/// Set a GLOBAL flag that URIs without a scheme will not be accepted
void setGlobalAcceptUrisWithoutScheme(bool accept);

// Return the object info for either a file or an S3 object
FsObjectInfo getUriObjectInfo(const std::string & filename);

// Return the object info for either a file or an S3 object, or null if
// it doesn't exist
FsObjectInfo tryGetUriObjectInfo(const std::string & filename);

// Return the size of an URI for either a file or an s3 object
size_t getUriSize(const std::string & filename);

// Return an etag for either a file or an s3 object
std::string getUriEtag(const std::string & filename);

// Return the scheme for the URI
std::string getUriScheme(const std::string & uri);

// Return the path (everything after the scheme) for the URI
std::string getUriPath(const std::string & uri);

/* Create the directories for the given path.  For S3 it does nothing;
   for normal directories it does mkdir -p

   use cases:
   "/some/filename" gives "/some"
   "/some/dirname/" gives "/some/dirname/"
   "dirname" throws */

void makeUriDirectory(const std::string & uri);

// Erase the object at the given uri
bool eraseUriObject(const std::string & uri, bool throwException = true);

// Erase the object at the given uri
bool tryEraseUriObject(const std::string & uri);

/** For each file matching the given prefix in the given bucket, call
    the callback.

    \param uriPrefix       Where to start (eg, directory name)
    \param onObject        Callback to call when an object is found
    \param onSubdir        Callback to call when a subdirectory is found.
                           If there is no callback or it returns false,
                           then subdirectories will be skipped (not
                           recursed into).
    \param delimiter       Delimiter to separate path names
    \param startDepth      Initial depth of recursion
    \param startAt         Object at which to start recursion, relative to
                           uriPrefix.  Used to continue a previous iteration.

    Will return false if the result of an onOjbect call was false, true
    otherwise.
*/
bool forEachUriObject(const std::string & uriPrefix,
                      const OnUriObject & onObject,
                      const OnUriSubdir & onSubdir = nullptr,
                      const std::string & delimiter = "/",
                      const std::string & startAt = "");


// wrappers around "basename" and "dirname" from the libc
std::string baseName(const std::string & filename);
std::string dirName(const std::string & filename);


/****************************************************************************/
/* UX FUNCTIONS                                                             */
/****************************************************************************/

/* The checkWritability function will try to create the folders required
 * to write to the given url and then attempt to open an output stream to
 * that location. This is meant to be used before running a big job that would
 * then try to write its output to this location in order to catch this
 * error before spending the time running the job
 */
void
checkWritability(const std::string & url, const std::string & parameterName);


/****************************************************************************/
/* FILE COMMITER                                                            */
/****************************************************************************/

/* The FileCommiter class is meant to ensure that a given file is in a
 * consistent state and meant to exist. In practice, it gives a reasonable
 * guarantee that exceptions or abandonned writes will not leave incomplete
 * files lying around. Using RAII, we require the file to be "commited" at
 * destruction time and we erase it otherwise. */
struct FileCommiter {
    FileCommiter(const std::string & fileUrl)
        : fileUrl_(fileUrl), commited_(false)
    {
    }
    ~FileCommiter();

    void commit()
    {
        commited_ = true;
    }

private:
    const std::string & fileUrl_;
    bool commited_;
};

} // namespace MLDB


