// This file is part of MLDB. Copyright 2015 mldb.ai inc. All rights reserved.

/** archive.cc
    Jeremy Barnes, 14 September 2015
    Copyright (c) 2015 mldb.ai inc.  All rights reserved.

    FS handler for archives, backed by libarchive.
*/


#include "mldb/vfs/fs_utils.h"
#include "mldb/base/scope.h"
#include "mldb/vfs/filter_streams_registry.h"
#include "mldb/arch/exception.h"
#include <sstream>

// libarchive support
#include <archive.h>
#include <archive_entry.h>



using namespace std;


namespace MLDB {


struct ArchiveData {
    ArchiveData(std::streambuf * streambuf)
        : stream(streambuf)
    {
    }

    std::istream stream;
    char current[16384];
};

static ssize_t myread(struct archive *a, void *client_data, const void **buff)
{
    ArchiveData * data = reinterpret_cast<ArchiveData *>(client_data);
    if (!data->stream)
        return 0;
    *buff = data->current;
    data->stream.read(data->current, 16384);
    return data->stream.gcount();
}

static int myclose(struct archive *a, void *client_data)
{
    ArchiveData * data = reinterpret_cast<ArchiveData *>(client_data);
    delete data;
    return ARCHIVE_OK;
}

static bool list_archive(std::streambuf * streambuf,
                         std::function<bool (const std::string & filename,
                                             struct archive * a,
                                             struct archive_entry * entry)> cb)
{
    struct archive_entry *entry;
    struct archive * a = archive_read_new();
    Scope_Exit(archive_read_finish(a));

    archive_read_support_compression_all(a);
    archive_read_support_format_all(a);
    archive_read_open(a, new ArchiveData(streambuf), NULL, myread, myclose);
    while (archive_read_next_header(a, &entry) == ARCHIVE_OK) {
        string filename = archive_entry_pathname(entry);

        if (!cb(filename, a, entry))
            return false;
    }

    return true;
}


bool iterateArchive(std::streambuf * archive,
                    const OnUriObject & onObject)
{
    auto onArchiveEntry = [&] (const std::string & filename,
                               struct archive * a,
                               struct archive_entry * entry)
        {
            auto filetype = archive_entry_filetype(entry);

            switch (filetype) {
            case AE_IFREG: {
                auto info = std::make_shared<FsObjectInfo>();
                info->size
                    = archive_entry_size_is_set(entry) ?
                    archive_entry_size(entry) : -1;
                info->exists = true;
                if (archive_entry_mtime_is_set(entry)) {
                    info->lastModified = Date::fromSecondsSinceEpoch(archive_entry_mtime(entry) + 0.000000001 * archive_entry_mtime_nsec(entry));
                }
                else info->lastModified = Date::notADate();

                info->ownerId = std::to_string(archive_entry_uid(entry));
                const char * gname = archive_entry_gname(entry);
                if (gname)
                    info->ownerName = gname;
                //info.permissions = archive_entry_strmode(entry);

                auto open = [=] (const std::map<std::string, std::string> & options)
                    {
                        // For the moment, copy into a buffer and return
                        // a stringstream.  Streaming support later.
                            
                        std::ostringstream stream;
                            
                        size_t size = 0;
                        // This is a type exported by libarchive
                        __LA_INT64_T offset = 0;
                        const char * buff;
                        int r = archive_read_data_block(a, (const void **)&buff,
                                                        &size, &offset);
          
                        while (r != ARCHIVE_EOF) {
                            if (r < ARCHIVE_OK)
                                throw MLDB::Exception("Error extracting file");
                            stream.write(buff, size);
                            r = archive_read_data_block
                                (a, (const void **)&buff,
                                 &size, &offset);
                        }

                        std::shared_ptr<std::istream> result
                        (new std::istringstream(stream.str()));
                        return UriHandler(result->rdbuf(), result, info);
                    };

                return onObject(filename, *info, open, 1 /* depth */);
            }
            case AE_IFDIR: 
                //cerr << "*** got directory " << filename << endl;
                // for the moment, skip directories
            default:
                // Skip everything else
                return true;
            }
        };

    return list_archive(archive, onArchiveEntry);
}


struct ArchiveUrlFsHandler: UrlFsHandler {

    ArchiveUrlFsHandler()
    {
    }

    virtual FsObjectInfo getInfo(const Url & url) const
    {
        auto info = tryGetInfo(url);
        if (!info)
            throw MLDB::Exception("Couldn't get URI info for archive " + url.toString());
        return info;
    }

    virtual FsObjectInfo tryGetInfo(const Url & url) const
    {
        Utf8String archiveSource(url.toDecodedString());
        if (!archiveSource.removePrefix("archive+"))
            throw MLDB::Exception("archive URI '" + url.toString() + "' doesn't start with 'archive+' when getting object info");

        // Look for a # to get the filename
        auto it = archiveSource.rfind('#');
        if (it == archiveSource.end())
            throw MLDB::Exception("Extracting a file from an archive requires a # between archive URI and path within archive");

        Utf8String archiveUri(archiveSource.begin(), it);

        // unused...
        Utf8String toExtractPath(std::next(it), archiveSource.end());

        FsObjectInfo result;

        OnUriObject onObject = [&] (const std::string & archiveMemberUri,
                                    const FsObjectInfo & info,
                                    const OpenUriObject & open,
                                    int depth)
            {
                if (url.toString() == archiveMemberUri) {
                    result = info;
                    return false;
                }
                return true;
            };

        this->forEach(Url("archive+" + archiveUri), onObject, {},
                      "/" /* delimiter */, "" /* startAt */);
        
        return result;
    }

    virtual size_t getSize(const Url & url) const
    {
        return getInfo(url).size;
    }

    virtual std::string getEtag(const Url & url) const
    {
        return getInfo(url).etag;
    }

    virtual void makeDirectory(const Url & url) const
    {
        throw MLDB::Exception("Archive URIs don't support creating directories");
    }

    virtual bool erase(const Url & url, bool throwException) const
    {
        throw MLDB::Exception("Archive URIs don't support DELETE");
    }

    /** For each object under the given prefix (object or subdirectory),
        call the given callback.
    */
    virtual bool forEach(const Url & prefix,
                         const OnUriObject & onObject,
                         const OnUriSubdir & onSubdir,
                         const std::string & delimiter,
                         const std::string & startAt) const
    {
        Utf8String archiveSource(prefix.toString());
        if (!archiveSource.removePrefix("archive+"))
            throw MLDB::Exception("archive URI '" + archiveSource.rawString() + "' doesn't start with 'archive+' when listing archive contents");

        filter_istream archiveStream(archiveSource.rawString());

        auto onObject2 = [&] (const std::string & object,
                              const FsObjectInfo & info,
                              const OpenUriObject & open,
                              int depth)
            {
                return onObject(prefix.toString() + "#" + object, info, open, depth);
            };


        return iterateArchive(archiveStream.rdbuf(), onObject2);
    }
};

/** Register Archive with the filter streams API so that a filter_stream can be
    used to treat an Archive object as a simple stream.
*/
struct RegisterArchiveHandler {

    static UriHandler
    getArchiveHandler(const std::string & scheme,
                      const std::string & resource,
                      std::ios_base::open_mode mode,
                      const std::map<std::string, std::string> & options,
                      const OnUriHandlerException & onException)
    {
        if (mode != ios::in) {
            throw MLDB::Exception("Only input is accepted for archives");
        }

        Utf8String uri = scheme + "://" + resource;

        cerr << "uri = " << uri << endl;

        Utf8String archiveSource = uri;
        if (!archiveSource.removePrefix("archive+"))
            throw MLDB::Exception("archive URI '" + uri.rawString() + "' doesn't start with 'archive+' when opening archive member");

        //cerr << "archiveSource = " << archiveSource << endl;

        // Look for a # to get the filename
        auto foundIt = archiveSource.end();
        for (auto it = archiveSource.begin(), end = archiveSource.end();
             it != end;  ++it)
            if (*it == '#')
                foundIt = it;
        
        if (foundIt == archiveSource.end())
            throw MLDB::Exception("Extracting a file from an archive requires a # between archive URI and path within archive");

        Utf8String archiveUri(archiveSource.begin(), foundIt);

        //cerr << "archiveUri = " << archiveUri << endl;

        // unused...
        Utf8String toExtractPath(std::next(foundIt), archiveSource.end());

        //cerr << "uri = " << uri << " archiveSource = " << archiveSource
        //     << " archiveUri = " << archiveUri << " toExtractPath = "
        //     << toExtractPath << endl;

        UriHandler result;

        OnUriObject onObject = [&] (const std::string & archiveMemberUri,
                                    const FsObjectInfo & info,
                                    const OpenUriObject & open,
                                    int depth)
            {
                //cerr << "matching " << archiveMemberUri
                //<< " against " << uri << endl;
                if (uri.rawString() == archiveMemberUri) {
                    result = open(options);
                    return false;
                }
                return true;
            };

        forEachUriObject("archive+" + archiveUri.rawString(), onObject, {},
                         "/" /* delimiter */, "" /* startAt */);
        if (!result.buf)
            throw MLDB::Exception("Couldn't find resource " + toExtractPath.rawString()
                                + " in archive " + archiveUri.rawString());
        
        return result;
    }

    RegisterArchiveHandler()
    {
        registerUriHandler("archive", getArchiveHandler);
        registerUrlFsHandler("archive", new ArchiveUrlFsHandler());
    }

} registerArchiveHandler;


} // namespace MLDB

