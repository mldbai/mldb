// This file is part of MLDB. Copyright 2015 mldb.ai inc. All rights reserved.

/** docker.cc
    Jeremy Barnes, 14 September 2015
    Copyright (c) 2015 mldb.ai inc.  All rights reserved.

    FS handler for dockers, backed by libdocker.
*/


#include "mldb/vfs/fs_utils.h"
#include "mldb/base/scope.h"
#include "mldb/http/http_rest_proxy.h"
#include "mldb/types/vector_description.h"
#include "mldb/types/map_description.h"
#include "mldb/types/annotated_exception.h"
#include "mldb/vfs/filter_streams_registry.h"
#include "mldb/utils/string_functions.h"
#include "mldb/utils/split.h"
#include <unordered_set>
#include "archive.h"

using namespace std;


namespace MLDB {


/** A docker URI we break down to:

    <scheme>://(<registry>)/<owner>/<repo>(:<tag>)?<layer|all>/path/to/file.ext
*/
struct DockerUriComponents {
    DockerUriComponents(const Utf8String & uri)
    {
        auto [scheme, resource, found] = split_on_first(uri, "://");
        if (!found)
            throw AnnotatedException(400, "URI doesn't include a scheme",
                                      "uri", uri);
        
        vector<Utf8String> components;
        MLDB::split(components, resource, '/');

        cerr << "components = " << jsonEncodeStr(components) << endl;

        registry = components.at(0);
        owner = components.at(1);
        Utf8String repoTag = components.at(2);

        std::vector<Utf8String> rt;
        MLDB::split(rt, repoTag, ':');

        tag = "latest";

        if (rt.size() == 1) {
            repo = rt[0];
        }
        else if (rt.size() == 2) {
            repo = rt[0];
            tag = rt[1];
        }
        else throw MLDB::Exception("didn't understand tag name");

        for (unsigned i = 3;  i < components.size();  ++i) {
            object += '/' + components[i];
        }
    }

    std::string scheme;
    Utf8String registry;
    Utf8String owner;
    Utf8String repo;
    Utf8String tag;
    Utf8String object;

    Utf8String registryUri() const
    {
        return "https://" + registry;
    }

    Utf8String repoPath() const
    {
        return "/v1/repositories/" + owner + "/" + repo;
    }

    Utf8String repoUri() const
    {
        Utf8String result = scheme + "://" + registry + "/" + owner + "/" + repo;
        if (!tag.empty())
            result += ":" + tag;
        return result;
    }
};


struct DockerLayersUrlFsHandler: UrlFsHandler {

    HttpRestProxy proxy;

    virtual FsObjectInfo getInfo(const Url & url) const
    {
        auto info = tryGetInfo(url);
        if (!info)
            throw MLDB::Exception("Couldn't get URI info for docker " + url.toString());
        return info;
    }

    virtual FsObjectInfo tryGetInfo(const Url & url) const
    {
        FsObjectInfo result;

#if 0        
        Utf8String dockerSource(url.toString());
        if (!dockerSource.removePrefix("docker@"))
            throw MLDB::Exception("docker doesn't start with docker@");

        // Look for a # to get the filename
        auto it = dockerSource.rfind('#');
        if (it == dockerSource.end())
            throw MLDB::Exception("Extracting a file from an docker requires a # between docker URI and path within docker");

        Utf8String dockerUri(dockerSource.begin(), it);

        // unused...
        Utf8String toExtractPath(std::next(it), dockerSource.end());


        OnUriObject onObject = [&] (const std::string & dockerMemberUri,
                                    const FsObjectInfo & info,
                                    const OpenUriObject & open,
                                    int depth)
            {
                if (url.toString() == dockerMemberUri) {
                    result = info;
                    return false;
                }
                return true;
            };

        this->forEach(Url(dockerUri), onObject, {},
                      "/" /* delimiter */, "" /* startAt */);
#endif
        
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
        throw MLDB::Exception("Docker URIs don't support creating directories");
    }

    virtual bool erase(const Url & url, bool throwException) const
    {
        throw MLDB::Exception("Docker URIs don't support DELETE");
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
        DockerUriComponents c(prefix.toString());

        HttpRestProxy proxy(c.registryUri().extractAscii());
    
        std::string repoPath = c.repoPath().stealAsciiString();

        //proxy.debug = true;

        auto resp = proxy.get(repoPath + "/images");
    
        cerr << resp << endl;

        proxy.setCookieFromResponse(resp);

        auto resp2 = proxy.get((repoPath + "/tags/" + c.tag).uriEncode());
    
        cerr << resp2 << endl;

        string imageId = resp2.jsonBody().asString();

        cerr << "image id = " << imageId << endl;

        auto resp3 = proxy.get("/v1/images/" + imageId + "/ancestry");

        cerr << "resp3 = " << resp3 << endl;

        vector<string> images = jsonDecodeStr(resp3.body(), (vector<string> *)0);

        int layerNum = -1;

        for (auto & image: images) {
            ++layerNum;
            auto resp4 = proxy.get("/v1/images/" + image + "/json");
            auto md = resp4.jsonBody();

            auto info = std::make_shared<FsObjectInfo>();
            info->exists = true;
            info->userMetadata = jsonDecode(md, (map<string, Json::Value> *)0);
            info->size = md["Size"].asUInt();
            info->lastModified = jsonDecode<Date>(md["created"]);

            Utf8String uri = prefix.toString() + "/layer" + MLDB::format("%03d", layerNum);
            string setCookie = resp4.getHeader("set-cookie");
            Utf8String directUri = "https://" + c.registry + "/v1/images/" + image + "/layer";
            info->objectMetadata["directUri"] = directUri;
            info->objectMetadata["cookie"] = setCookie;

            auto open = [&] (const std::map<std::string, std::string> & options)
                {
#if 0
                    cerr << "opening docker layer" << endl;
                    
                    cerr << "directUri = " << directUri << endl;

                    proxy.debug = true;

                    auto resp = proxy.get("/v1/images/" + image + "/layer");

                    cerr << "resp = " << resp << endl;
#endif

                    std::map<std::string, std::string> options2 = options;
                    options2["http-set-cookie"] = setCookie;
                    
                    auto stream = std::make_shared<filter_istream>(directUri, options2);
                    return UriHandler(stream->rdbuf(), stream, info);
                };


            if (!onObject(uri, *info, open, 1 /* depth */))
                return false;
            
        }

        return true;
    }
};

struct DockerUrlFsHandler: UrlFsHandler {

    virtual FsObjectInfo getInfo(const Url & url) const
    {
        auto info = tryGetInfo(url);
        if (!info)
            throw MLDB::Exception("Couldn't get URI info for docker " + url.toString());
        return info;
    }

    virtual FsObjectInfo tryGetInfo(const Url & url) const
    {
        Utf8String dockerSource(url.toDecodedString());
        if (!dockerSource.removePrefix("docker@"))
            throw MLDB::Exception("docker doesn't start with docker@");

        // Look for a # to get the filename
        auto it = dockerSource.rfind('#');
        if (it == dockerSource.end())
            throw MLDB::Exception("Extracting a file from an docker requires a # between docker URI and path within docker");

        Utf8String dockerUri(dockerSource.begin(), it);

        // unused...
        Utf8String toExtractPath(std::next(it), dockerSource.end());

        FsObjectInfo result;

        OnUriObject onObject = [&] (const Utf8String & dockerMemberUri,
                                    const FsObjectInfo & info,
                                    const OpenUriObject & open,
                                    int depth)
            {
                if (url.toString() == dockerMemberUri) {
                    result = info;
                    return false;
                }
                return true;
                
            };

        this->forEach(Url(dockerUri), onObject, {},
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
        throw MLDB::Exception("Docker URIs don't support creating directories");
    }

    virtual bool erase(const Url & url, bool throwException) const
    {
        throw MLDB::Exception("Docker URIs don't support DELETE");
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
        std::unordered_set<Utf8String> doneFiles;
        std::unordered_set<Utf8String> whiteouts;

        // 1.  Open the layers
        auto onLayer = [&] (const Utf8String & uri,
                            const FsObjectInfo & info,
                            const OpenUriObject & open,
                            int depth) -> bool
            {
                // Open the archive
                UriHandler handler = open({});

                auto onObject2 = [&] (const Utf8String & uri,
                                      const FsObjectInfo & info,
                                      const OpenUriObject & open,
                                      int depth)
                {
                    auto [before, after, found] = split_on_first(uri, ".wh.");
                    if (found) {
                        whiteouts.insert(before + after);
                        return true;
                    }
                
                    if (doneFiles.count(uri)) {
                        //cerr << "skipping overwritten file " << uri << endl;
                        //cerr << "old info " << jsonEncode(doneFiles[uri])
                        //     << endl;
                        //cerr << "new info " << jsonEncode(info) << endl;
                        return true;
                    }

                    if (whiteouts.count(uri)) {
                        //cerr << "skipping whited out file " << uri << endl;
                    }

                    // Look up each path component to see if it's blacklisted
                    for (auto pos = uri.find('/');  pos != uri.end();
                         pos = uri.find('/', ++pos)) {
                        Utf8String component(uri.begin(), pos);

                        //cerr << "component = " << component << endl;

                        if (whiteouts.count(component))
                            //cerr << "whited out file " << uri
                            //     << " based on path component "
                            //     << string(uri, 0, pos) << endl;
                        return true;
                    }

                    // Return it
                    bool result = onObject(prefix.toString() + "/" + uri, info, open, depth + 1);

                    doneFiles.emplace(uri);
                    //cerr << "got file in docker archive " << uri << endl;

                    return result;
                };
                
                return iterateArchive(handler.buf, onObject2);
            };

        Utf8String uri = prefix.toString();
        uri.removePrefix("docker://");
        uri = "docker-layers://" + uri;

        forEachUriObject(uri.rawString(), onLayer, nullptr, "/", "" /* startAt */);

        return true;
    }
};

/** Register Docker with the filter streams API so that a filter_stream can be
    used to treat an Docker object as a simple stream.
*/
struct RegisterDockerHandler {

    static UriHandler
    getDockerHandler(const Utf8String & scheme,
                     const Utf8String & resource,
                     std::ios_base::openmode mode,
                     const std::map<std::string, std::string> & options,
                     const OnUriHandlerException & onException)
    {
        if (mode != ios::in) {
            throw MLDB::Exception("Cannot write to docker containers, only read");
        }

        Utf8String uriToFind = scheme + "://" + resource;

        DockerUriComponents c(uriToFind);

        // 1.  Open the docker repo
        static DockerUrlFsHandler handler;

        auto repoUri = c.repoUri();

        UriHandler result;
        
        OnUriObject onObject = [&] (const Utf8String & uri,
                                    const FsObjectInfo & info,
                                    const OpenUriObject & open,
                                    int depth)
            {
                if (uri == uriToFind) {
                    result = open(options);
                    return false;
                }
                return true;
            };
        
        handler.forEach(Url(repoUri), onObject, nullptr, "", "");

        if (!result.buf)
            throw MLDB::Exception("Couldn't find resource " + scheme + "://" + resource
                                + " in docker repo");
        
        return result;
    }

    RegisterDockerHandler()
    {
        registerUriHandler("docker", getDockerHandler);
        registerUrlFsHandler("docker", new DockerUrlFsHandler());
        registerUrlFsHandler("docker-layers", new DockerLayersUrlFsHandler());
    }

} registerDockerHandler;

} // namespace MLDB
