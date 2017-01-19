// This file is part of MLDB. Copyright 2015 mldb.ai inc. All rights reserved.

/** plugin_resource.cc
    Francois Maillet, 18 fevrier 2015
    Copyright (c) 2015 mldb.ai inc.  All rights reserved.

*/

#include "mldb/server/plugin_resource.h"

#include "mldb/ext/libgit2/include/git2.h"
#include "mldb/ext/libgit2/include/git2/clone.h"
#include "mldb/types/structure_description.h"
#include "mldb/vfs/filter_streams.h"

using namespace std;

namespace fs = boost::filesystem;


namespace MLDB {


/*****************************************************************************/
/* PLUGIN STRUCTURES                                                         */
/*****************************************************************************/

DEFINE_STRUCTURE_DESCRIPTION(PackageElementSources);

PackageElementSourcesDescription::
PackageElementSourcesDescription()
{
    addField("main", &PackageElementSources::main,
             "source for the main element of the plugin");
    addField("routes", &PackageElementSources::routes,
             "source for the routes element of the plugin");
    addField("status", &PackageElementSources::status,
             "source for the status element of the plugin");
}


DEFINE_STRUCTURE_DESCRIPTION(PluginResource);

PluginResourceDescription::
PluginResourceDescription()
{
    addField("address", &PluginResource::address,
             "URI or location of script to run (use this parameter OR 'source')");
    PackageElementSources elem;
    addField("source", &PluginResource::source,
             "source of script to run (use this parameter OR 'address')", elem);
    addField("args", &PluginResource::args,
             "Arguments to pass to script");
}


DEFINE_STRUCTURE_DESCRIPTION(ScriptResource);

ScriptResourceDescription::
ScriptResourceDescription()
{
    addField("address", &ScriptResource::address,
             "URI or location of script to run (use this parameter OR 'source')");
    addField("source", &ScriptResource::source,
             "source of script to run (use this parameter OR 'address')");
    addField("args", &ScriptResource::args,
             "Arguments to pass to script");
    addField("writeSourceToFile", &ScriptResource::writeSourceToFile,
             "If passing in the source as a string, write it to disk to a temporary "
              "location as if it was downloaded remotely. This should usually not be "
              "necessary.", false);
}


DEFINE_STRUCTURE_DESCRIPTION(PluginVersion);

PluginVersionDescription::
PluginVersionDescription()
{
    addField("branch", &PluginVersion::branch,
             "branch currently checkout");
    addField("revision", &PluginVersion::revision,
             "revision currently checked out");
    addField("address", &PluginVersion::address,
             "address");
    addField("message", &PluginVersion::message,
             "message");
}

ScriptLanguage parseScriptLanguage(const std::string lang)
{
    if(lang == "python")        return PYTHON;
    if(lang == "javascript")    return JAVASCRIPT;
    throw MLDB::Exception("Unknown language");
}

/*****************************************************************************/
/* PLUGIN RESSOURCE                                                          */
/*****************************************************************************/
    
LoadedPluginResource::
LoadedPluginResource(ScriptLanguage lang, ScriptType type,
                     Utf8String pluginId,
                     const PluginResource & resource) :
    url(resource.address), args(resource.args), MLDB_ROOT("/mldb_data/")
{
    pluginLanguage = lang;
    scriptType = type;

    if(!url.empty() && !resource.source.empty())
        throw MLDB::Exception("Cannot specify both address and source for plugin");

    // if this is a plugin, we need to create a folder in the plugins folder
    // that we'll be keeping around
    if(scriptType == PLUGIN) {
        // if we are running inside of docker. TODO this check is a little flimsy
        if(fs::exists(MLDB_ROOT)) {
            fs::create_directories(MLDB_ROOT / fs::path("plugins"));
            plugin_working_dir = MLDB_ROOT / fs::path("plugins") / fs::path(pluginId.rawString());
            cerr << " PLUGIN INIT. Inside docker. Plugin working dir: " << plugin_working_dir << endl;
        }
        // if not, create a /mldb_data in the working directory to mimic
        // the behaviour of when we'll be dockyfied
        else {
            fs::create_directories(fs::path(".") / MLDB_ROOT / "plugins");
            plugin_working_dir = fs::path(".") / MLDB_ROOT / "plugins" / fs::path(pluginId.rawString());
            cerr << " PLUGIN INIT. NOT inside docker. Plugin working dir: " << plugin_working_dir << endl;
        }
    }
    // if we're a script, create a temporary folder so we can do the checkout if required
    else {
        plugin_working_dir = fs::temp_directory_path() / fs::unique_path();
    }

    // if we're passing in the source
    if(!resource.source.empty() && !resource.source.writeSourceToFile) {
        source = resource.source;
        pluginLocation = SOURCE;
        return;
    }

    // TODO what is the folder already exists?! we could create two different plugins
    // with the same id, which would clash... ?
    if(fs::exists(plugin_working_dir))
        fs::remove_all(plugin_working_dir);

    version.address = url.path();

    auto createPluginDir = [&]
    {
        if(!fs::exists(plugin_working_dir)) {
            if(!fs::create_directories(plugin_working_dir))
                throw MLDB::Exception("Unable to create plugin directory: " +
                        plugin_working_dir.string());
        }

    };


    // if we're passing in source but we want it written to disk
    if(!resource.source.empty() && resource.source.writeSourceToFile) {
        createPluginDir();
        
        filter_ostream ostream(getElementLocation(MAIN));
        ostream << resource.source.main << endl;
        ostream.close();
    }
    // local file
    else if(url.scheme() == "file") {
        // for backwars comtpability and easy of use for pawa uasas,
        // we need to support the user giving us a folder or a file
        pluginLocation = LOCAL_FILE;

        fs::path source = fs::absolute(fs::path(url.path()));
        fs::path symlink;

        if (!fs::exists(source)) {
            throw MLDB::Exception("Source does not exist");
        }

        // if a file at the end, symlink main.xx to that file
        if(!fs::is_directory(source)) {
            if(!fs::create_directories(plugin_working_dir)) {
                throw MLDB::Exception("Unable to create plugin directory: " +
                                    plugin_working_dir.string());
            }
            symlink = fs::path(getElementLocation(MAIN));
        }
        // we're giving the path to a folder, symlink the folder and possibly also symlink. we
        // however force the user to provide a main.py since all existing code at the time of
        // creating this are giving filenames and not folders so this should break nothing
        else {
            symlink = fs::path(plugin_working_dir);
        }

        if(!fs::exists(symlink)) {
            cerr << "creating SYMLINK " << symlink.string() << " -> "
                 << source.string() << endl;
            fs::create_symlink(source, symlink);
        }
        else {
            cerr << "SYMLINK already exists! " << symlink.string() << " -> "
                 << source.string() << endl;
        }
    }
    //http{s}
    else if(url.scheme() == "http" || url.scheme() == "https") {
        pluginLocation = HTTP;

        createPluginDir();

        filter_istream istream(url);
        filter_ostream ostream(getElementLocation(MAIN));
        string line;
        while(getline(istream, line)) {
            ostream << line << endl;
        }

        istream.close();
        ostream.close();
    }
    // gist or git
    else if(url.scheme() == "gist" || url.scheme() == "git") {
        pluginLocation = GIT;
        string urlToClone = "https:" + url.path();

        string commitHash;
        size_t hashLocation = urlToClone.find('#');
        if(hashLocation != -1) {
            commitHash = urlToClone.substr(hashLocation + 1);
            urlToClone = urlToClone.substr(0, hashLocation);
        }

        cerr << MLDB::format("Cloning GIST %s -> %s", urlToClone, plugin_working_dir.string()) << endl;

        git_libgit2_init();

        // example: https://libgit2.github.com/libgit2/ex/HEAD/network/clone.html#git_clone-1
        git_repository * repo;
        git_clone_options clone_opts = GIT_CLONE_OPTIONS_INIT;
        git_checkout_options checkout_opts = GIT_CHECKOUT_OPTIONS_INIT;

        checkout_opts.checkout_strategy = GIT_CHECKOUT_SAFE;
        //   checkout_opts.progress_cb = checkout_progress;
        clone_opts.checkout_opts = checkout_opts;
        //   clone_opts.remote_callbacks.transfer_progress = &fetch_progress;
        //   clone_opts.remote_callbacks.credentials = cred_acquire_cb;
        //   clone_opts.remote_callbacks.payload = &pd;

        int rtn = git_clone(&repo,
                            urlToClone.c_str(),
                            plugin_working_dir.string().c_str(),
                            &clone_opts);
        if(rtn != 0) {
            const git_error *err = giterr_last();
            MLDB_TRACE_EXCEPTIONS(false);
            if (err) throw MLDB::Exception(MLDB::format("Git ERROR %d for %s: %s\n",
                                            err->klass, urlToClone, err->message));
            else throw MLDB::Exception("Git ERROR %d: no detailed info\n", rtn);
        }

        if(commitHash != "") {
            git_object *treeish = NULL;

            int error = git_revparse_single(&treeish, repo, commitHash.c_str());
            if(error != 0 || !treeish) {
                throw MLDB::Exception(MLDB::format("Error checking out commit '%s' for "
                            "repository '%s'", commitHash, urlToClone));
            }

            error = git_checkout_tree(repo, treeish, &checkout_opts);
            git_object_free(treeish);
        }


        //// Get current branch
        git_reference *head = NULL;
        version.branch = "failed to get current branch";

        int error = git_repository_head(&head, repo);

        if (error == GIT_EUNBORNBRANCH || error == GIT_ENOTFOUND) {
            version.branch = "NULL";
        }
        else if (!error) {
            version.branch = git_reference_shorthand(head);
        }
        git_reference_free(head);

        // Dereference HEAD to a commit
        git_object *head_commit;
        error = git_revparse_single(&head_commit, repo, "HEAD^{commit}");
        git_commit *commit = (git_commit*)head_commit;

        // Print some of the commit's properties
        printf("%s", git_commit_message(commit));
        const git_signature *author = git_commit_author(commit);
        printf("%s <%s>\n", author->name, author->email);
        const git_oid *tree_id = git_commit_tree_id(commit);

        char rev[1024] = {0};
        git_oid_tostr(rev, sizeof(rev), tree_id);
        version.revision = rev;

        //  git_time_t time                = git_commit_time(commit);
        version.message = git_commit_message(commit);

        // Cleanup
        git_object_free(head_commit);
        git_commit_free(commit);

        git_libgit2_shutdown();

    }
    else {
        throw MLDB::Exception("Unsupported URL: " + url.toString());
    }
}

LoadedPluginResource::
~LoadedPluginResource()
{
    cleanup();
}

void LoadedPluginResource::
cleanup()
{
    if(scriptType == SCRIPT && fs::exists(plugin_working_dir)) {
        cerr << " Cleaning up " + plugin_working_dir.string() << endl;
        fs::remove_all(plugin_working_dir);
    }
}

string LoadedPluginResource::
getFilenameForErrorMessages() const
{
    return url.toString();
}

string LoadedPluginResource::
getElementFilename(PackageElement elem) const
{
    switch(elem) {
        case MAIN:      return "main";
        case ROUTES:    return "routes";
        case STATUS:    return "status";
    }
    throw MLDB::Exception("Unsupported elem!");
}

string LoadedPluginResource::
getElementLocation(PackageElement elem) const
{
    if(pluginLocation == SOURCE)
        throw MLDB::Exception("no entrypoint for plugin configured with source");

    string extension;

    switch(pluginLanguage) {
    case PYTHON:
        extension = "py";
        break;
    case JAVASCRIPT:
        extension = "js";
        break;
    default:
        throw MLDB::Exception("unknown plugin language");
    }
    
    return (fs::path(plugin_working_dir) / 
            fs::path(getElementFilename(elem) + "." + extension)).string();
}

bool LoadedPluginResource::
packageElementExists(PackageElement elem) const
{
    if(pluginLocation == SOURCE) {
        return !(source.getElement(elem).empty());
    }

    string entrypoint = getElementLocation(elem);
    return fs::exists(entrypoint);
}

Utf8String LoadedPluginResource::
getScript(PackageElement elem) const
{
    if(pluginLocation == SOURCE)
        return source.getElement(elem);

    string filePath = getElementLocation(elem);
    cerr << "loading from: " << filePath << endl;
    filter_istream stream(filePath);
    std::ostringstream out;
    out << stream.rdbuf();
    stream.close();
    return Utf8String(out.str());
}

Utf8String LoadedPluginResource::
getScriptUri(PackageElement elem) const
{
    //if(pluginLocation == SOURCE)
    //    return source.getElementUri(elem);

    string extension;

    switch(pluginLanguage) {
    case PYTHON:
        extension = "py";
        break;
    case JAVASCRIPT:
        extension = "js";
        break;
    default:
        throw MLDB::Exception("unknown plugin language");
    }

    std::string urlStr = url.toString();

    if (urlStr.rfind("." + extension) == urlStr.size() - 1 - extension.size())
        return urlStr;
    
    return urlStr + "/" + getElementFilename(elem) + "." + extension;
}
    
fs::path LoadedPluginResource::
getPluginDir() const
{
    return fs::absolute(plugin_working_dir).normalize();
}



} // namespace MLDB

