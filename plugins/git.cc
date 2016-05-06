// This file is part of MLDB. Copyright 2015 Datacratic. All rights reserved.

/** git.cc
    Jeremy Barnes, 14 November 2015
    Copyright (c) Datacratic Inc.  All rights reserved.
*/

#include "mldb/core/procedure.h"
#include "mldb/core/dataset.h"
#include "mldb/server/per_thread_accumulator.h"
#include "mldb/types/url.h"
#include "mldb/types/structure_description.h"
#include "mldb/types/vector_description.h"
#include "mldb/types/any_impl.h"
#include "mldb/vfs/fs_utils.h"
#include "mldb/base/scope.h"
#include "mldb/jml/stats/distribution.h"
#include "mldb/base/parallel.h"
#include <boost/algorithm/string.hpp>
#include "mldb/http/http_exception.h"

#include <git2.h>
#include <git2/revwalk.h>
#include <git2/commit.h>
#include <git2/diff.h>

#define LIBGIT2_INT_VERSION (LIBGIT2_VER_MAJOR * 10000 \
                             + LIBGIT2_VER_MINOR * 100 \
                             + LIBGIT2_VER_REVISION)

/* libgit2 renamed a bunch of functions and defines between 0.19 and 0.22
 * We do the mapping here so the code below works on both versions
 */
#if LIBGIT2_INT_VERSION < 2200
#define git_libgit2_init git_threads_init
#define git_libgit2_shutdown git_threads_shutdown
#define git_checkout_options git_checkout_opts
#define git_diff git_diff_list
#define GIT_CHECKOUT_OPTIONS_INIT GIT_CHECKOUT_OPTS_INIT
#define GIT_EUNBORNBRANCH GIT_EORPHANEDHEAD

struct git_diff_stats {
    git_diff_stats()
        : files_changed(0), insertions(0), deletions(0)
    {
    }

    size_t files_changed;
    size_t insertions;
    size_t deletions;
};

void git_diff_stats_free(git_diff_stats * stats)
{
    delete stats;
}

int stats_each_file_cb(const git_diff_delta *delta,
                       float progress,
                       void *payload)
{
    git_diff_stats * stats = (git_diff_stats *)payload;
    ++stats->files_changed;
    return 0;
}

int stats_each_hunk_cb(const git_diff_delta *delta,
                       const git_diff_range *hunk,
                       const char *header,
                       size_t header_len,
                       void *payload)
{
    git_diff_stats * stats = (git_diff_stats *)payload;
    stats->insertions += hunk->new_lines;
    stats->deletions += hunk->old_lines;
    return 0;
}

int git_diff_get_stats(git_diff_stats **out, git_diff *diff)
{
    *out = new git_diff_stats();

    int error = git_diff_foreach(diff,
                                 stats_each_file_cb,
                                 stats_each_hunk_cb,
                                 nullptr,
                                 *out);

    if (error < 0) {
        delete *out;
        *out = 0;
    }

    return error;
}


size_t git_diff_stats_insertions(const git_diff_stats *stats)
{
    return stats->insertions;
}

size_t git_diff_stats_deletions(const git_diff_stats *stats)
{
    return stats->deletions;
}

size_t git_diff_stats_files_changed(const git_diff_stats *stats)
{
    return stats->files_changed;
}

struct GitFileOperation {
    GitFileOperation()
        : insertions(0), deletions(0)
    {
    }

    int insertions;
    int deletions;
    std::string op;
};

struct GitFileStats {
    GitFileStats()
        : insertions(0), deletions(0)
    {
    }

    std::map<std::string, GitFileOperation> files;
    int insertions;
    int deletions;
};

int stats_by_file_each_file_cb(const git_diff_delta *delta,
                               float progress,
                               void *payload)
{
    GitFileStats & stats = *((GitFileStats *)payload);
    GitFileOperation op;
    switch (delta->status) {
    case GIT_DELTA_UNMODIFIED: /** no changes */
        return 0;
    case GIT_DELTA_ADDED:  /** entry does not exist in old version */
        op.op = "added";
        break;
    case GIT_DELTA_DELETED:	  /** entry does not exist in new version */
        op.op = "deleted";
        break;
    case GIT_DELTA_MODIFIED:   /** entry content changed between old and new */
        op.op = "modified";
        break;
    case GIT_DELTA_RENAMED:    /** entry was renamed between old and new */
        op.op = "renamed";
        break;
    case GIT_DELTA_COPIED:     /** entry was copied from another old entry */
        op.op = "copied";
        break;
    case GIT_DELTA_IGNORED:    /** entry is ignored item in workdir */
        return 0;
    case GIT_DELTA_UNTRACKED:  /** entry is untracked item in workdir */
        return 0;
    case GIT_DELTA_TYPECHANGE: /** type of entry changed between old and new */
        return 0;
    default:
        throw std::logic_error("git status");
    }

    if (delta->old_file.path)
        stats.files[delta->old_file.path] = op;

    return 0;
}

int stats_by_file_each_hunk_cb(const git_diff_delta *delta,
                               const git_diff_range *hunk,
                               const char *header,
                               size_t header_len,
                               void *payload)
{
    GitFileStats & stats = *((GitFileStats *)payload);
    if (delta->old_file.path)
        stats.files[delta->old_file.path].deletions += hunk->old_lines;
    if (delta->new_file.path)
        stats.files[delta->new_file.path].insertions += hunk->new_lines;
    stats.insertions += hunk->new_lines;
    stats.deletions += hunk->old_lines;
    return 0;
}

GitFileStats git_diff_by_file(git_diff *diff)
{
    GitFileStats result;

    int error = git_diff_foreach(diff,
                                 stats_by_file_each_file_cb,
                                 stats_by_file_each_hunk_cb,
                                 nullptr,
                                 &result);

    if (error < 0) {
        throw Datacratic::HttpReturnException(400, "Error traversing diff: "
                                              + std::string(giterr_last()->message));
    }

    return result;
}


#endif


using namespace std;
using Datacratic::getDefaultDescription;

namespace Datacratic {
namespace MLDB {


/*****************************************************************************/
/* GIT IMPORTER                                                              */
/*****************************************************************************/

struct GitImporterConfig : ProcedureConfig {
    static constexpr const char * name = "import.git";

    GitImporterConfig()
        : revisions({"HEAD"}), importStats(false), importTree(false),
          ignoreUnknownEncodings(true)
    {
        outputDataset.withType("sparse.mutable");
    }

    Url repository;
    PolyConfigT<Dataset> outputDataset;
    std::vector<std::string> revisions;
    bool importStats;
    bool importTree;
    bool ignoreUnknownEncodings;

    // TODO
    // when
    // where
    // limit
    // offset
    // select (instead of importStats, importTree)
};

DECLARE_STRUCTURE_DESCRIPTION(GitImporterConfig);

DEFINE_STRUCTURE_DESCRIPTION(GitImporterConfig);

GitImporterConfigDescription::
GitImporterConfigDescription()
{
    addField("repository", &GitImporterConfig::repository,
             "Git repository to load from.  This is currently limited to "
             "file:// urls which point to an already cloned repository on "
             "local disk.  Remote repositories will need to be checked out "
             "beforehand using the git command line tools.");
    addField("outputDataset", &GitImporterConfig::outputDataset,
             "Output dataset for result.  One row will be produced per commit. "
             "See the documentation for the output format.",
             PolyConfigT<Dataset>().withType("sparse.mutable"));

    std::vector<std::string> defaultRevisions = { "HEAD" };
    addField("revisions", &GitImporterConfig::revisions,
             "Revisions to load from Git (eg, HEAD, HEAD~20..HEAD, tags/*). "
             "See the gitrevisions (7) documentation.  Default is all revisions "
             "reachable from HEAD", defaultRevisions);
    addField("importStats", &GitImporterConfig::importStats,
             "If true, then import the stats (number of files "
             "changed, lines added and lines deleted)", false);
    addField("importTree", &GitImporterConfig::importTree,
             "If true, then import the tree (names of files changed)", false);
    addField("ignoreUnknownEncodings",
             &GitImporterConfig::ignoreUnknownEncodings,
             "If true (default), ignore commit messages with unknown encodings "
             "(supported are ISO-8859-1 and UTF-8) and replace with a "
             "placeholder.  If false, messages with unknown encodings will "
             "cause the commit to abort.");

    addParent<ProcedureConfig>();
}

struct GitImporter: public Procedure {

    GitImporter(MldbServer * owner,
                PolyConfig config_,
                const std::function<bool (const Json::Value &)> & onProgress)
        : Procedure(owner)
    {
        config = config_.params.convert<GitImporterConfig>();
    }

    GitImporterConfig config;

    std::string encodeOid(const git_oid & oid) const
    {
        char shortsha[10] = {0};
        git_oid_tostr(shortsha, 9, &oid);
        return string(shortsha);
    };

    // Process an individual commit
    std::vector<std::tuple<ColumnName, CellValue, Date> >
    processCommit(git_repository * repo, const git_oid & oid) const
    {
        string sha = encodeOid(oid);

        auto checkError = [&] (int error, const char * msg)
            {
                if (error < 0)
                    throw HttpReturnException(500, string(msg) + ": "
                                              + giterr_last()->message,
                                              "repository", config.repository,
                                              "commit", string(sha));
            };


        git_commit *commit;
        int error = git_commit_lookup(&commit, repo, &oid);
        checkError(error, "Error getting commit");
        Scope_Exit(git_commit_free(commit));


        const char *encoding           = git_commit_message_encoding(commit);
        const char *messageStr         = git_commit_message(commit);
        git_time_t time                = git_commit_time(commit);
        int offset_in_min              = git_commit_time_offset(commit);
        const git_signature *committer = git_commit_committer(commit);
        const git_signature *author    = git_commit_author(commit);
        //const git_oid *tree_id         = git_commit_tree_id(commit);
        git_diff *diff = nullptr;
        Scope_Exit(git_diff_list_free(diff));

        Utf8String message;
        if (!encoding || strcmp(encoding, "UTF-8") == 0) {
            message = Utf8String(messageStr);
        }
        else if (strcmp(encoding,"ISO-8859-1") == 0) {
            message = Utf8String::fromLatin1(messageStr);
        }
        else if (config.ignoreUnknownEncodings) {
            message = "<<<couldn't decode message in "
                + string(encoding) + " character set>>>";
        }
        else {
            throw HttpReturnException(500,
                                      "Can't decode unknown commit message encoding",
                                      "repository", config.repository,
                                      "commit", string(sha),
                                      "encoding", encoding);
        }

        vector<string> parents;

        unsigned int parentCount = git_commit_parentcount(commit);
        for (unsigned i = 0;  i < parentCount;  ++i) {
            const git_oid *nth_parent_id = git_commit_parent_id(commit, i);
            git_commit *nth_parent = nullptr;
            int error = git_commit_parent(&nth_parent, commit, i);

            checkError(error, "Error getting commit parent");

            Scope_Exit(git_commit_free(nth_parent));

            parents.emplace_back(encodeOid(*nth_parent_id));

            if (i == 0 && parentCount == 1
                && (config.importStats || config.importTree)) {
                const git_oid * parent_tree_id = git_commit_tree_id(nth_parent);
                if (parent_tree_id) {
                    git_tree * tree = nullptr;
                    git_tree * parentTree = nullptr;

                    error = git_commit_tree(&tree, commit);
                    checkError(error, "Error getting commit tree");
                    Scope_Exit(git_tree_free(tree));


                    error = git_commit_tree(&parentTree, nth_parent);
                    checkError(error, "Error getting parent tree");
                    Scope_Exit(git_tree_free(parentTree));

                    error = git_diff_tree_to_tree(&diff, repo, tree, parentTree, NULL);
                    checkError(error, "Error diffing commits");

                    git_diff_find_options opts = GIT_DIFF_FIND_OPTIONS_INIT;
                    opts.flags = GIT_DIFF_FIND_RENAMES |
                        GIT_DIFF_FIND_COPIES |
                        GIT_DIFF_FIND_FOR_UNTRACKED;

                    error = git_diff_find_similar(diff, &opts);

                    checkError(error, "Error detecting renames");
                }
            }
        }

        Date timestamp = Date::fromSecondsSinceEpoch(time + 60 * offset_in_min);

        Utf8String committerName(committer->name);
        Utf8String committerEmail(committer->email);
        Utf8String authorName(author->name);
        Utf8String authorEmail(author->email);

        std::vector<std::tuple<ColumnName, CellValue, Date> > row;
        row.emplace_back(ColumnName("committer"), committerName, timestamp);
        row.emplace_back(ColumnName("committerEmail"), committerEmail, timestamp);
        row.emplace_back(ColumnName("author"), authorName, timestamp);
        row.emplace_back(ColumnName("authorEmail"), authorEmail, timestamp);
        row.emplace_back(ColumnName("message"), message, timestamp);
        row.emplace_back(ColumnName("parentCount"), parentCount, timestamp);

        for (auto & p: parents)
            row.emplace_back(ColumnName("parent"), p, timestamp);

        int filesChanged = 0;
        int insertions = 0;
        int deletions = 0;

        if (diff) {
            GitFileStats stats = git_diff_by_file(diff);

            filesChanged = stats.files.size();
            insertions = stats.insertions;
            deletions = stats.deletions;

            row.emplace_back(ColumnName("insertions"), insertions, timestamp);
            row.emplace_back(ColumnName("deletions"), deletions, timestamp);
            row.emplace_back(ColumnName("filesChanged"), filesChanged, timestamp);

            for (auto & f: stats.files) {
                if (!config.importTree) break;

                row.emplace_back(ColumnName("file"), f.first, timestamp);

                if (f.second.insertions > 0)
                    row.emplace_back(ColumnName("file." + f.first + ".insertions"),
                                     f.second.insertions, timestamp);
                if (f.second.deletions > 0)
                    row.emplace_back(ColumnName("file." + f.first + ".deletions"),
                                     f.second.deletions, timestamp);
                if (!f.second.op.empty())
                    row.emplace_back(ColumnName("file." + f.first + ".op"),
                                     f.second.op, timestamp);
            }
        }

        //cerr << "id " << sha << " had " << filesChanged << " changes, "
        //     << insertions << " insertions and " << deletions << " deletions "
        //     << message << " parents " << parentCount << endl;

        return row;
    }

    virtual RunOutput run(const ProcedureRunConfig & run,
                          const std::function<bool (const Json::Value &)> & onProgress) const
    {
        auto runProcConf = applyRunConfOverProcConf(config, run);
        auto checkError = [&] (int error, const char * msg)
            {
                if (error < 0)
                    throw HttpReturnException(500, string(msg) + ": "
                                              + giterr_last()->message,
                                              "repository", runProcConf.repository);
            };

        git_libgit2_init();
        Scope_Exit(git_libgit2_shutdown());

        git_repository * repo;

        Utf8String repoName(runProcConf.repository.toString());
        repoName.removePrefix("file://");

        int error = git_repository_open(&repo, repoName.rawData());
        checkError(error, "Error opening git repository");
        Scope_Exit(git_repository_free(repo));

        // Create the output dataset
        std::shared_ptr<Dataset> output;
        if (!runProcConf.outputDataset.type.empty()
            || !runProcConf.outputDataset.id.empty()) {
            output = createDataset(server, runProcConf.outputDataset,
                                   nullptr, true /*overwrite*/);
        }

        git_revwalk *walker;
        error = git_revwalk_new(&walker, repo);
        checkError(error, "Error creating commit walker");
        Scope_Exit(git_revwalk_free(walker));

        for (auto & r: runProcConf.revisions) {
            if (r.find("*") != string::npos)
                error = git_revwalk_push_glob(walker, r.c_str());
            else if (r.find("..") != string::npos)
                error = git_revwalk_push_range(walker, r.c_str());
            else error = git_revwalk_push_ref(walker, r.c_str());

            if (error < 0)
                throw HttpReturnException(500, "Error adding revision: "
                                          + string(giterr_last()->message),
                                          "repository", runProcConf.repository,
                                          "revision", r);
        }
        vector<git_oid> oids;

        git_oid oid;
        while (!git_revwalk_next(&oid, walker)) {
            oids.push_back(oid);
        }

        struct Accum {
            Accum(const Utf8String & filename)
            {
                rows.reserve(1000);

                int error = git_repository_open(&repo, filename.rawData());
                if (error < 0)
                    throw HttpReturnException(400, "Error opening Git repo: "
                                              + string(giterr_last()->message));

            }

            ~Accum()
            {
                git_repository_free(repo);
            }

            std::vector<std::pair<RowName, std::vector<std::tuple<ColumnName, CellValue, Date> > > > rows;

            git_repository * repo;
        };

        PerThreadAccumulator<Accum> accum([&] () { return new Accum(repoName); });

        cerr << "processing " << oids.size() << " commits" << endl;

        auto doProcessCommit = [&] (int i)
            {
                if (i && i % 100 == 0)
                    cerr << "imported commit " << i << " of " << oids.size() << endl;

                Accum & threadAccum = accum.get();
                auto row = processCommit(repo, oids[i]);
                threadAccum.rows.emplace_back(RowName(encodeOid(oids[i])),
                                              std::move(row));

                if (threadAccum.rows.size() == 1000) {
                    output->recordRows(threadAccum.rows);
                    threadAccum.rows.clear();
                }
            };

        parallelMap(0, oids.size(), doProcessCommit);

        for (auto & t: accum.threads) {
            output->recordRows(t->rows);
        }

        output->commit();

        RunOutput result;

        return result;
    }

    virtual Any getStatus() const
    {
        return Any();
    }

    GitImporterConfig procConfig;
};

RegisterProcedureType<GitImporter, GitImporterConfig>
regGit(builtinPackage(),
       "Import a Git repository's metadata into MLDB",
       "procedures/GitImporter.md.html");


} // namespace MLDB
} // namespace Datacratic
