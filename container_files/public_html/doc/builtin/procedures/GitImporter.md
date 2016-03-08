# Git importer procedure

This procedure allows commit metadata from a local git repository
to be imported.

This allows for exploration and prediction to be performed over Git
repositories.

## Configuration

![](%%config procedure import.git)

The `repository` parameter should point to a *local directory* that
contains a `.git` subdirectory with the git metadata.  It is not
necessary that the local directory be checked out, in other words
a bare repository is acceptable.

The `revision` parameter is a GIT revision specification.  For example,
it can be `HEAD` or `/tags/*` or `/revs/*`.  See the [gitrevisions documentation] (https://www.kernel.org/pub/software/scm/git/docs/gitrevisions.html) for
more details on how a revspec is specified.

## Output format

Each row will contain the following fixed columns:

- `committer`: the full name of the committer
- `committerEmail`: the email address of the committer
- `author`: the full name of the author
- `authorEmail`: the email address of the author
- `message`: the text of the commit message
- `parentCount`: the number of parent revisions.  This will be one for
  normal commits, zero for the initial commit and two or more for
  merge commits.
- `parent`: the commit ID of each of the parent commits.  This can be
  used to traverse the tree of commits.

In addition, if the `importStats` option is true, then non-merge commits
will have the following columns:

- `insertions`: the number of inserted lines in the commit
- `deletions`: the number of deleted lines in the commit
- `filesChanged`: the total number of files changed in the commit

In addition, if the `importTree` option is true, then non-merge commits
will have the following columns for each file that was modified by the
commit:

- `file`: the name of each file modified by this commit.
- `file.<path>.insertions`: the number of lines inserted into the given
  filename.
- `file.<path>.deletions`: the number of lines deleted from the given
  filename.
- `file.<path>.op`: the operation on the file.  This will be one of

  - `added` meaning the file was added in the commit.
  - `deleted` meaning the file was deleted by the commit.
  - `modified` meaning the file was modified by the commit.
  - `renamed` meaning the file was renamed by the commit.
  - `copied` meaning the file was copied by the commit.


## Example

To load the Docker git repository into a dataset that has been checked
out from `https://github.com/docker/docker.git` in `/mldb_data/docker`,
the following procedure can be posted and run:

```python
mldb.put("/v1/procedures/gitimport", {
    "type": "import.git",
    "params": {
        "repository": "file:///mldb_data/docker",
        "outputDataset": "git",
        "runOnCreation": true
    }
})
```

This can then be queried for the top 5 contributors in terms of commits:

```sql
SELECT count(*) AS cnt,
       author,
       min(when({*})) AS earliest,
       max(when({*})) AS latest,
       sum(filesChanged) as changes,
       sum(insertions) as insertions,
       sum(deletions) as deletions
FROM git
GROUP BY author
ORDER BY cnt DESC
LIMIT 5
```

which yields


cnt | author | earliest | latest | changes | insertions | deletion 
----|--------|----------|--------|---------|------------|---------------
1844 | "Michael Crosby" | "2013-06-03T21:39:00Z" | "2015-11-18T11:08:13Z" | 3239 | 71376 | 103630 
1483 | "Victor Vieux" | "2013-04-10T19:30:57Z" | "2015-10-14T17:46:59Z" | 2024 | 62727 | 72644 
1248 | "Solomon Hykes" | "2013-01-19T16:07:19Z" | "2015-11-05T15:22:37Z" | 2144 | 48645 | 127953
744 | "Tianon Gravi" | "2013-04-21T19:19:38Z" | "2015-11-17T09:44:36Z" | 1925 | 144679 | 33214
632 | "Jessie Frazelle" | "2014-09-02T15:18:32Z" | "2015-09-04T13:38:41Z" | 11 | 197 | 186 


