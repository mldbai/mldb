# The MLDB Development Process

MLDB development generally occurs along the following lines:

* An MLDB user asks what boils down to “how can I do X?”
    * this can take the form “(I wanted to get X so) I did Y but Z happened (instead)”
* A few scenarios ensue:
    * if X is not currently possible and
        * is not in scope for MLDB, nothing is done
        * is in scope for MLDB: features should be added to make X possible
    * if X is currently possible and
        * is not documented: documentation should be written
        * is documented: the documentation may need to be modified/expanded/clarified
        * is not a good way to do things: features should be added to make X easier
    * if X should be possible but isn’t due to a bug
        * the bug should be corrected
    * if Y is not the right way to get X and/or Z is the intended outcome of Y
        * the documentation should be clarified
    * if Z is the intended outcome of Y but shouldn’t be
        * Z should lead to X
    * etc...

The MLDB Team works by burning down a backlog of JIRA issues, each with a tightly-defined “Definition of Done” (DOD) carefully written and agreed upon to capture design decisions taken such that the product remains a coherent whole. This backlog becomes confusing and hard to manage if raw user input (“how can I do X?”) is mixed in with issues with agreed-upon DODs (e.g. “feature Y is behaves like Z, such that X is possible”). In order to efficiently work with a backlog and to simultaneously capture user feedback in a structured way, the MLDB Team uses two JIRA projects: one for feedback ([MLDBFB](https://datacratic.atlassian.net/secure/RapidBoard.jspa?rapidView=86&view=planning.nodetail)) and one as a backlog ([MLDB](https://datacratic.atlassian.net/secure/RapidBoard.jspa?rapidView=74&view=planning.nodetail)).

## MLDBFB JIRA Project: Questions, feedback, suggestions, bug reports, feature requests

* All user feedback should be captured as issues under the [MLDBFB JIRA project](https://datacratic.atlassian.net/secure/RapidBoard.jspa?rapidView=86&view=planning.nodetail), assigned to the default automatic assignee (which is the MLDB Product Owner), and may contain *suggested* DODs for eventual follow-on backlog items.
* The only important fields in an MLDBFB issue are the summary and description, there is no need to specify the type, priority, version etc
* The MLDB team should promptly respond to user feedback and help the user accomplish their task by providing useful responses such as pointers to the appropriate documentation, or workarounds
* MLDBFB issues may or may not lead to new backlog issues being created under the MLDB project.
* MLDBFB issues may be accompanied by a test case which can be PR'ed towards the `master` branch (see test case section below)

## MLDB JIRA Project: The Backlog and Definition of Done (DOD)

* The backlog of work to do on MLDB should be captured as issues under the [MLDB JIRA project](https://datacratic.atlassian.net/secure/RapidBoard.jspa?rapidView=74&view=planning.nodetail) and, in general, should be created by the MLDB Product Owner or an MLDB Tech Lead.
* If it’s not clear who the assignee should be, the issue should be left unassigned
* The only important fields in an MLDB issue are the summary and description, there is no need to specify the type, priority, version etc
* Issues under the MLDB project must always have a clear Definition of Done (DOD) in the description:
    * If the DOD is user-visible, it should either be written or +1'ed by the Product Owner
    * The preferred DOD is a test case (see test case section below), although a narrative explanation of the desired end state is acceptable.
    * A good DOD specifically calls out when specific performance characteristics are important
    * A set of steps to reproduce a bug is not a complete DOD, the DOD should include a clear positive statement of the state in which the system should be when the issue can be closed, such that someone can easily check if the system is in that state (i.e. “when a user runs through these steps, this happens”).
    * If no test case is included as part of the DOD, one should be created (see test case section below)
    * If the DOD describes new user-visible features, they should be documented
* Issues under the MLDB project should have, where reasonable, a Rationale section which explains the "why" behind the DOD, if this is not obvious. This could take the form of a link to an issue in the MLDBFB project, for example.

## Working on Issues in the Backlog (i.e. MLDB JIRA Project)

* Issues in the Backlog ideally transition from Unassigned to Assigned to In-Progress to Resolved to Closed
* **Unassigned/Assigned**: The MLDB Product Owner generally writes DODs and assigns issues to an MLDB Team Member.
* **In-Progress**: The MLDB Team Member assigned to an issue marks it In-Progress when the issue is actively being worked on.
* **Resolved**: The MLDB Team Member assigned to an issue marks it as resolved when the MLDB Team Member deems that the DOD is met by the state of the code in the `master` branch (i.e. all Pull Requests have been merged, see below).
  * The primary goal of the Pull Request process is to ensure that the rest of the MLDB Team is comfortable with the way the DOD was met. Discussions about the DOD should occur in the JIRA comments.
   * If the DOD included a test case that was marked as manual/expected-failure, the test case should no longer be manual/expected-failure when the issue is resolved
* **Closed**: The MLDB Product Owner validates that the DOD has been met on issues marked as resolved and either closes the issue or reopens it with a clarified/expanded DOD

## Committing & Pushing code

* Commits related to JIRA issues should include the JIRA issue ID in the commit message
* Non-trivial changes to the `mldb` repo or its dependencies (e.g. `pymldb`, `mldbpro` etc) should go through a [Pull-Request workflow](https://help.github.com/articles/using-pull-requests/) rather than being pushed directly onto the master branch
  1. for a JIRA issue with ID MLDB-999 which deals with "thing x", a branch should be created named something like `MLDB-999_thing_x` (e.g. with `git branch MLDB-999_thing_x && git checkout MLDB-999_thing_x`)
  2. commits should be added to this branch, and the branch pushed to Github
  3. a Pull-Request should be created on Github (the PR name should contain the JIRA issue ID), and not merged until another MLDB Team Member gives a "+1" in a comment, and no one gives a "-1"
  4. After a Pull-Request has been merged (not squash-merged, otherwise we lose issue-to-PR traceability), its feature-branch should be deleted
* Before pushing code to any branch, `make compile && make test` should run cleanly after an `rm -rf build`
* After pushing code, if our [Jenkins continuous integration system](http://gorn.datacratic.com:8080/job/MLDB/) reports test failures, those should be repaired (even if the tests pass locally)
  * The CI system only builds and tests the tip of every commit in Github, as polled every few minutes. It is not necessary that every commit in the history build cleanly

## Writing a test case

Test cases should be named after a JIRA issue (MLDB or MLDBFB) and should be PR'ed into the `master` branch, marked as `manual`. This will help the Product Owner keep track of what bugs are being reported and will give the team a chance to give feedback on the test case.

For a JIRA issue with ID [MLDBFB-336 and description "sample test"](https://datacratic.atlassian.net/browse/MLDBFB-336) (please create a relevant issue if none exists):

1. create a branch with a name like `MLDBFB-336-sample_test`
1. under the `mldb/testing` directory, create a file with a name like `MLDBFB-336-sample_test.py`
1. in that file, write some python code like [this sample](https://github.com/mldbai/mldb/blob/master/testing/MLDBFB-336-sample_test.py)

    1. **Important Note** if your test causes files to be created (i.e. dataset or model files) please name those files after your test and place them in the `tmp` directory (i.e. use `file://tmp/MLDBFB-336_sample.beh` rather than `file://sample.beh`)
1. add a line in `mldb/testing/mldb_testing.mk` which will add the test to the build system in manual mode, like this:

  ```
  $(eval $(call mldb_unit_test,MLDBFB-336-sample_test.py,,manual))
  ```

1. run `make MLDBFB-336-sample_test.py` to ensure that your testcase behaves the appropriate way (i.e. fails for the right reason)
1. add, commit and push your changes
1. go back to JIRA and mention the test name `MLDBFB-336-sample_test.py` in your issue MLDBFB-999 so that the assignee knows what the test case is
1. submit a Pull Request


More information about python unittest framework can be found at https://docs.python.org/2/library/unittest.html.

If your test file contains multiple tests, it is possible to run a specific one by adding arguments to the make command. In the previous example, we could use the following command to only run `SampleTest.test_select_x_works`.

```
make MLDBFB-336-sample_test.py ARGS="--run-script '[\"SampleTest.test_select_x_works\"]'"
```

If you need to validate query results, a helper function is available: `assertQueryResult`. It works on both flat results, obtained from `mldb.query`, and object results obtained from `mldb.get('/v1/query', q=query).json()`.

## Documentation

* internal development/release documentation such as this document should live in `<repo_root>/dev_doc`
* user-facing documentation should live in `<repo_root>/mldb/doc` (this is a symlink into `<repo_root>/mldb/container_files`)
   * diagrams included in user-facing documentation should ideally be assembled using Google Drawings and stored in the appropriate [Google Drive directory](https://drive.google.com/drive/u/0/folders/0B9t7IqAF0xkFTXJ4R1gySkFMdkU) so that they can later be edited by other team members
   * diagrams should be scaled to the correct size within Google Drawings and saved as SVGs into `<repo_root>/mldb/doc/builtin/img`
   * after saving, the `mungeSVG.sh` script in the `img` directory should be run, which will modify the SVGs so that they display at the correct size when referred to in HTML `img` tags instead of stretching to full-width

## Logging

Whenever possible use the logging framework provided in mldb/utils.  It has the advantage that one can turn messages
on or off without recompiling.  This prevents the constant commenting and uncommenting of log messages when debugging.
Note that MLDB's log messages are primarily useful for developers and IT to diagnose problems.  They are not intented
for users.  For user messages use the REST response.

There are four levels of logging:
* `debug` - This could be any message deemed useful to understand the state of a class, of a variable or any other
matter that help debugging.
* `info` - This could be a progress message, the status of an operation and so on.  These messages are shown by
default so they should not be in a loop or happen too often.
* `warn` - Any exceptional situation that is NOT an error.
* `error` - Any error or failure to complete an operation.

By default all messages at the `info` level are outputted by MLDB.  The messages are outputted to cout so that
docker can store them to file.  This can be changed using the MLDB configuration file.  A logging statement looks
like this:

```
logger->info() << "completed operation x in " << secs << " seconds;
```

Note that the trailing `endl` is not required and not permitted.

The cost of logging such messages to a level that is disabled is negligeable so you can safely leave logging statements
uncommented.  However, be aware that the cost of evaluating the stream arguments may be expensive.  A call like this

```
logger->debug() << "object is " << jsonEncode(largeStucture);
```

can be expensive even if the `debug` level is disabled.  This is why it is preferable to use the provided macros
when using the logging framework.  The above statement becomes:

```
DEBUG_MSG(logger) << "object is " << jsonEncode(largeStructure);
```

Here the stream arguments will not be evaluated and the call will by many order of magniture faster than
the previous one.
