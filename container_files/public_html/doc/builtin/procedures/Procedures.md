# Intro to Procedures

Procedures are named, reusable programs used to implement long-running batch operations with no return values. Procedures generally run over Datasets and can be configured via [SQL expressions](../sql/Sql.md). The outputs of a Procedure can include Datasets and files. Procedures are used to:

* Transform/clean up data
* Train machine learning models (by creating [Functions](../functions/Functions.md))
* Apply machine models in batch mode

## Running a Procedure

Creating a Procedure does not automatically cause it to run unless the `runOnCreation` flag is set. Procedures are run via a REST API call `POST /v1/procedures/<id>/runs {<parameters>}`, where `<parameters>` can override any of the parameters given to the procedure on creation.  For most procedures, it is possible to perform a first run on creation of the procedure by setting the flag `runOnCreation` to true in the parameters.  Refer to the specific procedure documentation to see if it supports it.

## Obtaining results of a procedure

A procedure may return results as follows:

* It may create entities (datasets, functions, other procedures, ...) that
  encapsulate the results of running the procedure.
* It may create artifacts (files on disk or on a remote file system, sftp, ...)
  that contain the outcome of running the procedure.
* Each run of a procedure has a JSON output similar to this:

        {
            "runStarted": "2015-10-21T19:33:00.091Z",
            "state": "finished",
            "runFinished": "2015-10-21T19:33:00.151Z",
            "id": "2015-10-21T19:33:00.090622Z-5bc7042b732cb41f"
        }
        
  When making a synchronous call (default) to create a run, that output is returned in
  the body of the response.  For asynchronous calls, that output
  is available by performing a `GET` on `/v1/procedures/<id>/runs/<id>`.
* In addition, each run has a JSON `details` output, which can be queried by performing a
  `GET` on `/v1/procedures/<id>/runs/<id>/details`.  This output depends on the procedure but it
  may contain a more detailed set of information about what was done including elements like
  logs of messages and errors.

## Getting the progress of a procedure

Once created, a procedure returns its progress via a `GET` at `/v1/procedures/<id>`.
This uri can be obtained from the `location` header that is part of the creation response.
Here is an example of a progress response for the `bucketize` procedure
```
"progress": {
        "steps": [
            {
                "started": "2016-12-15T19:43:52.9386692Z",
                "ended": "2016-12-15T19:43:52.9768956Z",
                "type": "percentile",
                "name": "iterating",
                "value": 1.0
            },
            {
                "started": "2016-12-15T19:43:52.9768965Z",
                "type": "percentile",
                "name": "bucketizing",
                "value": 0.8191999793052673
            }
        ]
    },
    "state": "executing",
    "id": "2016-12-15T19:43:52.938291Z-463496b56263af05"
}
```
Other procedures will have similar responses. Note that this is currently implemented for procedures
of type `transform`, `import.text` and `bucketize`.

## Cancelling a procedure

Procedures can take a long time to execute. It is possible to interrupt a running procedure using a
`PUT` at `/v1/procedures/<idp>/runs/<idr>/state` where `idp` is the procedure id and `idr` is the 
run id with the following payload
```
{
        "state": "cancelled"
}
```
Note that some processing is not cancellable.  As a result, the procedure may continue running for
some time before it is finally interrupted.

## Available Procedure Types

Procedures are created via a [REST API call](ProcedureConfig.md) with one of the following types:

![](%%availabletypes procedure table)

## See also

* ![](%%nblink _tutorials/Procedures and Functions Tutorial) 
