# Script Procedure

The script procedure is used to run a Python or Javascript script as a procedure. This allows a script to be run multiple times and to persist the output. It is equivalent to running a script using the Javascript or Python plugins, but through a procedure.

## Configuration

![](%%config procedure script.run)

With `ScriptResource` defined as:

![](%%type MLDB::ScriptResource)

### Passing arguments to the procedure for a run

Each run of the procedure can be parameterized by passing a Json blob `args` in the run configuration. The procedure's script can then access it in the same way Python or JS scripts can access arguments.

```python
mldb.put("/v1/procedures/<id>/runs/<runid>", {
    "params": {
        "args": <args>
    }
})
```

## Return values

- The JSON result of calling the script procedure will be returned in the `status`
output of the run.
- The full headers and details will be returned in the `details` output of the run.

![](%%type MLDB::ScriptOutput)

## See also

* the ![](%%doclink python plugin) Server-Side API
* the ![](%%doclink javascript plugin) Server-Side API

