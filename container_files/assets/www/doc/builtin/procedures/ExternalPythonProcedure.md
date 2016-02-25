# External Python Procedure (experimental)

The external python procedure is used to run a Python script in an external Python interpreter as a procedure. It is equivalent to running a script using the Python plugins, but through a procedure, and without any of the limitations of the built-in interpreter.

Note that since the code will be running in an external python process, MLDB's server-side Python API will not be available. You can however import and use the pymldb library.


## Configuration
![](%%config procedure experimental.external.procedure)

## Return values

The procedure will return the stdout, stderr and statistics about the process. If the last line of the stdout if valid JSON, it will be parsed for convenience and added to the returned JSON blob in the `return` key.

```javascript
{
    "status" : {
        "runResult" : {
            "returnCode" : 1,
            "state" : "RETURNED",
            "usage" : { ... }
        },
        "stderr" : "...",
        "stdout" : "...",
        "return": { ... }    
    }
}  
```

## See also
* The ![](%%doclink script.run procedure).
* The *Running a script* section of the  ![](%%doclink python plugin).

