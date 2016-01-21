# Python Plugins

This plugin allows for a plugin that is implemented in Python to be loaded
into MLDB to extend its functionality.

## Configuration

![](%%config plugin python)

With `PackageElementSources` defined as:

![](%%type Datacratic::MLDB::PackageElementSources)

If the `address` parameter is used, it may contain:

* `file:///mldb_data/<directory_within_mldb_data>`: a directory within the Docker container's mapped directory (the directory you specified with your `docker run` command) will be copied and its `main.py` file will be run, and `routes.py` will be run to handle REST requests.
* `git://` or `gist://`: the repo will be cloned and its `main.py` file will be run to initialize the plugin, and `routes.py` will be run to handle REST requests.
* `file:///mldb_data/<file_within_mldb_data>`: a Python file will be run from the Docker container's mapped directory.
* `http://<url>` or `https://<url>`: a Python file will be downloaded via HTTP(S) and executed.

If the `source` parameter is used, `main` will be executed to initialize the plugin and `routes` will be executed to handle REST requests (see `mldb.plugin.rest_params` below)

If the `args` parameter is used, its contents are available to the `main` Python code via the `mldb.plugins.args` variable (see below).

## <a name="API"></a> Server-side Python API

Plugins and scripts running within the MLDB instance will have access to an `mldb` object which lets them manipulate the database more efficiently than via HTTP network round-trips.

### `mldb` object (available to plugins and scripts)

* `mldb.log(message)` basic logging facility.
* `mldb.create_dataset(dataset_config)` creates and returns a dataset object (see below). Equivalent of an HTTP [`POST /v1/datasets`](/doc/rest.html#POST:/v1/datasets).
* `mldb.perform(verb, uri, [[query_string_key, query_string_value],...], payload, [[header_name, header_value],...])` efficiently emulates HTTP requests. See the [REST API documentation](/doc/rest.html) for available routes and payloads. 
    * The header `async:true` is supported to perform asynchronous call when creating expensive resources. When this header is used, the call will return immediately and the object will be created in the background.  One can track the progress of the operation by performing a "GET" on the resource.  The `state` field part of the `response` field will be set to `initializing` while the object is being created.  Once the creation is completed the `state` field will be set to `ok`.

### Filesystem access

There are two functions that allow access to the virtual filesystem of MLDB:

* `mldb.read_lines(uri, max_lines)` opens the given URI and returns a list with the first `max_lines` of the file. When `max_lines=-1` (default), all the lines are returned.
- `mldb.ls(uri)` lists the directory-like uri (currently `file://` and
  `s3://` URIs support listing) and returns the content.  The returned
  object has two fields: `dirs` contains an array of subdirectories, as
  URIs, and `objects` contains an associative array map of the objects
  in the directory, with the URI as the key and the following structure
  as the value:

  ![](%%type Datacratic::FsObjectInfo)

### `dataset` object (available to plugins and scripts)

* `dataset.record_row(row_name, [[col_name, value, timestamp],...])` records a row in the dataset
* `dataset.record_rows([ [ row_name, [[col_name, value, timestamp],...] ], ... ])` records multiple rows in the dataset.  It is more efficient than `record_row` in most circumstances.
* `dataset.record_column(column_name, [[row_name, value, timestamp],...])` records a column in the dataset.  Not all dataset types support recording of columns.
* `dataset.record_columns([ [ column_name, [[row_name, value, timestamp],...] ], ... ])` records multiple columns in the dataset.  Not all dataset types support recording of columns.
* `dataset.commit()` commits a dataset.  The behavior of committing varies by dataset
  type and some types may allow committing only once; see the documentation for the
  dataset type for more details.

### `mldb.script` object (available to scripts)

* `mldb.script.args` contains the value of the `args` key in the JSON payload of the HTTP request
* `mldb.script.set_return(return_val)` sets the return value of the script to be sent with the HTTP response

### `mldb.plugin` object (available to plugins)

* `mldb.plugin.args` contains the value of the `args` key in the JSON payload of the HTTP request
* `mldb.plugin.serve_static_folder(route, dir)` serve up static content under `dir` on the given plugin route `GET /v1/plugins/<id>/route`.
* `mldb.plugin.serve_documentation_folder(dir)` serve up documentation under `dir` on the plugin's documentation route (`GET /v1/plugins/<id>/doc`).  This will render files with a `.md` extension as HTML.See the [Documentation Serving](../DocumentationServing.md) page for more details.
* `mldb.plugin.rest_params`: object available within `routes.py` which represents an HTTP REST call. It has the following fields and methods:
    * `verb`: HTTP verb
    * `remaining`: URL fragment
    * `headers`: HTTP headers
    * `payload`: HTTP body
    * `contentType`: content type of HTTP body
    * `contentLength`: content length of HTTP body
* `mldb.plugin.set_return(body, return_code=200)`: available within `routes.py`, function called to write to HTTP response body and HTTP return code
    
### Handling a custom route

Calling `/v1/plugins/<id>/routes/<route>` will trigger the execution of the code in `routes.py`. The plugin developer must handle the `(verb, remaining)` tuple, available in the `mldb.plugin.rest_params` object. If it represents a valid route, the `set_return` function must be called with a non-null body, which will be returned in the response. If the function is not called or called with a null body, the HTTP response code will be 404.


## Plugin output

`GET /v1/plugins/<id>/routes/lastoutput` will return the output of the latest plugin code to have run: either the plugin loader or any subsequent calls to request handlers (see `mldb.plugin.set_request_handler()` above).

Script output will be returned as the HTTP response.

In either case, the output will look like this:

    {
        "logs": [<logs>], 
        "result": <return_value>,
    }

where `<logs>` come from calls to `mldb.log()` or using the python `print` statement to standard output or standard error, and `<return_value>` comes from a call to `mldb.set_return()` (see above).

If an exception was thrown, the output will look like

    {
        "logs": [<logs>], 
        "exception": <exception details>,
    }

The actual output object looks like this:

![](%%type Datacratic::MLDB::ScriptOutput)

with log entries looking like

![](%%type Datacratic::MLDB::ScriptLogEntry)

Exceptions are represented as

![](%%type Datacratic::MLDB::ScriptException)

with stack frames like

![](%%type Datacratic::MLDB::ScriptStackFrame)

Note that the Python plugin only fills in the `where` field of stack frame
entries.


