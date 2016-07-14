# Javascript Plugins

This plugin allows for a plugin that is implemented in Javascript to be loaded
into MLDB to extend its functionality.

## Configuration

![](%%config plugin javascript)

With `PackageElementSources` defined as:

![](%%type Datacratic::MLDB::PackageElementSources)

## Returning a result

In order to return a result, the value of the last statement evaluated must
evaluate to the result.  For example, if the last line of a script is `"hello"`,
then the script will return a string "hello" as its result.



## Results of script run

Running a Javsascript script directly as a plugin, procedure or function will
return a ScriptOutput result, that looks like this:

![](%%type Datacratic::MLDB::ScriptOutput)

with log entries looking like

![](%%type Datacratic::MLDB::ScriptLogEntry)

### Exceptions

Exceptions are represented as

![](%%type Datacratic::MLDB::ScriptException)

with stack frames like

![](%%type Datacratic::MLDB::ScriptStackFrame)


## <a name="API"></a> Server-side Javascript API

### Objects available

- the `mldb` object, available in the global context, allows the MLDB server
  itself to be accessed from the plugin or script
- the `plugin` object, available in the global context, allows the plugin
  to set itself up and access its own context.

### Accessing plugin creation parameters

The variable `plugin.args` gives the arguments that the plugin was
instantiated with.  This can be used by the user of the plugin to
provide configuration details of how the plugin should load.

### Logging

- `plugin.log(...)` allows a message to be logged to the plugin's log stream.
  This will be available in the plugin output.
- `mldb.log(...)` allows a message to be logged to MLDB's console.  This will
  be available only on the console output.

These accept multiple arguments and will accept any combination of scalars
(which are turned into strings then serialized) or objects (which are
serialized as their JSON representation).

### Calling out to MLDB

MLDB is a REST server, so all routes that are available on the REST interface
are also available from a Javascript plugin or script.

To call out, you can use `mldb.perform(method, route, queryParams, payload, httpHeaders)`.
The parameters are:

- `method` is a REST method, ie "GET", "POST", "PUT", or "DELETE"
- `route` is the route to call, ie "/v1/datasets"
- `queryParams` is either an array of `[string,string]` or an object that
  represents the query parameters (which are not passed in the route). Examples: `['a','b']` and `{a: 'b'}` will result in the query string `a=b`.
- `payload` is either an object or a string representing the payload.  If it's
  an object, it will be converted to JSON and the `content-type` will be set
  to `application/json`.  If it's a string, it will be passed as-is and the
  content-type will be set to `text/plain`.
  - `httpHeaders` is an array of pairs `[string, string]` or an object representing
  a header.  Currently, the header `async:true` is supported to perform
  asynchronous call when creating expensive resources.  You can specify this header
  as `{async:true}`.  When this header is specified, the call will return immediately
  and the object will be created in the background.  One can track the progress of
  the operation by performing a "GET" on the resource.  The `state` field part of the
  `response` field will be set to `initializing` while the object is being created.  Once
  the creation is completed the `state` field will be set to `ok`.

The result comes back as an object with the following fields:

- `responseCode` is an integer giving the HTTP response code;
- `contentType` is the HTTP content type
- `headers` is an array of `[string,string]` pairs giving the HTTP headers
  set on the response
- `response` is a string giving the bytes in the response
- `json` is set only if the resulting `content-type` is `application/json`.
  It is an object containing the JSON parsing of the response.

There are also convenience methods defined as:

- `get(resource, queryParams, httpHeaders)` which is the same as `perform("GET", resource, queryParams, "", httpHeaders)`
- `post(resource, payload, httpHeaders)` which is the same as `perform("POST", resource, [], payload, httpHeaders)`
- `put(resource, payload, httpHeaders)` which is the same as `perform("PUT", resource, [], payload, httpHeaders)`
- `del(resource, httpHeaders)` which is the same as `perform("DELETE", resource, [], "", httpHeaders)`

The following asynchronous methods are also defined.  They will not wait for
the resource to be created, but will return a `201` immediately with the URI
of the created resource, which can be polled until it is ready.

- `postAsync(resource, queryParams, payload, httpHeaders)` which is the same as `post` with the `async:true` header set
- `putAsync(resource, queryParams, payload, httpHeaders)` which is the same as `put` with the `async:true` header set


### Serving Static Content

`plugin.serveStaticFolder(route, dir)` will serve up static content under `dir`
on the given plugin route `route`.

`plugin.serveDocumentationFolder(route, dir)` will serve up documentation under
`dir` on the given plugin route `route`.  This will render markdown files
under a `.md` extension when accessed under `.html`.  See the
[Documentation Serving](../DocumentationServing.md "Documentation Serving")
page for more details.


### Random Number Generator

By calling `var rng = mldb.createRandomNumberGenerator()`, you can obtain a random
number generator that has access to more functionality than the built-in
Javascript version.  The methods supported are:

- Call `rng.seed(int)` with an integer argument to seed the generator.  If
  the seed argument is missing or zero, it will be seeded based upon the
  system random number generator.
- Call `rng.normal(mean = 0.0, std = 1.0)` to obtain a normally distributed random
  number with the given mean and standard deviation.  If either argument is not
  supplied, the defaults shown will be used.
- Call `rng.uniform(min = 0.0, max = 1.0)` to obtain a uniform distriubuted
  random number between the given min and max.  The interval is closed; that
  is that it is possible that exactly min or exactly max will be returned.

### JSON diff and patch

There are functions, useful for unit testing, to generate a JSON diff and to
apply a JSON patch:

- `diff = mldb.diff(obj1, obj2, strict=true)` will return a JSON object
  representing the differences between obj1 and obj2.  An empty return
  will indicate that the two are the same.
- `[patched, diff] = mldb.patch(obj, diff)` will apply a given diff to an
  object.  The return value is an array; element 0 is the patched object,
  and element 1 is the set of diffs that couldn't be applied.

### SQL

The following functions aid in working with SQL from within Javascript.

- `mldb.query(<sql statement>)` will parse and run the given query (which is
  an SQL string), and return an object with the full set of rows
  returned.  It is important to limit the number of results when querying
  large datasets, as otherwise MLDB could run out of memory.  For example,
  `mldb.query('SELECT * FROM dataset1 LIMIT 5')` will return the first five
  rows of the given table.
- `mldb.sqlEscape(<string>)` will turn the given string into an SQL string,
  including adding delimiters and escaping any SQL characters that need it.
  For example, `mldb.sqlEscape("It's hot")` will return "`'It''s hot'".  This
  can be used to help construct SQL queries containing strings that may
  include special characters.
  

### Interacting with MLDB data types

MLDB's atomic types are represented in Javascript as follows:

- Nulls are represented by Javascript's `null` value;
- Strings and numbers are represented by the Javascript equivalent type.
  This implies that currently, there is no way to work with 64 bit
  integers at full precision from Javascript.
- Timestamps are represented by Javascript's `Date` class.
- Time intervals are created by using the `mldb.createInterval()`
  function.  This argument takes either 3 parameters with the number
  of months (integral), days (integral) and seconds (floating point) in
  the interval, or an object with the structure 
  `{ months: xxx, days: xxx, seconds: xxx.yy }`.
- To represent a path (for example as a row name in recordRow), there are two
  options.  These may also be wrapped into a Path value using the `createPath()`
  function:
  - An individual atom (string or number) will be used as a one-element path
    with the value as the stringification of the passed element;
  - An array will be used as a compound path with the elements as specified.



### Filesystem access

There are two functions that allow access to the virtual filesystem of
MLDB:

- `mldb.openStream(uri)` opens the given URI, and returns a Stream
  object that can be used to read it.
- `mldb.ls(uri)` lists the directory-like uri (currently `file://` and
  `s3://` URIs support listing) and returns the content.  The returned
  object has two fields: `dirs` contains an array of subdirectories, as
  URIs, and `objects` contains an associative array map of the objects
  in the directory, with the URI as the key and the following structure
  as the value:

  ![](%%type Datacratic::FsObjectInfo)

#### Stream object

The Stream object has the following methods:

- `readLine()` returns the next line
- `readBytes(n)` returns the `n` next bytes and returns them as an array of unsigned
  8 bit integers
- `readJson()` returns a JSON value (object, array, number, string, boolean or null) from the stream and returns it
- `eof()` returns true if the stream is at the end of the file
- `readU8()` reads the next byte and returns it as an unsigned integer
- `readI8()` reads the next byte and returns it as a signed integer
- `readU16LE()` reads and returns the next two bytes as an unsigned little endian integer
- `readU16BE()` reads and returns the next two bytes as an unsigned big endian integer
- `readI16LE()` reads and returns the next two bytes as an signed little endian integer
- `readI16BE()` reads and returns the next two bytes as an signed big endian integer
- `readU32LE()` reads and returns the next four bytes as an unsigned little endian integer
- `readU32BE()` reads and returns the next four bytes as an unsigned big endian integer
- `readI32LE()` reads and returns the next four bytes as an signed little endian integer
- `readI32BE()` reads and returns the next four bytes as an signed big endian integer
- `readBlob(numBytes=-1,allowShortReads=false)` reads and returns the given
  number of bytes (or all remaining bytes, if `numBytes` is -1) into a `BLOB`
  Atom.  If EOF is reached before the required `numBytes` has been read, and
  `numBytes` is not -1, and `allowShortReads` is `false`, then an exception will
  be thrown.


### Dataset objects

A dataset object can be created using `mldb.createDataset(config)`.  The
config is the same as would be `POST`ed to `/v1/datasets` to create
the dataset.  If no `id` is passed in the `config` argument to
`createDataset(config)`, then the argument will be modified with the
`id` of the created dataset.  Similarly for the `params` object; if
default parameters are not specified, they will be filled out with the
actual parameters used.


This object has the following methods defined:

- `dataset.recordRow(rowName, tuples)` records a row.  Tuples is an array of
  three-element arrays with the format `[columnName, value, timestamp]`.
  The `columnName` is a non-empty string; the value is `null`, a
  string, or a number (integer or floating point, including infinity
  and NaN) and the timestamp is anything convertible to a Javascript
  `Date` object.
- `dataset.recordRows(rows)` records multiple rows, which may be more efficient
  in some dataset implementations.  Each row is a two-element tuple
  with the arguments to `recordRow`.
- `dataset.recordColumn(columnName, tuples)` is the same as recordRow, but
  instead of recording across a row it records down a column.  Not all
  datasets support recording of columns.
- `recordColumns(cols)` records multiple columns, in the same
  manner as `recordRows`.  Not all datasets support recording of columns.
- `dataset.commit()` will commit outstanding changes.  The behavior of commit
  depends upon the dataset implementation; please see the dataset
  documentation to understand how it works for each one.
- `dataset.id()` returns the id of the dataset
- `dataset.type()` returns the type of the dataset
- `dataset.config()` returns the configuration of the dataset
- `dataset.status()` returns the status of the dataset
- `dataset.details()` returns the details of the dataset
- `dataset.getTimestampRange()` returns the range of timestamps present in
  the dataset

### Procedure objects

A procedure object can be created using `mldb.createProcedure(config)`.
The `config` is the same JSON object that would be `POST`ed to `/v1/procedures`
to create the procedure.  If no `id` is passed, then the procedure is
anonymous and will have an `id` auto-generated.

The procedure object has the following methods defined:

- `procedure.run(args)` runs the procedure, with the given arguments,
  waits for it to finish, and returns the output of the procedure.  It's
  equivalent to `POST`ing to the `/runs` route of the procedure.
- `procedure.id()` returns the id of the procedure
- `procedure.type()` returns the type of the procedure
- `procedure.config()` returns the configuration of the procedure
- `procedure.status()` returns the status of the procedure
- `procedure.details()` returns the details of the procedure


### Function objects

A function object is created similarly to procedure and dataset objects.

It has the following methods defined:

- `function.call(args)` calls the function, with the given arguments,
  waits for it to finish, and returns the output of the function.
- `function.id()` returns the id of the function
- `function.type()` returns the type of the function
- `function.config()` returns the configuration of the function
- `function.status()` returns the status of the function
- `function.details()` returns the details of the function

## Debugging

The following are useful for debugging MLDB, but should not be used in normal
use of MLDB:

- `mldb.debugSetPathOptimizationLevel(level)` controls whether MLDB takes
  optimized or generic paths.  It can be used to unit-test the equivalence
  of optimized and non-optimized paths.  Setting to `"always"` (the default)
  will make MLDB always use optimized implementations when possible.  Setting
  to `"never"` has the opposite effect.  Setting to `"sometimes"` will
  randomly and non-deterministically choose whether or not to use an
  optimized path each time that one is encountered (50% probability of each).
  Note that this setting applies to the entire MLDB instance, and so should
  not be used in production.
