## FROM clauses

MLDB now allows for from (or input dataset) clauses in procedure, function and
dataset configurations to be an SQL table expression.  This makes the syntax
a lot lighter weight: before it may have been

```
PUT /v1/<kind>/<object> {
   'type': <type>
   'params': {
       'from': { 'id': 'existingDatasetName' }
   }
}
```

which can now be shortened to

```
PUT /v1/<kind>/<object> {
   'type': <type>
   'params': {
       'from': 'existingDatasetName'
   }
}
```

and allows more complex table expressions without creating extra objects:

```
PUT /v1/<kind>/<object> {
   'type': <type>
   'params': {
       'from': 'dataset1 JOIN dataset2 ON dataset1.x = dataset2.x'
   }
}
```


## Joins (MLDB-180)

July 31, 2015

MLDB now supports SQL join expressions, with some restrictions.
Documentation is [here] (datasets/JoinedDataset.md).

## Row valued functions (MLDB-763)

July 21, 2015

Select expressions over row-valued columns will now respect the name of the output
variable and nest them within the sub-row.  Previously, the `AS` clause was
ignored.  This may cause differences for selects, particularly where the `jseval`
or `object` functions are used.  The output of these may have a prefix equal to
the surface form of the expression.  To get the old behaviour, use `AS *` on the end
of the select clause. 


## CSV dataset (MLDB-749)

July 15, 2015

The `text.csv.tabular` dataset type has been added to allow for simple importing of
tabular CSV files.  Documentation is [here] (datasets/CsvDataset.md).

## Big renaming

June 26, 2015

Pipelines have been renamed to Procedures and Blocks have been renamed to Functions.

## Classifiers can now be introspected (MLDB-565)

June 24, 2015

The training of a classifier.train procedure, as well as a classifier function,
can now return a JSON representation of the trained classifier in their
status field.  This allows the introspection of trained classifiers.

This is available from the `/details` route of the procedure run or
classifier.apply function.

## `jseval` function can return rows (MLDB-704)

June 23, 2015

The `jseval` function can now return rows, which is enabled by returning
an object.  Each entry in the object maps the (key, value) onto the
(column name, value) of the returned row.  The timestamp recorded on
the value is the latest timestamp of all inputs passed to the function.

Note that is is not currently possible to return a row with custom
timestamps or multiple values for the same column from jseval.
 

## JS logging (MLDB-733)

June 20, 2015

It is now possible to log from a JS plugin using `mldb.log()` instead of
`plugin.log()` (which still works).  The first should be used for
logging related to the excution of the functionality; the second should
be used only for plugin-specific messages.


## Use indexes for WHERE clauses (MLDB-727)

June 19, 2015

Indexes are now used for the following expression patterns in WHERE
clauses:

- WHERE rowName() = constant
- WHERE constant = rowName()
- WHERE rowName() % constant op constant (op is =,!=<,>,<=,>=)
- WHERE column = constant
- WHERE column
- WHERE column IS TRUE
- WHERE column IS NOT NULL
- x AND y (where both x and y are in this list)
- x OR y (where both x and y are in this list)

This can considerably speed up a lot of MLDB queries.


## JS directory listing (MLDB-712)

June 19, 2015

The `mldb.ls` function has been added to allow for directories to be
listed from Javascript.

See the [JS documentation] (lang/Javascript.md).


## Cache directory (MLDB-717)

June 19, 2015

When using MLDB, it is possible to add an SSD cache by mounting it under
the /ssd_cache directory (for the Docker container) and, in batch mode,
adding the command line option `--cache-dir <dir>` to the batch command
line.

This directory will be used to cache downloads and allow memory mapping of
those files, enabling MLDB to use larger datasets than the available RAM
for some kinds of datasets (`beh` and `beh.binary`).

## Batch mode (MLDB-715)

June 18, 2015

It is now possible to run MLDB in batch mode, by launching the MLDB runner
directly and passing in the --run-script argument (both JS and Python
scripts are supported).  See the [documentation] (BatchMode.md).

## Horizontal operations (MLDB-461) (experimental)

June 14, 2015

It is now possible to calculate values across rows using `horizontal_count`
and `horizontal_sum`.  For example, `horizontal_sum(SELECT price*)` will
calculate the sum of the price elements.   [Documentation] (sql/ValueExpression.md).


## All URIs must include a scheme (MLDB-596)

June 14, 2015

Previously, depending upon the context, a naked URL could be passed and it
would be determined automatically as `http://` or `file://` depending upon...
well the source code, there was no way to no.  All URIs must now explicitly
include a scheme to avoid this ambiguity.


## `parse_sparse_csv` function (MLDB-499)

June 14, 2015

The function `parse_sparse_csv` has been added, which allows a line of a
CSV file to be imported as a sparse row. [Documentation] (sql/ValueExpression.md).


## Binary behaviour datasets have /saves route (MLDB-499)

June 14, 2015

The binary behaviour datasets (mutable and immutable) can now be saved by
posting to the `/v1/datasets/<dataset>/saves` route a JSON object that
looks like this:

```
{ "dataFileUrl": "scheme://uri" }
```

This allows for them to be saved independent of the `commit` mechanism.

## Text line dataset (MLDB-499)

June 13, 2015

The `text.line` dataset loads a text file into a dataset with
one row per line.  For example, to load Google's robots.txt file, it is
possible to perform:

```
var dataset_config = {
        type: 'text.line',
        id: 'google_robots.txt',
        params: {
            dataFileUrl: 'https://www.google.com/robots.txt'
        }
    };
```

[documentation] (datasets/TextLineDataset.md)


## Normalization functions (MLDB-389)

June 10, 2015

The following two functions were added to allow for normalization:

- `normalize(vec, p)` will return a version of the vector normalized in the L-p norm.  This
  means that `norm(normalize(vec, p)) = p` so long as vec is not zero.
- `norm(vec, p)` will return the L-p norm of vec.


## Timestamp functions (MLDB-686)

June 10, 2015

The functions `at`, `when`, `timestamp` and `now` are available to
interrogate, manipulate and modify timestamps on expressions.
The documentation is available under "timestamp functions"
[here] (sql/ValueExpression.md).

- `when(x)` returns the timestamp at which the expression `x` was known to be
  true.  Each expression in MLDB has an associated timestamp attached to it,
  which is used for unbiasing, and this returns that timestamp.

  For example, if `x` had a timestamp of last Monday, and `y` had a
  timestamp of last Tuesday, then `when(x + y)` would have a timestamp of
  last Tuesday, since on Monday the value of `y` wasn't known.
- `timestamp(x)` coerces the value of x to a timestamp:

  - if `x` is a string, it creates the timestamp by parsing the ISO8601
    string passed in.
  - if `x` is a number, it is interpreted as the number of seconds since
    the UNIX epoch (1 January, 1970 at 00:00:00).
  - otherwise, this will return null.
- `at(x, d)` returns the value of the expression `x`, but with the timestamp
  modified to be at timestamp `d`.
- `now()` returns the timestamp at the current moment, according to system
  time.  

## Subprocess plugin (experimental) (MLDB-684)

June 10, 2015

It is now possible to implement a plugin an a separate process that
will communicate with MLDB via HTTP.  This is an experimental
feature.  See the [documentation] (plugins/Subprocess.md).


## Function names are case-optional (MLDB-313)

June 5, 2015

Function names now may be specified in any case.  The algorithm used
to find them is as follows:

- If a function with a name in the same case exists, then that will be
  used.
- Otherwise, if a function with a name that matches the lowercase
  version of the requested function exists, then that will be used.
- Otherwise, the function is not found and an error is returned

## Procedure trainings have timestamps (MLDB-525)

June 5, 2015

Procedure trainings now record the timestamp of when the training
started and ended.  They will be returned by a status call:

```json
{
      "id" : "1",
      "state" : "finished",
      "trainingFinished" : "2015-06-05T17:15:56.206Z",
      "trainingStarted" : "2015-06-05T17:15:56.159Z"
}
```

## Select x returns latest x (MLDB-679)

June 5, 2015

The construct "select x" from a dataset row with multiple values for
x would previously return an arbitrary one of the values of x.  This
has been modified to always return the one with the *latest* timestamp. 


## Binary behaviour dataset (MLDB-631)

May 7, 2015

The `beh.binary` dataset type allows for legacy behaviour files with
values always equal to `1` to be accessed extremely efficiently from
MLDB.

The [documementation is here] (datasets/BinaryBehaviourDataset.md).

## classifier/probabilizer artifact parameter changes

In Procedures and Functions:

* `classifierUri (string)` -> `modelFileUrl (Url)`
* `probabilizerUri (string)` -> `modelFileUrl (Url)`
* NB: `Url` requires a protocol like `file://`

## Dataset Type name changes

* `mutable` -> `beh.mutable`
* `uptonow` -> `beh.live`
* `rotating` -> `beh.ranged`

## Type system and function changes (MLDB-308, MLDB-593)

May 6, 2015

Functions now must be able to enumerate a closed set of pins.

For functions that take fixed inputs and outputs, like the probabilizer
function which takes a `prob` as an input and produces a `score` as an
output, there is no change.  But for functions like the classifier
function, which take a variable or sparse input, all of these need
to be represented on a single pin.

Concretely, the classifier function now takes a `features` pin to
read its features from, instead of reading them from all input
pins.  So instead of something like this

    APPLY FUNCTION classifier WITH (* EXCLUDING (label)) EXTRACT (score)

you will need to pack those input features into an object
called `features` with the syntax

    APPLY FUNCTION classifier WITH (object(SELECT * EXCLUDING (label)) AS features) EXTRACT (score)

This fixes issues with a classifier not being able to use a feature
called score, for example.

Secondly, functions now model functions with objects (key-value pairs)
as inputs and outputs rather than a set of pin values which are mutated
by each function.  This avoids problems with pin name clashes.

Finally, the `serial` function type now accepts `with` and `extract`
clauses at each step.  These allow the sub-functions to operate in a different
namespace than the main function, which enables use-cases like having
multiple classifiers run as part of a serial function (previously they
would have had a name clash on the output pin).

To support all of these changes, the MLDB type system was adjusted.
Expressions (from SQL and functions) can now return more complex types,
including rows, objects, embeddings and JSON or C++ objects.  These will
be "flattened" down to column/value pairs when written to a dataset.
Previously, expression outputs were flattened down immediately, in
other words the type system of expressions matched that of datasets.
This also paves the way for functions of embeddings (eg, normalization),
rows and JSON transformations to be naturally supported in SQL
expressions.

The changes will require modifications to most code that uses functions
or `APPLY FUNCTION` expressions.


## Serial function accepts `with` and `extract` clauses (MLDB-593)

April 28, 2016

Serial functions now accept `with` and `extract` clauses, which makes
it possible to rename pins to avoid name conflicts and generally
makes running multiple functions much more reasonable.

The [documentation is here] (functions/Serial.md).


## SQL expression and query functions (MLDB-390)

April 26, 2015

It is now possible to run two kinds of SQL queries inside functions
in MLDB:

1.  To calculate an SQL expression based only upon the value of the
    input pins, you can use the `sql.expression` function
    ([here] (functions/SqlExpressionFunction.md)).
2.  To calculate an SQL expression by running a query over a dataset
    (with the input pins available in the expression), you can
    use the `sql.query` function ([here] (functions/SqlQueryFunction.md)).

The first is useful to perform arbitrary calculations with functions
without having to write a new function type.  The second is useful
as a means to perform a join.

Note: currently aggregate queries are not accepted in the sql query function.

## Selectable metric space for nearest neighbour embeddings (MLDB-598)

April 26, 2015

It is now possible to select the metric space used for nearest neighbour
embeddings.

The `embedding` dataset type takes a parameter `metric` which can be

- `cosine`: metric that compares the angles of points, not their
  magnitudes.  Useful for orthonormal embeddings like the SVD or those
  with a high number of dimensions.
- `euclidean`: metric that compares the distance between points.
  Useful for geometric embeddings like t-SNE.

See the [documentation] (datasets/EmbeddingDataset.md).

## `CAST (expr AS type)` operator (MLDB-506)

April 24, 2015

The SQL cast operator is now implemented to allow explicit conversion
between types.

The [documentation is here] (sql/ValueExpression.md) in the "CAST expression"
section.

## `implicit_cast` function (MLDB-599)

April 24, 2015

There is now an `implicit_cast` function that will attempt to convert
string arguments that aren't meant to be strings to a more specific
type, currently numbers.

See the [documentation] (sql/ValueExpression.md#Functions).


## SVD model parameter change (MLDB-597)

April 23, 2015

For consistency with the rest of the system, the `svdUri` parameter for
the SVD function is now called `modelFileUrl` and it is a Url, not a
filename (so file:// is required for local files).

## First draft of channel implementation (MLDB-135)

April 23, 2015

First iteration on channel implementation.

General idea is to have channels be implemented as a pair where the channel
ingests data and a corresponding dataset is used to read the output of the
channel.

This change with the proof-of-concept implementation of 2 common channel use
cases: rotating and streaming channels. The RotatingChannel works by
periodically dumping artefacts to a given folder which can then be accessed by
the RotatingDataset using a date range. The StreamingChannel works by publishing
recorded rows to an in-process pub-sub mechanism which can be subscribed to via
the StreamingDataset.

An additional MultiDataset is also provided to dispatch rows to multiple dataset
to enable both rotating and stream channels on the same data stream.

## `/v1/functions/<function>/application` parameters changes (MLDB-579)

April 22, 2015

The application route now takes its parameters as a JSON object
called "input" which needs to be URL encoded into the query string.

- before:  `GET /v1/functions/<function>/application?x=10&y=20&encoding=json
- after:   `GET /v1/functions/<function>/application?input=<<URL ENCODE {"x"=10,"y"=20}>>`
  which is `GET /v1/functions/<function>/application?input=%7B%22x%22%3D10%2C%22y%22%3D20%7D`

This removes ambiguity and allows for UTF-8 characters to be encoded properly.

## `recordRows`, `recordColumn` and `recordColumns` functions (MLDB-541)

April 21, 2015

It is possible to record columns, multiple rows and multiple columns
at once to MLDB.  This can be much more efficient than recording them
one at a time, although not all datasets support recording of columns
or have optimized codepaths.

The functionality is available through:

- REST at
  - `POST /v1/datasets/<dataset>/columns { columnName: "name", rows: [ [ rowName, val, ts ], ... ] }`
  - `POST /v1/datasets/<dataset>/multirows [ [ rowName, [ [ colName, val, ts], ... ] ], ... ]`
  - `POST /v1/datasets/<dataset>/multicolumns [ [ colName, [ [ rowName, val, ts], ... ] ], ... ]`
- Python at
  - `dataset.record_rows([ [ rowName, [ [ colName, val, ts], ... ] ], ... ])`
  - `dataset.record_column(columnName, [ [ rowName, val, ts], ... ])`
  - `dataset.record_columns([ [ columnName, [ [ rowName, val, ts], ... ] ], ... ])`
- JS at
  - `dataset.recordRows([ [ rowName, [ [ colName, val, ts], ... ] ], ... ])`
  - `dataset.recordColumn(columnName, [ [ rowName, val, ts], ... ])`
  - `dataset.recordColumns([ [ columnName, [ [ rowName, val, ts], ... ] ], ... ])`

See the [JS documentation] (lang/Javascript.md) and [Python documentation] (lang/Python.md)


## Multiple query output formats (MLDB-102)

April 17, 2015

The dataset `/v1/datasets/<datset>/query` route now accepts a
`format` parameter as well as `headers`, `rowNames` and `rowHashes`
to control the format of the output.

As an example, the `table` output format allows the following output:

```
[
   [ "_rowName", "x", "y", "z" ],
   [ "ex1", 0, 3, null ],
   [ "ex2", 1, 2, "yes" ],
   [ "ex3", 2, 1, null ],
   [ "ex4", 3, 0, "no" ]
]
```

and the "structure of arrays" (`soa`) output format allows for
the following which is useful for visualization libraries:

```
{
   "_rowName" : [ "ex1", "ex2", "ex3", "ex4" ],
   "x" : [ 0, 1, 2, 3 ],
   "y" : [ 3, 2, 1, 0 ],
   "z" : [ null, "yes", null, "no" ]
}
```

See the [documentation] (datasets/Datasets.md) for more details.
	

## Address field removed from PolyConfig (MLDB-372)

April 17, 2015

The `address` field in the PolyConfig object (used to configure all
entity types) no longer exists.  Instead, those objects that actually
used it (a couple of Dataset types, and the JS plugin) have added
it to their params.  This helps with validation and makes the interface
more consistent.

## Procedure steps that create entities (MLDB-546, MLDB-551)

April 17, 2015

MLDB can now create entities from procedure steps (all kinds: procedures,
functions, datasets, plugins).  This allows processing steps that aren't
explicitly a procedure to be wrapped in one and used as part of a multi
step procedure.

See the [Create Entity Procedure] (procedures/CreateEntityProcedure.md)
documentation for more details.

## Multi-step procedures (MLDB-515)

April 17, 2015

It is now possible to create a single procedure that will run multiple
steps in a single invocation.  See the
[Serial Procedure] (procedures/SerialProcedure.md) for more details, or
look at the test case for MLDB-515.


## SQL BETWEEN expressions (MLDB-504)

April 13, 2015

SQL `BETWEEN` expressions are now supported, like

   SELECT * FROM dataset WHERE price BETWEEN 16.00 AND 32.00


## SQL CASE expressions (MLDB-503)

April 12, 2015

SQL `CASE` expressions are now supported.  There are two flavors:

Simple case statements, which look like

    CASE expr
    WHEN val1 THEN result1
    WHEN val2 THEN result2
    ELSE result3
    END

Matched case statements, which look like

    CASE
    WHEN boolean1 THEN result1
    WHEN boolean2 THEN result2
    ELSE result3
    END

In both cases, there are an arbitrary number of `WHEN`s and the `ELSE` clauses are
optional.


Remi, April 10 2015

Configs are now loaded lazily after the the collections have been created and
the config stores have been attached. This fixes several race conditions (aka.
the infamous death loop) during startup.

## Death-Loop (MLDB-343)

Remi, April 10 2015

Configs are now loaded lazily after the the collections have been created and
the config stores have been attached. This fixes several race conditions (aka.
the infamous death loop) during startup.


## Nearest neighbours for known rows in embedding dataset (MLDB-509)

Jeremy, April 10 2015

It is now possible to ask for the nearest neighbours of an existing
row in an embedding dataset without needing to first look up its
embedding.  This is done with the new endpoint (for Embdding datasets
only)

```
GET /v1/datasets/<datasetName>/routes/rowNeighbours?row=rowName&numNeighbours=10&maxDistance=INFINITY
```

See the [Embedding Dataset Documentation] (datasets/EmbeddingDataset.md)
for more details.


## Syntax highlighting in documentation (MLDB-507)

Jeremy, April 10 2015

The documentation now includes syntax highlighting in the code functions.
This should happen automatically.  It's implemented using [highlight.js]
(https://highlightjs.org/).

## Inline code in documentation (MLDB-508)

Jeremy, April 10 2015

You can include inline code from an separate file that is served
alongside the documentation via the `%%codeeexample` macro.

See [the Documentation Serving documentation] (DocumentationServing.md)


## Type introspection (MLDB-497)

Jeremy, April 9 2015

It is now possible to programatically interrogate the type system, in order to
allow for automated UIs to be built.

See the [documentation] (rest/Types.md) or look at the MLDB-497 test case.


## Column expressions (MLDB-483)

Jeremy, April 7 2015

It is now possible to run a SELECT to choose via an SQL expression which columns
to include in the output of a query.  This is similar to running a query on a
transposed dataset to select the columns, but can be done as part of a single query.

For example, to select up to 1,000 columns having the most rows so long as they have
at least 100, the following would suffice:

```
(select) COLUMN EXPR (WHERE rowCount() > 100 ORDER BY rowCount() DESC, columnName() LIMIT 1000)
```

See the [Sql Select Expression documentation] (sql/SelectExpression.md) for details.


## Transposed datasets (MLDB-462)

Jeremy, April 7 2015

It is now possible to create a dataset that is a transposed version of another
dataset.  This is particularly useful when a simple SELECT would work if only
the dataset was rotated, rather than a much more complex expression.

Since datasets are required to have ready access to their inverse, the
operation should not be too expensive.


## SQL keywords are case-insensitive (MLDB-313)

Jeremy, April 1 2015

The keywords in SQL queries now match in a case-insentitive manner.
Note that function names are still case sensitive, and column names will
always be case sensitive.

## Nearest neighbours queries on Embedding dataset (MLDB-283)

Jeremy, April 1 2015

It is now possible to ask for the nearest neighbours of a point in an
embedding dataset.  This is done by calling `/v1/datasets/<dataset>/routes/neighbours`
passing in the column values of the point under consideration as query
parameters.  For example,

```
var dataset_config = {
    'type'    : 'embedding',
    'id'      : 'test',
};

var dataset = mldb.createDataset(dataset_config)

var ts = new Date();

function recordExample(row, x, y)
{
    dataset.recordRow(row, [ [ "x", x, ts ], ["y", y, ts] ]);
}

recordExample("ex1", 0, 0);
recordExample("ex2", 0, 1);
recordExample("ex3", 1, 0);
recordExample("ex4", 1, 1);

dataset.commit()

var res1 = mldb.get("/v1/datasets/test/routes/neighbours", {x:0.5,y:0.5}).json;

plugin.log(res1);

var res2 = mldb.get("/v1/datasets/test/routes/neighbours", {x:0.1,y:0.2}).json;

plugin.log(res2);
```

This will provide the following output

```
[
   [ "ex1", "397de880d5f0376e", 0.7071067690849304 ],
   [ "ex2", "ed64a202cef7ccf1", 0.7071067690849304 ],
   [ "ex3", "418b8ce19e0de7a3", 0.7071067690849304 ],
   [ "ex4", "213ca5902e95224e", 0.7071067690849304 ]
]

[
   [ "ex1", "397de880d5f0376e", 0.2236067950725555 ],
   [ "ex2", "ed64a202cef7ccf1", 0.8062257766723633 ],
   [ "ex3", "418b8ce19e0de7a3", 0.9219543933868408 ],
   [ "ex4", "213ca5902e95224e", 1.2041594982147220 ]
];

```


## Unknown query parameters no longer accepted (MLDB-263)

Jeremy, April 1 2015

Query parameters that are not understood will now cause responses
that look like the following:

```
HTTP/1.1 400 HTTP/1.1
Content-Type: application/json
Content-Length: 627
connection: Keep-Alive


{
   "details" : {
      "help" : {
         "jsonParams" : [
            {
               "cppType" : "Datacratic::PolyConfig",
               "description" : "Configuration of new plugin",
               "encoding" : "JSON",
               "location" : "Request Body"
            }
         ],
         "result" : "plugin status after creation"
      },
      "resource" : "/v1/plugins/myplugin",
      "unknownParameters" : [
         {
            "paramName" : "sync",
            "paramValue" : "true"
         }
      ],
      "verb" : "PUT"
   },
   "error" : "Unknown parameter(s) in REST call",
   "httpCode" : 400
}
```

All internal uses have been fixed, but be aware of the sprinkling of
`sync=true` parameters throughout external plugins.


## New /v1/query endpoint (MLDB-314)

Rémi, April 1st 2015

Added a new /v1/query endpoint for SQL statement which now accept a
FROM clause to select the dataset. mldb's query cell magic has also
been updated to use the new route.


## PUT and POST are synchronous by default (MLDB-305)

Jeremy, March 31 2015

From now on, a `PUT` and a `POST` will be synchronous by default (in
other words, they will function until the object is fully created).
The `sync=true` parameters currently sprinkled everywhere will no
longer have any effect, and in the near future will cause an error
in the request.

To achieve the old behaviour, you need to add an `Async: true`
HTTP header to the request.  This will require support from the
client libraries.

Note that `GET` and `DELETE` always were, and remain, synchronous.
There is no way to do an asynchronous `GET` (it doesn't make sense),
or an asynchronous `DELETE` (this is a gap).


## Changed format of stdout/err return in python plugin (MLDB-297)

Frank, March 31 2015

The return format for stderr/out is now a single list of tuples of
the following format: [timestamp, stderr|stdout, message]

## `TransformDataset` procedure (MLDB-409)

The TransformDataset procedure takes the output of an SQL query over
an input dataset and pipes it into a dataset.  This replaces the
applyFunctionToDataset procedure, with better functionality and a more
consistent syntax.

In conjunction with the rowName setting (MLDB-410) and the
ability to merge datasets, this also allows most kinds of joins
to be accomplished within MLDB.

See [Javascript Plugin Doc](procedures/TransformDataset.md).


Jeremy, March 30 2015


## JS diff and patch

Jeremy, March 30 2015

The JS interface now has `mldb.diff()` and `mldb.patch()` to help
with unit testing.  They perform a diff and patch between JSON
objects.

See [Javascript Plugin Doc](lang/Javascript.md).

## Ability to set row names of query output (MLDB-410)

Jeremy, March 30 2015

It is now possible to set the rowName (and rowHash) of the output of a
select expression.  This is done using the 'rowName' parameter for the
`/v1/datasets/xxx/query` call, or using the `SELECT ... NAMED <expr> ...`
for the `/v1/datasets/xxx/rawquery` endpoint.  This works both for
aggregate and normal queries.

The `rowName()` function is available as part of those expressions,
which allows for the old row name to be used to build the new row
name off.


## `EXCLUDING` clause now needs () around its members (MLDB-359)

Jeremy, March 27 2015

To avoid needing to understand complex parsing precedence rules, the
`EXCLUDING` clause always requires parantheses, even when there is a single
element in the clause.  This will likely break several examples in external
repos.

## Added a rawquery route for datasets

Rémi Attab, March 25 2015

Datasets can now be queried on the
`/v1/datasets/<dataset>/rawquery?q=<query-string>` route which will accept the
entire select statement as a single string.

## sum() aggregate (MLDB-327)

Jeremy, March 27 2015

There is now a `sum()` aggregate in MLDB's SQL implementation.

## JS convenience calls

Jeremy, March 27 2015

The JS interface now has `get()`, `put()`, `post()` and `del()` methods.
See [Javascript Plugin Doc](lang/Javascript.md).

## k-means function (MLDB-285)

Jeremy, 24 March, 2015

There is now a k-means function which can reclassify points.  The function of the
k-means procedure was also changed, to write a dataset of centroids rather than
a binary data structure.  See `platform/mldb/testing/MLDB-285-kmeans-function.js`
for details, as well as the documentation in
[K-Means Function](functions/Kmeans.md "K-Means Function").

## APPLY FUNCTION in select (MLDB-307)

It is now possible to perform an APPLY FUNCTION as part of a select.  This allows
a full select expression in the EXTRACT clause, rather than a simple expression
as in the APPLY FUNCTION used in an expression.

## Renamed various routes (MLDB-289)

- POST /datasets/<dataset>/rows/record to POST /datasets/<dataset>/rows
- GET /v1/datasets/<dataset>/columns/<columnName>/valueCounts to GET /v1/datasets/<dataset>/columns/<columnName>/valuecounts
- GET /functions/<function>/apply to GET /functions/<function>/application

## Renamed route /procedures/X/trainings to /procedures/X/runs (MLDB-265)

Since procedures are meant to be more general than model trainings, the
using `run` instead of `training` made more sense. The route was
renamed and examples updated.

## Script running is built into core plugin (MLDB-255)

The `python_runner` and `javascript_runner` plugin types have been removed.
Instead of posting scripts to those (which first requires them to be loaded),
the scripts should be posted to `/v1/types/plugins/javascript/routes/run` for
Javascript and `/v1/types/plugins/python/routes/run` for Python.

This means that you *no longer need to create a python or javascript runner
plugin at the beginning of examples and tests*.

Examples have been updated and tests pass, but there may have been some
places that were missed by the grepping.

This was enabled by allowing plugin types to register a route handler that is
called when a route on the type is called.

Associated with this change, exceptions in Python scripts are now reported
in the `exception` field of the response, not as a string.

## Multiclass mode (MLDB-173)

It is now possible to train a multiclass classifier by setting
`mode="categorical"` in the classifier config.  See
`platform/testing/MLDB-173-multiclass.js` for an example.


## Regression mode (MLDB-174)

Jeremy, March 19 2015

It is now possible to train a regressor by setting `mode="regression"`
in the classifier config.  See `platform/testing/MLDB-174-regression.js`
for an example.


## Updated relativity of path when serving static_folder in python plugin (MLDB-240)

Frank, March 19 2015

This changes the path relativity when serving static_folder in a python plugin. It is now relative to the plugin's home.

## Classifier weights (MLDB-198)

Jeremy, March 19 2015

The `weight` parameter of classifiers now works.  See 
`platform/testing/MLDB-198-classifier-weights.js` for a neat testcase
and the documentation of the [Classifier procedure](procedures/Classifier.md "Classifier Procedure").


## Type operators (MLDB-235)

Jeremy, March 17 2015

It is now possible to use the following operators to test the type
of an expression.  See [Sql](Sql.md "SQL Implementation").

- `expr IS [NOT] STRING` tests if the given expression is a string
- `expr IS [NOT] NUMBER` tests if the given expression is a number
- `expr IS [NOT] INTEGER` tests if the given expression is an integer


## Configurable path for static assets and documentation (MLDB-222)

Jeremy, March 16 2015

The paths under which MLDB serves its static assets are now configurable
in `mldb_runner`:

* `--static-assets-path` allows the path to static assets to be set (these
  are normally in `platform/mldb/static'
* `--static-doc-path` allows the path to built-in documentation to be
  set (this is normally in `platform/mldb/doc`)


## Documentation serving (MLDB-223)

Jeremy, March 16 2015

A plugin can now call `serveDocumentationDirectory(route, dir)` (JS) or
`serve_documentation_directory(route, dir)` (Python) to serve up documentation
that will allow Markdown transformation.  See
[Documentation Serving](DocumentationServing.md "Documentation Serving").


## Peer to peer mode removed (MLDB-60)

Jeremy, March 16 2015

In order to stabilize MLDB for release, the peer to peer functionality was
disabled.  This was the cause of race conditions in startup, and the source
of segmentation faults in the runner.  These should be fixed now.

The program lost the `--peer-listen-port`, `--peer-publish-port`,
`--peer-listen-host` and `--peer-publish-host` options.  This may cause
launch failures where these were explicitly specified.

## Dataset query endpoint changed (MLDB-221)

Jeremy, March 16 2015

The `/v1/datasets/<dataset>/select` endpoint has been renamed to
`/v1/datasets/<dataset>/query`.  Tests pass and code has been grepped,
but it is very possible that externally hosted plugins or those that
don't access that route directly may need to be updated.

## Quote switch (MLDB-202)

Jeremy, March 16 2015

The meaning of the single quote `'` and the double quote `"` has switched
in MLDB.  Single quotes represent strings; double quotes represent column
names.  They were originally reversed due to a misreading of the SQL standard;
they are now in line with both PostgreSQL and the SQL standard.

Hardly any tests failed due to the switch, which is somewhat concerning.
There are probably many tests which are set to manual or don't assert on
their output that need to be updated.  
	
## Probabilizer added (MLDB-59)

Jeremy, March 15 2015

It is now possible to train and apply a probabilizer.  See
`platform/testing/MLDB-59-probabilizer.js` and
[Probabilizer procedure](functions/ApplyProbabilizer.md "Apply Probabilizer Function")
documentation.

## Classifer now has `equalizationFactor` parameter

Jeremy, March 15 2015

JML's `equalizationFactor` parameter is now exposed.  This allows
classifiers on rare events to be trained.  The MLDB-59 example
shows how it's used, as does the Classifier documentation. 

## Accuracy Plugin Changes

Jeremy, March 15 2015

The accuracy plugin has been changed to work on the simple output of a
SELECT command.  This means support for `orderBy`, `offset` and `limit`,
but removed support for an explicit `function` argument as well as `with`
and `extract`.  Instead, you need to use

    score: "APPLY FUNCTION <functionname> WITH (<withexpression>) EXTRACT (<extractexpression>)"

in the configuration.

