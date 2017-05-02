# Working with the REST API

Interactions with MLDB occurs via a REST API ([fully documented here](../rest.html)), where REST stands for [Representational State Transfer](http://en.wikipedia.org/wiki/Representational_state_transfer).

## Structure

The structure of a REST API call is `<verb> <resource> <arguments>`, e.g. `PUT /v1/datasets {"type": "sparse.mutable"}`.

* **verbs**: There are only four possible verbs: `GET`, `POST`, `PUT` and `DELETE`. These four verbs correspond to those available in the [Hypertext Transfer Protocol](http://en.wikipedia.org/wiki/Hypertext_Transfer_Protocol) (HTTP) but it is possible to work with the MLDB API in-process without making use of HTTP or the network (see below).
* **resources**: MLDB exposes a number of resources ([fully documented here](../rest.html)) which correspond to the basic MLDB entities (datasets, procedures, functions, plugins) and their components (rows and columns for datasets, runs for procedures etc).
* **arguments**: MLDB API calls accept arguments in [query-string](http://en.wikipedia.org/wiki/Query_string) and/or [JSON](http://en.wikipedia.org/wiki/JSON) format.

## Calling the API over HTTP

The quickest way to get started with making HTTP calls to the MLDB API is with your **web browser** using our [REST API Interactive Documentation](../rest.html).

Beyond that, if MLDB is running on a server named `<host>` on port `<port>` then you can use any of a widely-available variety of tools to make HTTP calls to `http://<host>:<port>/<resource>`, for example:

* [`curl`](http://curl.haxx.se/) is a generic **command-line** tool available for many platforms which will allow you to make HTTP calls
* [`requests`](http://docs.python-requests.org/en/latest/) is an easy-to-use generic **Python** library for making HTTP requests
* [`httr`](http://cran.r-project.org/web/packages/httr/index.html) is an easy-to-use generic **R** library for making HTTP requests

## Calling the API over HTTP from Python with `pymldb`

If you are using the built-in [Notebook interface](Notebooks.md) or want to work with MLDB from Python, you can install [`pymldb`](Notebooks.md), which gives you access to an MLDB-specific library to interact with the API over HTTP, while hiding the details of HTTP from you. The ![](%%nblink _tutorials/Using pymldb Tutorial) will show you how to use `pymldb`.

## Exposing functions as a REST API

For most predictive applications, you will want to expose a function over
REST that implements the specific functionality required.  There are two
ways of doing this: by calling the `GET /v1/functions/<function>/application`
route, or via a plugin exposing a custom route.

### Making a function available via `GET /v1/functions/<function>/application`

As soon as a function is created, that route is automatically available, and
so this version requires no extra work.  However, as MLDB does not know the
data types that the function will be called with, it is required to re-bind
the function on every call, which can be very expensive.  This can reduce
performance by an order of magnitude.

In order to avoid this penalty, it is possible to pre-bind function calls
using the ![](%%doclink sql.expression function) as follows:

```JSON
PUT /v1/functions/wrapper {
    "type": "sql.expression",
    "params": {
        "expression": "original_function({args, arg2, ...})[output] AS *",
        "prepared": true
    }
}
```

This will create a new function, `/v1/functions/wrapper`, which has an
`application` route that will forward `arg1` and `arg2` (these should be
replaced with the real function arguments) to the `original_function`
function, and return the `output` field to the caller.  The key element
here is the `"prepared": true`, which causes the call to `original_function`
to be pre-bound and so the call is very fast; rates in the hundreds of
thousands of calls per second can be expected on a modern server for a
simple function.

### Making multiple predictions per REST call (high-level solution)

The `/v1/functions/<function>/batch` REST route allows for multiple
predictions to be made in one call.  This call will take a JSON object
or array as input, apply the function to each element, and recreate
the same structure with the result of the predictions.  For example,
if we set up the following function:

```JSON
PUT /v1/functions/score_one {
    "type": "sql.expression",
    "params": {
        "expression": "horizontal_sum(input)",
        "prepared": true,
        "raw": true,
        "autoInput": true
    }
}
```

(which effectively makes the `horizontal_sum` builtin available as a
REST function, and uses `autoInput` and `raw` parameters to automatically
associate the `input` parameter with the passed input value and the output
of the `horizontal_sum` function to the result), then we can call it like
so:

```JSON
GET /v1/functions/score_one/batch { "input": [[1,2,3],[4,5],[6],[]] }
```

which will return us

````
[6,9,6,0]
````

with one output per input entry.  Alternatively, we could call it with

```JSON
GET /v1/functions/score_one/batch {
     "input": {
         "one":   [1,2,3],
         "two":   [4,5],
         "three": [6],
         "four":  [] } }
```

which will return us

````
{ "one": 6, "two": 9, "three": 6, "four": 0 } 
````

with the function being applied to each member of the object.


### Allowing multiple predictions per REST call (low-level solution)

In some cases, it may make sense to batch several predictions together
into the same REST call (especially to reduce network overhead).  This
can also be achieved using the ![](%%doclink sql.query function) and the
`row_dataset()` table function:

```JSON
PUT /v1/functions/batcher {
    "type": "sql.query",
    "params": {
        "query": "SELECT original_function(value) as value, column FROM row_dataset($input)",
        "output": "NAMED_COLUMNS"
    }
}
```

The `batcher` function, when called, will take an argument `input` that should
be either an array or an object.  It will apply `original_function` to each
element of the array or object, and return the same but with the elements
replaced with the value of their functions.  For example, if we use
`horizontal_sum` for the original function, then applying the batched version
to `[[1,2,3],[4],[5,6,7],[]]` will return [6,4,18,NULL].

For maximum performance, it can be combined with the `wrapper` trick above.

### Making a function available via a plugin route

By defining a custom plugin, it is also possible to define completely custom
routes for MLDB to operate on.  The following example shows how to make a
given function available via REST to respond to the `GET /routes/predict`
request.

First, we create a Javascript plugin that will set itself up to respond to
the given route:

```javascript
var fnconfig = {
    type: "sql.expression",
    params: {
        expression: "horizontal_sum({*})",
        prepared: true
   }
};
var predictfn = mldb.createFunction(fnconfig);

function handleRequest(relpath, verb, resource, params, payload, contentType, contentLength,
                       headers)
{
    if (verb == "GET" && relpath == "/predict") {
        return predictfn.callJson(JSON.parse(params[0][1]));
    }
    throw "Unknown route " + verb + " " + relpath;
}

plugin.setRequestHandler(handleRequest);
```

Next, we `PUT` that function to create the API:

```JSON
PUT /v1/plugins/myapi {
    "type": "javascript",
    "params": {
        "source": {
            "main": "<above javascript code>"
        }
    }
}
```

Finally, we can `GET` the `predict` route in order to make a prediction:

```JSON
GET /v1/plugins/myapi/routes/predict { "input": [ 1, 2, 3 ] }
```

which returns 6.

## Working with APIs unable to send a body with a GET request

If you need to send a body with a `GET` request to MLDB and your interface
doesn't support it, you can `POST` to the special route `/v1/redirect/get`,
which was created exactly for that case. It basically acts as a proxy that
redirects `POST` requests as `GET` to internal MLDB routes.

Here is an example

```JSON
POST /v1/redirect/get {
    "target": "/v1/query",
    "body": {
        "q": "SELECT 'foo' AS bar"
    }
}
```

Which returns

```JSON
[{"rowName":"result","columns":[["bar","foo","-Inf"]]}]
```
