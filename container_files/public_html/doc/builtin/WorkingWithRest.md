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
