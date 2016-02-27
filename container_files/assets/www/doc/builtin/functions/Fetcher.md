# Fetcher Function

The fetcher function is used to fetch resources from a given file or
URL.

## Configuration
![](%%config function fetcher)

## Input and Output Values

The function takes a single input named `url` that contains the URL of the
resource to load.

It will return two output columns:

- `content` is a binary BLOB field containing the (binary) content that
  was loaded from the URL.  If there was an error, it will be null.
- `error` is a string containing the error message.  If the fetch
  succeeded, it will be null.

## Example

The following Javascript creates and calls a function that will return the
country code from an IP address from an external web service.

```python
mldb.put("/v1/functions/fetch", { "type": "fetcher" })
```

```sql
SELECT CAST (fetch({url: 'http://www.google.com'})[content] AS STRING)
```

## Limitations

- The fetcher function will only attempt one fetch of the given URL; for
  transient errors a manual retry will be required
- There is currently no timeout parameter.  Hung requests will timeout
  eventually, but there is no guarantee as to when.
- There is currently no rate limiting built in.
- There is currently no facility to limit the maximum size of data that
  will be fetched.
- There is currently no means to authenticate when fetching a URL,
  apart from using the credentials daemon built in to MLDB.
- There is currently no caching mechanism.
- There is currently no means to fetch a resource only if it has not
  changed since the last time it was fetched.

## Design notes

The `fetcher` function is a function, and not a builtin, to allow
configuration of authentication, caching and other parameters in
a future release to address the limitations above.

