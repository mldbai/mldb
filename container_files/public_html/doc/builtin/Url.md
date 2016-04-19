# Files and URLs

MLDB gives users full control over where and how data is persisted. Datasets can be saved and loaded from files. Procedures can create files and Functions can load up parameters from files.

If data is stored in a system such as S3 or HDFS, it is possible to run multiple instances of MLDB so as to distribute computational load. For example, one powerful and expensive machine could handle model training, storing model files on S3, while a load-balanced cluster of smaller, cheaper machines could load these model files and do real-time scoring over HTTP.

## Protocol Handlers

The following protocols are available for URLs:

- `http://` and `https://`: standard HTTP, to get a file from an HTTP server on the public
  internet or a private intranet.
- `s3://`: Refers to a file on Amazon's S3 service.  If the file is not public, then
  credentials must be added.
- `file://`: Refers to a file inside the MLDB container.  These resources are only
  accessible from the same container that created them.  A relative path (for example
  `file://filename.txt`) has two slashes after `file:`, and will create a file in the
   working directory of MLDB, that is the `mldb_data` directory.  An absolute path has
   three slashes after the `file:`, and will create a path relative to the root
   directory of the MLDB container (for example, `file:///mldb_data/filename.txt`).

A URL that is passed without a protocol will cause an error.

## Compression support

MLDB supports decompression of files using the gzip, bzip2, xz and lz4 algorithms.
In addition, it supports most compression schemes including zip and rar when
opening archives (see below).  This decompression is performed transparently
to the user.

## Accessing files inside archives

It is possible to extract files from archives, by using the `archive+` scheme
prefix.  This is done by:

1.  Obtain a URI to the archive itself, for example `http://site.com/files.zip'
2.  Add the prefix `archive+` to the file, to show that we should be entering
    in to the archive.  So our example becomes `archive+http://site.com/files.zip`.
3.  Add a `#` character and the path of the file within the archive to extract.
    So the final URI to load becomes `archive+http://site.com/files.zip#path/file.txt`

Most archive formats, including compressed, are supported with this scheme.
Note that extracting a file from an archive may require loading the entire
archive, so for complex manipulation of archives it's still better to use
external tools.

## Credentials

MLDB can store credentials and supply them whenever required.

Capabilities have been added to control how credentials are stored and used, and
to create temporary credentials to limit its permissions to exactly what are needed.

MLDB exposes a REST API which is accessible at the route `/v1/credentials`.

### Credentials rules

Credentials are specified by giving rules.  A credentials rule contains two parts:

1.  The credentials, which give the actual credentials to access a resource as well
    as the location of that resource and other metadata required to complete the request;
2.  A rule which tells us which requests are covered by this resource.

When a resource that requires credentials is requested, MLDB will scan the stored
rules for those credentials, passing it the resource and the context (user id, etc)
in which the request is being made.  MLDB will use the first matching rule that satisfies
the request.

### Credentials storage

Credentials stored in MLDB are persisted to disk by default.  This is different
than other MLDB entities.  As a consequence, if MLDB is restarted, the credentials
will be reloaded automatically.  Deleting a credential entity will also delete
its persisted copy.

### Credentials objects

Credentials are represented as JSON objects with the following fields.

![](%%type Datacratic::StoredCredentials)

with the `credential` field (which define the actual credentials and the resource to
access) as follows:

![](%%type Datacratic::Credential)

### Storing credentials

You can `PUT` (without an ID) or `POST` (with an ID) the following object to
`/v1/credentials` in order to store new credentials:

![](%%type Datacratic::CredentialRuleConfig)


### Example: storing Amazon Web Services S3 credentials

The first thing that you will probably want to do is to post some AWS S3
credentials into the credentials daemon, as otherwise you won't be able to
do anything on Amazon.

The way to do this is to `PUT` to `/v1/credentials/<id>`:

```python
mldb.put("/v1/credentials/mys3creds", {
    "store":
    {
        "resource":"s3://",
        "resourceType":"aws:s3",
        "credential":
        {
            "protocol": "http",
            "location":"s3.amazonaws.com",
            "id":<ACCESS KEY ID>,
            "secret":<ACCESS KEY>,
            "validUntil":"2030-01-01T00:00:00Z"
        }
    }
})
```

That will be stored in the `mldb_data/.mldb_credentials/mys3creds` file, and
automatically reloaded next time that the container gets loaded up.

Be very careful about the firewall rules on your machine; credentials
are stored in plain text on the filesystem inside the container, and
it is possible to run arbitrary Python code that can access that filesystem
from plugins.

It is dangerous to use credentials that
have more permission than is strictly required (usually, read and possibly write
access to a specific path on a bucket).  See, for example, the [Amazon Web Services](http://docs.aws.amazon.com/general/latest/gr/aws-access-keys-best-practices.html)
page on best practices with access keys.

## Credentials Resources

This section lists, per resource type, the type of credentials calls that are made
for that resource.

### Amazon Web Services

Requests to AWS all start with resourceType of `aws:`.

#### Amazon S3

Requests to read from an Amazon S3 account, under `s3://` will result in a
credentials request with the following parameters:

- `resourceType`: `aws:s3`
- `resource`: `s3://...`

The `extra` parameters that can be returned are:

- `bandwidthToServiceMbps`: if this is set, then it indicates the available total
  bandwidth to the S3 service in mbps (default 20).  This influences the timeouts
  that are calculated on S3 requests.
