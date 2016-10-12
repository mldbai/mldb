# Files and URLs

MLDB gives users full control over where and how data is persisted. Datasets can be saved and loaded from files. Procedures can create files and Functions can load up parameters from files.

If data is stored in a system such as S3 or HDFS, it is possible to run multiple instances of MLDB so as to distribute computational load. For example, one powerful and expensive machine could handle model training, storing model files on S3, while a load-balanced cluster of smaller, cheaper machines could load these model files and do real-time scoring over HTTP.

## Protocol Handlers

The following protocols are available for URLs:

- `http://` and `https://`: standard HTTP, to get a file from an HTTP server on the public
  internet or a private intranet.
- `s3://`: Refers to a file on Amazon's S3 service.  If the file is not public, then
  credentials must be added.
- `sftp://` Refers to a file on an SFTP server. Credentials must be added. If a custom port is used,
  it must simply be part of the url. (For example, `sftp://host.com:1234/`.) The same is true
  for the credentials location parameter. (To continue with the same example, `host.com:12345`.)
- `file://`: Refers to a file inside the MLDB container.  These resources are only
  accessible from the same container that created them.  A relative path (for example
  `file://filename.txt`) has two slashes after `file:`, and will create a file in the
   working directory of MLDB, that is the `mldb_data` directory.  An absolute path has
   three slashes after the `file:`, and will create a path relative to the root
   directory of the MLDB container (for example, `file:///mldb_data/filename.txt`).

A URL that is passed without a protocol will cause an error.

For examples of how to use the above protocol handlers, take a look at the 
![](%%nblink _tutorials/Loading Data Tutorial) and the ![](%%nblink _tutorials/Loading Data From An HTTP Server Tutorial).

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

MLDB exposes a REST API which is accessible at the route `/v1/credentials`. It
is also possible to specify credentials when starting MLDB.  Please refer
to the [BatchMode](BatchMode.md) Section for details.

### Credential rules

Credentials are specified by giving rules.  A credential rule contains two parts:

1.  The credentials, which gives the actual secret information to access a resource as well
    as the location of that resource and other metadata required for the service
    hosting the resource to complete the request;
2.  A rule which includes the resource type (e.g. S3) and a resource path prefix.
    The rule allows a user to store many credentials for the same service
    and control when the credentials is used.

When a resource that requires credentials is requested, MLDB will scan the stored
rules for matching credentials.  The matching is performed on the resource path
using prefixes.  For example, suppose that two sets of credentials are stored
in MLDB for the AWS S3 service, one set with read-only access

```
{
  "store":{
    "resource":"s3://public.example.com",
    "resourceType":"aws:s3",
    "credential":{
      "protocol":"http",
      "location":"s3.amazonaws.com",
      "id":"<READ ACCESS KEY ID>",
      "secret":"<READ ACCESS KEY>",
      "validUntil":"2030-01-01T00:00:00Z"
    }
  }
}
```

and one set with read-write access

```
{
  "store":{
    "resource":"s3://public.example.com/mystuff",
    "resourceType":"aws:s3",
    "credential":{
      "protocol":"http",
      "location":"s3.amazonaws.com",
      "id":"<READ-WRITE ACCESS KEY ID>",
      "secret":"<READ-WRITE ACCESS KEY>",
      "validUntil":"2030-01-01T00:00:00Z"
    }
  }
}
```
When requesting this resource `s3://public.example.com/text.csv`, MLDB will match the first
rule only because its `resource` field is a prefix of the resource path and it will therefore
use the read-only credentials. On the other-hand, if the resource
`s3://public.example.com/mystuff/text.csv` is requested, both rules will match but
MLDB will use the second one because it matches a longer prefix of the resource path.

### Credentials storage

Unlike other resources stored in MLDB, the credentials are persisted to disk under the `.mldb_credentials`
sub-folder of the `mldb_data` folder.  The credentials are persisted in clear so it is important to
protect them and users are encourage to put in place the proper safeguards on that location.
Deleting a credential entity will also delete its persisted copy.

### Credentials objects

Credentials are represented as JSON objects with the following fields.

![](%%type MLDB::StoredCredentials)

with the `credential` field (which define the actual credentials and the resource to
access) as follows:

![](%%type MLDB::Credential)

### Storing credentials

You can `PUT` (without an ID) or `POST` (with an ID) the following object to
`/v1/credentials` in order to store new credentials:

![](%%type MLDB::CredentialRuleConfig)


### Example: storing Amazon Web Services S3 credentials

The first thing that you will probably want to do is to post some AWS S3
credentials into MLDB, as otherwise you won't be able to
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

Requests to AWS all start with `resourceType` of `aws:`.

#### Amazon S3

Requests to read from an Amazon S3 account, under `s3://` will result in a
credentials request with the following parameters:

- `resourceType`: `aws:s3`
- `resource`: `s3://...`

The `extra` parameters that can be returned are:

- `bandwidthToServiceMbps`: if this is set, then it indicates the available total
  bandwidth to the S3 service in mbps (default 20).  This influences the timeouts
  that are calculated on S3 requests.


