# Running MLDB in batch mode

MLDB can be run in batch mode by running the executable directly via docker:

```
docker run \
  --rm=true \
  -v </absolute/path/to/mldb_data>:/mldb_data \
  -e MLDB_IDS="`id`" \
  quay.io/datacratic/mldb:latest /usr/bin/mldb_runner \
  --plugin-directory /opt/mldb/plugins \
  --run-script /mldb_data/<script>
```

The `<script>` argument must be local (in the mldb_data directory), as the
credentials daemon is not available when running MLDB in batch mode.

If you want MLDB to continue to run after the script is done, you may wish to
pass the HTTP arguments and make Docker listen on a port, as so:

```
docker run \
  --rm=true \
  -v </absolute/path/to/mldb_data>:/mldb_data \
  -e MLDB_IDS="`id`" \
  -p 127.0.0.1:<mldbport>:80 \
  quay.io/datacratic/mldb:latest /usr/bin/mldb_runner \
  --http-listen-port 80 \
  --plugin-directory /opt/mldb/plugins \
  --dont-exit-after-script \
  --run-script /mldb_data/<script>
```

# Passing arguments to the MLDB script

The `--script-args <json blob>` option can be used to pass in arguments to the run
script.  The argument should be in the form of a JSON object, array or value.
The arguments will be available to the script depending upon the language; see
the language documentation for more details.

The `--script-args-url <url>` option can be used to read the script arguments
from a URL.  The contents of the URL must be a JSON object, array or value.

# Making credentials available to MLDB in batch mode

In batch mode, MLDB has no credentials daemon available and so credentials
will need to be passed directly.

Credentials are JSON objects that have the following format:

![](%%type Datacratic::StoredCredentials)

with the `credential` field (which define the actual credentials and the resource to
access) as follows:

![](%%type Datacratic::Credential)

There are examples of credentials under the [Url] (Url.md) page.

Credentials can be passed in to MLDB in batch mode via one or both of the
following means:

1.  Passing them on the command line in a JSON blob, using the
    `--add-credential` command line option, with the credentials following.
    This can be used multiple times to pass in multiple credentials objects.
2.  Putting them into a file, and providing the URL of the file to the
    `--add-credentials-url` command line option.  If credentials are
    needed to access the URL, then they should be added using the
    `--add-credential` option before hand.

    The contents of the file at the URL can either be:
    - A text file with JSON credentials objects one after the other, or
    - A text file containing one JSON array, with each element of the array
      being a JSON credentials object.

# Loading plugins in batch mode

MLDB will not automatically load any plugins in batch mode.  To make it do
so, add the `--plugin-directory <dir>` option to the command line.  If the
directory contains an `mldb_plugin.json` file, then that will be loaded
immediately.  If the directory contains no `mldb_plugin.json` file, but
does contain subdirectories, than each subdirectory will be scanned
recursively and found plugins loaded.

Note that at a minimum, you will likely want to load up plugins from
`/opt/mldb/plugins` as in the above examples.

