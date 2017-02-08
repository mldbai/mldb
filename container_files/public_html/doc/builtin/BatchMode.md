# Running MLDB in batch mode

MLDB can be run in batch mode by running the executable directly via docker:

```
docker run \
  --rm=true \
  -v </absolute/path/to/mldb_data>:/mldb_data \
  -e MLDB_IDS="`id`" \
  quay.io/mldb/mldb:latest /opt/bin/mldb_runner \
  --plugin-directory /opt/mldb/plugins \
  --run-script /mldb_data/<script>
```

The `<script>` argument can be local (in the mldb_data directory) or hosted.
For privately accessible location, one must provide the proper credentials
on the command-line as documented below.

If you want MLDB to continue to run after the script is done, you may wish to
pass the HTTP arguments and make Docker listen on a port, as so:

```
docker run \
  --rm=true \
  -v </absolute/path/to/mldb_data>:/mldb_data \
  -e MLDB_IDS="`id`" \
  -p 127.0.0.1:<mldbport>:80 \
  quay.io/mldb/mldb:latest /opt/bin/mldb_runner \
  --http-listen-port 80 \
  --plugin-directory /opt/mldb/plugins \
  --dont-exit-after-script yes \
  --run-script /mldb_data/<script>
```

## Passing arguments to the MLDB script

The `--script-args <json blob>` option can be used to pass in arguments to the run
script.  The argument should be in the form of a JSON object, array or value.
The arguments will be available to the script depending upon the language; see
the language documentation for more details.

The `--script-args-url <url>` option can be used to read the script arguments
from a URL.  The contents of the URL must be a JSON object, array or value.

## Making credentials available to MLDB in batch mode

Credentials are JSON objects that have the following format:

![](%%type MLDB::StoredCredentials)

with the `credential` field (which define the actual credentials and the resource to
access) as follows:

![](%%type MLDB::Credential)

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
 3. Using the REST API as documented in [Url](Url.md) page.
 
## Loading plugins in batch mode

MLDB will not automatically load any plugins in batch mode.  To make it do
so, add the `--plugin-directory <dir>` option to the command line.  If the
directory contains an `mldb_plugin.json` file, then that will be loaded
immediately.  If the directory contains no `mldb_plugin.json` file, but
does contain subdirectories, than each subdirectory will be scanned
recursively and found plugins loaded.

Note that at a minimum, you will likely want to load up plugins from
`/opt/mldb/plugins` as in the above examples.

## Muting last log on error

When MLDB ends with an error condition, by default it logs data about the run.
It can be disabled by using the `--mute-final-output` flag.



## Environment Variables (advanced usage)

The following environment variables control MLDB:

- `RETURN_OS_MEMORY` (0 or 1, default 1): if this is set (which it is by
  default), the whole set of deallocated memory will be returned to the
  OS whenever a dataset is destroyed.  If you observe long pauses after
  a dataset is destroyed and have a dedicated machine or a container
  with dedicated memory, you can set this to 0 to disable that behavior.
- `PRINT_OS_MEMORY` (0 or 1, default 0): if this is set (it is *not* set
  by default), then each time a dataset is destroyed, the memory usage
  will be written to the console.  In addition, if `RETURN_OS_MEMORY=1`
  then the memory usage will be re-printed after the memory is returned
  to the operating system.
