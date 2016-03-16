# Docker Plugin

This plugin allows a binary plugin to be loaded using Docker.

*WARNING* This plugin currently makes no attempt to verify the shared library.
You should only load from sources that you trust.


## Configuration

![](%%config plugin experimental.docker)

## Startup

The plugin will attempt to call the function `mldbPluginEnter` within the
loaded shared library.  It should have the following signature:

```C++

#include "mldb/server/plugin.h"

extern "C" {

/// This is the function that MLDB will call to initialize the plugin.
/// It needs to return a newly created plugin object, that will be managed
/// by MLDB
MLDB::Plugin * mldbPluginEnter(MldbServer * server);

} // extern C

```


## Notes

1.  This plugin currently makes no attempt to verify the shared library.
    You should only load from Docker sources that you trust.
2.  Docker credentials and authentication aren't supported, so you will
    need to load from public Docker repositories.
3.  This plugin goes straight to the Docker repository; it does not query
    the local machine to see if it has a cached copy of the plugin.
4.  Only `quay.io` has been tested as a supported Docker repository.
