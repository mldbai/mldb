# The MLDB plugin SDK

The plugin SDK allows binary compatible plugins with MLDB to be
built.  It also allows for this process to be automated so that
it will happen automatically on the release of a new version of MLDB.

## Components

The MLDB plugin SDK includes the following components:

1.  A reduced set of header files that describe the internal ABI for
    plugins for MLDB.  Currently no ABI buffer layer is provided,
    although likely one will be in the future.
1.  A docker layer, on top of MLDB, which adds the necessary compilers,
    headers and libraries to build plugins for MLDB.
1.  Doxygen-generated documentation for the included header files.

## Releasing an SDK

```
make docker_mldb_sdk
```

This will make the following containers:

* `quay.io/mldb/mldb`
* `quay.io/mldb/mldb_sdk`

with the following tags

* `(username)-(branchname)-(commitid)`
* `(username)-(branchname)`
* `(username)_latest`
* `(branchname)`

If you want to push them to quay.io, add `DOCKER_PUSH=1`.
If you want to allow building from a dirty branch, add `DOCKER_ALLOW_DIRTY=1`.
If you want to make it a production release, add `DOCKER_TAG=latest`.

## Making a plugin automatically build

Using `quay.io` or `docker.io` you can set a MLDB plugin to automatically
build when one of the following events happens:

1.  Code is pushed to the GitHub repo with the plugin
2.  The underlying mldb_sdk container is uploaded

Firstly, you will need to set up a `Dockerfile` in the root of your plugin
repo.  This should build the whole container, probably by first copying the
source code inside the container and then running the build script.

## Plugin development process

### Compiling within docker from a separate repo

The easiest way to create a plugin is to compile with docker from a separate
repositiory.  This requires the creation of a Dockerfile with the build
instructions (likely a Makefile).  For local testing, it suffices to use

```
docker build -t <repo and tag name> . && docker push <repo and tag name>
```

to build the repo and push the image upstream.  A local MLDB instance can
then be used to load the plugin.

### Compiling within docker from a local directory

A more traditional development process will have the source code for the
plugin in a local directory, and will use Docker to build the plugin.
This can be done by mounting the local directory with source-code
read-only, and an output directory read-write, as follows:

```
docker run quay.io/mldb/mldb_sdk -v .:/source:ro -v output_dir:/output:rw make -C /source OUT_DIR=/output
```

With the appropriate Makefile, this will build the plugin within docker but
using the local filesystem for input and output.

### Compiling a plugin outside of Docker

This would involve creating an MLDB-compatible system environment, copying
the header files and MLDB binaries from an MLDB container into the system,
and using a traditional development process for the rest.

## Plugin interface

A loadable plugin is a shared library that provides the following interface:

```C++

extern "C" {

/// This is the function that MLDB will call to initialize the plugin.
/// It needs to return a newly created plugin object, that will be managed
/// by MLDB
MLDB::Plugin * mldbPluginEnter(MldbServer * server);

} // extern C


```

The plugin may register other types of entities (procedures, functions,
datasets, etc) as part of initialization or plugin entry.

An example of a sample plugin is included below (this is available [here](https://github.com/datacratic/mldb_sample_plugin) ).

### plugin.cc

```C++

#include "mldb/soa/types/value_description.h"
#include "mldb/server/function.h"
#include "mldb/server/plugin.h"

using namespace MLDB;
using namespace std;

/// We are defining a new Function here, so we need to derive from
/// MLDB::Function and implement that interface.
struct HelloWorldFunction: public MLDB::Function {

    /// Our constructor knows about the MLDB server we're integrated
    /// into, its configuration and includes a progress object that
    /// we can optionally use to give feedback into initialization
    /// progress (we don't use it, as initialization is intstantaneous).
    HelloWorldFunction(MLDB::MldbServer * server,
                       PolyConfig config,
                       std::function<bool (Json::Value)> onProgress)
        : MLDB::Function(server)
    {
    }

    /// Return the status of the plugin.  We return a simple constant
    /// string.  This is what will be returned from the
    /// GET /v1/functions/.../status API route.
    virtual Any getStatus() const
    {
        return std::string("A-OK");
    }

    /// Return the function information.  This tells MLDB about what the
    /// types of the input and output are, in order to allow for the
    /// function to be compiled as part of an SQL expression.  We
    /// return a single pin called "hello" with a string value, so that's
    /// what we return.
    virtual FunctionInfo getFunctionInfo() const
    {
        FunctionInfo result;
        result.output.addValue("hello", std::make_shared<MLDB::StringValueInfo>());
        return result;
    }

    /// Apply the function.  The applier contains information about the
    /// function itself, and the context gives us the input arguments.
    /// Since we don't do anything complicated or read the input, neither
    /// argument is used.
    virtual FunctionOutput apply(const FunctionApplier & applier,
                                 const FunctionContext & context) const
    {
        // Holds our result.
        FunctionOutput result;

        // Create the string "world" as of the current timestamp.  In MLDB,
        // every data point has an associated timestamp at which it's known.
        ExpressionValue world("world", Date::now());

        // Set the value of the "hello" column to our string
        result.set("hello", world);


        return result;
    }
};

/// Create the plugin itself.  In our case, we only want the plugin as a
/// way to load up the HelloWorld function, so we don't add any
/// functionality over the base class.

struct SamplePlugin: public MLDB::Plugin {

    SamplePlugin(MldbServer * server)
        : MLDB::Plugin(server)
    {
    }
    
    /// Override the getStatus method to return a custom method
    Any getStatus() const
    {
        return std::string("SamplePlugin is loaded");
    }
};

extern "C" {

/// This is the function that MLDB will call to initialize the plugin.
/// It needs to return a newly created plugin.
MLDB::Plugin * mldbPluginEnter(MldbServer * server)
{
    return new SamplePlugin(server);
}

} // extern C

/// Put this declaration in "file scope" so it doesn't clash with other
/// plugin initializations.
namespace {

/// Register our function with MLDB.  We say here that our function
/// hello.world takes an integer for configuration (which is ignored),
/// has the given description and has no documentation available.

static RegisterFunctionType<HelloWorldFunction, int>
regHelloWorldFunction("hello.world",
                    "Sample function that always returns hello = \"world\"",
                    "No documentation available");


} // file scope

```

### Makefile


```
default: plugin.so

CXX := g++ -fPIC -std=c++0x -O2 -I /opt/include

plugin.o:	plugin.cc
	$(CXX) -c -o plugin.o $<

plugin.so:	plugin.o
	$(CXX) -shared -o $@ $<
```

### Dockerfile

```
FROM quay.io/mldb/mldb_sdk:latest

COPY . /build
RUN cd /build && make -j2 -k
```

### Building and loading the plugin

To build the plugin on any machine with Docker installed, it should suffice to
run

```
docker build -t quay.io/mldb/mldb_sample_plugin . && docker push quay.io/mldb/mldb_sample_plugin
```

You can then load it using something like

```

var pluginConfig = {
    id: 'external1',
    type: 'experimental.external',
    params: {
        startup: {
            type: 'experimental.docker',
            params: {
                repo: 'quay.io/mldb/mldb_sample_plugin:latest',
                sharedLibrary: 'build/plugin.so'
            }
        }
    }
};


var resp = mldb.post('/v1/plugins', pluginConfig);

```
