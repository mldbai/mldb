# Shared Library Plugin

This plugin allows the loading of Linux shared libraries to extend the functionality
of MLDB.

The shared libraries should use the MLDB API exposed in the MLDB SDK, and
register all functionality during their static initialization phase.

## Configuration

![](%%config plugin sharedLibrary)

## Initialization

During the initialization, the plugin's shared library will be initialized as
normal, including running static initializers.  Thus, a plugin that simply
needs to run these doesn't need to do anything special: it will initialize
itself as it is loaded.

If the plugin needs extra initialization, or needs to register custom handlers
for routes, etc, it can expose an initialization function.

This function must be in the root namespace, and have the following
signature:

```
Datacratic::MLDB::Plugin *
mldbPluginEnterV100(Datacratic::MLDB::MldbServer * server);
```

That function will be called by the plugin loader each time a plugin is
registered, and will be passed the MldbServer instance of the server
that owns it.

The return value of that function is:

- `nullptr`, if the plugin doesn't need to override the plugin
  functionality;
- a pointer to a `Datacratic::MLDB::Plugin` instance constructed
  by the plugin using the `new` operator.  All of the plugin methods
  (routes, status, version, etc) will forward to that object, which will
  be freed when the plugin is unloaded.

This allows the plugin to provide additional functionality that is
linked to the MLDB server it's running under.


