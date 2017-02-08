# Plugins

MLDB is a totally modular system, with very little functionality in the core. The [dataset types](../datasets/DatasetConfig.md), [procedure types](../procedures/ProcedureConfig.md) and [function types](../functions/FunctionConfig.md) that are available are all loaded via MLDB's plugins mechanism.

MLDB plugins have access to a low-level C API which they can use to define new dataset types, procedure types and function types, and can even be used to define new plugin types to make it easier to write additional plugins in languages other than C (e.g. the ![](%%doclink python plugin) uses the C API to make it possible to write plugins in Python by wrapping parts of the C API and embedding a Python runtime in MLDB).

Creating new dataset types enables developers to integrate MLDB with new sources of data: datasets can pull from other databases, or can subscribe to event streams. Creating new procedure and function types enables developers to bring their own algorithms into MLDB.

Beyond defining new entity types, MLDB plugins allow developers to extend MLDB's [REST API](../WorkingWithRest.md) by adding new routes. This mechanism can be used to create rich user interfaces that interact with and are served by MLDB. See this [list of plugins](ExamplePlugins.md) for examples of what plugins can do.

## Loading plugins

Plugins are loaded into MLDB via a [REST API call](PluginConfig.md).

## Auto-loading plugins

MLDB will load up system plugins from `/opt/mldb/plugins` automatically on startup.

MLDB will recursively scan the directory `/mldb_data/plugins/autoload`, and for each file named `mldb_plugin.json` found,
it will autoload the plugin described by it. `mldb_plugin.json` must contain a JSON payload with a single top key, `config`,
having as a value an object similar to what needs to be `POST` or `PUT` when creating a plugin.

Here is an example for a python plugin.
```
{
    "config" : {
        "id" : "demo_plugin",
        "type" : "python",
        "params" : { }
    }
}
```
`params` can be left empty as MLDB will automatically set the "address" value to the path where the plugin was found.

## Writing Plugins

The C API SDK is not currently public, so only the ![](%%doclink python plugin) allows developers to create their own plugins, and the API it exposes allows plugins to execute Python code once at load-time, as well as in response to HTTP calls, by registering route-handlers. In addition, these plugins can serve up static and documentation content.

The process of writing a Python plugin is as follows:

* create a directory where the Python plugin-type can access it: in a public Git repository like Github or in the mapped `mldb_data` directory
    * this directory must contain a `main.py` file, containing code which will have access to the `mldb` object
    * this directory should contain a `doc` directory with an `index.md` file
* load the plugin using the main UI or the Notebook interface
* iterate

