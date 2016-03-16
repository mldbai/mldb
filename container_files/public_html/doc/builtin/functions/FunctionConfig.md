# Function Configuration

A function configuration object is used to create or load a [function] (Functions.md).

It is a JSON object that looks like this:

```json
{ 
  "id": <id>, 
  "type": <type>, 
  "params": { 
    <params> 
  } 
}
```

* `id` is a string that defines the URL at which the function will be available via the REST API
* `type` is a string that specified the function's type (see below)
* `params` is an object that configures the function, and whose contents will vary according to the type

Not all three of these fields are required in all contexts:

* one or both of `id` and `type` must be specified
    * if only `id` is specified, MLDB will assume this is a pre-existing function and will try to load it (an error will ensue if it doesn't already exist)
    * if `type` is specified, MLDB will assume that the function doesn't exist yet and will try to create it (an error will ensue if it already exists)
        * if `type` is specified without `id`, an id will be auto-generated
        * if `type` is specified with `id`, the function will be created with the specified `id` unless a function already exists with that id
        * if `type` is specified, then a corresponding `params` function must be specified if the type requires it

The following types of functions are available:

![](%%availabletypes function table)


## See also

- [Datasets] (../datasets/Datasets.md)
- [Procedures] (../procedures/Procedures.md)
- [Functions] (../functions/Functions.md)
- [Plugins] (../plugins/Plugins.md)
