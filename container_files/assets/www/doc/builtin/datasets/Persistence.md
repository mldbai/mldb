# Data Persistence and Consistency

Data persistence will vary from dataset type to dataset type. For starters, not all datasets types are persistable (e.g. the ![](%%doclink sqliteSparse dataset) is but the ![](%%doclink embedding dataset) is not) or even writable/mutable (e.g. the ![](%%doclink sparse.mutable dataset) is but the ![](%%doclink embedding dataset) is not).

In general, however, dataset types which can be persisted will take an `dataFileUrl` (or equivalent) parameter, which specifies a [Url](../Url.md) where to read or write data.

The persistence characteristics of that dataset type therefore depend on the underlying protocol: data loaded from `file://` can be memory-mapped directly but data loaded from `s3://` cannot etc. Note that `file://` is *volatile* if the MLDB docker container is not booted with an `mldb_data` persistent directory mapped to a directory in the host filesystem! See [Running MLDB](../Running.md) for more details.

## Persistence across MLDB instances and reboots

By default, MLDB stores a copy of all entity configurations, including dataset configurations, in the `mldb_data` directory of the docker container (see [Running MLDB](../Running.md)). Upon (re)boot, MLDB will attempt to reload all of the entity configurations it can find, including the loading of datasets from their URLs. 

This means that so long as an `mldb_data` directory is mapped to a filesystem, MLDB will generally safely reload its (persistable/persisted) state. This safety is greatly enhanced if data is persisted to something like S3 instead of the local filesystem. Persisting data to S3 also enables the use of multiple MLDB instances, which can all load data (datasets, [model artifacts](../procedures/Procedures.md)) from the same URLs on S3.