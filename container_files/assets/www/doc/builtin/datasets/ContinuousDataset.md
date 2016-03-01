# Continuous Dataset

The continuous dataset is used in MLDB to model a continuous stream of
data, for example that collected from a pixel or a log file.  It is the
primary means of collecting data (as opposed to loading data already stored
elsewhere) with MLDB.

This is a lower-level primitive that other, higher level functionality
can be built on top of.

## Data model

The continuous dataset models a stream of events, with each event
occuring on a point on a timeline (the timestamp field).  The continuous
dataset records events as they come in into an internal dataset, and
every so often (or when `commit` is called) the internal dataset is
written to files on disk.

![ContinuousDatasetModel](img/ContinuousDatasetModel.svg)

The recorded files can then be queried by timestamp, and combined
into a dataset that can be processed with MLDB as normal.

***Warning:*** The only timestamp that is processed by the continuous
dataset is the timestamp on the recorded rows.  The current system
time is not used anywhere.  In other words, if you record a timestamp
from 1980 in 2015, then it will be processed as if it occurred in 1980.

## Configuration of `continuous` Datasets

A recordable continuous dataset is created using the `continuous` dataset
type. To create a view of a time window of a continuous dataset, the
`continuous.window` dataset is used (see below).

![](%%config dataset continuous)


## Querying live data

Querying a continuous dataset will query ONLY the live data stream
that has not yet been committed.  Note that this is a live view,
and so results may change from one query to another.  For this reason
queries should be limited to lookups of rows by row name.  In particular,
analytical queries will likely fail due to different sub-queries
returning non-consistent results due to data being recorded in the
middle.

## Querying historical data with `continuous.window` Datasets

To create a view over historical data in a continuous dataset, it is
necessary to use the `continuous.window` dataset type.  This allows
for a time range to be specified, as well as a filter on the metadata
of datasets to be loaded.

![](%%config dataset continuous.window)


## Under the hood

The following section describes the design and implementation of the
continuous dataset, to aid in understanding of how to implement
advanced use-cases.

### Design

The continuous dataset builds on top of four core MLDB features to
implement its functionality:

1.  The set of available datasets is saved in a metadata dataset.
    This dataset has one entry per `commit`, and does not require
    high performance (it is written once per commit, and queried
    when a dataset is loaded).  The metadata dataset is a standard
    MLDB dataset.  If an ACID dataset is used (eg, the `sqlite.sparse`
    dataset), then the continuous dataset inherits its consistency,
    availability and durability.
2.  The (mutable) dataset into which current events are recorded is
    a standard MLDB dataset, and the choice of different mutable
    datasets will allow for different guarantees to be made.
3.  When the continuous dataset needs to create a new dataset to
    hold the next chunk of recorded data, it will call the MLDB
    procedure in the `createStorageDataset` configuration
    parameter.  This procedure should return a JSON response
    containing a `config` field that specifies a dataset config
    that can be loaded by MLDB to create the dataset.
4.  When the continuous dataset needs to save a chunk of recorded
    data, it will call the MLDB procedure in the `saveStorageDataset`
    configuration parameter.  This procedure will be passed the
    ID of the dataset to be saved in the `datasetId` argument,
    and returns a JSON object with two fields: `metadata`, which is
    a JSON object that will be recorded to the metadata dataset
    as user metadata, and `config`, which is a dataset configuration
    object that can be used by MLDB to load the saved dataset
    in the future.

### Performance

The continuous dataset can record up to 500,000 events per second on
a large server if they are presented in small batches of 1,000 events
or so.  The actual speed depends upon the following characteristics:

- The characteristics of the underlying recording dataset.  For example,
  the ![](%%doclink beh.mutable dataset) is slow when recording real-valued variables.
- The cardinality of the row and column space.  A very high cardinality
  (over 10,000 columns or so) requires extra memory accesses which
  reduces the speed.  A very low cardinality (under 100 columns or so)
  causes contention on internal data structures.
- How the examples are presented.  Presenting single events is much
  less efficient than presenting in small batches.
- The frequency of the commit() operation.  High performance requires
  that most of the events record to existing columns, and a commit()
  has the effect of creating a new dataset that doesn't know of any
  columns.


### Distributed usage

If a client-server database (like postgresql) is used as the metadata
dataset, then it is possible for multiple MLDB instances to share a
continuous dataset.  In particular,

- Multiple `continuous` datasets can share a distributed metadata dataset, which
  will allow them to all contribute data to the continuous dataset.
- A `continuous.window` dataset can point to a distributed metadata dataset,
  which will allow each of them to query over the entire window.

Note that issues of data distribution will need to be addressed at
the application level.  For example, loading up a `continuous.window`
dataset that queries data stored in multiple geographic locations may
be slow and expensive due to bandwidth cost and limitations.  In that
case, it may be better to tag the datasets with geographical location
and load them up in separate distributed MLDB instances, distributing
the query at the application level.


### Metadata format

The metadata is written in a sparse format, and so requires a dataset
that supports sparse queries.

The metadata dataset contains three main sets of data:

- Statistics.  This is an `earliest` and `latest` column which
  describes the time range of data that is present in the file.
- Configuration.  This lives under the prefix 'config.'.  It records
  the JSON configuration parameters required to make MLDB re-load
  the dataset.
- User metadata.  This lives under the prefix `md.`.  Any user-supplied
  data can be provided here.  For example, if there was a separate
  continuous dataset recording to the same metadata database for each
  country, then the `md.country` field might be recorded into the
  metadata database.  This would allow a `continuous.window` dataset to
  be constructed that only contained data from given countries.

### Limitations

- The underlying dataset's `commit` is only called just before the
  dataset is saved.  For most datasets, this means that on a crash of
  MLDB, all data that hasn't finished saving will be lost.  A write-
  ahead log will be added in a further release to protect against
  this eventuality.
- MLDB will not notice if a file mentioned in the metadata database
  is removed, and if this happens the construction of a window dataset
  will fail.
- If a file mentioned in the metadata database is rewritten or modified,
  MLDB will not notice that its contents are different, and will access
  the file as if it contained the original contents.  This may be
  surprising to the user.  The best way to avoid this happening is to
  never overwrite a file used for a dataset.
- The continuous dataset does not clean up old files.  This should be
  performed by an external process with access to the metadata database,
  which should be cleaned up consistently with respect the actual files
  (in other words, entries should be removed from the metadata database
  BEFORE they are removed from disk, and there should be a grace period
  to allow an in-progress load to finish loading a file before it
  disappears).
- There is no way to add existing datasets into a continuous dataset
  (ie, to bulk-load data that is already in MLDB).  This can be done 
  manually by writing it to a persistent dataset and adding the
  appropriate entries to the metadata database.
- Currently, only the ![](%%doclink beh.binary.mutable dataset) dataset type implements all
  of the functionality required to be reliably used with MLDB.

# Example

Here is a sample configuration of the `createStorageDataset` procedure.
The JS source of the procedure is:

```javascript
// Create a binary behaviour dataset to save into
var config = { type: "beh.binary.mutable" };
var dataset = mldb.createDataset(config);

// Construct our output, which has a `config` parameter to pass back
var output = { config: dataset.config() };

// Return our output
output;
```

And the `saveStorageDataset` procedure is:

```javascript
// This is the URI we save our dataset to
var uri = "file://mydata/" + new Date().toISOString() + ".beh";

// The REST address for the dataset
var addr = "/v1/datasets/" + args.datasetId; 

// Call its save route
var res = mldb.post(addr + "/routes/saves", { dataFileUrl: uri });

// Log the result of the REST call
mldb.log(res);

// Construct our response to return to MLDB, with our
var output = { metadata: mldb.get(addr).json.status, config: res.json, metadata: { country: 'france' } };

// Return the result as the output of the procedure
output;
```

Putting it together, we construct our continuous dataset as follows

```python
mldb.put("/v1/datatasets/example", {
    "id": "recorder",
    "type": "continuous",
    "params": {
        "commitInterval": "1s",
        "metadataDataset": {
            "type": "sqliteSparse",
            "id": "metadata-db",
            "params": {
                "dataFileUrl": "file://mydata/metadata.sqlite"
            }
        },
        "createStorageDataset": {
            "type": "script.run",
            "params": {
                "language": "javascript",
                "scriptConfig": {
                    "source": "<as above>"
                }
            }
        },
        "saveStorageDataset": {
            "type": "script.run",
            "params": {
                "language": "javascript",
                "scriptConfig": {
                    "source": "<as above>"
                }
            }
        }
    }
})
```

And a configuration to read data recorded in the last 3 days is

```python
mldb.put("/v1/datatasets/example2", {
    "id": "window",
    "type": "continuous.window",
    "params": {
        "metadataDataset": {
            "id": "metadata-db",
        },
        "from": "<3 days ago in ISO8601 format>",
        "to": "<now in ISO8601 format>",
    }
})
```

# See also

* The ![](%%doclink beh.mutable dataset) allows files
  of the given format to be created.
