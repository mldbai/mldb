# Tabular Dataset

The Tabular Dataset is used to represent dense datasets with more rows than
columns. It is ideal for storing text files such as Comma-Separated Values
(CSV) files, but will also do a good job at storing much JSON data or other
formats with one similar record per row.

The dataset will learn the available columns from the first data that is
recorded into it, and will be optimized to record many rows with the same
columns.  (By setting `unknownColumns` to `add`, it's not limited to
storing just the columns from the first data, but these will be the ones
for which fast storage is pre-allocated).

On a fast machine, it is capable of recording several million rows per second
from a CSV file, and will often require less memory to store the data than
the gzipped input file.  In other words, it's quite memory efficient.

## Configuration

![](%%config dataset tabular)


## Storing non-uniform data

The tabular dataset has support for storing non-uniform data, such as that
which comes from a JSON file with varying fields.  This can be accessed
by setting the `unknownColumns` field to `add`.  The other possible values
are listed below:

![](%%type MLDB::UnknownColumnAction)

## Saving and loading data

A tabular dataset which has an empty `dataFileUrl` parameter will be created
empty, and can be recorded to.  In order to save a copy, it is necessary to
`POST` to the `/saves` route:

```
mldb.post('/v1/datasets/train/routes/saves',
          { dataFileUrl: 'file://data.mldbds' });
```

which returns an object that can be `POST`ed to datasets to reload the data:

```
{
   "contentType" : "application/json",
   "json" : {
      "params" : {
         "dataFileUrl" : "file://data.mldbds"
      },
      "type" : "tabular"
   },
   "responseCode" : 200
}
```

A tabular dataset which is created with a `dataFileUrl` parameter will contain
the data loaded from the given file, but will not update the underlying file
on a commit; it is necessary to re-post to the saves route to do so.

The data format is a currently zip file split into blocks, which are
further split into
columns, each of which is serialized with metadata and one or more binary
blobs that can be memory mapped into place for the actual data; the meaning
depends upon the content of the column.  This enables the file to be loaded
rapidly and demand-paged into memory as required for queries; often a
file larger than the available memory can be loaded this way.

## Limitations

The tabular dataset has the following limitations:

- Each row has a single timestamp; it is not possible for different values
  in the row to have separate timestamps.
- Each column can only have a single value within a single row.

In practice, the former two limitations mean that rows can't be replaced,
only appended.

In addition,

- The dataset will work well up to tens of thousands of columns, but for
  extremely sparse data it will not be efficient due to a per-column
  overhead.  It's better to use a sparse dataset for these situations.
- It will not be queryable until it is committed the first time, however it
  may be committed more than once.  The cost of a commit operation, however,
  depends on the amount of data already in the dataset, and so it is generally
  not worth committing multiple times.
- It may only be loaded from the local filesystem (`file://` URLs).
- Commits on top of a loaded tabular dataset do not modify the underlying
  file; to save a new version the `save` route must be used.
- Currently, no commitment is being made to ensure that files from a different
  version of MLDB will be loadable from a new version.  At some point, this
  will change.