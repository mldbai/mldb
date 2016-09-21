# Tabular Dataset

The Tabular Dataset is used to represent dense datasets with more rows than
columns. It is ideal for storing text files such as Comma-Separated Values
(CSV) files.

The dataset will learn the available columns from the first data that is
recorded into it, and will be optimized to record many rows with the same
columns.  (By setting `unknownColumns` to `add`, it's not limited to
storing just the columns from the first data, but these will be the ones
for which fast storage is pre-allocated).

On a fast machine, it is capable of recording several million rows per second
from a 10 column CSV file.

## Configuration

![](%%config dataset tabular)


## Storing non-uniform data

The tabular dataset has support for storing non-uniform data, such as that
which comes from a JSON file with varying fields.  This can be accessed
by setting the `unknownColumns` field to `add`.  The other possible values
are listed below:

![](%%type MLDB::UnknownColumnAction)


## Limitations

The tabular dataset has the following limitations:

- Each row has a single timestamp; it is not possible for different values
  in the row to have separate timestamps.
- Each column can only have a single value within a single row.
- The dataset will work well up to tens of thousands of columns, but for
  extremely sparse data it will not be efficient due to a per-column
  overhead.  It's better to use a sparse dataset for these situations.
- It may only be committed once, and will not be queryable until it is
  committed the first time.  As a result, this dataset type is mostly
  useful for analytic, not operational data.
- It is not possible to save data from the Tabular dataset, apart from
  by writing it to a CSV file (see the ![](%%doclink csv.export procedure).
