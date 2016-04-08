# Mutable Sparse Matrix Dataset

The Sparse Matrix Dataset is used to store high cardinality sparse data.
It provides a reasonable baseline performance over all data types,
cardinalities and data shapes.

It is designed for the following situations:

- Datasets with up to millions of rows.
- Datasets with up to millions of columns.
- Data that is very sparse to dense
- To store discrete values, or continuous values

This dataset type is mutable, and only keeps its data in memory.  Saving
and loading will come in a future iteration.

The dataset is transactional.  Each row or set of rows will atomically
become visible on commit.

The dataset is fully indexed.  It can efficiently perform both row and
column based operations, as well as be transposed.

The dataset can store atomic types.  Rows will be flattened upon storage.

Each string based value is only stored once, so longer value like strings
can be stored, and the same value may be in many columns at once.

This is an experimental dataset.  It is not guaranteed to remain available
or compatible across releases.

## Configuration

![](%%config dataset sparse.mutable)

The `consistencyLevel` values are as follows:

![](%%type Datacratic::MLDB::WriteTransactionLevel)

The `favor` values are as follows:

![](%%type Datacratic::MLDB::TransactionFavor)

## Committing

The dataset is transactional, which means that each record operation will
atomically become visible once it's completed.

It is better to record to the dataset in chunks of 1000 to 100,000 rows
at a time to avoid having too many separate, individually visible
chunks of data available at any one time.

The `commit` operation will cause the dataset to optimize its internal
storage for maximum query speed.  This should be used once the entire
dataset has been recorded or infrequently during recording.  Note that
the commit operation can take several seconds on a large dataset and
will block all writes (but not reads) while it's taking place (the
writes will end up completing once the commit operation is done).

# See also

* ![](%%doclink beh.mutable dataset)
