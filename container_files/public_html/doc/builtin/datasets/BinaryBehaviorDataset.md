# Binary Behaviour Dataset

**This feature is part of the [MLDB Pro Plugin](../../../../doc/builtin/ProPlugin.md) and so can only be used in compliance with the [trial license](../../../../doc/builtin/licenses.md) unless a commercial license has been purchased**

The Behaviour Dataset is used to store pure binary behavioural data.  It can
only store the value `1`, but is extremely efficient for working with lots
of rows and columns.  It records timestamps with a resolution of one second.

It can be memory mapped, which allows it to work with data that is larger
than memory.

It is designed for the following situations:

- Datasets with up to hundreds of millions of rows.
- Datasets with up to millions of columns.
- For datasets where there are a lot more rows than columns.  For example, when
  rows represent users and columns represent things that they have done.
- Very sparse data, with a density factor of less than one percent.
- To store discrete values, not continuous values.  For example if you need to
  record purchase price, put it in a price bucket rather than recording the
  actual price.

It stores its data in a binary file format, normally with an extension
of `.beh`, which is specified by the `dataFileUrl` parameter.  This file format
is allows full random access to both the matrix and its inverse and is very
efficient in memory usage.

This dataset type is read-only, in other words it can only load up datasets
that were previously written from a legacy system.

## Configuration

![](%%config dataset beh.binary)

## Routes

The binary behaviour dataset exposes a `saves` route, which allows the dataset
to be saved to a given artifact URL.  This route has one single parameter passed
in the JSON body: `dataFileUrl` which is the URL of where the artifact (.beh file)
should be saved.

# See also

* The ![](%%doclink beh.binary.mutable dataset) is a mutable counterpart.
* The ![](%%doclink beh dataset) uses the same file format
  but allows values that aren't `1` to be stored.
