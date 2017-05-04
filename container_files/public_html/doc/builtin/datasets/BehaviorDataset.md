# Behaviour Dataset

**This feature is part of the [MLDB Pro Plugin](../../../../doc/builtin/ProPlugin.md) and so can only be used in compliance with the [trial license](../../../../doc/builtin/licenses.md) unless a commercial license has been purchased**

The Behaviour Dataset is used to store behavioural data.  It is designed for
the following situations:

- For datasets where there are a lot more rows than columns.  For example, when
  rows represent users and columns represent things that they have done.
- Very sparse data, with a density factor of less than one percent.
- To store discrete values, not continuous values.  For example if you need to
  record purchase price, put it in a price bucket rather than recording the
  actual price.

The reason for these restrictions is that the underlying data structure
stores (userid,feature,timestamp) tuples, rather than the
(userid,key,value,timestamp) format for MLDB.  A new "feature" is created for
every combination of (key,value) which can lead to a lot of storage being
taken up if a key has many values.

It stores its data in a binary file format, normally with an extension
of `.beh`, which is specified by the `dataFileUrl` parameter.  This file format
is allows full random access to both the matrix and its inverse and is very
efficient in memory usage.

This dataset type is read-only, in other words it can only load up datasets
that were previously written from an artifact.  See the
![](%%doclink beh.mutable dataset) for how to create
these files.

## Configuration

![](%%config dataset beh)


# See also

* The ![](%%doclink beh.mutable dataset) allows files
  of the given format to be created.
