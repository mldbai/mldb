# Mutable Binary Behaviour Dataset

**This feature is part of the [MLDB Pro Plugin](../../../../doc/builtin/ProPlugin.md) and so can only be used in compliance with the [trial license](../../../../doc/builtin/licenses.md) unless a commercial license has been purchased**

The Mutable Behaviour Dataset is used to store pure binary behavioural data.  It can
only store the value `1`, but is extremely efficient for working with lots
of rows and columns.  It records timestamps with a resolution of one second.

It is designed for the following situations:

- Datasets with up to hundreds of millions of rows.
- Datasets with up to millions of columns.
- For datasets where there are a lot more rows than columns.  For example, when
  rows represent users and columns represent things that they have done.
- Very sparse data, with a density factor of less than one percent.
- To store discrete values, not continuous values.  For example if you need to
  record purchase price, put it in a price bucket rather than recording the
  actual price.

It stores its data in memory, and allows full random access to both the matrix
and its inverse.  It is quite efficient in memory usage.  It can be simultaneously
read from and updated.

This dataset type is read-write.  It cannot be committed; it will always
keep its contents in memory; calling commit() on the dataset will simply make
it immutable, which speeds up access but makes any further modifications fail.
It can, however, be saved (see below).

## Configuration

![](%%config dataset beh.binary.mutable)

## Routes

This dataset inherits the `saves` route from the ![](%%doclink beh.binary dataset)
which can be used to save the data to a `.beh` file.

Note that currently, an (unfortunate) side effect of the saving process is to make
the dataset immutable.

# See also

* The ![](%%doclink beh.binary dataset) is an immutable counterpart.
* The ![](%%doclink beh dataset) uses the same file format
  but allows values that aren't `1` to be stored.
