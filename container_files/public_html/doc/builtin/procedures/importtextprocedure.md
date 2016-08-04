# Importing Text

This procedure is used to import data from text files with each line in the file corresponding to a row in the output dataset. Delimited file formats such as Comma-Separated Values (CSV) or Tab-Separated Values (TSV) are supported by configuring delimiters and quote characters.

## Configuration

![](%%config procedure import.text)


## Functions available when creating rows

The following functions are available in the `select`, `named`, `where` and `timestamp` expressions:

- `lineNumber()`: returns the line number in the file
- `rowHash()`: returns the internal hash value of the current row, useful for random sampling
- `fileTimestamp()`: returns the timestamp (last modified time) of the file
- `dataFileUrl()`: returns the URL of the file, from the configuration


## Notes

- The `named` clause must result in unique row names.  If the row names are not
  unique, the dataset will fail to be created when being indexed.  The default
  `named` expression, which is `lineNumber()`, will result in each line having
  a unique name.
- The number of rows skipped (due to a parsing error) will be returned in the
  `numLineErrors` field of the dataset status.
- The column used for the row name will *not* be automatically removed from the
  dataset.  If it needs to be removed, it should be done from the select
  using the `excluding (colName)` syntax.
- Columns can be renamed using the select statement.  For example, to add
  the prefix `xyz.` to each field, use `* AS xyz.*` in the `select` parameter.

## Examples

* The ![](%%nblink _tutorials/Loading Data Tutorial)
* The ![](%%nblink _demos/Predicting Titanic Survival) demo

## See also

* The ![](%%doclink import.json procedure) can be used to import a text file with one JSON per line.
