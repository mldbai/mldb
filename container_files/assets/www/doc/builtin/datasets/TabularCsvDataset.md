# Tabular CSV Dataset

The Tabular CSV Dataset is used to import CSV files into MLDB.  The
CSV file is parsed and stored in memory on creation.

## Configuration

![](%%config dataset text.csv.tabular)

## Functions available when evaluating CSV rows

The following functions are available in the `select`, `named`, `where` and `timestamp` expressions:

- `lineNumber()`: returns the line number of the CSV file
- `fileTimestamp()`: returns the timestamp (last modified time) of the CSV file
- `dataFileUrl()`: returns the URL of the datafile, from the configuration file


## Notes

- The `named` clause must result in unique row names.  If the row names are not
  unique, the dataset will fail to be created when being indexed.  The default
  `named` expression, which is `lineNumber()`, will result in each line having
  a unique name.
- The number of rows skipped (due to an error) will be returned in the
  `numLineErrors` field of the dataset status.
- The column used for the row name will *not* be automatically removed from the
  dataset.  If it needs to be removed, it should be done from the select
  using `... excluding (rowName)` syntax.
- Columns should be renamed using the select statement.  For example, to add
  the prefix `xyz.` to each field, use a `select` of `* AS xyz.*`.

# See also

