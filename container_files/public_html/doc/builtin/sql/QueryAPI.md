
# Query API: `GET /v1/query`

This endpoint accepts the following query-string parameters:

- `q`: a full [SQL query](Sql.md)
- `format`: string (default `full`), gives the output format.  Possible values are:
  - `full` (default): full sparse output as array of deep objects. 
      - All values for each cell are returned, with timestamps.
  - `table`: a table represented as an array of rows represented as
    a position arrays of values,with an optional header row (like a CSV file in JSON).   
      - Missing values are represented as null. 
      - Timestamp, interval and Nan/Inf values are converted to strings.
      - Latest value returned per cell, without timestamp
  - `aos` (for "array of structures"): an array of objects, one per row.
      - Each row is represented by an object, with column names as keys and
 cell values as the value.
      - Latest value returned per cell, without timestamp
  - `soa` (for "structure of arrays"): an object, with one entry per column.
      - Each column has an array with one value per row.
      - Missing values are represented as nulls.
      - Latest value returned per cell, without timestamp
  - `sparse`: an array of arrays of arrays. Same as `aos` format except that
    rows are represented as arrays of 2-element [column, value] arrays instead
    of objects. 
      - All values for each cell are returned, without timestamps
  - `atom`: a single atomic value, without the row name or the column name
      - The query will fail if anything else than a single row / column is returned.
- `headers`: boolean (default `true`), if `true` the table format will include a header.
- `rowNames`: boolean (default `true`), if `true` an implicit column called `_rowName` will
   be added, containing the row name.
- `rowHashes`: boolean (default `false`), if `true` an implicit column called
  `_rowHash` will be added. Forced to `true` when `format=full`.

Note that instead of passing the parameters in the query string, you can
alternatively pass them in the body.

### Cell value representation

JSON defines numerical, string, boolean and null representations, but not timestamps, intervals, NaN or Inf.
In order to deal with this, the output of the Query API (except when in `format=table` mode) will represent
these types of values as a JSON object as follows:

```python
{"ts" : "1969-07-20T01:02:03.000Z"}
{"interval": "3 MONTH 14D 1S"}
{"num": "NaN"}
{"num": "Inf"}
{"num": "-Inf"}
```

### Examples

For the following dataset, where all values have the timestamp `2015-01-01T00:00:00.000Z`:

| rowName | x | y | z |
|-----------|-----|-----|-----|
| row1      | 0   |   3 |
| row2      | 1   |   2 |"yes"|
| row3      | 2   |   1 |
| row4      | 3   |   0 | "no"|

Then the query

```sql
SELECT * ORDER BY rowName()
```
would return, depending on the parameters:


#### Default format with no `format` parameter or `format=full`

```javascript
[
   {
      "columns" : [
         [ "x", 0, "2015-01-01T00:00:00.000Z" ],
         [ "y", 3, "2015-01-01T00:00:00.000Z" ]
      ],
      "rowHash" : "397de880d5f0376e",
      "rowName" : "ex1"
   },
   {
      "columns" : [
         [ "z", "yes", "2015-01-01T00:00:00.000Z" ],
         [ "x", 1, "2015-01-01T00:00:00.000Z" ],
         [ "y", 2, "2015-01-01T00:00:00.000Z" ]
      ],
      "rowHash" : "ed64a202cef7ccf1",
      "rowName" : "ex2"
   },
   {
      "columns" : [
         [ "y", 1, "2015-01-01T00:00:00.000Z" ],
         [ "x", 2, "2015-01-01T00:00:00.000Z" ]
      ],
      "rowHash" : "418b8ce19e0de7a3",
      "rowName" : "ex3"
   },
   {
      "columns" : [
         [ "x", 3, "2015-01-01T00:00:00.000Z" ],
         [ "z", "no", "2015-01-01T00:00:00.000Z" ],
         [ "y", 0, "2015-01-01T00:00:00.000Z" ]
      ],
      "rowHash" : "213ca5902e95224e",
      "rowName" : "ex4"
   }
]
```

#### Table with `format=table`

```javascript
 [
   [ "_rowName", "x", "y", "z" ],
   [ "ex1", 0, 3, null ],
   [ "ex2", 1, 2, "yes" ],
   [ "ex3", 2, 1, null ],
   [ "ex4", 3, 0, "no" ]
]
```

#### Structure of Arrays with `format=soa`

```javascript
 {
   "_rowName" : [ "ex1", "ex2", "ex3", "ex4" ],
   "x" : [ 0, 1, 2, 3 ],
   "y" : [ 3, 2, 1, 0 ],
   "z" : [ null, "yes", null, "no" ]
}
```

### Array of Structures with `format=aos`

```javascript
[
 { "_rowName" : "ex1", "x" : 0, "y" : 3 },
 { "_rowName" : "ex2", "x" : 1, "y" : 2, "z" : "yes" },
 { "_rowName" : "ex3", "x" : 2, "y" : 1 },
 { "_rowName" : "ex4", "x" : 3, "y" : 0, "z" : "no"  }
]
```

#### Sparse format with `format=sparse`

```javascript
[
   [
      [ "_rowName", "ex1" ],
      [ "x", 0 ],
      [ "y", 3 ]
   ],
   [
      [ "_rowName", "ex2" ],
      [ "z", "yes" ],
      [ "x", 1 ],
      [ "y", 2 ]
   ],
   [
      [ "_rowName", "ex3" ],
      [ "y", 1 ],
      [ "x", 2 ]
   ],
   [
      [ "_rowName", "ex4" ],
      [ "x", 3 ],
      [ "z", "no" ],
      [ "y", 0 ]
   ]
]
```
