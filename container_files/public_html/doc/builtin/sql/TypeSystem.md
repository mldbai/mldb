# MLDB Type System

This page describes the type system MLDB uses to store and process data.

## Atomic types

MLDB's atomic types are the following:

- A null value
    - Null values can appear in queries as `null` (case-insensitive)
    - Null takes its standard SQL significance meaning "unknown". This means that the vast majority of expressions which contain a null value as an argument will evaluate to `null`, unless it is impossible for its value to be unknown. Cases of note:
      - `null = null` will evaluate to `null` as neither value is known, so the equality is unknown. The way to check for null values is to use the `IS` operator: `null IS NULL` will evaluate to `true`.
      - `false AND x` cannot evaluate to `true` for any value of `x` so this will evaluate to `false` even if `x` is `null`.
      - `true OR x` cannot evaluate to `false` for any value of `x` so this will evaluate to `true` even if `x` is `null`.
- An integer, which can take any value of a signed or unsigned 64 bit integer
    - Integers can appear in queries directly i.e. `1` or `-3`
    - `true` is a synonym for `1` and `false` is a synonym for `0`
- A double precision floating point value, including infinity, negative infinity
and NaN (not a number)
    - Doubles can appear in queries directly, optionally in exponential notation: `12.2`, `1.22e1`, `inf`, `nan` (case-insensitive)
- A string, which is any UTF8 encoded sequence of characters.
    - String literals can appear in queries surrounded in single-quote characters (i.e. `'`). Single-quote characters within string literals must be doubled (i.e. `''`). Strings are *always* encoded as UTF-8 Unicode characters.
- A timestamp, which is a point in time that is independent of any timezone.  It is
internally represented as the number of seconds since the 1st of January, 1970,
at GMT.
    - Timestamps can appear queries using the `TIMESTAMP` keyword, for example `TIMESTAMP '2010-01-02T23:45:33Z'` or `TIMESTAMP 1234`. Strings are parsed as ISO8601 strings and require a timezone offset and numbers are treated as seconds since '1970-01-01T00:00:00Z'.
- A time interval, which is a difference between two timestamps.  It is
internally represented as three fields expressing months, days, and seconds.
    - Time intervals can appear in queries using the  using the `INTERVAL` keyword and a string containing a sequence of values 
followed by one of the supported time units: second, minute, hour, day, week, month, and year, e.g. `INTERVAL '2 day 37 minute'`
The time units can be wholly capilalized (e.g. `YEAR`), and are always singular. 
The following abreviations also work in upper and lower case: 's' for second, 'm' for minute, 'h' for hour, 'd' for day,
'w' for week, and 'y' for years
A single leading `-` in the string will reverse the direction of the interval, e.g. `INTERVAL '-3 month 2 week'`.
- A binary blob.  This
  can be used to store or process arbitrary binary data, including that which
  contains characters that can't be represented as a string.
  - Binary blobs can appear in queries using the `base64_decode` function
    with a string as its argument.
- A path.  This type is a list of coordinates into an array or object, and is
  used as row and column names.  For example, the value `3` inside
  `{ "x": [ 1, 2, 3 ] }` will be represented by the path `x.2`, ie the structure
  `x` with the 3rd (zero-based) element.  Paths are in some ways similar to
  strings, but are internally structured as arrays of elements.

## Data Point Timestamps

Every data point stored or manipulated by MLDB has an associated timestamp, even if the data point is itself of type timestamp. Data point timestamps are specified on row creation or as inputs to procedures which create datasets. Literals appearing in queries have a timestamp of `-inf` but any value's timestamp can be modified in a query with the special `@` operator (which takes the same right-hand value as the `TIMESTAMP` keyword). For example, `1 @ '2010-01-02T23:45:33Z'` will have a finite timestamp in 2010.

## Complex types

Within SQL expressions and functions, the type system is more sophisticated.  In
addition to the types mentioned above, the following are permitted:

- Rows, which model the row of a dataset.  They are a set of
(column, value, timestamp) tuples where each value is either an atomic type or a complex type.
    - Rows can appear as literals in queries as [Select Expressions](SelectExpression.md) surrounded by curly braces (i.e. `{ }`). For example `{1 as x, 'a' as y}` or the equivalent `{x: 1, y: 'a'}`.
- Embeddings, which model coordinates in a multi dimensional space.  Each
  is addressed by one or more coordinates, starting at zero and counting
  up by one.
  
  An embedding can be:
  - a scalar is convertible to a zero-dimensional embedding with length
    one;
  - a single dimensional array with an arbitrary length and all values of
    a common type (string, integer, floating point number, etc).
    A one-dimensional array is known as a vector.
  - a two-dimensional array, with dimensions $$(x,y)$$.  This is known as
    a matrix.
  - a three or more dimensional array.  This is known as a 3-tensor, 4-tensor,
    etc.
  
  Embeddings can only contain atomic types as elements, not complex types.

  Embeddings can appear as literals in queries as comma-delimited
  [Value Expressions](ValueExpression.md) surrounded by square brackets
  (i.e. `[ ]`). For example `[1, 2, 3]` for  a vector or
  `[ [x,0], [0,x] ]` for a diagonal matrix that depends upon the value
  of the variable `x` (which must be an atomic type).


When comparing rows, MLDB first sorts the columns by name and performs a lexicographical comparison of the column's names and values.
To illustrate this, consider these rows:

| id |  unflattened_row | 
| ----- | --- |
| id_1   | { python : 1, java : 1, c++ : 3 } | 
| id_2   | { scala : 4, java : 3, c++ : 1 } |
| id_3   | { python : 1, ada : 2 } |

The rows are ordered in this way by MLDB when doing comparison:

| id |  unflattened_row | 
| ----- | --- |
| id_3   | { ada : 2, python : 1 } |
| id_2   | { c++ : 1, java : 3, scala : 4 } |
| id_1   | { c++ : 3, java : 1, python : 1 } | 

Similarly, MLDB uses embedding's values to lexicographical order embeddings.

### Complex type flattening

When a complex type is returned as part of an SQL query result or stored in a dataset, it is flattened into a set of columns with atomic values.

**Rows** are flattened column by column into the parent row with their existing name, either unprefixed if using the query syntax `as *` or prefixed with `<prefix>.` if using the query syntax `as <prefix>`. 

For example, `select {x: 1, y: 2} as output, {x: 3, y: 4} as *` yields 


 output.x | output.y | x | y
:----------:|:----------:|:---:|:---:
     1    |     2    | 3 | 4


**Embeddings** are flattened by creating one column per value, with
the name being an incrementing integer from 0 upwards, prefixed with `<prefix>.` if using the query syntax `as <prefix>`, otherwise a prefix will be automatically generated. 

For example, `select [1,2] as x` yields


 x.0  | x.1 
:----:|:----:
  4   |  6


As a result, sorting by column names where there are more than 9 columns
may give strange results, with 10 sorting before 2.  This can be
addressed at the application level.

<!-- NTD: will be addressed at the MLDB level with structured column names -->

