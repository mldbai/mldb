# MLDB Type System

This page describes the type system MLDB uses to store and process data.

## Atomic types

MLDB's atomic types are the following:

- A null value
    - Null values can appear in queries as `null` (case-insensitive)
- An integer, which can take any value of a signed or unsigned 64 bit integer
    - Integers can appear in queries directly i.e. `1` or `-3`
- A double precision floating point value, including infinity, negative infinity
and NaN
    - Doubles can appear in queries directly, optionally in exponential notation: `12.2`, `1.22e1`, `inf`, `nan` (case-insensitive)
- A string, which is any UTF8 encoded sequence of characters.
    - String literals can appear in queries surrounded in single-quote characters (i.e. `'`). Single-quote characters within string literals must be doubled (i.e. `''`). Strings are *always* encoded as UTF-8 Unicode characters.
- A timestamp, which is a point in time that is independent of any timezone.  It is
internally represented as the number of seconds since the 1st of January, 1970,
at GMT.
    - Timestamps can appear queries with the `to_timestamp()` function, for example `to_timestamp('2010-01-02T23:45:33Z')`
    - See [Builtin Functions](ValueExpression.md) for full details on input format.
- A time interval, which is a difference between two timestamps.  It is
internally represented as three fields expressing months, days, and seconds.
    - Time intervals can appear in queries using the `INTERVAL` keyword, for example `INTERVAL '1 month 2 days 5 minutes'`
    - See [Expressing Time Intervals](ValueExpression.md) for full details on input format.
- A binary blob.  This
  can be used to store or process arbitrary binary data, including that which
  contains characters that can't be represented as a string.
  - Binary blobs can appear in queries using the `base64_decode` function
    with a string as its argument.

These types are used in different ways, depending upon whether they are stored
in a dataset or processed as part of an expression.

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

| id |  row | 
| ----- | --- |
| id_1   | { python : 1, java : 1, c++ : 3 } | 
| id_2   | { scala : 4, java : 3, c++ : 1 } |
| id_3   | { python : 1, ada : 2 } |

The rows are ordered in this way by MLDB when doing comparison:

| id |  row | 
| ----- | --- |
| id_3   | { ada : 2, python : 1 } |
| id_2   | { c++ : 1, java : 3, scala : 4 } |
| id_1   | { c++ : 3, java : 1, python : 1 } | 

Similarly, MLDB uses embedding's values to lexicographical order embeddings.

### Complex type flattening

When a complex type is returned as part of an SQL query result or stored in a dataset, it is flattened into a set of columns with atomic values.

**Rows** are flattened column by column into the parent row with their existing name, either unprefixed if using the query syntax `as *` or prefixed with `<prefix>.` if using the query syntax `as <prefix>`. 

For example, `select {x: 1, y: 2} as output, {x: 3, y: 4} as *` yields 

```
 output.x    output.y    x   y
----------  ----------  --- ---
     1           2       3   4
```

**Embeddings** are flattened by creating one column per value, with
the name being an incrementing integer from 0 upwards, prefixed with `<prefix>.` if using the query syntax `as <prefix>`, otherwise a prefix will be automatically generated. 

For example, `select [1,2] as x` yields

```
 x.0    x.1 
-----  -----
  4      6
```

As a result, sorting by column names where there are more than 9 columns
may give strange results, with 10 sorting before 2.  This can be
addressed at the application level.
<!-- NTD: will be addressed at the MLDB level with structured column names -->

