# SQL Where Expressions

An SQL Where
expression is a standard SQL [Value Expression](ValueExpression.md), and is
coerced to a boolean type after evaluation. The SQL `WHERE` clause is used to select 
a subset of rows to process. Only rows which are `true` are kept;
rows that evaluate to `null` or `false` are discarded.

## Optimization of Where Expressions over datasets using indexes

The following patterns in a Where expression will be optimized against a dataset by
using the column index for the given column:

- `WHERE rowName() = constant`
- `WHERE constant = rowName()`
- `WHERE rowName() IN (constant, constant, ...)`
- `WHERE rowName() % constant op constant` (op is =,!=<,>,<=,>=)
- `WHERE column = constant`
- `WHERE column`
- `WHERE column IS TRUE`
- `WHERE column IS NOT NULL`
- `WHERE x AND y` (where both x and y are in this list)
- `WHERE x OR y` (where both x and y are in this list)

This can considerably speed up a lot of MLDB queries.

