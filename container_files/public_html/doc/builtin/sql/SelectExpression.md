# SQL Select Expressions

An SQL Select expression is used to transform a set of input rows into a
set of output rows, by applying expressions across the set of columns in
the rows.  The input and output columns are both named and there are
separate parts of the expression to choose the column name and to choose
the value of the column.

Since MLDB is a sparse database designed for datasets with up to millions of
columns, there is extra syntax which helps you to manipulate groups of
columns together.

## Basic Syntax

The following basic syntax (as supported in standard SQL) can be used as
normal:

- A comma-delimited list of `<value-expr> AS <name>` clauses will select each expression, and create a matching column in the output with the corresponding name. 
    - `value-expr` is a [Value Expression](ValueExpression.md)
    - `name` must be a valid column name: it must be in double-quotes (`"`) if it clashes with a reserved word or doesn't start with a letter and contain only ASCII letters, numbers and underscore characters.
    - the `AS <name>` portion can be omitted, in which case a name will be automatically generated

In addition, non-SQL-standard `<name>: <value-expr>` are also accepted in Select Expressions.

There are also extensions that make it easier to work with millions of
columns. The following are accepted within the comma-delimited list of clauses in 
a select expression:

- `*` will select all columns and copy them into the output.
- `* EXCLUDING (col1)` will select and copy all columns except for the
  one called `col1` to the output.
- `* EXCLUDING (col1, col2)` will select and copy all but `col1` and
  `col2`.
- `y*` will select all columns starting with a `y`
- `* EXCLUDING (col1, x*)` will select and copy all, apart from those
  called `col1` or those whose name starts with an `x`
- `y* AS z*` will select all columns starting with a `y`, and change
  the `y` to a `z` in the output column name.
- `y* EXCLUDING (yellow) AS z*` will do the same thing, but will
  exclude the column `yellow` from the output.


## Selecting columns programatically via a column expression

MLDB is different from traditional SQL databases in that it supports millions
of columns, and columns may be very sparse as there is no enforced schema
around the row.

In some instances, it may be advantageous to select columns based upon an
SQL expression rather than based on pattern matching on the name.  This is
possible using a column expression, which effectively creates a table with
all columns in it and allows a `SELECT` to run over that table to choose the
ones required.

The syntax looks like this:

```
COLUMN EXPR ( AS <name> WHERE <value-expr> ORDER BY <order-by-expr> OFFSET <int> LIMIT <int>)
```

The components of the expression are as follows:

- the `AS` clause is a string-valued expression that gives the name of the resulting column.  If not specified, it defaults to columnName().
- the `WHERE` clause is a boolean-coerced [Value Expression](ValueExpression.md) that is used to decide if a column will be selected or not.  If not specified, it defaults to true.
- the `ORDER BY` clause is an [Order-By Expression](OrderByExpression.md) (which only makes sense if `OFFSET` or `LIMIT` are used) ranks the columns in the given order, presumably to only take a subset of them.
- the `OFFSET n` clause will skip the top `n` columns that came through the `ORDER BY` clause.  By default, `n` is zero: no rows are skipped.
- the `LIMIT m` clause will select only the top `m` columns.  By default `m` is infinity: there is no limit.

As an example, to select up to 1,000 columns with the most rows in them,
but none that has less than 100, you would use

```
COLUMN EXPR (WHERE rowCount() > 100 ORDER BY rowCount() DESC, columnName() LIMIT 1000)
```
Note that this syntax is not part of SQL, it is an MLDB extension.

### Built-in functions available in column expressions

The following functions are available in the context of a column expression:

- `columnName()` is the name of the column under consideration.  It is the same
  as the `columnPath()` elements concatenated with a `.` character.
- `columnPath()` is the structured path to the column under consideration.
- `columnPathElement(n)` is the nth element of the column path of the column
  under consideration.  If n is less than zero, it will be a distance from the
  end (for example, -1 is the last element).  For a columnName of `x.y.2`, then
  `columnPathElement(0)` will be `x`, `columnPathElement(1)` will be `y` and
  `columnPathElement(2)` is equivalent to `columnPathElement(-1)` which will
  be `2`. 
- `rowCount()` is the number of rows that have a value for this column, including explicit NULLs.
