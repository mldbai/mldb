# SQL implementation in MLDB

MLDB implements a query language based upon SQL's `select` syntax.
This is used both for efficient evaluation of expressions, and to 
specify queries. 

## Query Clauses

- `SELECT <`[`Select Expression`](SelectExpression.md)`>` specifies the columns to put in the output dataset
- `NAMED <`[`Value Expression`](ValueExpression.md)`>` specifies the name of the rows in the output dataset
- `FROM <`[`From Expression`](FromExpression.md)`>` specifies the dataset(s) to select from
- `WHEN <`[`When Expression`](WhenExpression.md)`>` specifies which values to put in the output dataset based on their timestamp
- `WHERE <`[`Where Expression`](WhereExpression.md)`>` specifies which rows in the input contribute to the output dataset
- `GROUP BY <`[`Group-By Expression`](GroupByExpression.md)`>` specifies how to group the output for aggregate functions
- `HAVING <`[`Where Expression`](WhereExpression.md)`>` specifies the groups to select
- `ORDER BY <`[`Order-By Expression`](OrderByExpression.md)`>` specifies the order to output the results
- `LIMIT <int>` specifies the number of output rows
- `OFFSET <int>` specifies how many output rows to skip

## General Syntax Rules

* Queries are not whitespace-sensitive, and may contain newline characters.
* SQL comments are supported: `--` denotes the start of a line comment and `/* ... */` denotes a block comment which can span multiple lines
* Keywords and operators are not case-sensitive.
* Double-quote (i.e. `"`) denote identifiers and single-quote characters (i.e. `'`) denote strings
* Identifiers such as the names of dataset, column and function are case sensitive and may contain any Unicode character.
* Identifiers may appear unquoted in queries if and only if they start with a letter and contain only ASCII letters, numbers and underscore characters (i.e. `_`), and don't clash with a reserved word such as `from` or `timestamp`. In all other cases they must be surrounded by double-quote characters (i.e. `"`). Double-quote characters within quotes must be doubled (i.e. `""`). 
    * For example, to
    refer to a dataset, column or function called `François says "hello", eh?` in a query, you would need to
    surround it in double-quotes (because of the spaces, punctuation and non-ASCII character) and double the inner double-quotes: `"François says ""hello"", eh?"`.
* Column and row names are special identifiers: they are the string representation of column and row paths, as detailed in the [Intro to Datasets](../datasets/Datasets.md)
* The dot, or period, character (i.e. `.`) is an indirection operator, much like the minus, or dash, character (i.e. `-`) is a mathematical operator, so while either may appear in an quoted identifier, confusion and excess quoting can be avoided by avoiding this practice. The only punctuation mark which is not and will never be an operator is the underscore character (i.e. `_`).


## See also

* [Query API](QueryAPI.md)
* [introduction to Datasets](../datasets/Datasets.md)
* [the MLDB Type System](TypeSystem.md)



