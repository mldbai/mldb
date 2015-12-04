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
- `OFFSET <int>` specifies how many output rows to skip
- `LIMIT <int>` specifies the number of output rows

## General Syntax Rules

* Queries are not whitespace-sensitive, and may contain newline characters.
* SQL comments are supported: `--` denotes the start of a line comment and `/* ... */` denotes a block comment which can span multiple lines
* Keywords and operators are not case-sensitive.
* Dataset, column and function names are case sensitive and can appear in queries unquoted if and only if they start with a letter and contain only ASCII letters,
numbers and underscore characters, and don't clash with a
reserved word. In all other cases they must be surrounded by double-quote characters (i.e. `"`). Double-quote characters within quoted names must be doubled (i.e. `""`). 
    * For example, to
    refer to a dataset, column or function called `François says "hello", eh?` in a query, you would need to
    surround it in double-quotes (because of the spaces, punctuation and non-ASCII character) and double the inner double-quotes: `"François says ""hello"", eh?"`.


## See also

* [Query API](QueryAPI.md)
* [introduction to Datasets](../datasets/Datasets.md)
* [the MLDB Type System](TypeSystem.md)



