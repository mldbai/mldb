# SQL Value Expressions

An SQL Value Expression is the basic building block of an SQL query: it is an expression that evaluates to a single value. A value expression is either a constant, a column reference, or the result of applying operators and/or functions to constants and/or column references.

## Table of Contents

* [Constants](#constants)
* [Column references](#Columnreferences)
* [Operators](#constants)
* [Calling Functions](#CallingFunctions)
* [List of Built-in Functions](#builtinfunctions)
* [List of Built-in Aggregate Functions](#aggregatefunctions)


## <a name="constants"></a>Constants

A Value expression can include [literal representations of constants](TypeSystem.md) such as numbers and strings etc.

## <a name="Columnreferences"></a>Column references

To refer to a column, you use its name, in accordance with the [quoting rules](Sql.md).  So to read the value of the column `x` and add one, use `x + 1`.

*N.B. References to non-existent columns are always evaluated as `NULL`.*

### Referring to columns by dataset

When querying a single dataset, it is allowed but unnecessary to specify the name of that dataset when referring a column. However, in some cases, 
the query will run on several datasets (See [From Expression](FromExpression.md)). In those cases, you need to specify the name or alias of the dataset the desired column is from using a 
`.` operator.
For example, to refer the column `y` from a dataset whose name or alias is `x`, you must use `x.y` in the value expression. 

If the dataset has been aliased (e.g. `FROM dataset AS x`), you **must** use the alias `x` instead of the original name `dataset`.

In cases where the name or alias of the column or the name or alias of the dataset contains a `.`, you can use double-quotes to resolve any ambiguity.
For example, if you have a join between a dataset named `x` with a column `y.z` and a dataset named `x.y` with column `z` :


* `x.y.z`       will refer to dataset x column y.z
* `"x.y".z`     will refer to dataset x.y column z
* `"z".x.y`     will return an error because there is no dataset named z


Alternatively, you can alias the conflicting dataset's name to something unambiguous. In the previous example, if we alias the datasets with
`FROM x AS blue JOIN y as red` then:

* `x.y.z`       will return an error because neither x, x.y nor x.y.z refer to a dataset's alias
* `"x.y".z`     will return an error because the dataset x.y as been aliased to 'red'
* `blue.y.z`    will refer to dataset blue (x) column y.z
* `red.z`       will refer to dataset red (y.z) column z


## <a name="operators"></a>Operators

The following standard SQL operators are supported by MLDB.  An
operator with lower precedence binds tighter than one with a
higher predecence, so for example `x + y * z` is the same as
`x + (y * z)`.  Expressions at the same precendence level are 
always are left associative, that is the expression
`x / y % z` is evaluated as `(x / y) % z`.

  Operator  |  Type              | Precedence 
:----------:|--------------------|:------------
     `~`      |  unary arithmetic  |          1 
     `*` , `/` , `%`      |  binary arithmetic |          2 
     `+` , `-`      |  unary arithmetic  |          3 
     `+` , `-`      |  binary arithmetic |          3 
     `&` , <code>&#124;</code> , `^`      |  binary bitwise    |          3 
     `=` , `!=`, `>` , `<` , `>=` , `<=` , `<>` , `!>` , `!<`       |  binary comparison |          4 
     `NOT`    |  unary boolean     |          5 
     `AND` , `OR`     |  binary boolean    |          7 

<!--
     ALL      unary unimp                 7  All true 
     ANY      unary unimp                 7  Any true 
     SOME     unary unimp                 7  Some true

     BETWEEN  unary unimp                 7  Between operator 
     IN       unary unimp                 7  In operator 
     LIKE     unary unimp                 7  Like operator 

-->

### Operators on time values

Timestamps and time intervals have specific rules when using binary operators. Here are the supported operators and 
the types that will result from each operation:


  Operator  |  Left hand Value    |    Right Hand Value  | Resulting type   
  :--------:|---------------------|----------------------|-----------------
     `+` , `-`      |  Timestamp          |    Number*           | Timestamp       
     `+` , `-`      |  Timestamp          |    Time Interval     | Timestamp
     `+` , `-`      |  Time Interval      |    Number*           | Time Interval 
     `+` , `-`      |  Time Interval      |    Time Interval     | Time Interval 
     `*` , `/`      |  Time Interval      |    Number           | Time Interval    


*When used in conjunction with Timestamps or Time Intervals, Numbers implicitly represent days.


Note that the operators `+` and `*` are commutative in all cases.


### `BETWEEN` expressions

SQL `BETWEEN` expressions are a shorthand way of testing for an
open interval.  The expression `x BETWEEN y AND z` is the same as `x >= y AND x <= z` except that the `x` expression will only be evaluated once.


### `CASE` expressions

SQL `CASE` expressions are used to return different expressions
depending upon the value or truth of an expression.  There are two flavors:

Simple case statements, which look like

```sql
CASE expr
  WHEN val1 THEN result1
  WHEN val2 THEN result2
  ELSE result3
END
```

for example,

```sql
CASE x % 2
  WHEN 0 THEN 'even'
  ELSE 'odd'
END
```

Matched case statements, which look like

```sql
CASE
  WHEN boolean1 THEN result1
  WHEN boolean2 THEN result2
  ELSE result3
END
```

for example,

```sql
CASE
  WHEN x % 15 = 0 THEN 'multiple of 5 and 3'
  WHEN x % 5 = 0 THEN 'multiple of 5'
  WHEN x % 3 = 0 THEN 'multiple of 3'
  ELSE 'very approximately prime'
END
```

In both cases, there are an arbitrary number of `WHEN` clauses and the `ELSE` clauses are
optional. If no `ELSE` clause is present and no `WHEN` clause matches, the result is `null`.

### `CAST` expressions

SQL `CAST` expressions allow the type of an expression to be coerced
into another type.  The main use is to convert between strings and
numbers. See also [the MLDB Type System](TypeSystem.md).

The syntax is

```sql
CAST (expression AS type)
```

where `expression` is any SQL value expression, and `type` is one of the
following:

  - string
  - integer
  - number
  - boolean
  - timestamp

The integer, number and boolean conversions will work with strings
and other numbers.

The timestamp conversions will work with strings, which MUST be
[ISO 8601] (http://en.wikipedia.org/wiki/ISO_8601) strings,
and numbers, which are assumed to represent seconds since the 1st of
January, 1970, GMT.

A `NULL` value will always cast to a `NULL` value.  In addition, if it
is not possible to convert a value, then a `NULL` will be returned.

### IS [NOT] expressions

These expressions are used to test the type or value of an expression.
They bind tightly, that is to say that `x + 1 IS NOT NULL` would be
interpreted as `x + (1 IS NOT NULL)`, which is probably not what was
intended. See also [the MLDB Type System](TypeSystem.md).

- `expr IS [NOT] NULL` tests if the given expression is of null type
- `expr IS [NOT] TRUE` tests if the given expression evaluates to true
- `expr IS [NOT] FALSE` tests if the given expression evaluates to false
- `expr IS [NOT] STRING` tests if the given expression is a string
- `expr IS [NOT] NUMBER` tests if the given expression is a number
- `expr IS [NOT] INTEGER` tests if the given expression is an integer
- `expr IS [NOT] TIMESTAMP` tests if the given expression is a timestamp

### [NOT] IN expression

This expression tests if the value in the left hand side is (or is not) included
in a set of values on the right hand side.  There are four ways to specify the set on the right hand side:

1.  As a sub-select (`x IN (SELECT ...)`)
2.  As an explicit tuple (`x IN (val1, val2, ...)`)
3.  As the keys of a row expression (`x IN (KEYS OF expr)`)
4.  As the values of a row expression (`x IN (VALUES OF expr)`)

The first two are standard SQL; the second two are MLDB extensions and are
made possible by MLDB's sparse data model.

#### IN expression with sub-select

The right hand side can be the result of a sub `SELECT` statement.
For example `expr IN (SELECT x FROM dataset)` will test if the value
expressed by `expr` is equal to any of the values in the x column of
the dataset. If the `SELECT` statement returns more than a single column,
they will all be tested (this is different from standard SQL, which will
ignore all but the first column, and due to MLDB's sparse column model).

#### IN expression with explicit tuple expression

For example: `expr IN (3,5,7,11)`

#### IN (KEYS OF ...) expression

For example: `expr IN (KEYS OF tokenize({text: sentence}))`

That will evaluate to true if expr is a word within the given sentence.

#### IN (VALUES OF ...) expression

For example: `expr IN (VALUES OF [3, 5, 7, 11])`

is equivalent to expr IN (3, 5, 7, 11), but allows a full row expression
to be used to construct the set, rather than enumerating tuple elements.


## <a name="CallingFunctions"></a>Calling Functions</h2>

Built-in functions (see below for a list) can accept multiple arguments of any type and return a single value of any type and can be applied by name with parameters in parentheses, for example:

```sql
built_in_function(1, 'a')
```

[User-defined functions](../functions/Functions.md) are applied in the same way except that they always accept a single row-valued input value as an argument and return a single row-valued output, for example:

```sql
user_defined_function( {some_number: 1, some_string: 'a'} )
```

It can also accept the row returned from another user-defined function, for example:

```sql
user_defined_function_a(user_defined_function_b( {some_number: 1, some_string: 'a'} ))
```

Furthermore, since it is frequently necessary to access a subset of the columns from the output of a user-defined function, their application can be followed by an accessor in square brackets, for example:

```sql
user_defined_function( {some_number: 1, some_string: 'a'} )[ <accessor> ]
```

* If `accessor` is a column name, then the value of that column will be returned
* If `accessor` is a row, then a row will be returned.

Let's look at a hypothetical user-defined function with name `example` whose type defined the following input and output values:

* Input
  * `x`: integer
  * `y`: row of integers
* Output:
  * `scaled_y`: embedding of integers, each of which is the corresponding value of `y` times `x`
  * `sum_scaled_y`: integer, the sum of the values of `scaled_y` 
  * `input_length`: number of columns in `y`

Accessing the `sum_scaled_y` output value would look like: 

```sql
example( {x: 10, y: [1, 2, 3]} )[sum_scaled_y]
```

Accessing a row containing only the `sum_scaled_y` and `input_length` output values would look like: 

```sql
example( {x: 10, y: [1, 2, 3]} )[ {sum_scaled_y, input_length} ]
```

Note that this syntax is not part of SQL, it is an MLDB extension.


## <a name="builtinfunctions"></a>List of Built-in Functions

### Dataset-provided functions

- `rowHash()`: returns the internal hash value of the current row, useful for random sampling and providing a stable [order](OrderByExpression.md) in query results
- `rowName()`: returns the name the current row 
- `columnCount()`: returns the number of columns with explicit values set in the current row 

### Encoding and decoding functions

- `implicit_cast(x)`: attempts to convert `x` to a
  number according to the following recipe:
  - if `x` is the empty string, return `null`
  - if `x` is a string that can be converted to a number, return the number
  - otherwise, return `x` unchanged
- `base64_encode(blob)` returns the base-64 encoded version of the blob
  (or string) argument as a string.
- `base64_decode(string)` returns a blob containing the decoding of the
  base-64 data provided in its argument.
- `parse_json(string)` returns a row with the JSON decoding of the
  string in the argument.  Nested arrays and objects will be parsed
  recursively; no flattening is performed.
- `extract_column(row)` extracts the given column from the row, keeping
  only its latest value by timestamp.

### Numeric functions

- `pow(x, y)`: returns x to the power of y.
- `exp(x)`: returns **e** (the Euler number) raised to the power x.
- `ln(x)`: returns the natural logarithm of x.
- `ceil(x)`: returns the smaller integer not less than x.
- `floor(x)`: returns the largest integer not greater than x.
- `mod(x, y)`: returns x modulo y.  The value of x and y must be an integer.
- `abs(x)`: returns the absolute value of x.
- `sqrt(x)`: returns the square root of x.  The value of x must be greater or equal to 0.
- `quantize(x, y)`: returns x rounded to the precision of y.  Here are some examples:

expression|result
----------------------|-----
`quantize(2.17, 0.001)` | 2.17
`quantize(2.17, 0.01)`  | 2.17
`quantize(2.17, 0.1)`   | 2.2
`quantize(2.17, 1)`     | 2
`quantize(2.17, 10)`    | 0
`quantize(-0.1, 1)`     | 0
`quantize(0, 10000)`    | 0
`quantize(217, 0.1)`    | 217
`quantize(217, 1)`      | 217
`quantize(217, 10)`     | 220
`quantize(217, 100)`    | 200
`quantize(-217, 100)`   | -200


- `replace_nan(x, y)`: replace all NaNs in x by y.
- `replace_inf(x, y)`: replace all Inf in x by y.
- `binomial_lb_80(trials, successes)` returns the 80% lower bound using the Wilson score.
- `binomial_ub_80(trials, successes)` returns the 80% upper bound using the Wilson score.

More details on the [Binomial proportion confidence interval Wikipedia page](https://en.wikipedia.org/wiki/Binomial_proportion_confidence_interval).

### String functions

- `lower(string)` returns the lowercase version of the string, according to the
  system locale.
- `upper(string)` returns the uppercase version of the string, according to the
  system locale.
- `regex_replace(string, regex, replacement)` will return the given string with
  matches of the `regex` replaced by the `replacement`.  Perl-style regular
  expressions are supported.
- `regex_match(string, regex)` will return true if the *entire* string matches
  the regex, and false otherwise.  If `string` is null, then null will be returned.
- `regex_search(string, regex)` will return true if *any portion of * `string` matches
  the regex, and false otherwise.  If `string` is null, then null will be returned.

### Timestamp functions

- `when(x)` returns the timestamp at which the expression `x` was known to be
  true.  Each expression in MLDB has an associated timestamp attached to it,
  which is used for unbiasing, and this returns that timestamp.
    - For example, if `x` had a timestamp of last Monday, and `y` had a
      timestamp of last Tuesday, then `when(x + y)` would have a timestamp of
      last Tuesday, since on Monday the value of `y` wasn't known.
- `to_timestamp(x)` coerces the value of x to a timestamp:
  - if `x` is a string, it creates the timestamp by parsing the ISO8601
    string passed in.
  - if `x` is a number, it is interpreted as the number of seconds since
    the UNIX epoch (1 January, 1970 at 00:00:00).
  - otherwise, this will return null.
- `at(x, d)` returns the value of the expression `x`, but with the timestamp
  modified to be at timestamp `d`.
- `now()` returns the timestamp at the current moment, according to system
  time.
- `min_timestamp(x)` returns the minimum timestamp represented in the scalar
  or object `x`.
- `max_timestamp(x)` returns the maximum timestamp represented in the scalar
  or object `x`.
- `date_part(unit, x)` returns the subfield `unit` of timestamp `x`. The following are the supported units:
  
  - `microsecond` as the total number of microseconds after the rounded down second.
  - `millisecond` as the total number of millisecond after the rounded down second.
  - `second` as the number of seconds after the minute (0-59)
  - `minute` as the number of minutes after the hour (0-59)
  - `hour` as the hour of the day (0-23)
  - `day` as the day of the month (1-31)
  - `dow` as the day of the week, starting on sunday (0-6)
  - `doy` as the number of days elapsed since january 1st of the same year (0-364/365)
  - `isodow` as the ISO-8601 day of the week, starting on monday (1-7)
  - `isodoy` as the number of the day starting from monday on the 1st ISO-8601 week of the year (1-371)
  - `week` as the number of full weeks elapsed since January 1st of the year (0-51)
  - `isoweek` as the ISO-8601 week number (1-53)
  - `month` as the number of the date's month (1-12)
  - `quarter` as the number of the date's quarter (1-4)
  - `year` as the gregorian calendar year of the date
  - `isoyear` as the ISO-8601 calendar year of the date

- `date_trunc(unit, x)` will truncate the timestamp `x` to the specified `unit`.
  - For example, `date_trunc('month', '1969-07-24')` will return `'1969-07-01'`
  - `day`, `dow`, `doy`, `isodow`, `isodoy` will all truncate to the day

### Vector space functions

- `norm(vec, p)` will return the L-`p` norm of `vec`.
- `normalize(vec, p)` will return a version of the `vec` normalized in the L-`p` norm.  This means that `norm(normalize(vec, p), p) = 1` for any non-zero `vec`.
- `vector_diff(vec1, vec2)` will return an elementwise difference `vec1 - vec2`,
   where both are assumed to be embeddings.  The lengths of the two must be the same.
- `vector_sum(vec1, vec2)` will return an elementwise sum `vec1 + vec2`, where
  both are assumed to be embeddings.  The lengths of the two must be the same.
- `vector_product(vec1, vec2)` will return an elementwise product `vec1 * vec2`, where
  both are assumed to be embeddings.  The lengths of the two must be the same.
- `vector_quotient(vec1, vec2)` will return an elementwise quotient `vec1 / vec2`, where
  both are assumed to be embeddings.  The lengths of the two must be the same.
  Divisions by zero will result in NaN values.

### <a name="importfunctions"></a>Data import functions

- `tokenize(str, {splitchars: ',', quotechar: '"', offset: 0, limit: null, value: null, min_token_length: 1, ngram_range:[1, 1]})`
can be used to create bag-of-tokens representations of strings, by returning a row whose
columns are formed by tokenizing `str` by splitting along `splitchars` and whose values by default are the
number of occurrences of those tokens within `str`. For example `tokenize('a b b c c c', {splitchars:' '})` will return the row `{'a': 1, 'b': 2, 'c': 3}`
  - `offset` and `limit` are used to skip the first `offset` tokens and only generate `limit` tokens
  - `value` (if not set to `null`) will be used instead of token-counts for the values of the columns in the output row
  - `quotechar` is interpreted as a single character to delimit tokens which may contain the `splitchars`, so by default `tokenize('a,"b,c"')` will return the row `{'a':1,'b,c':1}`
  - `min_token_length` is used to specify the minimum length of tokens that are returned
  - `ngram_range` is used to specify the n-grams to return. `[1, 1]` will return only unigrams, while `[2, 3]` will return bigrams and trigrams, where tokens are joined by underscores. For example, `tokenize('Good day world', {splitchars:' ', ngram_range:[2,3]})` will return the row `{'Good_day': 1, 'Good_day_world': 1, 'day_world': 1}`
- `token_extract(str, n, {splitchars: ',', quotechar: '"', offset: 0, limit: null, min_token_length: 1})` will return the `n`th token from `str` using the same tokenizing rules as `tokenize()` above. Only the tokens respecting the `min_token_length` will be considered

### JSON unpacking <a name="unpack_json"></a>

The `unpack_json(str)` function will parse the string `str` as a JSON
object and unpack it into multiple columns following the  algorithm
outlined below. Note that JSON objects shown in the tables below are
string representations of the JSON.

Each `(key, value)` pair will be recorded as the column name and cell value respectively. The line `{"a": 5, "b": true}` is recorded as:

| *rowName* | *a* | *b* |
|-----------|-----|-----|
| row1 | 5 | 1 |

(note that `true` and `false` in JSON come out as `1` and `0`, since
SQL doesn't have a boolean type).

If the value is an object, we apply the same logic recursively, adding a
period (`.`) between the keys at each level. The line
 `{"a": 5, "c": {"x": "hola"}, "d": {"e": {"f": "amigo"}}}`
is recorded as:

| *rowName* | *a* | *c.x* | *d.e.f* |
|-----------|-----|-------|---------|
| row1 | 5 | hola | amigo |


If the value is an array that contains only atomic types (strings, bool or
numeric), we encode them as a one-hot vector. As shown in the example below,
the `value` in the JSON will be appended to the column name and the cell
value will be set to `true`. The line `{"a": 5, "b": [1, 2, "abc"]}`
is recorded as:

| *rowName* | *a* | *b.1* | *b.2* | *b.abc* |
|-----------|-----|-----|-------|-----------|
| row1 | 5 | 1 | 1 | 1 |

If the value is an array that contains only objects, we unpack the array
putting one JSON object per column encoded as a string. The line
`{"a": 5, "b": [{"z": 1}, {"y": 2}]}` is recorded as:

| *rowName* | *a* | *b.0* | *b.1* |
|-----------|-----|-----|-------|
| row1 | 5 | {"z": 1} | {"y": 2} |

If the value is an array that contains at least one non-atomic type (array,
object), we encode them as the string representation of the JSON. The line
`{"a": 5, "b": [1, 2, {"xyz":"abc"}]}` is recorded as:

| *rowName* | *a* | *b* |
|-----------|-----|-----|
| row1 | 5 | [1, 2, {"xyz":"abc"}] |


The ![](%%doclink import.json procedure) can be used to import a text file
where each line is a JSON object. The ![](%%doclink melt procedure) can be
used on columns representing arrays of objects to create a row per array
element.


## <a name="aggregatefunctions"></a>Aggregate Functions

- `avg` calculates the average of all values in the group.  It works in
  double precision floating point only.
- `sum` calculates the sum of all values in the group.  It works in
  double precision floating point only.
- `min` calculates the minimum of all values in the group.
- `max` calculates the maximum of all values in the group.
- `count` calculates the number of non-null values in the group.
    - `count(*)` is a special function which will count the number of rows in the group with non-null values in any column
- `pivot(columnName, value)` will accumulate a single row per group, with the
  column name and value given.  This can be used with a group by clause to
  transform a dense dataset of (actor,action,value) records into a sparse
  dataset with one sparse row per actor, for example to create one-hot feature vectors or term-document or cooccurrence matrices.

### Aggregates of rows

Each of the standard aggregate functions may also be applied to row values.  This
has the effect of calculating a separate aggregate for each column in the row, and
returns a row-valued result.  For example, to calculate the total count of each
sparse column in a dataset, the following would suffice:

```sql
SELECT count({*})
```

which would return a row with one count for each column in the dataset.  This
functionality is useful to write generic queries that operate without prior
knowledge of the column names, and to make queries on datasets with thousands
or millions of column feasible.

## Horizontal Operations

It is possible to perform operations across a subset of values in a row
using these functions. All the following functions operate on the non-null 
values in a row or a subset of the row. Note that the row is flattened to calculate
the values.

- `horizontal_count(<row>)` returns the number of non-null values in the row.
- `horizontal_sum(<row>)` returns the sum of the non-null values in the row.
- `horizontal_avg(<row>)` returns the average of the non-null values in the row.
- `horizontal_min(<row>)` returns the minimum of the non-null values in the row.
- `horizontal_max(<row>)` returns the maximum of the non-null value in the row.


## Evaluating a JS function from SQL (Experimental)

The SQL function `jseval` allows for the inline definition of functions using Javascript. This function takes the following arguments:

1.  A text string containing the text of the function to be evaluated.  This
    must be a valid Javascript function, which will return with the `return`
    function.  For example, `return x + y`.  This must be a constant string,
    it cannot be an expression that is evaluated at run time.
2.  A text string containing the names of all of the parameters that will be
    passed to the function, as they are referred to within the function.  For
    example, `x,y`.  This must be a constant string, it cannot be an expression
    that is evaluated at run time.
3.  As many argument as are listed in part 2, in the same order.  These can be
    any SQL expressions and will be bound to the parameters passed in to the
    function.

The result of the function will be the result of calling the function on the
supplied arguments.  This will be converted into a result as follows:

- A `null` will remain a `null`
- A Javascript number, string or `Date` will be converted to the equivalent
  MLDB number, string or timestamp;
- An object (dictionary) will be converted to a row

In all cases, the timestamp on the output will be equal to the latest of the
timestamps on the arguments passed in to the function.

As an example, to calculate the Fibonnaci numbers from SQL (somewhat
inefficiently), one could write

```sql
SELECT jseval('
function fib(x) {
    if (x == 1) return 1;
    if (x == 2) return 1;
    return fib(x - 1) + fib(x - 2);
}
return fib(i);
', 'i', i
```

or to parse a comma separated list of 'key=value' attributes into a row, one could write

```sql
SELECT jseval('
var fields = csv.split(",");
var result = {};
for (var i = 0;  i < fields.length;  ++i) {
    var field = fields[i];
    var kv = field.split("=");
    result[kv[0]] = kv[1];
}
return result;
', 'csv', expression_to_generate_csv)
```

The `mldb` Javascript object is available from the function; this can notably used to
log to the console to aid debugging. Documentation for this object can be found with the
![](%%doclink javascript plugin) documentation.

