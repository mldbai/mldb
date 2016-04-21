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
:----------:|--------------------|:------------:
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
- `expr IS [NOT] INTERVAL` tests if the given expression is a time interval

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

### [NOT] LIKE expression

This expression tests if a string on the left-hand side matches an SQL wildcard pattern on the right hand side.

The `%` character will substitute for 0 or more characters. For example: `x LIKE 'abc%'` will test if x is a string that starts with `abc`.

The `_` character will substitute for a single character. For example: `x LIKE 'a_a'` will test if x is a string that has 3 characters that starts and ends with `a`.

For more intricate patterns, you can use the `regex_match` function.

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
- `extract_column(row)` extracts the given column from the row, keeping
  only its latest value by timestamp.
- <a name="parse_json"></a>`parse_json(string, {arrays: string})` returns a row with the JSON decoding of the
  string in the argument. If the `arrays` option is set to `'parse'` (this is the default) then nested arrays and objects will be parsed recursively; no flattening is performed. If the `arrays` option is set to `'encode'`, then arrays containing only scalar values will be one-hot encoded and arrays containing only objects will contain the string representation of the objects. 

  Here are examples with the following JSON string:

```javascript
{
  "a": "b", 
  "c": {"d": "e"}, 
  "f": ["g","h"], 
  "i": [ {"j":"k"}, {"l":"m"} ] 
}
```

With `{arrays: 'parse'}` the output will be:

| a | c.d |f.0 |f.1 | i.0.j | i.0.j |
|:---:|:----:|:----:|:----:|:------:|:------:|
| 'b' | 'e'   | 'g'    | 'h'  |  'k'   |  'm'   |


With `{arrays: 'encode'}` the output will be:

| a | c.d | f.g | f.h | i.0 | i.1
|:---:|:---:|:---:|:-----:|:------:|:------:
| 'b' | 'e' | 1 | 1   | '{"j":"k"}' | '{"l":"m"}'


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

- `earliest_timestamp(x)` returns the earliest timestamp associated with the scalar
  or object `x`.
- `latest_timestamp(x)` returns the maximum timestamp associated with the scalar
  or object `x`.
- `distinct_timestamps(x)` returns an embedding of the distinct timestamps of a scalar
  or object `x`.
- `x @ d` or `at(x, d)` returns the value of the expression `x`, but with the timestamp
  modified to be at timestamp `d`.
- `now()` returns the timestamp at the current moment, according to system
  time.
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

- `norm(vec, p)` will return the L-`p` norm of `vec`. The L-0 norm is the count of non-zero
   elements.
- `normalize(vec, p)` will return a version of the `vec` normalized in the
   L-`p` norm such that `normalize(vec, p) = vec / norm(vec, p)`.
- `vector_diff(vec1, vec2)` will efficiently return an elementwise difference `vec1 - vec2`,
   where both are assumed to be embeddings.  The lengths of the two must be the same.
- `vector_sum(vec1, vec2)` will efficiently return an elementwise sum `vec1 + vec2`, where
  both are assumed to be embeddings.  The lengths of the two must be the same.
- `vector_product(vec1, vec2)` will efficiently return an elementwise product `vec1 * vec2`, where
  both are assumed to be embeddings.  The lengths of the two must be the same.
- `vector_quotient(vec1, vec2)` will efficiently return an elementwise quotient `vec1 / vec2`, where
  both are assumed to be embeddings.  The lengths of the two must be the same.
  Divisions by zero will result in NaN values.
- `flatten(val)` will take a n-dimensional embedding and flatten it down
  into a one-dimensional embedding containing all of the elements.  The
  elements will be taken from end end dimensions first, ie
  `flatten([ [ 1, 2], [3, 4] ])` will be `[1, 2, 3, 4]`.

### <a name="importfunctions"></a>Data import functions

- `tokenize(str, {splitchars: ',', quotechar: '', offset: 0, limit: null, value: null, min_token_length: 1, ngram_range:[1, 1]})`
can be used to create bag-of-tokens representations of strings, by returning a row whose
columns are formed by tokenizing `str` by splitting along `splitchars` and whose values by default are the
number of occurrences of those tokens within `str`. For example `tokenize('a b b c c c', {splitchars:' '})` will return the row `{'a': 1, 'b': 2, 'c': 3}`
  - `offset` and `limit` are used to skip the first `offset` tokens and only generate `limit` tokens
  - `value` (if not set to `null`) will be used instead of token-counts for the values of the columns in the output row
  - `quotechar` is interpreted as a single character to delimit tokens which may contain the `splitchars`, so by default `tokenize('a,"b,c"', {quotechar:'"'})` will return the row `{'a':1,'b,c':1}`
  - `min_token_length` is used to specify the minimum length of tokens that are returned
  - `ngram_range` is used to specify the n-grams to return. `[1, 1]` will return only unigrams, while `[2, 3]` will return bigrams and trigrams, where tokens are joined by underscores. For example, `tokenize('Good day world', {splitchars:' ', ngram_range:[2,3]})` will return the row `{'Good_day': 1, 'Good_day_world': 1, 'day_world': 1}`
- `token_extract(str, n, {splitchars: ',', quotechar: '', offset: 0, limit: null, min_token_length: 1})` will return the `n`th token from `str` using the same tokenizing rules as `tokenize()` above. Only the tokens respecting the `min_token_length` will be considered



## <a name="aggregatefunctions"></a>Aggregate Functions

The following standard SQL aggregation functions are supported. They may only be used in SELECT and HAVING clauses. If an aggregation function appears in the SELECT clause and no GROUP BY clause is used, an empty GROUP BY clause will be inferred.

- `avg` calculates the average of all values in the group.  It works in
  double precision floating point only.
- `sum` calculates the sum of all values in the group.  It works in
  double precision floating point only.
- `min` calculates the minimum of all values in the group.
- `max` calculates the maximum of all values in the group.
- `count` calculates the number of non-null values in the group.
    - `count(*)` is a special function which will count the number of rows in the group with non-null values in any column

The following useful non-standard aggregation functions is also supported:

- `latest`, `earliest` will return the values with the latest or earliest timestamp in the group
- `pivot(columnName, value)` will accumulate a single row per group, with the
  column name and value given.  This can be used with a group by clause to
  transform a dense dataset of (actor,action,value) records into a sparse
  dataset with one sparse row per actor, for example to create one-hot feature vectors or term-document or cooccurrence matrices.
- `string_agg(expr, separator)` will coerce the value of `expr` and that of
  `separator` to a string, and produce a single string with the concatenation
  of `expr` separated by `separators` at internal boundaries.  For example,
  if `expr` is `"one"`, `"two"` and `"three"` in the group, and separator is
  `', '` the output will be `"one, two, three"`.

### Aggregates of rows

Every aggregate function can operate on single columns, just like in standard SQL, but they can also operate on multiple columns via complex types like rows and scalars.  This
has the effect of calculating a separate aggregate for each column in the input, and
returns a row-valued result.  For example, to calculate the total count of each
 column in a dataset, the following would suffice:

```sql
SELECT count({*})
```

which would return a row with one count for each column in the dataset.  This
functionality is useful to write generic queries that operate without prior
knowledge of the column names, and to make queries on datasets with thousands
or millions of column feasible.

## Vertical, Horizontal and Temporal Aggregation

The standard SQL aggregation functions operate 'vertically' down columns. MLDB datasets are transposable matrices, so MLDB also supports 'horizontal' aggregation. In addition, MLDB supports a third, temporal dimension, so 'temporal' aggregation is also supported:

- Vertical aggregation functions
  - `vertical_count(<row>)` alias of `count()`, operates on columns.
  - `vertical_sum(<row>)` alias of `sum()`, operates on columns.
  - `vertical_avg(<row>)` alias of `avg()`, operates on columns.
  - `vertical_min(<row>)` alias of `min()`, operates on columns.
  - `vertical_max(<row>)` alias of `max()`, operates on columns.
  - `vertical_latest(<row>)` alias of `latest()`, operates on columns.
  - `vertical_earliest(<row>)` alias of `earliest()`, operates on columns.
- Horizontal aggregation functions
  - `horizontal_count(<row>)` returns the number of non-null values in the row.
  - `horizontal_sum(<row>)` returns the sum of the non-null values in the row.
  - `horizontal_string_agg(<row>, <separator>)` returns the string aggregator of the value of row, coerced to strings, separated by separator.
  - `horizontal_avg(<row>)` returns the average of the non-null values in the row.
  - `horizontal_min(<row>)` returns the minimum of the non-null values in the row.
  - `horizontal_max(<row>)` returns the maximum of the non-null value in the row.
  - `horizontal_latest(<row>)` returns the non-null value in the row with the latest timestamp.
  - `horizontal_earliest(<row>)` returns the non-null value in the row with the earliest timestamp.
- Temporal aggregation functions
  - `temporal_count(<row>)` returns the number of non-null values per cell.
  - `temporal_sum(<row>)` returns the sum of the non-null values per cell.
  - `temporal_avg(<row>)` returns the average of the non-null values per cell.
  - `temporal_min(<row>)` returns the minimum of the non-null values per cell.
  - `temporal_max(<row>)` returns the maximum of the non-null value per cell.
  - `temporal_latest(<row>)` returns the non-null value with the latest timestamp per cell.
  - `temporal_earliest(<row>)` returns the non-null value with the earliest timestamp per cell.


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
', 'i', i)
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

