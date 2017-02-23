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

To refer to a column, you use its name, which is the string representation of its path, as explained in the [Intro to Datasets](../datasets/Datasets.md) and must be used in accordance with the [quoting rules](Sql.md).  So to read the value of the column `x` and add one, use `x + 1`.

> **Unlike in conventional SQL,** references to non-existent columns are always evaluated as `NULL`. A common mistake is to use double-quotes to represent a string, which usually results in a reference to a non-existent column, and therefore `NULL`.

### Referring to columns by dataset

When querying a single dataset, it is allowed but unnecessary to specify the name of that dataset when referring a column. However, in some cases, 
the query will run on several datasets (See [From Expression](FromExpression.md)). In those cases, you need to specify the name or alias of the dataset the desired column is from using a 
`.` operator.
For example, to refer the column `y` from a dataset whose name or alias is `x`, you must use `x.y` in the value expression. 

If the dataset has been aliased (e.g. `FROM dataset AS x`), you **must** use the alias `x` instead of the original name `dataset`.

## <a name="operators"></a>Operators

The following standard SQL operators are supported by MLDB.  An
operator with lower precedence binds tighter than one with a
higher predecence, so for example `x + y * z` is the same as
`x + (y * z)`.  Expressions at the same precedence level are 
always left associative, that is the expression
`x / y % z` is evaluated as `(x / y) % z`.

  Operator  |  Type              | Precedence 
:----------:|:--------------------:|:------------:
     `.`      |  indirection  |          0 
     `@`      |  timestamp association  |          0 
     `~`      |  unary arithmetic  |          1 
     `*` , `/` , `%`      |  binary arithmetic |          2 
     `+` , `-`      |  unary arithmetic  |          3 
     `+` , `-`      |  binary arithmetic |          3 
     `&` , <code>&#124;</code> , `^`      |  binary bitwise    |          3 
     `=` , `!=`, `>` , `<` , `>=` , `<=`       |  binary comparison |          4 
     `NOT`    |  unary boolean     |          5 
     `AND`    |  binary boolean    |          6 
     `OR`     |  binary boolean    |          7 

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
It has the same precedence as binary comparisons (`=` , `!=`, `>` , `<` , `>=` , `<=`).


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
  - path

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
made possible by MLDB's sparse data model. It has the same precedence as the unary not (`NOT`).

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

For example: `expr IN (KEYS OF tokenize('sentence'))`

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

This expression has the same precedence as the unary not (`NOT`).

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

These functions are always available when processing rows from a dataset, and
will change values on each row under consideration. See the [Intro to Datasets](../datasets/Datasets.md) documentation for more information about names and paths.

<a name="rowHash"></a>

- `rowHash()`: returns the internal hash value of the current row, useful for random sampling and providing a stable [order](OrderByExpression.md) in query results
- `rowName()`: returns the name the current row 
- `rowPath()` is the structured path to the row under consideration.
- `rowPathElement(n)` is the nth element of the `rowPath()` of the row
   under consideration.  Negative indexing is supported, meaning that if n is less than zero, 
   it will be a distance from the end (for example, -1 is the last element, -2 is the second to last). 
   For a rowName of `x.y.2`, then `rowPathElement(0)` will be `x`, `rowPathElement(1)` will be `y` 
   and `rowPathElement(2)` is equivalent to `rowPathElement(-1)` which will be `2`. If n is 
   bigger than the number of elements in the row path, NULL will be returned.
- `columnCount()`: returns the number of columns with explicit values set in the current row
- `leftRowName()` and `rightRowName()`: in the context of a join, returns the name of the row that was joined on the left or right side respectively.


### Path manipulation functions

 See the [Intro to Datasets](../datasets/Datasets.md) documentation for more information about names and paths.

- `stringify_path(path)` will return a string representation its argument, with the
  elements separated by periods and any elements with periods or quotes
  quoted (and internal quotes doubled).  This is what is used by the `rowName()`
  function to convert from the structured `rowPath()` representation.  For
  example, the path `['x', 'hello.world']` when passed through would
  return the string `'x."hello.world"'`.  This is the inverse of `parse_path`
  (below).
- `parse_path(string)` will return its argument as a structured path
  which may be used for example as the result of a `NAMED` clause.  This is the
  inverse of `stringify_path` (above).
- `path_element(path, n)` will return element `n` of the given `path`.
- `path_length(path)` will return the number of elements in the given `path`.
- `flatten_path(path)` will return a path with a single element that encodes
  the entire `path` passed in, in the same manner as `stringify_path`.  This
  is useful where a series of nested values need to be turned into a flat set
  of columns for another function or a vector aggregator.  By using
  `COLUMN EXPR (AS flatten_path(columnPath()))` an entire object can be
  flattened in this manner.
- `unflatten_path(path)` is the inverse of `flatten_path`.  It requires that
  the input path have a single element, and will turn it back into a variable
  sized path.  Using `COLUMN EXPR (AS unflatten_path(columnPath()))` an entire
  object can be unflattened in this manner.


### Encoding and decoding functions

- `implicit_cast(x)`: attempts to convert `x` to a
  number according to the following recipe:
  - if `x` is the empty string, return `null`
  - if `x` is a string that can be converted to a number, return the number
  - otherwise, return `x` unchanged
- `hash(expr)` returns a hash of the value of            `expr`.  Hashing a `null`
  value will always return a `null`.  Internally, this uses
  the [Highway Tree Hash](https://github.com/google/highwayhash) which is
  claimed to be likely secure whilst retaining good speed.  See also
  [`rowHash()`](#rowHash).
- `base64_encode(blob)` returns the base-64 encoded version of the blob
  (or string) argument as a string.
- `base64_decode(string)` returns a blob containing the decoding of the
  base-64 data provided in its argument.
- `extract_column(row)` extracts the given column from the row, keeping
  only its latest value by timestamp.
- `print_json(expr)` returns string with the value of expr converted to JSON.  If
  there is ambiguity in the expression (for example, the same key with multiple
  values), then one of the values of the key will be chosen to represent the value
  of the key.
- <a name="parse_json"></a>`parse_json(string, {arrays: 'parse', ignoreErrors: false})` returns a row with the JSON decoding of the
  string in the argument. If the `arrays` option is set to `'parse'` (this is the default) then nested arrays and objects will be parsed recursively; no flattening is performed. If the `arrays` option is set to `'encode'`, then arrays containing only scalar values will be one-hot encoded and arrays containing only objects will contain the string representation of the objects. If the `ignoreErrors` option is set to `true`, the function will return NULL for strings that do not parse
  as valid JSON. It will throw an exception otherwise.

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

The full set of options to the `parse_json` function are as follows:

![](%%type MLDB::Builtins::ParseJsonOptions)

and the possible values for the `arrays` field are:

![](%%type MLDB::JsonArrayHandling)


### Numeric functions

- `pow(x, y)`: returns `x` to the power of `y`.
- `exp(x)`: returns _e_ (the Euler number) raised to the power `x`.
- `ln(x)`: returns the natural logarithm of `x`.
- `log(x)`: returns the base-10 logarithm of `x`.
- `log(b, x)`: returns the base-`b` logarithm of `x`.
- `ceil(x)`: returns the smaller integer not less than `x`.
- `floor(x)`: returns the largest integer not greater than `x`.
- `mod(x, y)`: returns `x` modulo `y`.  The value of `x` and `y` must be an integer. Another way to get the modulo is `x % y`.
- `abs(x)`: returns the absolute value of `x`.
- `sqrt(x)`: returns the square root of `x`.
- `sign(x)`: returns the sign of `x` (-1, 0, +1).
- `isnan(x)`: returns true if `x` is `NaN` in the floating point representation.
- `isinf(x)`: return true if `x` is +/- infinity in the floating point representation.
- `isfinite(x)`: returns true if `x` is neither infinite nor `NaN`.
- `sin(x)`, `cos(x)` and `tan(x)` are the normal trigonometric functions;
- `asin(x)`, `acos(x)` and `atan(x)` are the normal inverse trigonometric functions;
- `atan2(x, y)` returns the two-argument arctangent of `x` and `y`, in other
  words the angle (in radians) of the point through `x` and `y` from the origin
  with respect to the positive `x` axis;
- `sinh(x)`, `cosh(x)` and `tanh(x)` are the normal hyperbolic functions;
- `asinh(x)`, `acosh(x)` and `atanh(x)` are the normal inverse hyperbolic functions.
- `quantize(x, y)`: returns `x` rounded to the precision of `y`.  Here are some examples:

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


- `replace_nan(x, y)`: replace all `NaN`s and `-NaN`s in `x` by `y`.  Works on scalars or rows.
- `replace_inf(x, y)`: replace all `Inf`s and `-Inf`s in `x` by `y`.  Works on scalars or rows.
- `replace_not_finite(x, y)`: replace all `Inf`s, `-Inf`s and `NaN`s in `x` by `y`.  Works on scalars or rows.
- `replace_null(x, y)`: replace all `null`s in `x` by `y`.  Works on scalars or rows.
- `clamp(x,lower,upper)` will clamp the value `x` between the `lower` and `upper` bounds.
- `binomial_lb_80(trials, successes)` returns the 80% lower bound using the Wilson score.
- `binomial_ub_80(trials, successes)` returns the 80% upper bound using the Wilson score.

More details on the [Binomial proportion confidence interval Wikipedia page](https://en.wikipedia.org/wiki/Binomial_proportion_confidence_interval).

### Constant functions

The following functions return numerical constants:

- `pi()` returns the value of *pi*, the ratio of a circle's circumference to its
  diameter, as a double precision floating point number.
- `e()` returns the value of *e*, the base of natural logarithms, as a double
  precision floating point number.

### String functions

- `lower(string)` returns the lowercase version of the string, according to the
  system locale.
- `upper(string)` returns the uppercase version of the string, according to the
  system locale.
- `length(string)` returns the length of the string.
- `remove_prefix(string, prefix)` returns the string with the specified prefix removed if present.
- `remove_suffix(string, suffix)` returns the string with the specified suffix removed if present.
- `regex_replace(string, regex, replacement)` will return the given string with
  matches of the `regex` replaced by the `replacement`.  Perl-style regular
  expressions are supported.  It is normally preferable that the `regex` be a
  constant string; performance will be very poor if not as the regular expression
  will need to be recompiled on every application.
- `regex_match(string, regex)` will return true if the *entire* string matches
  the regex, and false otherwise.  If `string` is null, then null will be returned.
  It is normally preferable that the `regex` be a
  constant string; performance will be very poor if not as the regular expression
  will need to be recompiled on every application.
- `regex_search(string, regex)` will return true if *any portion of * `string` matches
  the regex, and false otherwise.  If `string` is null, then null will be returned.
  It is normally preferable that the `regex` be a
  constant string; performance will be very poor if not as the regular expression
  will need to be recompiled on every application.
- `levenshtein_distance(string, string)` will return the [Levenshtein distance](https://en.wikipedia.org/wiki/Levenshtein_distance), 
  or the *edit distance*, between the two strings.

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

### Set operation functions

- `jaccard_index(expr, expr)` will return the [Jaccard index](https://en.wikipedia.org/wiki/Jaccard_index), also
  known as the *Jaccard similarity coefficient*, on two sets. The sets are specified using two row expressions.
  The column names will be used as values, meaning this function can be used
  on the output of the [`tokenize`](#importfunctions) function. The function will return 1 if the sets are equal, and 0 if they are 
  completely different.

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
- `reshape(val, shape)` will take a n-dimensional embedding and reinterpret it
  as a N-dimensional embedding of the provided shape containing all of the
  elements, allowing for example a 1-dimensional vector to be re-interpreted
  as a 2-dimensional array. The shape argument is an embedding containing the
  size of each dimension.  This will fail if the number of elements in `shape`
  is not the same as the number of elements in `val`.
- `reshape(val, shape, newel)` is similar to the two argument version of
  `reshape`, but allows for the number of elements to be different.  If the
  number of elements increases, new elements will be filled in with the
  `newel` parameter.
- `shape(val)` will take a n-dimensional embedding and return the size of each dimension as as array.
- `concat(x, ...)` will take several embeddings with identical sizes in all
  but their last dimension and join them together on the last dimension.
  For single dimension embeddings, this is normal concatenation.  For two
  dimension embeddings, this will join them vertically.  And so forth.
- `slice(val, index)` will take an n-dimensional embedding and select only
  the `index`th element of the last index.  For example, with a `m x n` embedding
  `x` a single row can be selected with `x[index]` (returning a `n` element
  embedding).  Whereas `slice(x, index)` will return the `index`th *column*
  as an `m` element embedding. 

### <a name="geofunctions"></a>Geographical functions

The following functions operate on latitudes and longtitudes and can be used to
calculate things to do with locations on Earth:

- `geo_distance(lat1, lon1, lat2, lon2)` calculates the great circle distance from
  the point at `(lat1, lon1)` to the point at (lat2, lon2)` in meters assuming that
  the Earth is a perfect sphere with a radius of 6371008.8 meters.  It will be
  accurate to within 0.3% anywhere on earth, apart from near the North or South
  Poles.

### <a name="signalprocfunctions"></a>Signal processing functions

The following functions provide digital signal processing capabilities:

- `fft(data [,direction='forward' [,type='real']])` performs a fast fourier
   transform on the given data.  `direction` can be `'forward'` or `'backward'`
   and controls the direction of the transform (the default is `'forward'`).
   `type` controls whether the data in the time domain is `'real'` or `'complex'`
   valued (default is real).  `data` must be an embedding of `n` reals (for the
   real case) or an `n` by 2 embedding (for the complex case), and `n` must be
   divisible by 32 (you can zero-pad the data otherwise).
   <p>The output of the forward FFT function is always complex valued, with
   the real and imaginary components in a `n` by 2 embedding on the output.
   Note that for real-valued FFTs, the imaginary part of the first (DC) component
   contains the half-frequency real component, unlike most FFT implementations.
   This needs to be maintained for the `reverse` direction to work, but will
   need to be handled in any analysis that is performed in the frequency
   domain.
- `phase(data)` takes a `n` by 2 embedding, with real and complex
  parts, and returns an `n` element embedding with the phase angle.
- `amplitude(data)` takes a `n` by 2 embedding, with real and complex
  parts, and returns an `n` element embedding with the amplitude.
- `real(data)` takes an `n` by 2 embedding, and returns the a `n` element
  embedding with the real parts.
- `imag(data)` takes an `n` by 2 embedding, and returns the a `n` element
  embedding with the real parts.
- `impulse(n)` returns an `n` element real embedding with the impulse function,
  with the first element 1 and the rest zero.
- `shifted_impulse(n, e)` returns an impulse function of length `n`
  time-shifted by `e` steps, ie zeros everywhere apart from the `e`th element
  which is one.

### <a name="imagefunctions"></a>Image processing functions

The following functions provide image processing capabilities:

- `parse_exif(blob)` takes a JPEG image blob and parses basic EXIF information from it. It should be used in combination with the `fetcher()` function. The returned values are:

![](%%type MLDB::Builtins::ExifMetadata)

### <a name="blobfunctions"></a>Blob functions

The following functions are specific to blob data:

- `blob_length(x)` returns the length (in bytes) of the blob `x`

### <a name="httpfunctions"></a>Web data functions

The following functions are used to extract and process web data.

#### `fetcher(str)`

Fetches resources from a given file or URL. It acts as the
default version of [function fetcher](../functions/Fetcher.md.html). It returns
two output columns:

* `content`, a binary BLOB field containing the (binary) content that was loaded from the URL. If there was an error, it will be null.
* `error`, a string containing the error message. If the fetch succeeded, it will be null.

**Example**

The following query will use fetcher to return the country code from an IP
address from an external web service.

```sql
SELECT CAST (fetcher('http://www.geoplugin.net/json.gp?ip=158.245.13.123')[content] AS STRING)
```

**Limitations**

  - The fetcher function will only attempt one fetch of the given URL; for
  transient errors a manual retry will be required
  * There is currently no timeout parameter.  Hung requests will timeout
  eventually, but there is no guarantee as to when.
  * There is currently no rate limiting built in.
  * There is currently no facility to limit the maximum size of data that
  will be fetched.
  * There is currently no means to authenticate when fetching a URL,
  apart from using the credentials daemon built in to MLDB.
  * There is currently no caching mechanism.
  * There is currently no means to fetch a resource only if it has not
  changed since the last time it was fetched.

#### `extract_domain(str, {removeSubdomain: false})`

Extracts the domain name from a URL. Setting the option `removeSubdomain` to `true` will return only the domain without the subdomain. Note that the string passed in must be a complete and valid URL. If a scheme (`http://`, etc) is not present, an error will be thrown.

The full set of options to the `extract_domain` function are as follows:

![](%%type MLDB::Builtins::ExtractDomainOptions)


See also the ![](%%doclink http.useragent function) that can be used to parse a user agent string.

### <a name="importfunctions"></a>Data import functions

- `tokenize(str, {splitChars: ',', quoteChar: '', offset: 0, limit: null, value: null, minTokenLength: 1, ngramRange:[1, 1]})`
can be used to create bag-of-tokens representations of strings, by returning a row whose
columns are formed by tokenizing `str` by splitting along `splitChars` and whose values by default are the
number of occurrences of those tokens within `str`. For example `tokenize('a b b c c c', {splitChars:' '})` will return the row `{'a': 1, 'b': 2, 'c': 3}`.
- `token_extract(str, n, {splitChars: ',', quoteChar: '', offset: 0, limit: null, minTokenLength: 1})` will return the `n`th token from `str` using the same tokenizing rules as `tokenize()` above. Only the tokens respecting the `minTokenLength` will be considered, and ngram options are ignored.
- `split_part(str, splitChars)` will return an embedding of all tokens as separated by the provided `splitChars`.

Parameters to `tokenize` and `token_extract` are as follows:

![](%%type MLDB::TokenizeOptions)


## <a name="aggregatefunctions"></a>Aggregate Functions

The following standard SQL aggregation functions are supported. They may only be used in SELECT and HAVING clauses. If an aggregation function appears in the SELECT clause and no GROUP BY clause is used, an empty GROUP BY clause will be inferred.

- `avg` returns the average of all values in the group.  It works in
  double precision floating point only.
- `sum` returns the sum of all values in the group.  It works in
  double precision floating point only.
- `min` returns the minimum of all values in the group.
- `max` returns the maximum of all values in the group.
- `count` returns the number of non-null values in the group.
    - `count(*)` is a special function which will count the number of rows in the group with non-null values in any column
- `count_distinct` returns the number of unique, distinct non-null values in the group.

The following useful non-standard aggregation functions are also supported:

- `latest`, `earliest` will return the values with the latest or earliest timestamp in the group
- `pivot(columnName, value)` will accumulate a single row per group, with the
  column name and value given.  This can be used with a group by clause to
  transform a dense dataset of (actor,action,value) records into a sparse
  dataset with one sparse row per actor, for example to create one-hot feature vectors or term-document or cooccurrence matrices.
- `string_agg(expr, separator [, sortField])` will coerce the value of `expr`
   and that of `separator` to a string, create a list of all values sorted 
   by the `sortField`
   (which is null if not specified) breaking ties by sorting by `expr` as a
   string, and produce a single string with the concatenation of `expr`
   separated by `separator` at internal boundaries on the list.  For example,
   if `expr` is `"one"`, `"two"` and `"three"` in the group, and `separator` is
   `', '` the output will be `"one, two, three"`.  The `sortField` can be used
   to ensure that the values over multiple `string_agg` calls are in the,
   same order, for example are in order of time or in row order of the
   underlying dataset.  Note that the `rowPath()` can be used in the
   `sortField` to achieve that result.

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
  - `vertical_stddev(<row>)` alias of `stddev()`, operates on columns.
  - `vertical_variance(<row>)` alias of `variance()`, operates on columns.
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

## <a name="jseval"></a>Evaluating a JavaScript function from SQL

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

You can also take a look at the ![](%%nblink _tutorials/Executing JavaScript Code Directly in SQL Queries Using the jseval Function Tutorial) for examples of how to use the `jseval` function.

## <a name="try"></a>Handling errors line by line

When processing a query and an error occurs, the whole query fails and no 
result is returned, even if only a single line caused the error. The `try` function is 
meant to handle this type of situation. The first argument is the expression to 
*try* to apply. The optional second argument is what will be returned if an error 
is encountered. It can be any value expression, including other functions and 
other `try` functions. If no second argument is given, the error is returned as a string.
The `try` function is analogous to a try/catch block in other programming languages.

### Example usage

```sql
SELECT try(parse_json('foo'), 'err')
```

Here, `parse_json('foo')` will fail. Since the second argument is provided, the
value "err" will be returned.

```sql
SELECT try(parse_json('foo'))
```

Again, `parse_json('foo')` will fail. Since the second argument was left blank,
the error message generated by MLDB will be returned.

If the result of the `try` function is expected to be a row expression, then 
both arguments supplied must return row expressions, like in the following
example:

```sql
SELECT try(parse_json('foo'), {}) AS *
```

As a counter example, the following two calls will both fail 
when an error is encoutered because the function will
return a string, and strings cannot be used with `AS *`.

```sql
SELECT try(parse_json('foo')) AS *
SELECT try(parse_json('foo'), 'err') AS *
```

Note that the `try` function only applies to runtime exceptions, not to syntax
errors or bind-time failures.
