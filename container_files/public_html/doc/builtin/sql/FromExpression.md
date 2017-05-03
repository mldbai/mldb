# SQL From Expressions

An SQL From expression specifies the dataset(s) on which a query will run.

## Datasets and Aliases

The simplest type of From expression is simply a dataset name.  In that
case, the query will run on the given dataset. The dataset name can be followed by `as <alias>`, in which case the query will be run on the given dataset, as if it was named `<alias>`. For example:

```sql
SELECT x.* FROM dataset AS x
```

## Subqueries

A full query in parentheses (i.e. `( )`) can be substituted for a dataset name. For example:

```sql
SELECT * FROM (SELECT * FROM dataset WHERE column1 = 2) as subselect WHERE column2 = 4'
```

## Joins

From Expressions can be combined together to perform joins with the pattern `<FromExpression1> <JoinType> <FromExpression2> ON <ValueExpression>`. For example:

```sql
SELECT * 
FROM (SELECT * FROM dataset WHERE column1 = 2) as subselect 
    JOIN  dataset2 as x ON subselect.column2 = x.column3 + 1

```

`<FromExpression1>` and `<FromExpression2>` are called the "left side" and "right side" of the join, respectively, and the `<ValueExpression>` is called the "join condition".

The following `<JoinType>`s are supported:

**`left JOIN right`, `left INNER JOIN right`**

The output contains a row for each combination of rows of `left` and rows of `right` that satisfies the join condition.

**`left LEFT JOIN right`, `left LEFT OUTER JOIN right`**

First, an inner join is performed. Then, for each row in `left` that does not satisfy the join condition with any row in `right`, a joined row is added with null values in columns of `right`. The output therefore always has at least one row for each row in `left`.

**`left RIGHT JOIN right`, `left RIGHT OUTER JOIN right`**

First, an inner join is performed. Then, for each row in `right` that does not satisfy the join condition with any row in `left`, a joined row is added with null values in columns of `left`. This is the converse of a left join: the output will always have a row for each row in `right`.

**`left OUTER JOIN right`, `left FULL JOIN right`, `left FULL OUTER JOIN right`**

First, an inner join is performed. Then, for each row in `left` that does not satisfy the join condition with any row in `right`, a joined row is added with null values in columns of `right`. Also, for each row of `right` that does not satisfy the join condition with any row in `left`, a joined row with null values in the columns of `left` is added. The output therefore always has at least one row for each row of both `left` and `right`.

## <a name="sample-function"></a>Sample

Queries can be made to a sample of a dataset by using the `sample` function in the FROM expression. For example:

```sql
SELECT x.* FROM sample(dataset, {rows: 25, withReplacement: FALSE}) AS x
```

The second argument, a row expression  with the dataset's configuration, is optional. When it is not specified, it 
use the sampled dataset's default parameters.

See ![](%%doclink sampled dataset) for more details.

##<a name="transpose-function"></a> Transpose 
Queries can be made to the transpose of a dataset by using the transpose() function in the FROM expression. For example:

```sql
SELECT x.* FROM transpose(dataset) AS x
```

See ![](%%doclink transposed dataset) for more details.

## <a name="merge-function"></a>Merge

Queries can be made to the union of several datasets by using the merge() function in the FROM expression. For example:

```sql
SELECT x.* FROM merge(dataset1, dataset2, dataset3) AS x
```

See ![](%%doclink merged dataset) for more details.

## Using rows as a dataset

In some circumstances, it may be useful to use a row as a dataset,
particularly when using the ![](%%doclink sql.query function) or a
sub-select.  This can be done using the syntax

```sql
SELECT ... FROM row_dataset(expression) ...
```

if a dataset with one row per *column* in the input row is required, or

```sql
SELECT ... FROM atom_dataset(expression) ...
```

if a dataset with one row per *atom* in the input row is required
(in other words, the row is pre-flattened and a table is made of
the scalar values at the leaf nodes).

When this construct is used, a dataset is constructed with one row
for each column or atom in the expression, with one column called `value`
containing the value of the column, and one column called `column`
with the column name.  For example, the query

```sql
SELECT * FROM row_dataset({x: 1, y:2, z: 'three'})
```

would yield the following result for both `row_dataset` and `atom_dataset`:


column  |  value
:------:|:-------:
x       |  1
y       |  2
z       |  "three"

whereas the following query

```sql
SELECT * FROM row_dataset({w: {x: 1, y:1, y:2, z: 'three'}})
```

would yield the following:


column  |  value
:------:|:-------:
w       |  {x: 1, y:1, y:2, z: 'three'}

whereas replacing `row_dataset` with `atom_dataset` would yield

```sql
SELECT * FROM atom_dataset({w: {x: 1, y:1, y:2, z: 'three'}})
```

column    |  value
:--------:|:-------:
w.x       |  1
w.y       |  2
w.z       |  "three"

The `COLUMN EXPR` and `STRUCTURED COLUMN EXPR` constructs can be used to
similar effect when the goal is to apply a function or transformation to
each element of a row, rather then convert it into a table.

### Caveats

The `column` column of the `row_dataset` and `atom_dataset` functions will
be of type `PATH`.  This will not compare true with any literal string,
which can be surprising.  If it is necessary to compare it with a string, it is
necessary to convert the string into a path with `CAST ('stringtocomparewith' AS PATH)` or `parse_path('stringtocomparewith')`.

## See also

* The ![](%%nblink _tutorials/Virtual Manipulation of Datasets Tutorial) for examples of `sample` and `transpose`
* The ![](%%nblink _demos/Investigating the Panama Papers) demo for examples of `join`

