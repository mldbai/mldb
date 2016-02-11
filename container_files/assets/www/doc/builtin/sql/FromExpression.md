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

## Sample

Queries can be made to a sample of a dataset by using the sample() function in the FROM expression. For example:

```sql
SELECT x.* FROM sample(dataset, {rows: 25, withReplacement: FALSE}) AS x
```

See ![](%%doclink sampled dataset) for more details.

## Transpose 
Queries can be made to the transpose of a dataset by using the transpose() function in the FROM expression. For example:

```sql
SELECT x.* FROM transpose(dataset) AS x
```

See ![](%%doclink transposed dataset) for more details.

## Merge

Queries can be made to the union of several datasets by using the merge() function in the FROM expression. For example:

```sql
SELECT x.* FROM merge(dataset1, dataset2, dataset3) AS x
```

See ![](%%doclink merged dataset) for more details.

<!--

## Using rows as a dataset

In some circumstances, it may be useful to use a row as a dataset,
particularly when using the ![](%%doclink sql.query function) or a
sub-select.  This can be done using the syntax

```sql
SELECT ... FROM row_dataset(expression) ...
```

When this construct is used, a dataset is constructed with one row
for each column in the expression, with one column called `value`
containing the value of the column, and one column called `column`
with the column name.  For example, the query

```sql
select * from row_dataset({x: 1, y:2, z: 'three'})
```

would yield the following result:


column  |  value
:------:|:-------:
x       |  1
y       |  2
z       |  "three"

-->
