# SQL From Expressions

An SQL From expression specifies the dataset(s) on which a query will run.

## Datasets and Aliases

The simplest type of From expression is simply a dataset name.  In that
case, the query will run on the given dataset. The dataset name can be followed by `as <alias>`, in which case the query will be run on the given dataset, as if it was named `<alias>`. For example:

```
SELECT x.* FROM dataset AS x
```

## Subqueries

A full query in parentheses (i.e. `( )`) can be substituted for a dataset name. For example:

```
SELECT * FROM (SELECT * FROM dataset WHERE column1 = 2) as subselect WHERE column2 = 4'
```

## Joins

From expressions can be combined together to perform outer joins with the pattern `<FromExpression> JOIN <FromExpression> ON <ValueExpression>`. For example:

```
SELECT * 
FROM (SELECT * FROM dataset WHERE column1 = 2) as subselect 
    JOIN  dataset2 as x ON subselect.column2 = x.column3 + 1

```

## Sample

Queries can be made to a sample of a dataset by using the sample() function in the FROM expression. For example:

```
SELECT x.* FROM sample(dataset, {rows: 25, withReplacement: FALSE}) AS x
```

See ![](%%doclink sampled dataset) for more details.

## Transpose

Queries can be made to the transpose of a dataset by using the transpose() function in the FROM expression. For example:

```
SELECT x.* FROM transpose(dataset) AS x
```

See ![](%%doclink transposed dataset) for more details.

## Merge

Queries can be made to the union of several datasets by using the merge() function in the FROM expression. For example:

```
SELECT x.* FROM merge(dataset1, dataset2, dataset3) AS x
```

See ![](%%doclink merged dataset) for more details.
