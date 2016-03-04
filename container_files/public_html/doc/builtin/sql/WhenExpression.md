# SQL When Expressions

An SQL When expression is a standard SQL [Value Expression](ValueExpression.md), and is coerced to a boolean type after evaluation. The `WHEN` clause is used to select values based on their timestamp. Only values with timestamps (accesed via the special `timestamp()` function) that cause the expression to evaluate true are kept; values resulting in evaluations of null or false are discarded.

Note that this syntax is not part of SQL, it is an MLDB extension, and is evaluated after the `WHERE` clause.

## Using When and Where Clauses

The `WHERE` clause is evaluated before the `WHEN` clause.  To illustrate that, consider the
browse _events_ dataset containing these values

| users / event |  click   | view  |
| ----- | --- | --- |
| bob   | link1 @ 2015-01-01T00:00:00 | google.com @ 2015-01-01T00:01:00 | 
| alice | link2 @ 2015-01-01T00:03:00 | mldb.ai @ 2015-01-01T00:06:00 |

One can select the events in the first five minutes of 2015 with this query

```sql
SELECT * FROM events 
WHEN timestamp() BETWEEN '2015-01-01T00:00:00' AND '2015-01-01T00:05:00'
```

The output is

| users / event |  click   | view  |
| ----- | --- | --- |
| bob   | link1 @ 2015-01-01T00:00:00 | google.com @ 2015-01-01T00:01:00 | 
| alice | link2 @ 2015-01-01T00:03:00 | null |

Similarly, one can select users who viewed _mldb.ai_ with this query

```sql
SELECT * FROM events 
WHERE view = 'mldb.ai'
```

| users / event |  click   | view  |
| ----- | --- | --- |
| alice | link2 @ 2015-01-01T00:03:00 | mldb.ai @ 2015-01-01T00:06:00 |

Lastly, if we combined the `WHEN` and the `WHERE` clauses together 

```sql
SELECT * FROM events 
WHEN timestamp() BETWEEN '2015-01-01T00:00:00' AND '2015-01-01T00:05:00' 
WHERE view = 'mldb.ai'
```

we get:

| users / event |  click  |
| ----- | --- | 
| alice | link2 @ 2015-01-01T00:03:00 | 

This is because the `WHERE` clause is evaluated prior to the `WHEN` clause.
