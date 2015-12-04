# SQL Query Function

The SQL query function type allows the creation of a function to perform an
[SQL query](../sql/Sql.md) against a dataset.
The query is parameterized on the function input values.
SQL query function allows for joins to be implemented
in MLDB.

Functions created this way executes the given query against the given dataset
and outputs the `SELECT` expression applied to the first matching
row (ie, with an `OFFSET` of 0 and a `LIMIT` of 1).  If multiple
rows are matched, all but the first will be ignored.

## Configuration

![](%%config function sql.query)

## Accessing function input values

The function input values are declared in the SQL query by
prefixing their name with a `$` character.  For UTF-8 names
or those that are not simple identifiers, it is necessary to
put them inside '"' characters.  For example,

- `$x` refers to the input value named `x`.
- `$"x and y"` refers to the input value named `x and y`.

## Restrictions

- It is normally best to use a query with a tight where clause
  so that it is not necessary to scan the whole table.  Otherwise
  the queries may be very slow.
- Input values (`$x`) cannot be used within the FROM clause
  (in particular, as part of the conditions for the JOIN).  In some
  cases, this can be worked around by putting the condition in the
  WHERE clause.
  

## Example

The following function configuration add information about a user
looked up from a `users` table.  The user to be queried is read from
the `userId` input value.  A second `details` input value controls
whether or not and extra field is calculated.

The function outputs three values:

- the `age` of the user that is read directly from the `age` column of the `users`
  dataset;
- the `gender` which is also read directly from the `gender` column of the
  `users` dataset;
- the `activationYears` which is equal to 1/365 of the
  `activationDays` column of the `users` dataset if the `details`
  value is true, or `null` otherwise.

```
{
    id: "query",
    type: "sql.query",
    params: {
        select: "age, gender, CASE WHEN $details THEN activationDays / 365.0 ELSE NULL AS activationYears",
        from: { id: 'users' },
        where: 'rowName() = $userId'
    }
}
```

## See also

* [MLDB's SQL Implementation](../sql/Sql.md)
* the ![](%%doclink sql.expression function) calculates pure SQL
  expressions, without a dataset involved.
