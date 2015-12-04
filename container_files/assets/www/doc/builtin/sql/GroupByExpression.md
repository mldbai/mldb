# SQL Group-By Expressions

An SQL Group-By expression is simply a comma-delimited list of [Value Expressions](ValueExpression.md).

## Extra functions and variables available when executing `group by` expressions

When executing a group by expression, the following extra fuctions and
variables are available:

- The expressions that were used in the group by clause.  For example, if you
  group on `label,value`, then you can access `label` and `value` within the
  expression.
- The `rowName()` function will be a string version of the JSON encoded array
  of grouping fields.  This is the default value of the row.
- The `groupKeyElement(n)` or `group_key_element(n)` function takes an integer
  index and returns the value of that element of the group expression.  So if
  the expression was grouped by `label, value * 10`, then `group_key_element(0)`
  will have the value of the expression `label`, and `group_key_element(1)`
  will have the value of the expression `value * 10`.

