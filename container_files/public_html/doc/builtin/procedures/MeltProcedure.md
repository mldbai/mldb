# Melt Procedure

This procedure type allows you to perform a melt operation on a dataset. Melting 
a dataset takes a set of columns to keep fixed and a set of columns to melt and
creates a new rows, one per column to melt, while copying as is the columns to 
keep fixed.

An example usage is importing JSON data where some fields contain an array of 
objects and the way we want to process it is one object per row.

## Configuration

![](%%config procedure melt)

## Example

![](%%notebookexample procedure melt)


## See also

* The [parse_json](../sql/ValueExpression.md.html#parse_json) builtin function can perform 
JSON unpacking to a text cell in an SQL query

