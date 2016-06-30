# Joined Dataset 

The joined dataset creates a view on an SQL join between two tables.  The
join is evaluated up front, which may require considerable processing time.

Currently, the join condition has the following restrictions:

- It must be either a single boolean expression or an AND of multiple
  boolean expressions
- One expression must be of the form `x = y`, where x refers only to
  variables on the left side and y refers only to variables on the right
  side
- The rest of the expressions may only refer to either the left side or
  the right side, not both. 

## Configuration

![](%%config dataset joined)

## See Also

* The ![](%%doclink merged dataset) is equivalent to joining two datasets on the left hand row name being equal to the right hand row name.
* The ![](%%doclink transposed dataset) is another dataset transformation
* The ![](%%nblink _demos/Investigating the Panama Papers) demo

