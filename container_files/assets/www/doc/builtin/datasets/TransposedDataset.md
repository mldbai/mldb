# Transposed Dataset

A matrix transpose swaps the rows and columns of a matrix, in other words it rotates
it 90 degrees.  The transposed dataset is a dataset adaptor that takes another
dataset and performs the same function.

In other words, rows become columns and columns become rows.

This is useful when performing selects across columns instead of down rows.

The transposition operation is virtual, in other words no copy is made of the
dataset.

## Configuration

![](%%config dataset transposed)


## Examples

* The ![](%%nblink _demos/Mapping Reddit) demo notebook

## See also

* The ![](%%doclink merged dataset) is another dataset transformation
* The ![](%%doclink transform procedure) can be used to modify a dataset ready for merging
