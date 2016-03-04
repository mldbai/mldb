# SVD Row Embedding

Functions of this type embed a row of a dataset into an SVD space, producing a singular vector,
using a model previously trained by an  ![](%%doclink svd.train procedure).

## Configuration

![](%%config function svd.embedRow)

## Input and Output Value

Functions of this type has a single input value called `row` which is a row. The columns that
are expected in this row depend on the features that were trained into the SVD model.
For example, if in the training the input value was `"select": "x,y"`, then the function will 
expect two columns called `x` and `y`.

These functions have a single output value called `embedding` which contains a row. This row
will contain columns with names prefixed with the `outputColumn` parameter of the ![](%%doclink svd.train procedure) that trained the model, followed by a 4 digit number for each of the singular values. By default, the `outputColumn` parameter is `svd` so the columns of the output row will be `svd0001`, `svd0002`, etc.

## Validation

When an SVD procedure is trained, it infers the type of input values and does feature
extraction based upon the types seen in training.  If the type of an input
value passed into the function doesn't match the input value type seen in training, then the
SVD may not give sensible outputs.  This happens in the following
situations:

- A column was always a number in training, but a string value is passed in
  on the corresponding input value;
- A column had string values in training, but the string value passed in was
  not seen at all in training.

The `acceptUnknownValues` parameter controls what happens in this situation.
If the value of that parameter is `true`, then a column with an unknown value
will be ignored completely.  If the value of the parameter is `false`, then
the application of the function will return an error.

The main use of that parameter is to catch errors in the development phase,
for example accidentially encoding a parameter as a string when it should be
an int or mixing column names.

## Examples

* The ![](%%nblink _demos/Recommending Movies) demo notebook

## See Also

* [Wikipedia SVD Article](http://en.wikipedia.org/wiki/Singular_value_decomposition)
* the ![](%%doclink svd.train procedure) trains an SVD


