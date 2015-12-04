# t-SNE Application Function

This function re-applies a t-SNE model that was trained with a
![](%%doclink tsne.train procedure) to new data points.

It is especially useful where a known layout has been produced,
but some of the underlying data behind that layout has
changed and a new layout that maps points in the same space
is needed.

## Input and Output Values

The t-SNE function takes the same input value as the procedures and outputs
a single value named `embedding` with as many dimensions as the output was trained on.

## Configuration

![](%%config function tsne.embedRow)


# See also

* [Wikipedia t-SNE Article](http://en.wikipedia.org/wiki/T-distributed_stochastic_neighbor_embedding)
* The ![](%%doclink svd.embedRow function) is often used before the t-SNE
  function to turn a sparse dataset into an embedding.
* The ![](%%doclink tsne.train procedure) trains the model
