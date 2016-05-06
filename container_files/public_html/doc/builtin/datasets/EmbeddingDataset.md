# Embedding Dataset

The embedding dataset can store a fixed-length coordinate vector in each row.
It is used to store the output of embeddings, and enables them to be queried
efficiently for nearest neighbours type queries.

The embedding dataset has strict requirements:

* Each row can only be recorded once
* Each recorded row must have exactly the same set of columns
* Each column value must be a number, and not an infinity or a NaN
* No column can have a null value, or a string value

Currently, the embedding dataset can only exist in memory.

The dataset is typically used as the
output of a procedure that generates the embedding, such as the ![](%%doclink tsne.train procedure), the ![](%%doclink svd.train procedure) or the ![](%%doclink kmeans.train procedure)

## Configuration

![](%%config dataset embedding)

### Metric space

The metric field has the following possibilities:

![](%%type Datacratic::MLDB::MetricSpace)


## Querying Nearest Neighbors

The embedding dataset stores an index in a [Vantage Point Tree] which allows
for efficient queries of points that are close in the embedding space.  This
can be used for nearest-neighbors searches, which when combined with a good
embedding algorithm can be used to implement recommendations.

See the ![](%%doclink nearest.neighbors function) for more details.

## Examples

* The ![](%%nblink _demos/Recommending Movies) demo notebook
* The ![](%%nblink _demos/Exploring Favourite Recipes) demo notebook

## See Also

* [Vantage Point Tree] is the data structure used to allow quick lookups
* the ![](%%doclink nearest.neighbors function) is used to find nearest neighbors in an embedding dataset.
* the ![](%%doclink kmeans.train procedure) is another way of identifying similar points.
* the ![](%%doclink svd.train procedure) procedure is often used to train an embedding with a high number of dimensions
* the ![](%%doclink tsne.train procedure) can be used to train a 2 or 3 dimensional embedding

[Vantage Point Tree]: http://en.wikipedia.org/wiki/Vantage-point_tree "Vantage Point Tree"
