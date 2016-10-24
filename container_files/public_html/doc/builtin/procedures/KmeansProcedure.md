# K-Means Training Procedure

This procedure trains a K-means clustering model and stores the result model (i.e. cluster centroids)
into an output dataset, as well as storing the cluster labels for its input dataset into a separate output dataset.

## Configuration

![](%%config procedure kmeans.train)

![](%%type MLDB::MetricSpace)

## Training

The k-means procedure is used to take a set of points, each of which is
characterized by its coordinates in an embedding space, and group them such
that each one belongs to one cluster.  The clusters are described by a single
point that is the cluster centroid.

The input dataset has one row per point, with the coordinates being in the
columns.  There must be the same set of numeric coordinates per row.

As an example, the following input would be suitable for the k-means algorithm:


|  rowName   |  x  |  y  |
|----------|---|---|
| row1     | 1 | 4 |
| row2     | 1 | 3 |
| row3     | 3 | 1 |
| row4     | 4 | 1 |


Using the k-means procedure with the Euclidean metric to create two clusters (i.e. with `metric` set to `Euclidean` and `numClusters` set to 2),
the output would be the centroids:

|_rowName |x|y|
|----------|---|---|
| "0" | 1   | 3.5 |
| "1" | 3.5 | 1   |

Using the output of the procedure to classify new points is done by calculating
the distance from the point to each of the cluster centroids, and then assigning
the point to the cluster with the shortest distance.

## Examples

* The ![](%%nblink _demos/Mapping Reddit) demo notebook
* The ![](%%nblink _demos/Visualizing StackOverflow Tags) demo notebook

# See also

* [Wikipedia k-means Article](http://en.wikipedia.org/wiki/K-means_clustering)
* the ![](%%doclink kmeans function) applies the centroids to new data points to deternine
   which cluster they fit in to.
