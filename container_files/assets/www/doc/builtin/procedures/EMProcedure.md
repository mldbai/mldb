# EM Training Procedure

This procedure trains an Estimation-Maximisation (EM) clustering algorithm with a Gaussian Mixture Model and stores the result mixture model (i.e. cluster centroids and covariance matrix) into an output dataset, as well as storing the cluster labels for its input dataset into a separate output dataset.

## Configuration

![](%%config procedure em.train)

## Training

The EM procedure is a clustering algorithm. It is used to take a set of points, each of which is
characterized by its coordinates in an embedding space, and group them such
that each one belongs to one cluster.  The clusters are described by a single
point that is the cluster centroid, as well as a N by N covariance matrix. 

The input dataset has one row per point, with the coordinates being in the
columns. There must be the same set of numeric coordinates per row.

As an example, the following input would be suitable for the EM algorithm:


|  *rowName*   |  *x*  |  *y*  |
|----------|---|---|
| row1     | 1 | 4 |
| row2     | 1 | 3 |
| row3     | 3 | 1 |
| row4     | 4 | 1 |


Using the output of the procedure to classify new points is done by calculating
the distance from the point to each of the cluster centroids using the multivariate normal distribution density function.

Because the size of the dense covariance matrix grows to the square of the number of dimentions in the input data, it is strongly 
recommended to use dimensionality reduction to brind the number of input dimention to 10 or less. This should also improve the accuracy 
of the result, as with most clustering algorithms.

# See also

* the ![](%%doclink em function) applies the centroids to new data points to deternine
   which cluster they fit in to.
