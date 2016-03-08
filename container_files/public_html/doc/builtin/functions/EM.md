# Gaussian Clustering Function

The gaussian clustering function type takes the output model of the gaussian clustering procedure and applies
it to new data in order to assign it to clusters.

## Configuration

![](%%config function gaussianclustering)

Functions of this type load their internal state from a file, which is identified
with the `modelFileUrl` parameter. The model file must be previously saved using the 
gaussian clustering procedure.

In the application of the function, the `embedding` input parameter provide a coordinate
vector to evaluate. The distance from that input to each of the centroids
is then calculated using the multivariate normal distribution density function, and in the 
function outputs the value `cluster` containing the numerical identifier of the cluster that is
the closest to the given input.

## Input and Output Values

Functions of this type have a single input called `embedding` which is a row. The columns that
are expected in this row are the same as the columns that were provided in the training procedure.

These functions have a single output value called `cluster`, which is the numerical identifier of the cluster
which is closest to the input according to the multivariate normal distribution density function. Clusters
are numbered `0` to `N-1` where `N` is the number of clusters.

# See also

* ![](%%doclink gaussianclustering.train procedure) trains a gaussian clustering function.

