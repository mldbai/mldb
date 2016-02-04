# Gaussian Clustering Function

The gaussian clustering function type takes the output dataset of the gaussian clustering procedure and applies
it to new data in order to assign it to clusters.

## Configuration

![](%%config function em)

Functions of this type load their internal state from a dataset, which is identified
with the `centroids` parameter, which lists one cluster per row, with the columns 
providing the coordinates of the centroid of the cluster as well as dense covariance matrix,
and the row name being the name of the cluster (this is the output format of the ![](%%doclink em.train procedure)).  

The `select` parameter
tells the system how to extract an embedding from the row, and the `where`
parameter allows only a subset of the clusters to be loaded by the gaussian clustering 
function.

In the application of the function, the same features as in the `centroids`
dataset are extracted from the inputs of the function to create a coordinate
vector for the input.  The distance from that input to each of the centroids
is then calculated using the multivariate normal distribution density function, and the 
function outputs the value `cluster` containing the name of cluster that is
the closest to the given centroid.

## Input and Output Values

Functions of this type have a single input called `embedding` which is a row. The columns that
are expected in this row are the same as the columns in the `centroids` dataset with
which this function is configured.

These functions have a single output value called `cluster`, which is the name of the row
in the `centroids` dataset whose columns describe the point which is closest to the 
input according to the multivariate normal distribution density function.

# See also

* ![](%%doclink em.train procedure) trains a gaussian clustering function.

