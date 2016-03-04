# t-SNE Training Procedure

The t-SNE procedure is used to visualize complex datasets in a map.  It does
a good job at representing the structure of high dimensional data. This procedure
trains a t-SNE model and stores the model file to disk and/or applies the model to 
the input data to produce an embedded output dataset.

## Configuration

![](%%config procedure tsne.train)

The t-SNE procedure takes a high dimensional embedding as an input (often
created from the application of the [SVD Procedure](Svd.md) and creates a low-dimensional
embedding of the same data points (typically, there are two or three output
dimensions).  The `input` parameter points to a read-only dataset that is queried
by the t-SNE training, and the `output` parameter describes a dataset to which
the coordinates are written.

The input dataset can be filtered and modified with select and where statements.
The where statement, in particular, may be necessary to limit the number of
rows that are used and therefore limit the run-time of the algorithm.  The algorithm
used is [Barnes-Hut SNE] (http://lvdmaaten.github.io/publications/papers/JMLR_2014.pdf),
which can produce maps of up to 100,000 points or so in a reasonable run-time.

The `perplexity` parameter requires further explanation.  It controls how many
neighbours each data point will try to have.  Modifying the value of the parameter
will affect the "clumpiness" of the data; for visualizing data it's pretty
reasonable to hand-tune this parameter until a pleasing clustering is obtained.


## Examples

* The ![](%%nblink _demos/Mapping Reddit) demo notebook
* The ![](%%nblink _demos/Visualizing StackOverflow Tags) demo notebook


## See also

[//]: # (UNCOMMENT WHEN READY * the ![](%%doclink tsne.embedRow function) re-applies a t-SNE model to a new point.)
* [Wikipedia t-SNE Article](http://en.wikipedia.org/wiki/T-distributed_stochastic_neighbor_embedding)
* the ![](%%doclink svd.train procedure) trains an SVD, often used as the input to t-SNE.


