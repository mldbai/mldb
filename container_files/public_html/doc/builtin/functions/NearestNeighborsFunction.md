# Nearest Neighbors Function

The `embedding.neighbors` function type returns informatin about the nearest
neighbor rows in an existing ![](%%doclink embedding dataset) to an arbitrary point.

## Configuration

![](%%config function embedding.neighbors)

## Input and Output Values

Functions of this type have the following input values:

* `coords`: name of row for which to find neighbors, or embedding representing the point in space for which to find neighbors
* `num_neighbours`: optional integer overriding the function's default value if specified 
* `max_distance`: optional double overriding the function's default value if specified

Functions of this type have the following output values:
* `neighbors`: an embedding of the rowPaths of the nearest neighbors in order of proximity
* `distances`: a row of rowName to distance for the nearest neighbors

## See also

* ![](%%doclink embedding dataset)

