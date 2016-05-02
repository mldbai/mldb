# Nearest Neighbors Function

The Nearest Neighbors type creates a function that returns the nearest
neighbors of a known row or an arbitrary point 
in an existing ![](%%doclink embedding dataset).

## Configuration

![](%%config function embedding.neighbors)

## Input and Output Values

Functions of this type have the following input values:

* `coords`: name of row for which to find neighbors, or embedding representing the point in space for which to find neighbors
* `num_neighbours`: optional integer overriding the function's default value if specified 
* `max_distance`: optional double overriding the function's default value if specified

Functions of this type have a single output value `neighbors`
which is a row with `keys=rownames` and `values=distances`.

## See also

* ![](%%doclink embedding dataset)

