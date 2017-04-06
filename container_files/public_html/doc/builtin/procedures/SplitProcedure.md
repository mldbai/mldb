# Dataset Split Procedure

This procedure splits a dataset into several while respecting the distribution 
of the presence of one or several labels.

This can be used, for example, to split a dataset with multiple labels
between training, testing and validation sets.

## Algorithm

The procedure uses a greedy algorithm and is not guaranteed to return the optimal solution.
The algorithm will try to assure than every label is represented in all splits.
If a label is represented in fewer rows than there are splits, the rows will be placed in the 
firsts splits.

## Configuration

![](%%config procedure split.train)
