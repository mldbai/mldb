# Dataset Stats Procedure

This procedure generates general statistics about a dataset. The output is a
dataset with a single row named result and the following columns:
* dataPointsCount: The count of datapoints.
* distinctTimestampsCount: The count of distinct timestamps.
* earliestTimestamp: The earliest timestamp.
* latestTimestamp: The latest timestamp.
* rowsCount: The count of distinct subjects.
* cellsCount: The count of non empty cells. (The sum of the counts of columns per row.)
* columnsCount : The count of columns.

## Configuration

![](%%config procedure dataset.stats)
