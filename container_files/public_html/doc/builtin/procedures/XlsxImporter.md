# Xlsx importer (experimental)

This procedure will import all worksheets from a `.xlsx` (not `.xls`)
workbook into an MLDB dataset.

## Configuration

![](%%config procedure experimental.import.xlsx)


## Output

Each cell will be converted to its normal data type.  The row names are in the
format `Sheet:cell`, for example `Sheet1:01` for the first row.  The column
names are the Excel column IDs, for example `A` for the first column and
`AA` for the 26th column.

