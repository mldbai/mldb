# Text Dataset (Line by Line)

The Text Dataset is used to turn a text file into a dataset.  Each line in the
dataset is exposed as a separate row.  This is a good way to import a text file
where each line is a row, via the ![](%%doclink transform procedure).

The dataset has two columns:

- `lineNumber` contains a number giving the line number of the text file
- `lineText` contains the text of the line, without any trailing newline

## Configuration

![](%%config dataset text.line)

## Examples

The following configuration function will create a dataset with one line per entry
in Google's robots.txt file:

```
var dataset_config = {
        type: 'text.line',
        id: 'google_robots.txt',
        params: {
            dataFileUrl: 'https://www.google.com/robots.txt'
        }
    };
```

## See also

* The ![](%%doclink transform procedure) can be used to process a dataset