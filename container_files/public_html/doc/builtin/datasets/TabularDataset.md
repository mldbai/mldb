# Tabular Dataset

The Tabular Dataset is used to represent dense datasets with more rows than columns. It is ideal for storing text files such as Comma-Separated Values (CSV) files.

## Configuration

Datasets of this type do not take any parameters and are created via the ![](%%doclink import.text procedure) or the ![](%%doclink transform procedure) using this as the `outputDataset` field value:

```json
{ 
  "id": "yourdatasetname", 
  "type": "tabular" 
}
```
