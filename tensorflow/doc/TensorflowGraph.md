# Tensorflow Graph Function (Experimental)

The Tensorflow Graph function creates a function which, when called,
will apply a Tensorflow graph to the given inputs.

## Configuration

![](%%config function tensorflow.graph)

## Example

The following function configuration will load the Tensorflow "inception"
model (which is made publicly available by Google) to predict the top 5
classes of an arbitrary image passed in as a URL.

```python
# The URI from which we load the Inception model.  We're assuming
# that you've already downloaded it from Google at the following
# URL; you can also load it directly from Google if you have
# access to a lot of bandwidth by replacing the file:// URL below
# with the following:
# https://storage.googleapis.com/download.tensorflow.org/models/inception_dec_2015.zip

inceptionUrl = 'file://inception_dec_2015.zip'

# This sets up a fetcher function, which will download a given URL
# and return it as a blob.
mldb.put('/v1/functions/fetch', {
    "id": 'fetch',
    "type": 'fetcher',
    "params": {
    }
})


# The labels for the Inception classifier live within the zip file
# downloaded above.  We read them into a dataset so that we can
# join against them later on and turn category numbers into category
# names.
mldb.put('/v1/procedures/imagenetLabels', {
    "type": 'import.text',
    "params": {
        "dataFileUrl": 'archive+' + inceptionUrl + '#imagenet_comp_graph_label_strings.txt',
        "headers": ['label'],
        "outputDataset": "imagenetLabels",
        "runOnCreation": True
    }
})

# This function takes the output of an inception graph, which is a
# 1x1008 matrix, and joins the top 5 scores against the image labels,
# producing a result set that contains an ordered set of category
# labels.  The line numbers of the dataset start at 1, so we need to
# subtract one to join with the label names.
mldb.put('/v1/functions/lookupLabels', {
    "type": 'sql.query',
    "params": {
        "query": """
            SELECT il.label AS column, scores.value AS value 
            FROM row_dataset($scores) AS scores 
            JOIN imagenetLabels AS il 
            ON CAST(scores.column AS INTEGER) = (CAST (il.rowName() AS INTEGER) - 1) 
            ORDER BY scores.value DESC 
            LIMIT 5
        """,
        "output": 'NAMED_COLUMNS'
    }
})

# Finally, we create the main function.  This is passed in a URL to
# classify as the url argument, and will download the image, process
# it through the inception net, and return the top 5 categories with
# their weights as output.
#
# The image itself is fed into the DecodeJpeg/contents node, and the
# output is read from softmax node of the graph.
mldb.put('/v1/functions/imageEmbedding', {
    "type": 'tensorflow.graph',
    "params": {
        "modelFileUrl": 'archive+' + inceptionUrl + '#tensorflow_inception_graph.pb',
        "inputs": 'fetch({url})[content] AS "DecodeJpeg/contents"',
        "outputs": "lookupLabels({scores: flatten(softmax)}) AS *"
    }
})

filename = "https://upload.wikimedia.org/wikipedia/commons/thumb/5/58/Calle_E_Monroe_St%2C_Chicago%2C_Illinois%2C_Estados_Unidos%2C_2012-10-20%2C_DD_04.jpg/560px-Calle_E_Monroe_St%2C_Chicago%2C_Illinois%2C_Estados_Unidos%2C_2012-10-20%2C_DD_04.jpg"

mldb.log("classifying " + filename)

res = mldb.query('SELECT incept({url: ' + mldb.sqlEscape(filename) + '})[output] AS *')

mldb.log(res)
```

This example will classify the following image of Chicago's riverfront and skyline:

<img src="https://upload.wikimedia.org/wikipedia/commons/thumb/5/58/Calle_E_Monroe_St%2C_Chicago%2C_Illinois%2C_Estados_Unidos%2C_2012-10-20%2C_DD_04.jpg/560px-Calle_E_Monroe_St%2C_Chicago%2C_Illinois%2C_Estados_Unidos%2C_2012-10-20%2C_DD_04.jpg" width=120></img>

and return these results:

```
[
   {
      "columns" : [
         [ "pier", 0.2122125625610352, "2015-12-03T18:23:04Z" ],
         [ "breakwater", 0.07164951413869858, "2015-12-03T18:23:04Z" ],
         [ "lakeside", 0.06921710819005966, "2015-12-03T18:23:04Z" ],
         [ "monitor", 0.06208378449082375, "2015-12-03T18:23:04Z" ],
         [ "airship", 0.05300721898674965, "2015-12-03T18:23:04Z" ]
      ]
   }
]
```


## See also

* TensorFlow's [Image Recognition tutorial](https://www.tensorflow.org/versions/r0.7/tutorials/image_recognition/index.html)
