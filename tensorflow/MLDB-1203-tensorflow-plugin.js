// The URI from which we load the Inception model.
// Taken from:
// https://storage.googleapis.com/download.tensorflow.org/models/inception_dec_2015.zip
// Hosted on public.mldb.ai for caching.
//var inceptionUrl = 'file://inception_dec_2015.zip';  // Use this to test with a local copy
var inceptionUrl = 'https://public.mldb.ai/models/inception_dec_2015.zip';
var imgUrl = "http://public.mldb.ai/testing/tensorflow/Calle_E_Monroe_St,_Chicago,_Illinois,_Estados_Unidos,_2012-10-20,_DD_04.jpg";
var imgUrlSql = mldb.sqlEscape(imgUrl);

// GPU testing
var devices = [ "/cpu:.*" ];
if (args && args.GPUS && args.GPUS >= 1) {
    devices.push("/gpu:.*");
}
mldb.log("Devices: " + devices);

var res = mldb.query("SELECT tf_Cos(1.23, {T: { type: 'DT_DOUBLE'}}) AS res");
mldb.log(res);

var res = mldb.query("SELECT tf_MatrixInverse([[1,2,3],[4,-5,6],[-7,8,9]], { T: { type: 'DT_DOUBLE' } }) AS res");
mldb.log(res);

// This sets up a fetcher function, which will download a given URL
// and return it as a blob.
var fetcherConfig = {
    id: 'fetch',
    type: 'fetcher',
    params: {
    }
};
var fetcher = mldb.createFunction(fetcherConfig);
mldb.query("SELECT tf_DecodeJpeg(fetch({url: "
           + imgUrlSql
           + "})[content], {ratio: 8, try_recover_truncated: {bool: 0}}) as px");

// The labels for the Inception classifier live within the zip file
// downloaded above.  We read them into a dataset so that we can
// join against them later on.
var loadLabelsConfig = {
    type: 'import.text',
    params: {
        dataFileUrl: 'archive+' + inceptionUrl + '#imagenet_comp_graph_label_strings.txt',
        headers: ['label'],
        outputDataset: {
            id: "imagenetLabels",
        },
        runOnCreation: true
    }
};

var lbls = mldb.post('/v1/procedures', loadLabelsConfig);

// This function takes the output of an inception graph, which is a
// 1x1008 matrix, and joins the top 5 scores against the image labels,
// producing a result set that contains an ordered set of category
// labels.  The line numbers of the dataset start at 1, so we need to
// subtract one to join with the label names.
var lookupLabelsConfig = {
    id: 'lookupLabels',
    type: 'sql.query',
    params: {
        query: 'SELECT il.label AS column, scores.value AS value '
            +  'FROM row_dataset($scores) AS scores '
            +  'JOIN imagenetLabels AS il '
            +  'ON CAST(scores.column AS INTEGER) = (CAST (il.rowName() AS INTEGER) - 1) '
            +  'ORDER BY scores.value DESC '
            +  'LIMIT 5',
        output: 'NAMED_COLUMNS'
    }
};

var lookuplbls = mldb.createFunction(lookupLabelsConfig);

// Finally, we create the main function.  This is passed in a URL to
// classify as the url argument, and will download the image, process
// it through the inception net, and return the top 5 categories with
// their weights as output.
//
// The image itself is fed into the DecodeJpeg/contents node, and the
// output is read from softmax node of the graph.
//
// Depending on the GPUS argument, this is done first on the CPUs, then on the GPUs
for (device of devices) {
    mldb.log("Running on device: " + device);
    var fnConfig = {
        id: 'incept',
        type: 'tensorflow.graph',
        params: {
            modelFileUrl: 'archive+' + inceptionUrl + '#tensorflow_inception_graph.pb',
            inputs: 'fetch({url})[content] AS "DecodeJpeg/contents"',
            outputs: "lookupLabels({scores: flatten(softmax)}) AS *",
            devices: device
        }
    };
    var fn = mldb.createFunction(fnConfig);
    //mldb.log(mldb.get('/v1/functions/incept/details'));
    
    var constant = mldb.query("select tf_extract_constant('incept', ['mixed','conv', 'batchnorm', 'beta'])");
    mldb.log(constant);
    
    constant = mldb.query("select tf_extract_constant('incept', parse_path('mixed.conv.batchnorm.gamma'))");
    mldb.log(constant);
    
    
    mldb.log("classifying", imgUrl);
    var res = mldb.query('SELECT incept({url: ' + imgUrlSql + '})[output] AS *');
    mldb.log(res);
    
    mldb.put('/v1/functions/inception', {
        "type": 'tensorflow.graph',
        "params": {
            "modelFileUrl": 'archive+' + inceptionUrl + '#tensorflow_inception_graph.pb',
            "inputs": 'fetch({url})[content] AS "DecodeJpeg/contents"',
            "outputs": "softmax"
        }
    });
    
    mldb.log(mldb.query("SELECT inception({url: " + imgUrlSql + "}) as *"));
    mldb.log(mldb.query("SELECT flatten(inception({url: " + imgUrlSql + "})[softmax]) as *"));
}

"success"
