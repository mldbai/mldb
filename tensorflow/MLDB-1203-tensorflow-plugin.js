var inceptionUrl = 'file://inception_dec_2015.zip';

var fetcherConfig = {
    id: 'fetch',
    type: 'fetcher',
    params: {
    }
};

var fetcher = mldb.createFunction(fetcherConfig);

var labelsConfig = {
    id: 'imagenetLabels',
    type: 'text.csv.tabular',
    params: {
        dataFileUrl: 'archive+' + inceptionUrl + '#imagenet_comp_graph_label_strings.txt',
        headers: ['label']
    }
};

var lbls = mldb.createDataset(labelsConfig);

var lookupLabelsConfig = {
    id: 'lookupLabels',
    type: 'sql.query',
    params: {
        query: 'SELECT il.label AS column, scores.value AS value '
            +  'FROM ROW TABLE ($scores) AS scores '
            +  'JOIN imagenetLabels AS il '
            +  'ON scores.rowName() = \'[0,\' + (CAST (il.rowName() AS INTEGER) - 1) + \']\' '
            +  'ORDER BY scores.value DESC '
            +  'LIMIT 5',
        output: 'NAMED_COLUMNS'
    }
};

var lookuplbls = mldb.createFunction(lookupLabelsConfig);

var fnConfig = {
    id: 'incept',
    type: 'tensorflow.graph',
    params: {
        modelFileUrl: 'archive+' + inceptionUrl + '#tensorflow_inception_graph.pb',
        inputs: 'fetch({url})[content] AS "DecodeJpeg/contents"',
        outputs: "lookupLabels({scores: softmax}) AS *"
    }
};

var fn = mldb.createFunction(fnConfig);

var classifyConfig = {
    id: 'incept',
    type: 'sql.expression',
    params: {
        expression: 'SELECT label, score FROM '
    }
};

mldb.log(mldb.get('/v1/functions'));

//mldb.log('labels are', labels);


//mldb.log("details = ", fn.details());

//var filename = "https://avatars0.githubusercontent.com/u/112556?v=3&s=460";
var filename = "ext/tensorflow/tensorflow/examples/label_image/data/grace_hopper.jpg";
var filename = "https://upload.wikimedia.org/wikipedia/commons/thumb/6/68/The_Wrestlers_by_William_Etty_YORAG_89.JPG/1920px-The_Wrestlers_by_William_Etty_YORAG_89.JPG";
var filename = "https://upload.wikimedia.org/wikipedia/commons/6/6f/Soyuz_TMA-19M_spacecraft_approaches_the_ISS.jpg";
var filename = "https://upload.wikimedia.org/wikipedia/commons/1/18/Cardiff_City_Hall_cropped.jpg";
var filename = "https://upload.wikimedia.org/wikipedia/commons/thumb/6/66/Maureen_O%27Hara_1947_2.jpg/198px-Maureen_O%27Hara_1947_2.jpg";
var filename = "https://upload.wikimedia.org/wikipedia/commons/thumb/5/58/Calle_E_Monroe_St%2C_Chicago%2C_Illinois%2C_Estados_Unidos%2C_2012-10-20%2C_DD_04.jpg/560px-Calle_E_Monroe_St%2C_Chicago%2C_Illinois%2C_Estados_Unidos%2C_2012-10-20%2C_DD_04.jpg";

mldb.log("classifying", filename);

var res = mldb.query('SELECT incept({url: ' + mldb.sqlEscape(filename) + '}) AS *');

mldb.log(res);

mldb.log(res);

if (false) {

    var blob = mldb.openStream(filename).readBlob();

    var result = fn.call({ jpeg: blob});


    //mldb.log("result is ", result);

    var vals = result.values.output[0];
    var maxVal = 0;
    var maxIdx = 0;

    mldb.log("categories for image", filename);

    for (var i = 0;  i < vals.length;  ++i) {
        if (vals[i] > 0.01) {
            mldb.log(labels[i], vals[i]);
        }
        if (vals[i] > maxVal) {
            maxVal = vals[i];
            maxIdx = i;
        }
    }

    mldb.log("max val is", maxVal, "at index", maxIdx, labels[maxIdx]);
}

//mldb.log("details = ", fn.details());

var preprocessConfig = {
    id: 'preprocess',
    type: 'sql.expression',
    params: {
    }
};        
        
var preprocessConfig = {
    id: 'preprocess',
    type: 'tensorflow.graph',
    params: {
        expr: "(tf.ResizeBilinear "
            + "  (CAST "
            + "    (CASE WHEN regex_match($filename, '\.png') "
            + "       THEN tf.DecodePng(tf.ReadFile($filename)) "
            + "       ELSE tf.DecodeJpeg(tf.ReadFile($filename)) "
            + "     DONE) AS TENSOR OF FLOAT, "
            + "   299, 299)"
            + "- 128.0) / 128.0"
/*        
        graph: [
            {
                name: 'read',
                op: 'ReadFile',
                input: [ "filename.jpg" ]
            },
            {
                name: 'decode',
                op: 'DecodePng'
                input: [ "filename.jpg" ]
            },
            {
                name: 'size',
                op: 'ResizeBilinear'
            },
            {
                name: 'cast'
                op: 'Cast'
            },
            {
                name: 'sub',
                op: 'Sub'
            },
            {
                name: 'div',
                op: 'Div'
            }
        ]
*/
    }
};

//var fn = mldb.createFunction(preprocessConfig);

//var query = mldb.sql("select @incept({ image: @preprocess({image: load_image('image.png')})})");



"success"
