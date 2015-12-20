var fnConfig = {
    id: 'inception',
    type: 'tensorflow.graph',
    params: {
        modelFileUrl: 'archive+file://inception_dec_2015.zip#tensorflow_inception_graph.pb',
    }
};

var fn = mldb.createFunction(fnConfig);

var labelsStream = mldb.openStream('archive+file://inception_dec_2015.zip#imagenet_comp_graph_label_strings.txt');

var labels = [];

while (!labelsStream.eof()) {
    try {
        labels.push(labelsStream.readLine());
    } catch (e) {
        break;
    }
}

//mldb.log('labels are', labels);


//mldb.log("details = ", fn.details());

//var filename = "https://avatars0.githubusercontent.com/u/112556?v=3&s=460";
var filename = "ext/tensorflow/tensorflow/examples/label_image/data/grace_hopper.jpg";
var filename = "https://upload.wikimedia.org/wikipedia/commons/thumb/6/68/The_Wrestlers_by_William_Etty_YORAG_89.JPG/1920px-The_Wrestlers_by_William_Etty_YORAG_89.JPG";
var filename = "https://upload.wikimedia.org/wikipedia/commons/6/6f/Soyuz_TMA-19M_spacecraft_approaches_the_ISS.jpg";

var blob = mldb.openStream(filename).blob();

var result = fn.call({ jpeg: blob});

mldb.log("result is ", result);

var vals = result.values.output[0];
var maxVal = 0;
var maxIdx = 0;

for (var i = 0;  i < vals.length;  ++i) {
    if (vals[i] > maxVal) {
        maxVal = vals[i];
        maxIdx = i;
    }
}

mldb.log("max val is", maxVal, "at index", maxIdx, labels[maxIdx]);

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

var fn = mldb.createFunction(preprocessConfig);

var query = mldb.sql("select @incept({ image: @preprocess({image: load_image('image.png')})})");



"success"
