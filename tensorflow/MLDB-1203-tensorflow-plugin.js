var fnConfig = {
    id: 'inception',
    type: 'tensorflow.graph',
    params: {
        modelFileUrl: 'archive+file://inception_dec_2015.zip#tensorflow_inception_graph.pb',
    }
};

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
    }
*/
};

var fn = mldb.createFunction(preprocessConfig);

var query = mldb.sql("select @incept({ image: @preprocess({image: load_image('image.png')})})");



"success"
