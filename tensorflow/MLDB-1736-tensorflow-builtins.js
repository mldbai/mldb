// This file is part of MLDB. Copyright 2016 mldb.ai inc. All rights reserved.

function assertEqual(expr, val)
{
    if (expr == val)
        return;
    if (JSON.stringify(expr) == JSON.stringify(val))
        return;

    mldb.log(expr, 'IS NOT EQUAL TO', val);

    throw "Assertion failure";
}


var res1 = mldb.query("SELECT tf_Cos(1.23, {T: { type: 'DT_DOUBLE'}}) AS res");
var res2 = mldb.query("SELECT cos(1.23) AS res");

mldb.log(res1[0].columns[0]);
mldb.log(res2[0].columns[0]);

assertEqual(res1[0].columns[0][1],
            res2[0].columns[0][1]);

var res = mldb.query("SELECT tf_MatrixInverse([[1,2],[3, 4]], { T: { type: 'DT_DOUBLE' } }) AS res");

mldb.log(res);

var expected = [
   {
      "columns" : [
         [ "res.0.0", -1.9999999999999998, "NaD" ],
         [ "res.0.1", 1.0, "NaD" ],
         [ "res.1.0", 1.4999999999999998, "NaD" ],
         [ "res.1.1", -0.49999999999999994, "NaD" ]
      ],
      "rowName" : "result"
   }
];

assertEqual(res, expected);

var ops = mldb.get("/v1/plugins/tensorflow/routes/ops");

mldb.log(ops);

var op = mldb.get("/v1/plugins/tensorflow/routes/ops/EncodePng");

mldb.log(op);

var png = mldb.query("SELECT tf_EncodePng([[[0,0,0,0]]], {T: { type: 'DT_UINT8'}}) AS png NAMED 'png'");
mldb.log(png);

var png = mldb.query("SELECT tf_EncodePng([[[1,1,1,0]]], {}) AS png NAMED 'png'");
mldb.log(png);

var png = mldb.query("SELECT tf_EncodePng([[[1,1,1,0]]]) AS png NAMED 'png'");
mldb.log(png);

var conv_output = mldb.query("SELECT tf_Conv2D({input: [[[[1], [1], [1]], [[1], [1], [1]], [[1], [1], [1]]]], filter: [[[[1]], [[2]]], [[[2]], [[1]]]]}, {T: { type: 'DT_FLOAT'}, padding: 'SAME', strides: [ 1, 1, 1, 1] }) AS res")
mldb.log(conv_output)

"success"
