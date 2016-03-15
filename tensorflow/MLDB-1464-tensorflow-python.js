var scriptBase = 'file://mldb/ext/tensorflow/';
var scriptUri = scriptBase + 'tensorflow/examples/tutorials/mnist/fully_connected_feed.py';

var config = {
    address: scriptUri
};

var resp = mldb.post("/v1/types/plugins/python/routes/run", config);

mldb.log(resp);

