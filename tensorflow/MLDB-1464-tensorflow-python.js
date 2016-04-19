var scriptBase = 'file://mldb/ext/tensorflow/';
var scriptUri = scriptBase + 'tensorflow/examples/tutorials/mnist/fully_connected_feed.py';
//var scriptUri = scriptBase + 'tensorflow/examples/tutorials/mnist/__init__.py';

var config = {
    address: scriptUri
    //source: "print 'hello'"
};

var resp = mldb.post("/v1/types/plugins/python/routes/run", config);

mldb.log(resp);

