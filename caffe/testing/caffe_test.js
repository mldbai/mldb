var procConfig = {
    id: 'caffe',
    type: 'neural.caffe.train',
    params: {
        networkUrl: 'file://ext/caffe/examples/mnist/lenet_train_test.prototxt'
    }
};

var proc = mldb.createProcedure(procConfig);

mldb.log(proc);

var run = proc.run({});

mldb.log(run);

"success"

