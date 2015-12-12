var procConfig = {
    id: 'caffe',
    type: 'neural.caffe.train',
    params: {
        solverUrl: 'file://ext/caffe/examples/mnist/lenet_solver.prototxt'
    }
};

var proc = mldb.createProcedure(procConfig);

mldb.log(proc);

var run = proc.run({});

mldb.log(run);

"success"

