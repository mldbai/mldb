mldb.requirePlugin('sensors');

var config = {
    type: 'mldb.memory',
    id: 'memory',
    params: {
    }
};

var sensor = mldb.createSensor(config);

mldb.log("got sensor");
mldb.log(sensor.status());

mldb.log(mldb.query("select read_sensor('memory') / 1024 / 1024 as *"));
mldb.log(mldb.query("select read_sensor('memory') / 1024 / 1024 as *"));

"success"
