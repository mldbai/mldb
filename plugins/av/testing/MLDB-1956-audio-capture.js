var config = {
    type: 'av.capture.audio',
    params: {
    }
};

var sensor = mldb.createSensor(config);

mldb.log("got sensor");
mldb.log(sensor.status());

mldb.log(sensor.latest());

"success"

