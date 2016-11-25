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

var loggerConfig = {
    type: 'logger',
    id: 'logger',
    params: {
        select: "read_sensor('memory') / 1024 / 1024 as mem",
        interval: "100us",
        output: {
            type: "dataset",
            params: {
                dataset: "historical_memory"
            }
        }
    }
};

var resp = mldb.post("/v1/sensors", loggerConfig);

mldb.log(resp);

mldb.log(mldb.query("select read_sensor('logger') as *"));

// Wait for some data to accumulate
mldb.sleep(10);

mldb.post("/v1/datasets/historical_memory/commit", {});


mldb.log(mldb.query("select * from historical_memory order by rowPath() limit 10"));


"success"
