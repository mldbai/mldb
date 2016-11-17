
if False:
    mldb_wrapper = None
mldb = mldb_wrapper.wrap(mldb) # noqa

mldb.log("Loading 3DS");
mldb.put("/v1/procedures/3ds_proc", {
    "type": "import.3ds",
    "params": {
        'dataFileUrl' : 'file://es15.3ds',
        "vertexDataset": {
            "id": "vertices",
        },
        "faceDataset": {
            "id": "faces",
        },
        "runOnCreation": True,
    }
})

mldb.log(mldb.query("SELECT count(*), min(z), max(z) FROM vertices"))
mldb.log(mldb.query("SELECT count(*), min(x), max(x) FROM faces"))

mldb.put('/v1/procedures/export', {
            'type' : 'export.csv',
            'params' : {
                'exportData' : 'select * from vertices',
                'dataFileUrl' : 'file://' + "pipelinemodel.csv"
            }
        })

mldb.put('/v1/procedures/export', {
            'type' : 'export.csv',
            'params' : {
                'exportData' : 'select * from faces',
                'dataFileUrl' : 'file://' + "pipelinefaces.csv"
            }
        })

mldb.script.set_return("success")