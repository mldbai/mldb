
var resp = mldb.put('/v1/procedures/import', {type: 'mongodb.import'});
if (resp["json"]["error"] != "failed to create the initial run") {
    throw "Failed to load plugin";
}
mldb.log(resp);

"success"
