
var resp = mldb.put('/v1/procedures/import', {type: 'mongodb.import', params: { runOnCreation: true} });

mldb.log(resp);

"success"
