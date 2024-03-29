# This file is part of MLDB. Copyright 2015 mldb.ai inc. All rights reserved.

import json
from mldb import mldb

result = mldb.perform("PUT", "/v1/datasets/rcp", [], {
    'type' : 'beh',
    'params' : {
        'dataFileUrl': 'file://mldb/mldb_test_data/rcp.beh'
    }
})
assert result["statusCode"] < 400, result["response"]

mldb.log(result)

out = mldb.perform("GET", "/v1/query", [
    ["q", "select * from rcp order by rowName() limit 20" ], ["format", "sparse"] ], {})

mldb.log(out);

result = mldb.perform("PUT", "/v1/procedures/svd1", [], {
    'type' : 'svd.train',
    'params' : {
        'trainingData': {'from' : {'id':'rcp'}, 'limit': 1000},
        'columnOutputDataset': {'id': 'svd1','type':'embedding'}
    }
})
assert result["statusCode"] < 400, result["response"]

result = mldb.perform("POST", "/v1/procedures/svd1/runs", [], {})
assert result["statusCode"] < 400, result["response"]

result = mldb.perform("PUT", "/v1/procedures/svd2", [], {
    'type' : 'svd.train',
    'params' : {
        'trainingData': {'from' : {'id':'rcp'}, 'limit': 1000},
        'columnOutputDataset': {'id': 'svd2','type':'embedding'}
    }
})
assert result["statusCode"] < 400, result["response"]

result = mldb.perform("POST", "/v1/procedures/svd2/runs", [], {})
assert result["statusCode"] < 400, result["response"]


result = mldb.perform("GET", "/v1/query", [
    ["q", "select * from svd1 order by rowName() limit 50" ], ["format", "table"] ], {})
assert result["statusCode"] < 400, result["response"]
svd1 = json.loads(result["response"])

result = mldb.perform("GET", "/v1/query", [
    ["q", "select * from svd2 order by rowName() limit 50" ], ["format", "table"] ], {})
assert result["statusCode"] < 400, result["response"]
svd2 = json.loads(result["response"])

for row1, row2 in zip(svd1, svd2):
    for value1, value2 in zip(row1, row2):
        assert value1 == value2, "{} {}".format(value1, value2)

request.set_return("success")
