# This file is part of MLDB. Copyright 2015 Datacratic. All rights reserved.

import json
from urllib import urlopen

######### Trivial regression test

ds1 = mldb.create_dataset({
        'type': 'sparse.mutable',
        'id': 'dataset1'})

#create the dataset
ds1.record_row('1', [['label', 39, 0], ['x', 0.2, 0], ['y', -0.3, 0]])
ds1.record_row('2', [['label', 39, 0], ['x', 0.6, 0], ['y', -0.7, 0]])
ds1.record_row('3', [['label', 39, 0], ['x', 0.9, 0], ['y', -0.4, 0]])
ds1.record_row('4', [['label', 72, 0], ['x', -0.2, 0], ['y', 0.9, 0]])
ds1.record_row('5', [['label', 72, 0], ['x', -0.45, 0], ['y', 0.5, 0]])
ds1.record_row('6', [['label', 72, 0], ['x', -0.56, 0], ['y', 0.2, 0]])

ds1.commit()

#create the procedure
result = mldb.perform("PUT", "/v1/procedures/svm_classif", [], {
        "type": "svm.train",
        "params": {
            "trainingDataset": { "id": "dataset1" },
            "modelFileUrl": "file://tmp/MLDB-991.svm"            
        }
    })
response = json.loads(result['response'])
mldb.log(response)
assert result['statusCode'] == 201

#run the procedure
result = mldb.perform('POST', '/v1/procedures/svm_classif/runs')
response = json.loads(result['response'])
mldb.log(response)
assert 300 > result['statusCode'] >= 200

#create the function
result = mldb.perform('PUT', '/v1/functions/svm_function', [], {
        'type': 'svm',
        'params': {"modelFileUrl": "file://tmp/MLDB-991.svm"}})

mldb.log(result)
assert result['statusCode'] == 201, 'failed to create the function: %s' % result['response']

#test the function
result = mldb.perform('GET', '/v1/functions/svm_function/application', [['input', { 'embedding' : {'x':1,'y':-1 }}]], {})
mldb.log(result)
assert json.loads(result['response'])['output']['output'] == 39

result = mldb.perform('GET', '/v1/functions/svm_function/application', [['input', { 'embedding' : {'x':-1,'y':1 }}]], {})
mldb.log(result)
assert json.loads(result['response'])['output']['output'] == 72


######### trivial test with a different kernel

test_procedure_config = {
    "type":"svm.train",
    "params":{
        "trainingDataset":{"id":"dataset1"},
        "configuration":{"kernel": 1},
        "modelFileUrl": "file://tmp/MLDB-991-2.svm"
    }
}

result = mldb.perform("PUT", "/v1/procedures/svm_classif2", [], test_procedure_config)
response = json.loads(result['response'])
mldb.log(response)

result = mldb.perform('POST', '/v1/procedures/svm_classif2/runs')
response = json.loads(result['response'])
mldb.log(response)
assert 300 > result['statusCode'] >= 200

result = mldb.perform('PUT', '/v1/functions/svm_function2', [], {
        'type': 'svm',
        'params': {"modelFileUrl": "file://tmp/MLDB-991-2.svm"}})

mldb.log(result)
assert result['statusCode'] == 201, 'failed to create the function: %s' % result['response']

result = mldb.perform('GET', '/v1/functions/svm_function2/application', [['input', { 'embedding' : {'x':1,'y':-1 }}]], {})
mldb.log(result)
assert json.loads(result['response'])['output']['output'] == 39

result = mldb.perform('GET', '/v1/functions/svm_function2/application', [['input', { 'embedding' : {'x':-1,'y':1 }}]], {})
mldb.log(result)
assert json.loads(result['response'])['output']['output'] == 72

############# Iris dataset classification test

irisdataset = mldb.create_dataset({ "type": "sparse.mutable", "id": "iris_dataset" })

for i, line in enumerate(open("./mldb/testing/dataset/iris.data")):

    cols = []
    line_split = line.split(',')
    if len(line_split) != 5:
        continue
    cols.append(["sepal length", float(line_split[0]), 0])
    cols.append(["sepal width", float(line_split[1]), 0])
    cols.append(["petal length", float(line_split[2]), 0])
    cols.append(["petal width", float(line_split[3]), 0])
    cols.append(["label", hash(line_split[4]) % 1000, 0])
    irisdataset.record_row(str(i+1), cols)

irisdataset.commit()

result = mldb.perform("GET", "/v1/datasets/iris_dataset/query", [
    ["select", "*" ], ["format", "table"], 
    ["rowNames", "true"], ["headers", "true"]
], {})

mldb.log(result)

result = mldb.perform("PUT", "/v1/procedures/svm_iris", [], {
        "type": "svm.train",
        "params": {
            "trainingDataset": { "id": "iris_dataset" },
            "modelFileUrl": "file://tmp/MLDB-991-iris.svm"
        }
    })
response = json.loads(result['response'])
mldb.log(response)
assert result['statusCode'] == 201

result = mldb.perform('POST', '/v1/procedures/svm_iris/runs')
response = json.loads(result['response'])
mldb.log(response)
assert 300 > result['statusCode'] >= 200

result = mldb.perform('PUT', '/v1/functions/svm_iris_function', [], {
        'type': 'svm',
        'params': {"modelFileUrl": "file://tmp/MLDB-991-iris.svm"}})
mldb.log(result)
assert 300 > result['statusCode'] >= 200

result = mldb.perform("GET", "/v1/datasets/iris_dataset/query", [
    ["select", "label, svm_iris_function({{* excluding (label)} as embedding}) as result" ], ["format", "table"], 
    ["rowNames", "true"], ["headers", "true"]
], {})

mldb.log(result)
assert 300 > result['statusCode'] >= 200

result = mldb.perform("GET", "/v1/datasets/iris_dataset/query", [
    ["select", "count(*) as result" ], ["where", "svm_iris_function({{* excluding (label)} as embedding})[output] != label"], ["format", "table"], 
    ["rowNames", "false"], ["headers", "false"]
], {})

mldb.log(result)
assert 300 > result['statusCode'] >= 200
assert json.loads(result['response'])[0][0] == 2 #cross-regression gived two classification errors over 150 

############ SVM-regression testing  ###########

def mypolynomial( x, y):
    return 0.3*pow(x,2) + 2.4*y - 1.7;

ds3 = mldb.create_dataset({
        'type': 'sparse.mutable',
        'id': 'dataset3'})

row_count = 2;

ds3.record_row('1', [['label', mypolynomial(0,0), 0], ['x', 0, 0], ['y', 0, 0]])
ds3.record_row('2', [['label', mypolynomial(1,0), 0], ['x', 1, 0], ['y', 0, 0]])
ds3.record_row('3', [['label', mypolynomial(2,0), 0], ['x', 2, 0], ['y', 0, 0]])
ds3.record_row('4', [['label', mypolynomial(0,1), 0], ['x', 0, 0], ['y', 1, 0]])
ds3.record_row('5', [['label', mypolynomial(1,1), 0], ['x', 1, 0], ['y', 1, 0]])
ds3.record_row('6', [['label', mypolynomial(2,1), 0], ['x', 2, 0], ['y', 1, 0]])
ds3.record_row('7', [['label', mypolynomial(0,2), 0], ['x', 0, 0], ['y', 2, 0]])
ds3.record_row('8', [['label', mypolynomial(1,2), 0], ['x', 1, 0], ['y', 2, 0]])
ds3.record_row('9', [['label', mypolynomial(2,2), 0], ['x', 2, 0], ['y', 2, 0]])

ds3.commit()

result = mldb.perform("GET", "/v1/datasets/dataset3/query", [
    ["select", "*" ], ["format", "table"], 
    ["rowNames", "true"], ["headers", "true"]
], {})

mldb.log(result)

result = mldb.perform("PUT", "/v1/procedures/svm_regression", [], {
        "type": "svm.train",
        "params": {
            "trainingDataset": { "id": "dataset3" },
            "modelFileUrl": "file://tmp/MLDB-991-regression.svm",
            "svmType":"regression"
        }
    })
response = json.loads(result['response'])
mldb.log(response)
assert result['statusCode'] == 201

result = mldb.perform('POST', '/v1/procedures/svm_regression/runs')
response = json.loads(result['response'])
mldb.log(response)
assert 300 > result['statusCode'] >= 200

result = mldb.perform('PUT', '/v1/functions/svm_regression_function', [], {
        'type': 'svm',
        'params': {"modelFileUrl": "file://tmp/MLDB-991-regression.svm"}})
mldb.log(result)
assert 300 > result['statusCode'] >= 200

result = mldb.perform("GET", "/v1/datasets/dataset3/query", [
    ["select", "label, svm_regression_function({{* excluding (label)} as embedding}) as result" ], ["format", "table"], 
    ["rowNames", "true"], ["headers", "true"]
], {})

mldb.log(result)
assert 300 > result['statusCode'] >= 200

result = mldb.perform("GET", "/v1/datasets/dataset3/query", [
    ["select", "sum(abs(svm_regression_function({{* excluding (label)} as embedding})[output] - label)) as totalError" ], ["format", "table"], 
    ["rowNames", "false"], ["headers", "false"]
], {})

mldb.log(result)
assert 300 > result['statusCode'] >= 200
assert json.loads(result['response'])[0][0] < 5 #less than 5.0 total error 

mldb.script.set_return("success")



