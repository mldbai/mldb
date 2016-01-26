# This file is part of MLDB. Copyright 2015 Datacratic. All rights reserved.

import json
from urllib import urlopen


dataset = mldb.create_dataset({  
    "id": "iris",
    "type": "text.csv.tabular",
    "params": {
        "dataFileUrl": "file://mldb/testing/dataset/iris.data",
        "headers": [ "a", "b", "c", "d", "class" ]
    }
})

result = mldb.perform("PUT", "/v1/functions/feats", [], {
    'type' : 'sql.expression',
    'params' : {
        "expression": "{a,b,c,d} as row"
    }
})
assert result["statusCode"] < 400, result["response"]

result = mldb.perform("PUT", "/v1/procedures/create_train_set", [], {
    'type' : 'transform',
    'params' : {
        "inputData": "select feats({*}) as *, class='Iris-setosa' as label from iris",
        "outputDataset": "train_set",
        "runOnCreation": True
    }
})
assert result["statusCode"] < 400, result["response"]

result = mldb.perform("PUT", "/v1/procedures/train", [], {
    'type' : 'classifier.train',
    'params' : {
        'trainingData' : """
            select 
                {* EXCLUDING(label)} as features,
                label
            from train_set
        """,
        "modelFileUrl": "file://tmp/MLDB-1273.cls",
        "configuration": {
            "dt": {
                "type": "decision_tree",
                "max_depth": 8,
                "verbosity": 3,
                "update_alg": "prob"
            },
        },
        "algorithm": "dt",        
        "functionName": "cls",
        "runOnCreation": True
    }
})
assert result["statusCode"] < 400, result["response"]

def query(sql):
    result = mldb.perform("GET", "/v1/query", [["q", sql], ["format", "table"]], {})
    assert result["statusCode"] < 400, result["response"]
    return result["response"]

with_flattening =  query("""
                select cls({features: {
                    a as "row.a", b as "row.b", c as "row.c", d as "row.d"
                    }}) as *
                from iris 
                limit 10
                """)

without_flattening =  query("""
                select cls({features: {feats({*}) as *}}) as *
                from iris 
                limit 10
                """)

assert with_flattening == without_flattening, "results do not match"

mldb.script.set_return("success")

