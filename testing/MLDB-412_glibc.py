# This file is part of MLDB. Copyright 2015 Datacratic. All rights reserved.


dataset = mldb.create_dataset({
        "type": "sparse.mutable",
        "id": "x"
    })

dataset.record_row("a", [["b",1,1]])
dataset.record_row("b", [["b",1,1]])
dataset.record_row("c", [["b",1,1]])
dataset.record_row("d", [["b",1,1]])
dataset.record_row("e", [["b",1,1]])
dataset.record_row("f", [["b",1,1]])
dataset.record_row("g", [["b",1,1]])
dataset.commit()



mldb.perform("GET", "/v1/datasets/x/query", 
    [['select', 'max("b")'], ['groupBy', '1']], {})
mldb.script.set_return("success")
