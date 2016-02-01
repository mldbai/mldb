# This file is part of MLDB. Copyright 2016 Datacratic. All rights reserved.

utf8LiteralConfig = {
    "type": "transform",
    "params": {
        "inputData": "SELECT 'françois'",
        "outputDataset": "output"
    }
}

rez = mldb.perform('PUT', '/v1/procedures/transform', [], utf8LiteralConfig)
assert rez['statusCode'] == 201, 'expected the transform to be created with utf-8 literal'

mldb.script.set_return("success")

