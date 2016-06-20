# This file is part of MLDB. Copyright 2016 Datacratic. All rights reserved.

from random import random, seed
seed(1234)

mldb = mldb_wrapper.wrap(mldb) # noqa

dataset = mldb.create_dataset({
    'type'    : 'sparse.mutable',
    'id'      : "ds"
})

n=100
d=4
for i in range(n):
    dataset.record_row('row' + str(i),
        [['x' + str(j), random(), 0] for j in range(d)]
        + [['y', random() > .6, 0]]
    )

dataset.commit()


mldb.log(mldb.query('select * from ds limit 10'))

conf = {
    'type': 'sql.expression',
    'params': {
        'expression': 'sp: {x: x0 + x1, y}',
        'prepared': False
    }
}

# some random sql expression
mldb.put('/v1/functions/simple', conf)
conf['params']['prepared'] = True
mldb.put('/v1/functions/simple_prep', conf)

# and some random query using it
a = mldb.query('select simple({*})[sp] as * from ds limit 2')
b = mldb.query('select simple_prep({*})[sp] as * from ds limit 2')
mldb.log(a)
mldb.log(b)
assert a == b


# something more complex

# first train a model
mldb.post('/v1/procedures', {
    'type': 'classifier.train',
    'params': {
        'trainingData': 'select features: {x*}, label: y from ds',
        'algorithm': 'dt',
        "configuration": {
            "dt": {
                "type": "decision_tree",
                "max_depth": 8,
                "verbosity": 3,
                "update_alg": "prob"
            }
        },
        'mode': 'boolean',
        'modelFileUrl': 'file://model.cls.gz',
        'functionName': 'score',
        'runOnCreation': True
    }
})

# then use it's scorer inside a (prepared) sql.expression
conf = {
    'type': 'sql.expression',
    'params': {
        'expression': 'score({features: {x*}, label: y}) as score',
        'prepared': False
    }
}
mldb.put('/v1/functions/complex', conf)
conf['params']['prepared'] = True
mldb.put('/v1/functions/complex_prep', conf)

a = mldb.query('select complex({*}) as * from ds limit 2')
b = mldb.query('select complex_prep({*}) as * from ds limit 2')
mldb.log(a)
mldb.log(b)
assert a == b

mldb.script.set_return("success")
