#
# MLDB-956-sql-comments.py
# mldb.ai inc, 2015
# This file is part of MLDB. Copyright 2015 mldb.ai inc. All rights reserved.
#

mldb = mldb_wrapper.wrap(mldb) # noqa

dataset_config = {
    'type'    : 'sparse.mutable',
    'id'      : 'example'
}

dataset = mldb.create_dataset(dataset_config)

dataset.record_row('row1', [ [ "x", 15, 0 ] ])

mldb.log("Committing dataset")
dataset.commit()

#single line block comment
result = mldb.get(
    '/v1/query',
    q='select /*We choose to go to the moon*/ power(x, 2) from example')
mldb.log(result)
assert result.json()[0]['columns'][0][1] == 225

#single line comment
result = mldb.get(
    '/v1/query',
    q="""select --We choose to go to the moon in this decade and do the other things
         power(x, 2) from example""")
mldb.log(result)
assert result.json()[0]['columns'][0][1] == 225

#multiple line block comment
result = mldb.get('/v1/query',
                  q="""select /*not because they are easy,
                       but because they are hard*/ power(x, 2) from example""")
mldb.log(result)
assert result.json()[0]['columns'][0][1] == 225

#comment nesting
result = mldb.get(
    '/v1/query',
    q="""select /*because that goal will serve to organize  -- and measure the best of our energies
         and skills*/ power(x, 2) from example""")
mldb.log(result)
assert result.json()[0]['columns'][0][1] == 225

#comment nesting
result = mldb.get(
    '/v1/query',
    q="""select /*****because that challenge is one that we are willing to accept
         one we are unwilling to postpone --and one which we intend to, /*win, -- and the others, too.
         */ power(x, 2) from example""")
mldb.log(result)
assert result.json()[0]['columns'][0][1] == 225

mldb.script.set_return('success')
