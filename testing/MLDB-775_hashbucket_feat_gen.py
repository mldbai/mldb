#
# MLDB-775_hashbucket_feat_gen.py
# datacratic, 2015
# this file is part of mldb. copyright 2015 datacratic. all rights reserved.
#
mldb = mldb_wrapper.wrap(mldb) # noqa


import datetime

dataset_config = {
    'type'    : 'sparse.mutable',
    'id'      : 'toy'
}

dataset = mldb.create_dataset(dataset_config)
mldb.log("data loader created dataset")

now = datetime.datetime.now()

for i in xrange(5):
    dataset.record_row("example-%d" % i, [["fwin", i, now],
                                          ["fwine", i*2, now]])

mldb.log("Committing dataset")
dataset.commit()

#add an explain function
script_func_conf = {
    "id":"featHasher",
    "type":"experimental.feature_generator.hashed_column",
    "params": {
        "numBits": 8
    }
}
script_func_output = mldb.put("/v1/functions/" + script_func_conf["id"],
                              script_func_conf)
mldb.log("The resulf of the script function creation "
         + script_func_output.text)
mldb.log("passed assert")


# requires "as args" because args is the input pin
select = "featHasher({{*} as columns})[hash]";

query_output = mldb.get("/v1/datasets/toy/query", select=select, limit="10")

js_resp = query_output.json()

for line in js_resp:
    assert len(line["columns"]) == 256
    mldb.log(line)
    mldb.log("----")

mldb.script.set_return("success")
