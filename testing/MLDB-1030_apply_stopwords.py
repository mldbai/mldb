#
# MLDB-1030_apply_stopwords.py
# Datacratic, 2015
# This file is part of MLDB. Copyright 2015 Datacratic. All rights reserved.
#

import datetime

mldb = mldb_wrapper.wrap(mldb) # noqa

dataset_config = {
    'type'    : 'sparse.mutable',
    'id'      : "toy"
}


dataset = mldb.create_dataset(dataset_config)
now = datetime.datetime.now()

dataset.record_row("elem1", [ ["title", "patate where when poire when", now]])
dataset.record_row("elem2", [ ["title", "allo where what he a allo", now]])

dataset.commit()


#add function
func_conf = {
    "type":"filter_stopwords",
    "params": {}
}
func_output = mldb.put("/v1/functions/stop", func_conf)
mldb.log(func_output)


# baggify our words
baggify_conf = {
    "type": "transform",
    "params": {
        "inputData": "select tokenize(title, {splitchars:' ', quotechar:'', "
                     "min_token_length: 2}) as * from toy",
        "outputDataset": {
            "id": "bag_of_words",
            "type": "sparse.mutable"
        }
    }
}
baggify_output = mldb.put("/v1/procedures/baggify", baggify_conf)
mldb.log(baggify_output)

run_output = mldb.post("/v1/procedures/baggify/runs")
mldb.log(run_output)

# query all
rez = mldb.get("/v1/query",
               q="select * from bag_of_words order by rowName() ASC")
mldb.log(rez.json())

def do_check(my_rez):
    words = [[x[0] for x in line["columns"]] for line in my_rez]
    assert set(["patate", "poire"]) == set(words[0])
    assert ["allo"] == words[1]

# query while applying stopwords
rez = mldb.get("/v1/query",
               q="select stop({words: {*}})[words] as * from bag_of_words "
                 "order by rowName() ASC")
js_rez = rez.json()
mldb.log(js_rez)
do_check(js_rez)


#####
# try both operations at once
rez = mldb.get("/v1/query", q="""
    select stop({
                    words: tokenize(title, {min_token_length:2, splitchars: ' ', quotechar: ''})
                }
            )[words] as *
    from toy
    order by rowName() ASC""")
js_rez = rez.json()
mldb.log(js_rez)
do_check(js_rez)


#####
# the following shouldn't error out (MLDB-1689)
# rez = mldb.get("/v1/query", q=""" select stop({ words: {} } ) """)


mldb.script.set_return("success")
