#
# MLDB-1092_conf_interval.py
# mldb.ai inc, 2015
# This file is part of MLDB. Copyright 2015 mldb.ai inc. All rights reserved.
#

mldb = mldb_wrapper.wrap(mldb) # noqa
import datetime


def assert_val_for_col(rows, key, goodVal):
    for row in rows:
        if row[0] == key:
            assert abs(row[1] - goodVal) < 0.001
            return True

    raise Exception("Could not find key '"+key+"'")


result = mldb.get('/v1/query',
                  q="SELECT binomial_ub_80(200, 35) as ub, "
                    "binomial_lb_80(200, 35) as lb")
assert_val_for_col(result.json()[0]["columns"], "ub", 0.2120410)


# Create toy dataset
dataset_config = {
    'type'    : 'sparse.mutable',
    'id'      : 'toy'
}

dataset = mldb.create_dataset(dataset_config)
now = datetime.datetime.now()

dataset.record_row("u1", [["trials", 200, now], ["succ", 35, now]])
dataset.record_row("u2", [["trials", 500, now], ["succ", 35, now]])
dataset.commit()

result = mldb.get('/v1/query',
                  q="""SELECT binomial_ub_80(trials, succ) as ub,
                       binomial_lb_80(trials, succ) as lb
                       from toy
                       order by rowName() ASC""")
js_rez = result.json()
mldb.log(js_rez)
# 35/200
assert_val_for_col(js_rez[0]["columns"], "ub", 0.212)
assert_val_for_col(js_rez[0]["columns"], "lb", 0.143)

# 35/500
assert_val_for_col(js_rez[1]["columns"], "lb", 0.0567)


mldb.script.set_return("success")
