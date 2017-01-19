#
# MLDB-775_hashbucket_feat_gen.py
# mldb.ai inc, 2015
# this file is part of mldb. copyright 2015 mldb.ai inc. all rights reserved.
#

import datetime
import unittest

mldb = mldb_wrapper.wrap(mldb) # noqa

class Mldb775(MldbUnitTest):
    @classmethod
    def setUpClass(self):
        dataset_config = {
            'type'    : 'sparse.mutable',
            'id'      : 'toy'
        }

        dataset = mldb.create_dataset(dataset_config)
        mldb.log("data loader created dataset")

        now = datetime.datetime.now()

        for i in xrange(5):
            dataset.record_row("example-%d" % i, [["fwin", i, now],
                                                  ["fwine", i*2, now],
                                                  ["fwinette", i**4, now]])

        mldb.log("Committing dataset")
        dataset.commit()


        ## create functions
        script_func_conf = {
            "id":"featHasher",
            "type":"feature_hasher",
            "params": {
                "numBits": 2,
                "mode": "columns"
            }
        }
        rez = mldb.put("/v1/functions/" + script_func_conf["id"],
                                      script_func_conf)


        script_func_conf = {
            "id":"featHasher2",
            "type":"feature_hasher",
            "params": {
                "numBits": 2,
                "mode": "columnsAndValues"
            }
        }
        rez = mldb.put("/v1/functions/" + script_func_conf["id"],
                                      script_func_conf)


    def test_function(self):
        rez = mldb.query("select featHasher({{*} as columns})[hash] from toy")
        for line in rez:
            self.assertEqual(len(line), 2**2 + 1)


    def test_colNVal_func(self):
        rez = mldb.query("""
                select
                    featHasher({{*} as columns})[hash] as a,
                    featHasher2({{*} as columns})[hash] as b
                from toy""")


        # make sure the values returned by both functions are not equal
        colIndexes = { colName: colId for colId, colName in enumerate(rez[0]) }
        for line in rez[1:]:
            for i in xrange(4):
                colA = "a.hashColumn%d" % i
                colB = "b.hashColumn%d" % i
                if line[colIndexes[colA]] != line[colIndexes[colB]]:
                    break
            else:
                raise Exception("identical")


mldb.run_tests()

