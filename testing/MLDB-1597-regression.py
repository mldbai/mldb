# This file is part of MLDB. Copyright 2016 mldb.ai inc. All rights reserved.

import unittest

mldb = mldb_wrapper.wrap(mldb) # noqa

class Mldb1597Test(MldbUnitTest):

    @classmethod
    def setUpClass(cls):

        # the raw data
        mldb.post("/v1/procedures", {
            "type": "import.text",
            "params":{
                "dataFileUrl": "http://public.mldb.ai/regression_test.csv.gz",
                "select": """
                    'd'+date_part('dow', timestamp(jseval('return new Date(d);', 'd', day))) as dow,
                    a_int,
                    b_1, b_2, b_1/b_2 as b_ratio,
                    c_1 as c,
                    d_1+d_2-c_2 as d,
                    e_1, e_2, e_1/e_2 as e,
                    c_1+c_2-d_1-d_2 as p,
                    1-(d_1+d_2-c_2)/c_1 as r
                """,
                "limit" : 100,
                "outputDataset": {"id":"ds", "type":"tabular"},
                "runOnCreation": True
            }
        })

        # the stats
        mldb.post("/v1/procedures", {
            "type": "transform",
            "params":{
                "inputData": """
                select
                    dow, a_int,
                    sum(e_1)/sum(e_2) as e,
                    avg({b_1, b_2}) as *,
                    avg(b_1)/avg(b_2) as b_ratio,
                    1-sum(d_1+d_2-c_2)/sum(c_1) as r
                from ds
                group by dow, a_int
                """,
                "outputDataset": {"id":"ds_stats", "type":"tabular"},
                "runOnCreation": True
            }
        })

        # the training data
        mldb.post("/v1/procedures", {
                "type": "transform",
                "params":{
                    "inputData": """
                        select *
                        from ds left join ds_stats on (ds.dow + ds.a_int = ds_stats.dow + ds_stats.a_int)
                        limit 10
                    """,
                    "outputDataset": {
                        "id":"ds_train",
                        "type":"tabular",
                        "params": {
                            "unknownColumns":"add"
                        }
                    },
                    "runOnCreation": True
                }
            })

    def test_operator_precedence(self):
        self.assertTableResultEquals(
            mldb.query("select (4/2) between 0 and 1 as boolean"),
            [ [ "_rowName",  "boolean"],
              [ "result", False] ])

        self.assertTableResultEquals(
            mldb.query("select 4/2 between 0 and 1 as boolean"),
            [ [ "_rowName",  "boolean"],
              [ "result", False] ])

        self.assertTableResultEquals(
            mldb.query("select (4/2) between 0 and 5 as boolean"),
            [ [ "_rowName",  "boolean"],
              [ "result", True] ])

        self.assertTableResultEquals(
            mldb.query("select 4/2 between 0 and 5 as boolean"),
            [ [ "_rowName",  "boolean"],
              [ "result", True] ])

        # the division should be performed before the between
        # this was throwing an exception before
        mldb.query("""
        select count(*) from ds group by dow
        having sum(c)/sum(d) between -1 and 1
        """)

        # the negation should be taken before the in operation
        resp1 = mldb.query("""
        select * from ds where r in (-nan) limit 1
        """)
        mldb.log(resp1)

        resp2 = mldb.query("""
        select * from ds where -nan in (r) limit 1
        """)
        self.assertEqual(resp1, resp2)

        resp1 = mldb.query("""
        select * from ds where r in (-inf) limit 1
        """)
        mldb.log(resp1)

        resp2 = mldb.query("""
        select * from ds where -inf in (r) limit 1
        """)
        self.assertEqual(resp1, resp2)

    def test_order_by_with_aggregate(self):
        mldb.query("""
        select
            sum(c) as s
        from ds
        group by dow
        order by sum(c)
        """)

        mldb.query("""
        select
            1-(0.001+sum(d))/(0.001+sum(c)) as r,
            sum(c - d) as p
        from ds
        group by dow
        order by 1-(0.001+sum(d))/(0.001+sum(c))
        """)

    def run_query_and_compare(self, query):
        resp = mldb.query(query)
        mldb.log(resp)
        # expecting the header + 10 lines
        self.assertEqual(len(resp), 10 + 1)

        # columns in the same order as the input
        mldb.log(resp[1])
        self.assertEqual(resp[0], [
            "_rowName",
            "left_table.asc",
            "left_table.const",
            "left_table.desc",
            "right_table.const",
            "right_table.index",
            "right_table.mod"
        ], "following asserts depend on this layout")

        for line in resp[1:]:
            self.assertEqual(line[1], line[5], "expected equal values on these fields")
            self.assertEqual(line[2], line[4], "expected equal values on these fields")

    def test_left_join_with_and(self):
        left = mldb.create_dataset({ "id": "left_table", "type": "tabular" })
        for i in range(0,10):
            left.record_row("a" + str(i),[["asc", i, 0], ["desc", 10 - i, 0], ["const", 729, 0]])
        left.commit()

        right = mldb.create_dataset({ "id": "right_table", "type": "tabular" })
        for i in range(0,10):
            right.record_row("b" + str(i),[["index", i, 0], ["mod", i%2, 0], ["const", 729, 0]])
        right.commit()

        self.run_query_and_compare("""
        select *
        from left_table left join right_table
        on (left_table.asc = right_table.index
        and left_table.const = right_table.const)
        """)

        self.run_query_and_compare("""
        select *
        from left_table left join right_table
        on (left_table.asc + left_table.const =
        right_table.index + right_table.const)
        """)

    def test_join_with_and(self):
        resp = mldb.query('select * from ds_train')
        mldb.log(resp)

        mldb.post("/v1/procedures", {
                "type": "transform",
                "params":{
                    "inputData": """
                        select *
                        from ds left join ds_stats on (ds.dow=ds_stats.dow and ds.a_int=ds_stats.a_int)
                        limit 10
                    """,
                    "outputDataset": {
                        "id":"ds_train2",
                        "type":"tabular",
                        "params": {
                            "unknownColumns":"add"
                        }
                    },
                    "runOnCreation": True
                }
            })

        resp2 = mldb.query('select * from ds_train2')
        mldb.log(resp2)

        # equivalent join conditions should be returning the same dataset
        # this is a very weak check because the columns and the row ordering
        # of these two equivalent joins are currently very different
        self.assertEqual(len(resp), len(resp2), 'expected response sizes to match')

    @unittest.skip("awaiting MLDB-1659")
    def test_r2_bug(self):

        mldb.query("select 11.0 as score, ds.c as label from ds")

        # r2 should not be null every time score has only zeros after the decimal point
        result = mldb.post("/v1/procedures", {
            "type": "classifier.test",
            "params": {
                "testingDataOverride": "select 11.0 as score, ds.c as label from ds",
                "mode": "regression",
                "runOnCreation": True
            }
        })
        r2 = result.json()["status"]["firstRun"]["status"]["r2"]
        self.assertTrue( r2 is not None )

    def test_function_creation_bug(self):
        mldb.post("/v1/procedures", {
            "type": "import.text",
            "params":{
                "dataFileUrl": "http://public.mldb.ai/narrow_test.csv.gz",
                "outputDataset": "narrow",
                "runOnCreation": True
            }
        })

        # it seems that the training fails to save the function but we proceed to testing
        # where we try to use the function but then can't find it
        # 1) we should not move to testing if function-creation fails
           # we should report that function-creation failed
        # 2) function creation should not fail for a dt on this dataset

        mldb.put("/v1/procedures/train", {
            "type": "classifier.experiment",
            "params": {
                "experimentName": "x",
                "inputData": "select {a} as features, b as label from narrow",
                "algorithm": "dt",
                "mode": "regression",
                "configurationFile": "./mldb/container_files/classifiers.json",
                "modelFileUrlPattern": "file://tmp/MLDB-1597-creation$runid.cls",
                "runOnCreation": True
            }
        })

    @unittest.skip("illustrative test only, no asserts")
    def test_permutations(self):
        def train(features, label, algo):
            try:
                result = mldb.post("/v1/procedures", {
                    "type": "classifier.experiment",
                    "params": {
                        "experimentName": "ds",
                        "inputData":
                            "select { %s } as features, %s as label from ds_train" % (
                                ",".join(features), label),
                        "algorithm": algo,
                        "mode": "regression",
                        "modelFileUrlPattern": "file://tmp/MLDB-1597-$runid.cls",
                        "configurationFile": "./mldb/container_files/classifiers.json",
                        "runOnCreation": True
                    }
                })
                mldb.log(result)
                self.assertEqual(result.status_code, 201, result)
                #result.json()["status"]["firstRun"]["status"]["aggregatedTest"]["r2"]["mean"]
                mldb.log("passed")
            except Exception as e:
                mldb.log("failed")
                mldb.log(e)
                print features, label, algo, e
                #raise e


        # all of these permutations should either work or have clear error messages
        for l in ["idonotexist", "ds.c", "ds.d", "ds.e", "ds.r", "ds.p"]:
            for a in ["dt", "bdt", "glz_linear"]:
                for f in [
                    ["ds.b_ratio"],
                    ["ds.b_ratio", "ds.dow"],
                    [ u'ds.a_int', u'ds.b_1', u'ds.b_2'],
                    [ u'ds.a_int', u'ds.b_1', u'ds.b_2', 'ds.dow']
                ]:
                    train(f,l,a)

mldb.run_tests()


