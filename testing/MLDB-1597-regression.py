
# This file is part of MLDB. Copyright 2016 Datacratic. All rights reserved.




import unittest

mldb = mldb_wrapper.wrap(mldb) # noqa

class Mldb1597Test(MldbUnitTest):  

    @classmethod
    def setUpClass(cls):
        
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

    @unittest.skip("test")
    def test_remaining(self):
        # BUG: the commented-out clause should not cause a segfault

        mldb.query("""
        select 
            1-(0.001+sum(d))/(0.001+sum(c)) as r,
            sum(c - d) as p
        from ds 
        group by dow
        --order by 1-(0.001+sum(cost))/(0.001+sum(income)) --this is the culprit
        """)

        # BUG: theses calls should all return a bunch of rows, just like the one after

        mldb.query("""
        select * from ds where r in (-nan)
        """)

        mldb.query("""
        select * from ds where -nan in (r)
        """)

        mldb.query("""
        select * from ds where r in (-inf)
        """)

        mldb.query("""
        select * from ds where -inf in (r)
        """)

        # setup
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

        # BUG
        # the commented-out join condition should work instead of the hack on the 
        # next line
        mldb.post("/v1/procedures", {
                "type": "transform",
                "params":{
                    "inputData": """
                        select *
                        from ds left join ds_stats on (
                            -- this doesn't work: ds.dow=ds_stats.dow and ds.a_int=ds_stats.a_int
                            ds.dow + ds.a_int = ds_stats.dow + ds_stats.a_int
                        )
                    """,
                    "outputDataset": {"id":"ds_train", "type":"tabular"},
                    "runOnCreation": True
                }
            })

        # BUG: 
        # r2 should not be null every time score has only zeros after the decimal point
        mldb.post("/v1/procedures", {
            "type": "classifier.test",
            "params": {
                "testingData": "select 11.0 as score, ds.c as label from ds_train",
                "mode": "regression",
                "runOnCreation": True
            }
        })

        #setup

        def train(features, label, algo):
            try:
                result = mldb.post("/v1/procedures", {
                    "type": "classifier.experiment",
                    "params": {
                        "experimentName": "ds",
                        "trainingData": 
                            "select { %s } as features, %s as label from ds_train" % (
                                ",".join(features), label),
                        "algorithm": algo,
                        "mode": "regression",
                        "modelFileUrlPattern": "file:///mldb_data/$runid.cls",
                        "runOnCreation": True
                    }
                })
                result.json()["status"]["firstRun"]["status"]["aggregatedTest"]["r2"]["mean"]
            except Exception as e:
                print features, label, algo, e


        # all of these permutations should either work or have clear error messages
        for l in ["ds.c", "ds.d", "ds.e", "ds.r", "ds.p"]:
            for a in ["dt", "bdt", "glz_linear"]:
                for f in [
                    ["ds.b_ratio"],
                    ["ds.b_ratio", "ds.dow"],
                    [ u'ds.a_int', u'ds.b_1', u'ds.b_2'],
                    [ u'ds.a_int', u'ds.b_1', u'ds.b_2', 'ds.dow']
                ]:
                    train(f,l,a)

mldb.run_tests()


