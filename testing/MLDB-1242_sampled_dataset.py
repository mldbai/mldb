# ##
# Francois Maillet, 11 janvier 2016
# This file is part of MLDB. Copyright 2016 mldb.ai inc. All rights reserved.
# ##

import unittest
import random
import datetime

if False:
    mldb_wrapper = None
mldb = mldb_wrapper.wrap(mldb) # noqa


class SampledDatasetTest(unittest.TestCase):

    @classmethod
    def setUpClass(cls):
        # Create toy dataset
        dataset_config = {
            'type'    : 'sparse.mutable',
            'id'      : "toy"
        }

        dataset = mldb.create_dataset(dataset_config)
        now = datetime.datetime.now()

        for i in xrange(500):
            dataset.record_row("u%d" % i, [["feat1", random.gauss(5, 3), now]])

        dataset.commit()

    def test_base(self):
        sampled_dataset_conf = {
            "type": "sampled",
            "params": {
                "dataset": {"id": "toy"},
                "rows": 10
            }
        }
        mldb.put("/v1/datasets/pwet", sampled_dataset_conf)

        rez = mldb.get("/v1/query", q="SELECT * FROM pwet")
        self.assertEqual(len(rez.json()), 10)

    def test_too_many_requested_rows(self):
        # too many requested rows without sampling
        sampled_dataset_conf = {
            "type": "sampled",
            "params": {
                "dataset": {"id": "toy"},
                "rows": 25000,
                "withReplacement": False
            }
        }
        with self.assertRaises(mldb_wrapper.ResponseException) as re:
            mldb.put("/v1/datasets/patate", sampled_dataset_conf)
        self.assertEqual(re.exception.response.status_code, 400)

        sampled_dataset_conf["params"]["withReplacement"] = True
        mldb.put("/v1/datasets/patate", sampled_dataset_conf)

        # try to insert and make sure we get an exception
        with self.assertRaises(mldb_wrapper.ResponseException) as re:
            mldb.post("/v1/datasets/patate/rows", {
                "rowName": "patato",
                "columns": [["a", "b", 0]]
            })
        self.assertEqual(re.exception.response.status_code, 400)

    def test_fraction(self):
        # with fraction
        sampled_dataset_conf = {
            "type": "sampled",
            "params": {
                "dataset": "toy",
                "fraction": 0.5
            }
        }
        mldb.put("/v1/datasets/pwet", sampled_dataset_conf)

        rez = mldb.get("/v1/query", q="SELECT * FROM pwet")
        self.assertEqual(len(rez.json()), 250)

        sampled_dataset_conf["params"]["fraction"] = 5
        with self.assertRaises(mldb_wrapper.ResponseException) as re:
            mldb.put("/v1/datasets/pwet", sampled_dataset_conf)
        self.assertEqual(re.exception.response.status_code, 400)

        sampled_dataset_conf["params"]["fraction"] = 0
        with self.assertRaises(mldb_wrapper.ResponseException) as re:
            mldb.put("/v1/datasets/pwet", sampled_dataset_conf)
        self.assertEqual(re.exception.response.status_code, 400)

        sampled_dataset_conf["params"]["fraction"] = -1
        with self.assertRaises(mldb_wrapper.ResponseException) as re:
            mldb.put("/v1/datasets/pwet", sampled_dataset_conf)
        self.assertEqual(re.exception.response.status_code, 400)

    def test_queries(self):
        rez = mldb.get(
            "/v1/query",
            q="select * from sample(toy, {rows: 25000, withReplacement: 1})")
        self.assertEqual(len(rez.json()), 25000)

        rez = mldb.get("/v1/query", q="select * from sample(toy, {rows: 25})")
        self.assertEqual(len(rez.json()), 25)

    def test_seed_works(self):
        # test seed works
        rez = mldb.get("/v1/query",
                       q="select * from sample(toy, {rows: 1, seed: 5})")
        rez2 = mldb.get("/v1/query",
                        q="select * from sample(toy, {rows: 1, seed: 5})")
        self.assertEqual(rez.json()[0], rez2.json()[0])

        rez = mldb.get("/v1/query", q="select * from sample(toy, {rows: 1})")
        rez2 = mldb.get("/v1/query", q="select * from sample(toy, {rows: 1})")
        self.assertNotEqual(rez.json()[0], rez2.json()[0])

    def test_default_options(self):
        rez = mldb.get(
            "/v1/query",
            q="select * from sample(toy)")
        self.assertEqual(len(rez.json()), 1)

    def test_functions_with_invalid_params(self):
        with self.assertRaises(mldb_wrapper.ResponseException) as re:
            mldb.get("/v1/query", q="select * from sample(toy, {rows: 1, fraction: 0.2})")
        with self.assertRaises(mldb_wrapper.ResponseException) as re:
            mldb.get("/v1/query", q="select * from sample(toy, {fraction: 0})")
        with self.assertRaises(mldb_wrapper.ResponseException) as re:
            mldb.get("/v1/query", q="select * from sample(toy, {fraction: -0.2})")
        with self.assertRaises(mldb_wrapper.ResponseException) as re:
            mldb.get("/v1/query", q="select * from sample(toy, {fraction: 2})")

    def test_sampled_over_merged(self):
        # MLDB-1431
        mldb.post('/v1/procedures', {
            'type' : 'transform',
            'params' : {
                'inputData' : "SELECT * FROM toy LIMIT 100",
                'outputDataset' : {'id' : 'test_sampled_over_merged_ds1'}
            }

        })
        mldb.post('/v1/procedures', {
            'type' : 'transform',
            'params' : {
                'inputData' : "SELECT * FROM toy LIMIT 100 OFFSET 100",
                'outputDataset' : {'id' : 'test_sampled_over_merged_ds2'}
            }

        })
        mldb.put('/v1/datasets/test_sampled_over_merged_merged', {
            'type' : 'merged',
            'params' : {
                'datasets' : [{'id' : 'test_sampled_over_merged_ds1'},
                              {'id' : 'test_sampled_over_merged_ds2'}]
            }
        })
        mldb.put('/v1/datasets/test_sampled_over_merged_sampled', {
            'type' : 'sampled',
            'params' : {
                'dataset' : {'id' : 'test_sampled_over_merged_merged'},
                'fraction' : 0.99
            }
        })
        mldb.query("""
            SELECT COLUMN EXPR (AS columnName() ORDER BY rowCount() DESC)
            FROM test_sampled_over_merged_sampled""")

    def test_cant_create_wo_ds(self):
        # MLDB-1977
        msg = "You need to define the dataset key"
        with self.assertRaisesRegexp(mldb_wrapper.ResponseException, msg) as re:
            mldb.put('/v1/datasets/sampled', {
                'type' : 'sampled',
                'params' : {
                    'fraction' : 0.99
                }
            })

if __name__ == '__main__':
    mldb.run_tests()
