#
# MLDBFB-422_sql_invalid_count_issue.py
# Mich, 2016-03-18
# This file is part of MLDB. Copyright 2016 mldb.ai inc. All rights reserved.
#

import unittest
import tempfile

mldb = mldb_wrapper.wrap(mldb)  # noqa


def create_beh_dataset():
    input_dataset_id = "testBehMerged"
    ds = mldb.create_dataset({
        'id' : input_dataset_id,
        'type' : 'beh.binary.mutable'
    })
    ds.record_row("a37a8cde-e202-11e5-bb8e-58659e037002", [
        ["14:seg:4944", 1, 0],
        ["14:seg:287", 1, 0],
        ["14:seg:838", 1, 0]
    ])
    ds.commit()

    dataset_name = 'testConv'
    mldb.delete('/v1/datasets/' + dataset_name)
    target = "(\"14:seg:280\")"
    select = 'testConv : CASE (' + target + ') WHEN 1 THEN 1 ELSE 0 END @ 0'
    mldb.log(select)
    mldb.put("/v1/procedures/testConv", {
        "type" : "transform",
        "params" : {
            "inputData" : 'SELECT {} FROM {}'.format(select, input_dataset_id),
            "outputDataset" : {"id" : dataset_name, "type" : "beh.mutable"},
            'runOnCreation' : True
        }
    })


def bucketize_score(percentile_buckets):
    mldb.log("Bucketing scores")
    dataset_name = "scoreBucketized"
    mldb.put("/v1/procedures/" + dataset_name, {
        "type" : "bucketize",
        "params" : {
            "inputData" : "SELECT * FROM score ORDER BY score DESC",
            "outputDataset" : {
                "id" : dataset_name,
                "type" : "beh.mutable"
            },
            "percentileBuckets" : percentile_buckets,
            'runOnCreation' : True
        }
    })


class CountGroupByTest(MldbUnitTest):  # noqa

    @classmethod
    def setUpClass(cls):
        mldb.post('/v1/procedures', {
            'type' : 'import.text',
            'params' : {
                'outputDataset' : {
                    'id' : 'score',
                    'type' : 'beh.mutable'
                },
                'dataFileUrl' :
                    'file://mldb/testing/fixtures/MLDBFB-422_issue.tsv',
                'offset' : 1,
                'headers' : ['rowName', 'ts', 'score'],
                'named' : 'rowName',
                'delimiter' : '\t',
                'select' : 'score',
                'runOnCreation' : True
            }
        })
        bucketize_score({ "0-100" : [0, 100] })

        mldb.log("Creating test dataset")
        create_beh_dataset()

        mldb.log("Creating master dataset")
        dataset_name = 'master'
        mldb.delete("/v1/datasets/" + dataset_name)
        mldb.put("/v1/datasets/" + dataset_name, {
            "type" : "merged",
            "params" : {"datasets" : [
                {"id" : "testConv"},
                {"id" : "scoreBucketized"}
            ]}
        })

        mldb.log("Sanitize dataset")
        mldb.post('/v1/procedures', {
            'type' : 'transform',
            'params' : {
                'inputData' : "SELECT * FROM master",
                'outputDataset' : {
                    'id' : 'masterSanitized',
                    'type' : 'beh.mutable'
                },
                'runOnCreation' : True
            }
        })
        mldb.post('/v1/datasets/masterSanitized/commit')

    def run_queries(self, ds):
        res1 = mldb.query(
            "SELECT count(*) FROM {} WHERE testConv IS NOT NULL".format(ds))
        res2 = mldb.query("""
            SELECT sum(c) FROM (
                SELECT testConv, c: count(*)
                FROM {}
                GROUP BY testConv)
            WHERE testConv IS NOT NULL""".format(ds))
        self.assertEqual(res1[1][1], res2[1][1])

        res1 = mldb.query(
            "SELECT count(*) FROM {} WHERE testConv IS NULL".format(ds))
        res2 = mldb.query("""
            SELECT c FROM (
                SELECT testConv, c: count(*)
                FROM {}
                GROUP BY testConv)
            WHERE testConv IS NULL""".format(ds))
        self.assertEqual(res1[1][1], res2[1][1])

    def test_it(self):
        """
        Testing after all the manipulations
        """
        self.run_queries('masterSanitized')

    def test_counter_example(self):
        """
        Testing after save & reload
        """
        f = tempfile.NamedTemporaryFile(dir='build/x86_64/tmp')
        mldb.post('/v1/procedures', {
            'type' : 'transform',
            'params' : {
                'inputData' : 'SELECT * FROM masterSanitized',
                'outputDataset' : {
                    'type' : 'beh.mutable',
                    'params' : {
                        'dataFileUrl' : 'file://' + f.name
                    }
                },
                'runOnCreation' : True
            }
        })
        mldb.put('/v1/datasets/reloaded', {
            'type' : 'beh',
            'params' : {
                'dataFileUrl' : 'file://' + f.name
            }
        })
        self.run_queries('reloaded')

if __name__ == '__main__':
    mldb.run_tests()
