#
# MLDB-1209_statstable_bias_noise_test.py
# Francois Maillet, 2016-07-04
# This file is part of MLDB. Copyright 2016 Datacratic. All rights reserved.
#

import datetime
import random

mldb = mldb_wrapper.wrap(mldb)  # noqa

class MLDB1209StatstableBiasNoiseTest(MldbUnitTest):  # noqa

    @classmethod
    def setUpClass(cls):

        probs = [
                [['a.com', 0.2],
                 ['b.com', 0.02],
                 ['c.com', 0.1]],
                [['montreal', 0.02],
                 ['brossard', 0.1],
                 ['laprairie', 0.2]]
            ]


        dataset_config = {
            'type'    : 'sparse.mutable',
            'id'      : 'bid_requests'
        }

        dataset = mldb.create_dataset(dataset_config)
        now = datetime.datetime.now()

        for i in xrange(25000):
            host_id = random.randint(0,2)
            region_id = random.randint(0,2)

            click = random.random() < probs[0][host_id][1] and \
                        random.random() < probs[1][region_id][1]

            dataset.record_row("u%d" % i, [["host", probs[0][host_id][0], now],
                                           ["region", probs[1][region_id][0], now],
                                           ["label", click, now]])

        dataset.commit()
        
        # train stats tables
        mldb.put("/v1/procedures/train_st", {
            "type": "statsTable.train",
            "params": {
                "trainingData": "select host, region from bid_requests",
                "outcomes": [["label", "label = 1"]],
                "statsTableFileUrl": "file:///tmp/mldb-1209.st",
                "runOnCreation": True
            }
        })


    def test_it(self):

        mldb.put("/v1/functions/getRawCounts", {
            "type": "statsTable.getCounts",
            "params": {
                "statsTableFileUrl": "file:///tmp/mldb-1209.st",
                "injectNoise": False
            }
        })
        
        mldb.put("/v1/functions/getNoisyCounts", {
            "type": "statsTable.getCounts",
            "params": {
                "statsTableFileUrl": "file:///tmp/mldb-1209.st",
                "injectNoise": True
            }
        })

        mldb.log(mldb.query("""
        select 
            cnts.counts.label.host / cnts.counts.trial.host as ctr_host,
            cnts.counts.label.region / cnts.counts.trial.region as ctr_region
        from (
            select getRawCounts({keys: {*}}) as cnts from bid_requests limit 5
        )
        
        """))


        mldb.put("/v1/procedures/transf", {
            "type": "transform",
            "params": {
                "inputData": """
                    select 
            cnts.counts.label.host / cnts.counts.trial.host as ctr_host,
            cnts.counts.label.region / cnts.counts.trial.region as ctr_region,
            label
                from (

                        select
                            getRawCounts({keys: {*}}) as cnts, label
                            from bid_requests
                    )
                """,
                "outputDataset": "training_raw",
                "runOnCreation": True
            }
        })


        rez = mldb.put("/v1/procedures/train_raw", {
            "type": "classifier.experiment",
            "params": {
                "experimentName": "train_raw",
                "inputData": """
                    select {ctr*} as features,
                            label
                    from training_raw
                """,
                "kfold": 3,
                "algorithm": "bglz",
                "configurationFile": "file://mldb/container_files/classifiers.json",
                "modelFileUrlPattern": "file:///tmp/mldb-1209.cls",
                "runOnCreation": True
            }
        })

        mldb.log(rez)

if __name__ == '__main__':
    mldb.run_tests()


