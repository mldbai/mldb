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
                "statsTableFileUrl": "file://tmp/mldb-1209.st",
                "runOnCreation": True
            }
        })

        #######
        topic_words = [ ['paint', 'music', 'orchestra'], # arts
                        ['hockey', 'helmet', 'ball'], # sports
                        ['microscope', 'electron', 'Fibonacci'] # science
                    ]
        
        topics = ['arts', 'sports', 'sciences']
        common_words = ['the', 'chair', 'book', 'room', 'angry']

        dataset_config = {
            'type'    : 'sparse.mutable',
            'id'      : 'documents'
        }

        dataset = mldb.create_dataset(dataset_config)
        now = datetime.datetime.now()

        for i in xrange(2500):
            doc = []

            topic = i % 3

            # add topic words
            for j in xrange(10):
                doc.append(topic_words[topic][random.randint(0,2)])
            # add some random words
            for k in xrange(10):
                doc.append(common_words[random.randint(0,4)])

            dataset.record_row("u%d" % i, [["doc", ' '.join(doc), now],
                                           ["topic", topics[topic], now]])
                                               
        dataset.commit()
        
        # train stats tables
        mldb.put("/v1/procedures/gen_stats", {
            "type": "statsTable.bagOfWords.train",
            "params": {
                "trainingData":  "select tokenize(doc, {splitChars: ' '}) as * from documents",
                "outcomes":  [["arts", "topic='arts'"], ["sports", "topic='sports'"], ["sciences", "topic='sciences'"]],
                "statsTableFileUrl":  "file://tmp/mldb-1209-2.st",
                "outputDataset": "doc_stats"
            }
        })

    def test_stats_table_bag_with_noise(self):
        mldb.put("/v1/functions/posneg_bag", {
            "type": "statsTable.bagOfWords.posneg",
            "params": {
                "outcomeToUse": "arts",
                "statsTableFileUrl": "file://tmp/mldb-1209-2.st"
            }
        })

        raw = mldb.query("select posneg_bag({ words: tokenize('the paint and music book', {splitChars: ' '}) }) as *")

        mldb.put("/v1/functions/noisy_posneg_bag", {
            "type": "statsTable.bagOfWords.posneg",
            "params": {
                "outcomeToUse": "arts",
                "statsTableFileUrl": "file://tmp/mldb-1209-2.st",
                "injectNoise": True
            }
        })

        for i in xrange(10):
            noise = mldb.query("select noisy_posneg_bag({ words: tokenize('the paint and music book', {splitChars: ' '}) }) as *")
            for j in xrange(len(raw[1])):
                self.assertAlmostEqual(raw[1][j], noise[1][j], 
                                       places=1, 
                                       msg="the noisy values must be close to the original values")

    def test_stats_table_with_noise(self):

        mldb.put("/v1/functions/getRawCounts", {
            "type": "statsTable.getCounts",
            "params": {
                "statsTableFileUrl": "file://tmp/mldb-1209.st",
                "injectNoise": False
            }
        })
        
        mldb.put("/v1/functions/getNoisyCounts", {
            "type": "statsTable.getCounts",
            "params": {
                "statsTableFileUrl": "file://tmp/mldb-1209.st",
                "injectNoise": True
            }
        })

        raw = mldb.query("""
            select getRawCounts({keys: {host:'c.com', label:0, region:'brossard'}}) as *
        """)


        count_in_range = 0
        count_lower = 0
        count_higher = 0
        count_equal = 0

        for i in xrange(1000):
            noise = mldb.query("""
                select getNoisyCounts({keys: {host:'c.com', label:0, region:'brossard'}}) as *
            """)

            for j in range(1, len(raw[1])):
                if raw[1][j] > noise[1][j]:
                    count_lower += 1
                if raw[1][j] < noise[1][j]:
                    count_higher += 1
                if raw[1][j] == noise[1][j]:
                    count_equal += 1
                if abs(noise[1][j] - raw[1][j]) < 6:
                    count_in_range += 1

        self.assertAlmostEqual(float(count_lower - count_higher) / float(count_higher + count_lower + count_equal), 0,
                               places=1, 
                               msg="noisy values should be equally distributed around the count" )

        self.assertTrue(count_equal > 0, 
                        msg="it is expected that some values will not be altered by the noise")

        self.assertGreater(float(count_in_range) / float(count_higher + count_lower + count_equal), 
                           0.8, 
                           msg="it is expected that the noisy values will be close to the original values")

if __name__ == '__main__':
    mldb.run_tests()


