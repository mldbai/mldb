#
# MLDB-1694_flatten_embedding.py
# 2016-05-30
# This file is part of MLDB. Copyright 2016 mldb.ai inc. All rights reserved.
#

import unittest

mldb = mldb_wrapper.wrap(mldb) # noqa

class Mldb1694(MldbUnitTest):  
    @classmethod
    def setUpClass(self):
        inceptionUrl = 'http://public.mldb.ai/models/inception_dec_2015.zip'

        mldb.put('/v1/functions/fetch', {
            "type": 'fetcher',
            "params": {}
        })

        mldb.put('/v1/functions/inception', {
            "type": 'tensorflow.graph',
            "params": {
                "modelFileUrl": 'archive+' + inceptionUrl + '#tensorflow_inception_graph.pb',
                "inputs": 'fetch({url})[content] AS "DecodeJpeg/contents"',
                "outputs": "softmax"
            }
        })

        mldb.log("pwet!")

        self.amazingGrace = "https://public.mldb.ai/datasets/tensorflow-demo/grace_hopper.jpg"
 
    def test_prediction_works(self):
        self.assertTableResultEquals(
            mldb.query("""select round(pred * 10000) as pred from transpose(
                            (
                                SELECT inception({url: '%s'}) as *
                                NAMED 'pred'
                            )
                        )
                        order by pred DESC limit 5
                """ % self.amazingGrace),
            [
                ["_rowName","pred"],
                ["softmax.0.866", 8080],
                ["softmax.0.794",  228],
                ["softmax.0.896",   95],
                ["softmax.0.849",   94],
                ["softmax.0.926",   77]
            ])


    def test_flattened_prediction_works(self):
        # this version does the same prediction as the test above
        # but accesses the output parameter softmax and flattens
        # the embedding
        self.assertTableResultEquals(
            mldb.query("""select round(pred * 10000) as pred from transpose(
                            (
                                SELECT flatten(inception({url: '%s'})[softmax]) as *
                                NAMED 'pred'
                            )
                        )
                        order by pred DESC limit 5
                """ % self.amazingGrace),
            [
                ["_rowName","pred"],
                ["866", 8080],
                ["794",  228],
                ["896",   95],
                ["849",   94],
                ["926",   77]
            ])


mldb.run_tests()

