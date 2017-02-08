#
# MLDB-1119_pooling_function.py
# mldb.ai inc, 2016
# This file is part of MLDB. Copyright 2016 mldb.ai inc. All rights reserved.
#
import datetime
import unittest

mldb = mldb_wrapper.wrap(mldb) # noqa

class Mldb1119(MldbUnitTest):  

    @classmethod
    def setUpClass(self):
        # Create embedding dataset
        dataset_config = {
            'type'    : 'embedding',
            'id'      : 'wordEmbedding'
        }

        dataset = mldb.create_dataset(dataset_config)
        now = datetime.datetime.strptime('Jun 1 2005  1:33PM', '%b %d %Y %I:%M%p')

        dataset.record_row("allo", [["x", 0.2, now], ["y", 0, now]])
        dataset.record_row("mon",  [["x", 0.8, now], ["y", 0.95, now]])
        dataset.record_row("beau", [["x", 0.4, now], ["y", 0.01, now]])
        dataset.record_row("coco", [["x", 0, now],   ["y", 0.5, now]])
        dataset.commit()

        # Create bag of words dataset
        dataset_config = {
            'type'    : 'sparse.mutable',
            'id'      : 'bag_o_words'
        }

        dataset = mldb.create_dataset(dataset_config)

        dataset.record_row("doc1",  [["allo", 1, now], ["coco", 1, now]])
        dataset.record_row("doc2",  [["allo", 1, now], ["mon", 1, now],
                                     ["beau", 1, now]])
        dataset.record_row("doc3",  [["patate", 1, now]])
        dataset.record_row("doc4",  [["j'ai", 1, now]])
        dataset.commit()


    def test_pooling(self):
        # create pooling function
        conf = {
            "type": "pooling",
            "params": {
                "embeddingDataset": "wordEmbedding",
                "aggregators": ["avg", "max"]
            }
        }
        res = mldb.put("/v1/functions/poolz", conf)
        mldb.log(res.json())

        res = mldb.get(
            "/v1/query",
            q="select poolz({words: {*}})[embedding] as word2vec from bag_o_words")
        js_res = res.json()
        mldb.log(js_res)


        def assert_val(res, rowName, colName, value):
            for row in res:
                if row["rowName"] != rowName:
                    continue

                for col in row["columns"]:
                    if col[0] == colName:
                        self.assertLess(abs(col[1] - value), 0.0001)
                        return True

                # did not find col
                self.assertTrue(False)

            # did not find row
            self.assertTrue(False)

        # max of x dim for allo or coco
        assert_val(js_res, "doc1", "word2vec.2", 0.2)
        # avg of y dim for allo, mon, beau
        assert_val(js_res, "doc2", "word2vec.1", 0.32)
        # no match
        assert_val(js_res, "doc4", "word2vec.0", 0)

    
    # MLDB-1733
    def test_returns_null_if_null_input(self):
        mldb.put("/v1/procedures/megatron", {
            "type": "transform",
            "params": {
                "inputData": """
                    select patat* from bag_o_words
                """,
                "outputDataset": "bag_o_words_nulls",
                "skipEmptyRows": False,
                "runOnCreation": True
            }
        })

        # the following fails with "Expected row expression" since some rows
        # have no columns because we removed them in the transform above.
        res = mldb.get(
            "/v1/query",
            q="select poolz({words: {*}})[embedding] as word2vec from bag_o_words_nulls")
        mldb.log(res.json())

        expected = [
                    {
                        "rowName": "doc4",
                        "columns": [
                            [
                                "word2vec.0",
                                0,
                                "NaD"
                            ],
                            [
                                "word2vec.1",
                                0,
                                "NaD"
                            ],
                            [
                                "word2vec.2",
                                0,
                                "NaD"
                            ],
                            [
                                "word2vec.3",
                                0,
                                "NaD"
                            ]
                        ]
                    },
                    {
                        "rowName": "doc3",
                        "columns": [
                            [
                                "word2vec.0",
                                0,
                                "2005-06-01T13:33:00Z"
                            ],
                            [
                                "word2vec.1",
                                0,
                                "2005-06-01T13:33:00Z"
                            ],
                            [
                                "word2vec.2",
                                0,
                                "2005-06-01T13:33:00Z"
                            ],
                            [
                                "word2vec.3",
                                0,
                                "2005-06-01T13:33:00Z"
                            ]
                        ]
                    },
                    {
                        "rowName": "doc2",
                        "columns": [
                            [
                                "word2vec.0",
                                0,
                                "NaD"
                            ],
                            [
                                "word2vec.1",
                                0,
                                "NaD"
                            ],
                            [
                                "word2vec.2",
                                0,
                                "NaD"
                            ],
                            [
                                "word2vec.3",
                                0,
                                "NaD"
                            ]
                        ]
                    },
                    {
                        "rowName": "doc1",
                        "columns": [
                            [
                                "word2vec.0",
                                0,
                                "NaD"
                            ],
                            [
                                "word2vec.1",
                                0,
                                "NaD"
                            ],
                            [
                                "word2vec.2",
                                0,
                                "NaD"
                            ],
                            [
                                "word2vec.3",
                                0,
                                "NaD"
                            ]
                        ]
                    }
                ]
        self.assertEqual(res.json(), expected)

mldb.run_tests()

