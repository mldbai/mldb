# -*- coding: utf-8 -*-

# ##
# Mathieu Marquis Bolduc, March 2nd 2017
# This file is part of MLDB. Copyright 2017 mldb.ai inc. All rights reserved.
# ##

mldb = mldb_wrapper.wrap(mldb) # noqa

class FastTextTest(MldbUnitTest):

        @classmethod
        def setUpClass(self):
            mldb.put("/v1/procedures/csv_proc", {
                        "type": "import.text",
                        "params": {
                            'dataFileUrl' : 'file://mldb/testing/dataset/fasttext_train.csv',
                            "outputDataset": {
                                "id": "src_train",                    
                            },
                            "ignoreBadLines" : True,
                            "allowMultiLines" : True,
                            "structuredColumnNames" : True,
                            "limit" : 10000,
                        }
                    }) 


            mldb.put("/v1/procedures/baggify", {
                "type": "transform",
                "params": {
                    "inputData": """
                    select Theme, tokenize(lower(Body), {splitChars:' ,.:;«»[]()%!?', quoteChar:'', minTokenLength: 2}) 
                    as tokens from src_train       
                    """,
                    "outputDataset": {
                        "id": "bag_of_words",
                        "type": "sparse.mutable"
                    },
                }
            })

        def test_fasttext(self):            

            cls_config = {
                "my_fasttext": {
                    "type": "fasttext",
                    "verbosity" : 0,
                    "dims" : 4,
                    "epoch" : 5,
                }
            }

            mldb.put("/v1/procedures/trainer", {
                "type": "classifier.train",
                "params": {
                    "trainingData": "SELECT {tokens.*} as features, Theme as label FROM bag_of_words",
                    "modelFileUrl": "file://tmp/src_fasttext.cls",
                    "functionName" : 'myclassify',
                    "algorithm": "my_fasttext",
                    "mode": "categorical",
                    "runOnCreation": True,
                    "configuration": cls_config
                }
            })

            res = mldb.query("SELECT myclassify({features : {tokenize(lower(' hockey '), {splitChars:' ,.:;«»[]()%!?', quoteChar:'', minTokenLength: 2}) as tokens} }) as * ")
            self.assertTableResultEquals(res, [
                [
                    "_rowName",
                    "scores.\"\"\"Politique\"\"\"",
                    "scores.\"\"\"Sports\"\"\""
                ],
                [
                    "result",
                    -0.7370663285255432,
                    -0.6548283100128174
                ]
            ]);

            res = mldb.query("SELECT myclassify({features : {tokenize(lower(' hillary '), {splitChars:' ,.:;«»[]()%!?', quoteChar:'', minTokenLength: 2}) as tokens} }) as * ")
            self.assertTableResultEquals(res, [
            [
                "_rowName",
                "scores.\"\"\"Politique\"\"\"",
                "scores.\"\"\"Sports\"\"\""
            ],
            [
                "result",
                -0.6585947871208191,
                -0.7329930067062378
            ]
        ]);


        def test_fasttext_regression_error(self):
            cls_config = {
                "my_fasttext": {
                    "type": "fasttext",
                    "verbosity" : 0,
                    "dims" : 4,
                    "epoch" : 5,
                }
            }

            msg = "FastText classifier does not currently support regression mode"
            with self.assertRaisesRegexp(mldb_wrapper.ResponseException, msg):
                mldb.put("/v1/procedures/trainer", {
                    "type": "classifier.train",
                    "params": {
                        "trainingData": "SELECT {tokens.*} as features, rowHash() as label FROM bag_of_words",
                        "modelFileUrl": "file://tmp/src_fasttext_error.cls",
                        "functionName" : 'myclassify',
                        "algorithm": "my_fasttext",
                        "mode": "regression",
                        "runOnCreation": True,
                        "configuration": cls_config
                    }
                })

        def test_fasttext_boolean(self):
            cls_config = {
                "my_fasttext": {
                    "type": "fasttext",
                    "verbosity" : 0,
                    "dims" : 4,
                    "epoch" : 5,
                }
            }

            mldb.put("/v1/procedures/trainer", {
                "type": "classifier.train",
                "params": {
                    "trainingData": """SELECT {tokens.*} as features, 
                                              Theme = 'Sports' as label 
                                       FROM bag_of_words
                                    """,
                    "modelFileUrl": "file://tmp/src_fasttext_error.cls",
                    "functionName" : 'myclassify',
                    "algorithm": "my_fasttext",
                    "mode": "boolean",
                    "runOnCreation": True,
                    "configuration": cls_config
                }
            })

            res = mldb.query("SELECT myclassify({features : {tokenize(lower(' hockey '), {splitChars:' ,.:;«»[]()%!?', quoteChar:'', minTokenLength: 2}) as tokens} }) as * ")
            self.assertTableResultEquals(res, [
                [
                    "_rowName",
                    "score"
                ],
                [
                    "result",
                    -0.6548283100128174
                ]
            ])

            res = mldb.query("SELECT myclassify({features : {tokenize(lower(' hillary '), {splitChars:' ,.:;«»[]()%!?', quoteChar:'', minTokenLength: 2}) as tokens} }) as * ")
            self.assertTableResultEquals(res, [
                [
                    "_rowName",
                    "score"
                ],
                [
                    "result",
                    -0.7329930067062378
                ]
            ])

        def test_fasttext_explain(self):

            mldb.log("explain")

            cls_config = {
                "my_fasttext": {
                    "type": "fasttext",
                    "verbosity" : 0,
                    "dims" : 4,
                    "epoch" : 5,
                }
            }

            mldb.put("/v1/procedures/trainer", {
                "type": "classifier.train",
                "params": {
                    "trainingData": "SELECT {tokens.*} as features, Theme as label FROM bag_of_words",
                    "modelFileUrl": "file://tmp/src_fasttext_explain.cls",
                    "functionName" : 'myclassify',
                    "algorithm": "my_fasttext",
                    "mode": "categorical",
                    "runOnCreation": True,
                    "configuration": cls_config
                }
            })
            
            mldb.put("/v1/functions/explain", {
                "type": "classifier.explain",
                "params": {
                    "modelFileUrl": "file://tmp/src_fasttext_explain.cls",
                }
            })

            res = mldb.query("""SELECT explain({features : {tokenize(lower(' hockey Alabama Futbol'), {splitChars:' ,.:;«»[]()%!?', quoteChar:'', minTokenLength: 2}) as tokens},
                                                label : 'Politique'}) as * 
                            """)

            self.assertTableResultEquals(res, [
                [
                    "_rowName",
                    "bias",
                    "explanation.tokens.alabama",
                    "explanation.tokens.futbol",
                    "explanation.tokens.hockey"
                ],
                [
                    "result",
                    0,
                    -0.006820799317210913,
                    -0.07053825259208679,
                    -0.08547607064247131
                ]
            ]);

            with self.assertRaisesRegexp(mldb_wrapper.ResponseException, "label not in model"):
                res = mldb.query("""SELECT explain({features : {tokenize(lower(' hockey Alabama Futbol'), {splitChars:' ,.:;«»[]()%!?', quoteChar:'', minTokenLength: 2}) as tokens},
                                                    label : 'Futurama'}) as * 
                                """)
 
mldb.run_tests()
