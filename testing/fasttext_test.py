# -*- coding: utf-8 -*-

# ##
# Mathieu Marquis Bolduc, March 2nd 2017
# This file is part of MLDB. Copyright 2017 mldb.ai inc. All rights reserved.
# ##

mldb = mldb_wrapper.wrap(mldb) # noqa

class FastTextTest(MldbUnitTest):
        def test_fasttext(self):

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

            cls_config = {
                "my_fasttext": {
                    "type": "fasttext",
                    "verbosity" : 0,
                    "dims" : 4,
                    "epoch" : 5,
                }
            }

            mldb.log(
            mldb.put("/v1/procedures/trainer", {
                "type": "classifier.train",
                "params": {
                    "trainingData": """SELECT {tokens.*} as features, 
                                              Theme as label 
                                       FROM bag_of_words
                                    """,
                    "modelFileUrl": "file://tmp/src_fasttext.cls",
                    "functionName" : 'myclassify',
                    "algorithm": "my_fasttext",
                    "mode": "categorical",
                    "runOnCreation": True,
                    "configuration": cls_config
                }
            }))

            res = mldb.query("SELECT myclassify({features : {tokenize(lower(' hockey '), {splitChars:' ,.:;«»[]()%!?', quoteChar:'', minTokenLength: 2}) as tokens} }) as * ")
            mldb.log(res)
            self.assertTableResultEquals(res, [
                [
                    "_rowName",
                    "scores.\"\"\"Politique\"\"\"",
                    "scores.\"\"\"Sports\"\"\""
                ],
                [
                    "result",
                    -1.975644588470459,
                    -0.15154990553855896
                ]
            ]);

            res = mldb.query("SELECT myclassify({features : {tokenize(lower(' hillary '), {splitChars:' ,.:;«»[]()%!?', quoteChar:'', minTokenLength: 2}) as tokens} }) as * ")
            mldb.log(res)
            self.assertTableResultEquals(res, [
                [
                    "_rowName",
                    "scores.\"\"\"Politique\"\"\"",
                    "scores.\"\"\"Sports\"\"\""
                ],
                [
                    "result",
                    -0.10275973379611969,
                    -2.3465042114257812
                ]
            ]);

mldb.run_tests()
