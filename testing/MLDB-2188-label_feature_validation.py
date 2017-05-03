# -*- coding: utf-8 -*-

# ##
# Mathieu Marquis Bolduc, May 1st 2017
# This file is part of MLDB. Copyright 2017 mldb.ai inc. All rights reserved.
# ##

mldb = mldb_wrapper.wrap(mldb) # noqa

import unittest

class LabelFeatureValidationTest(MldbUnitTest):

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

    def test_categorical(self):

        cls_config = {
                "my_fasttext": {
                    "type": "fasttext",
                    "verbosity" : 0,
                    "dims" : 4,
                    "epoch" : 5,
                }
            }

        with self.assertMldbRaises(expected_regexp="Dataset column 'Theme' is used in both label and feature"):
            mldb.put("/v1/procedures/trainer", {
                "type": "classifier.train",
                "params": {
                    "trainingData": "SELECT {Theme} as features, Theme as label FROM bag_of_words",
                    "modelFileUrl": "file://tmp/src_fasttext.cls",
                    "functionName" : 'myclassify',
                    "algorithm": "my_fasttext",
                    "mode": "categorical",
                    "runOnCreation": True,
                    "configuration": cls_config
                }
            })

    def test_categorical_expression(self):

        cls_config = {
                "my_fasttext": {
                    "type": "fasttext",
                    "verbosity" : 0,
                    "dims" : 4,
                    "epoch" : 5,
                }
            }

        with self.assertMldbRaises(expected_regexp="classifier.train only accepts wildcard and column names"):
            mldb.put("/v1/procedures/trainer", {
                "type": "classifier.train",
                "params": {
                    "trainingData": "SELECT {Theme} as features, Theme + '_catsup' as label FROM bag_of_words",
                    "modelFileUrl": "file://tmp/src_fasttext.cls",
                    "functionName" : 'myclassify',
                    "algorithm": "my_fasttext",
                    "mode": "categorical",
                    "runOnCreation": True,
                    "configuration": cls_config
                }
            })
    
    def test_wildcard(self):

        cls_config = {
                "my_fasttext": {
                    "type": "fasttext",
                    "verbosity" : 0,
                    "dims" : 4,
                    "epoch" : 5,
                }
            }

        with self.assertMldbRaises(expected_regexp="Dataset column 'tokens.alabama' is used in both label and feature because of label wildcard 'tokens'"):
            mldb.put("/v1/procedures/trainer", {
                "type": "classifier.train",
                "params": {
                    "trainingData": "SELECT {tokens.*} as features, {tokens.*} as label FROM bag_of_words",
                    "modelFileUrl": "file://tmp/src_fasttext.cls",
                    "functionName" : 'myclassify',
                    "algorithm": "my_fasttext",
                    "mode": "multilabel",
                    "runOnCreation": True,
                    "configuration": cls_config
                }
            })

    def test_wrong_excluding(self):

        cls_config = {
                "my_fasttext": {
                    "type": "fasttext",
                    "verbosity" : 0,
                    "dims" : 4,
                    "epoch" : 5,
                }
            }

        # This is the correct excluding 
        mldb.put("/v1/procedures/trainer", {
            "type": "classifier.train",
            "params": {
                "trainingData": "SELECT {* EXCLUDING (tokens.*)} as features, {tokens.* as *} as label FROM bag_of_words",
                "modelFileUrl": "file://tmp/src_fasttext.cls",
                "functionName" : 'myclassify',
                "algorithm": "my_fasttext",
                "mode": "multilabel",
                "runOnCreation": True,
                "configuration": cls_config
            }
        })

        # This only excludes "tokens" not "tokens.alabama" so it should raise
        with self.assertMldbRaises(expected_regexp="Dataset column 'tokens.alabama' is used in both label and feature because of label wildcard 'tokens'"):
            mldb.put("/v1/procedures/trainer", {
                "type": "classifier.train",
                "params": {
                    "trainingData": "SELECT {* EXCLUDING (tokens)} as features, {tokens.* as *} as label FROM bag_of_words",
                    "modelFileUrl": "file://tmp/src_fasttext.cls",
                    "functionName" : 'myclassify',
                    "algorithm": "my_fasttext",
                    "mode": "multilabel",
                    "runOnCreation": True,
                    "configuration": cls_config
                }
            })

    def test_wrong_excluding_structured(self):

        mldb.put("/v1/procedures/", {
        "type": "transform",
        "params": {
            "inputData": """
                SELECT *, {tokens.* as *} as label FROM bag_of_words
            """,
            "outputDataset": {
                "id": "bag_of_words2",
                "type": "sparse.mutable"
                },
            }
        })

        mldb.log(mldb.query("SELECT * FROM bag_of_words2"))

        cls_config = {
                "my_fasttext": {
                    "type": "fasttext",
                    "verbosity" : 0,
                    "dims" : 4,
                    "epoch" : 1,
                }
            }

        # This is the correct excluding 
        mldb.put("/v1/procedures/trainer", {
            "type": "classifier.train",
            "params": {
                "trainingData": "SELECT {* EXCLUDING (label.*)} as features, label FROM bag_of_words2",
                "modelFileUrl": "file://tmp/src_fasttext.cls",
                "functionName" : 'myclassify',
                "algorithm": "my_fasttext",
                "mode": "multilabel",
                "runOnCreation": True,
                "configuration": cls_config
            }
        }) 

        # This only excludes "label" not "label.alabama" so it should raise
        with self.assertMldbRaises(expected_regexp="Dataset column 'label.alabama' is used in both label and feature"):
            mldb.put("/v1/procedures/trainer", {
                "type": "classifier.train",
                "params": {
                    "trainingData": "SELECT {* EXCLUDING (label)} as features, label FROM bag_of_words2",
                    "modelFileUrl": "file://tmp/src_fasttext.cls",
                    "functionName" : 'myclassify',
                    "algorithm": "my_fasttext",
                    "mode": "multilabel",
                    "runOnCreation": True,
                    "configuration": cls_config
                }
            })


if __name__ == '__main__':
    mldb.run_tests()