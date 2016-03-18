#
# MLDB-1101-tf-idf.py
# Datacratic, 2015
# This file is part of MLDB. Copyright 2015 Datacratic. All rights reserved.
#
import unittest

mldb = mldb_wrapper.wrap(mldb) # noqa

class TfIdfTest(MldbUnitTest):

    @classmethod
    def setUpClass(self):
        dataset = mldb.create_dataset({ "id": "example", "type": "sparse.mutable" })
        #this is our corpus
        dataset.record_row(
            "row1", [["test", "peanut butter jelly peanut butter jelly", 0]])
        dataset.record_row(
            "row2", [["test", "peanut butter jelly time peanut butter jelly time", 0]])
        dataset.record_row(
            "row3", [["test", "this is the jelly song", 0]])
        
        dataset.commit()

        # baggify our words
        baggify_conf = {
            "type": "transform",
            "params": {
                "inputData": "select tokenize(test, {"
                "splitchars:' ', quotechar:'', min_token_length: 2}) "
                "as * from example",
                "outputDataset": {
                    "id": "bag_of_words",
                    "type": "sparse.mutable"
                },
                "runOnCreation": True
            }
        }
        mldb.put("/v1/procedures/baggify", baggify_conf)

        # The procedure will count the number of documents each word appears in
        tf_idf_conf = {
            "type": "tfidf.train",
            "params": {
                "trainingData": "select * from bag_of_words",
                "modelFileUrl": "file://tmp/MLDB-1101.idf",
                "outputDataset": {
                    "id": "tf_idf",
                    "type": "sparse.mutable"
                },
                "functionName": "tfidffunction",
                "runOnCreation": True
            }
        }
        mldb.put("/v1/procedures/tf_idf_proc", tf_idf_conf)

        func_conf = {
            "type": "tfidf",
            "params": {
                "modelFileUrl": "file://tmp/MLDB-1101.idf",
                "tfType" : "augmented",
                "idfType" : "inverseMax",
                }
        }
        mldb.put("/v1/functions/tfidffunction2", func_conf)

    @classmethod
    def get_column(self, result, column, row_index = 0):
        response = result.json()
        for col in response[row_index]['columns']:
            if col[0] == column:
                return col[1]
        assert False, 'failed to find column named: ' + column

    def test_document_frequencies(self):

        self.assertTableResultEquals(
            mldb.query("select * from tf_idf order by rowName() ASC"),
            [ [ "_rowName", "count"],
              ["butter", 2],
              ["is", 1],
              ["jelly", 3],
              ["peanut", 2],
              ["song", 1],
              ["the", 1],
              ["this", 1],
              ["time", 1] ]
        )
        
    def test_tfidf_lowerbound(self):
        rez = mldb.get('/v1/query', q="select tfidffunction({tokenize('time', {' ' as splitchars}) as input}) as *")
        mldb.log(rez)
        self.assertTrue(TfIdfTest.get_column(rez, "output.time") > 0, "expected positive tf-idf for 'time'")


    def check_relative_values(self, scorer):
        query = "select " + scorer + "({tokenize('jelly time butter butter bristol', " \
            "{' ' as splitchars}) as input}) as *"
        rez = mldb.get('/v1/query', q=query)
        mldb.log(rez)
        self.assertTrue(TfIdfTest.get_column(rez, "output.bristol") > TfIdfTest.get_column(rez, 'output.jelly'),
                        "'bristol' is more relevant than 'jelly' because it shows only in this document")
        self.assertTrue(TfIdfTest.get_column(rez, "output.butter") >= TfIdfTest.get_column(rez, 'output.jelly'), 
                        "'butter' more relevant than 'jelly' because it occurs moreoften")
        self.assertTrue(TfIdfTest.get_column(rez, 'output.time') > TfIdfTest.get_column(rez, 'output.jelly'),
                         "'time' should be more relevant than 'jelly' because its rarer in the corpus")

    def test_tfidf_relative_values(self):
        self.check_relative_values('tfidffunction')
        self.check_relative_values('tfidffunction2')
        rez = mldb.get("/v1/query",
                       q="select tfidffunction2({tokenize('jelly time', "
                       "{' ' as splitchars}) as input}) as *  "
                       "order by rowName() ASC")
        
        self.assertTrue(TfIdfTest.get_column(rez, 'output.time') > TfIdfTest.get_column(rez, 'output.jelly'),
                        "'time' should be more relevant than 'jelly' because its rarer in the corpus")

    def test_without_tokenize(self):
        rez = mldb.get("/v1/query",
                       q="select tfidffunction2({{jelly: 1, time : 1} as input}) as *  "
                       "order by rowName() ASC")

        self.assertTrue(TfIdfTest.get_column(rez, 'output.time') > TfIdfTest.get_column(rez, 'output.jelly'),
                        "'time' should be more relevant than 'jelly' because its rarer in the corpus")


mldb.run_tests()
