#
# MLDB-1101-tf-idf.py
# mldb.ai inc, 2015
# This file is part of MLDB. Copyright 2015 mldb.ai inc. All rights reserved.
#
import unittest

mldb = mldb_wrapper.wrap(mldb) # noqa

class TfIdfTest(MldbUnitTest):

    # this is our corpus
    corpus = [
        'peanut butter jelly peanut butter jelly',
        'peanut butter jelly time peanut butter jelly time',
        'this is the jelly song']

    @classmethod
    def setUpClass(self):
        dataset = mldb.create_dataset({ "id": "example", "type": "sparse.mutable" })
        for idx, doc in enumerate(self.corpus):
            dataset.record_row(
            "row" + str(idx), [["test", doc, 0]])
        dataset.commit()

        # baggify our words
        baggify_conf = {
            "type": "transform",
            "params": {
                "inputData": "select tokenize(test, {"
                "splitChars:' ', quoteChar:'', minTokenLength: 2}) "
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

        augmented_inverse_func_conf = {
            "type": "tfidf",
            "params": {
                "modelFileUrl": "file://tmp/MLDB-1101.idf",
                "tfType" : "augmented",
                "idfType" : "inverseMax",
                }
        }
        mldb.put("/v1/functions/tfidffunction_augmented_inverse", augmented_inverse_func_conf)

        raw_inverse_func_conf = {
            "type": "tfidf",
            "params": {
                "modelFileUrl": "file://tmp/MLDB-1101.idf",
                "tfType" : "raw",
                "idfType" : "inverse",
                }
        }
        mldb.put("/v1/functions/tfidffunction_raw_inverse", raw_inverse_func_conf)

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
        rez = mldb.get('/v1/query', q="select tfidffunction({tokenize('time', {' ' as splitChars}) as input}) as *")
        mldb.log(rez)
        self.assertTrue(TfIdfTest.get_column(rez, "output.time") > 0, "expected positive tf-idf for 'time'")


    def check_relative_values(self, scorer):
        query = "select " + scorer + "({tokenize('jelly time butter butter bristol', " \
            "{' ' as splitChars}) as input}) as *"
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
        self.check_relative_values('tfidffunction_augmented_inverse')
        rez = mldb.get("/v1/query",
                       q="select tfidffunction_augmented_inverse({tokenize('jelly time', "
                       "{' ' as splitChars}) as input}) as *  ")

        self.assertTrue(TfIdfTest.get_column(rez, 'output.time') > TfIdfTest.get_column(rez, 'output.jelly'),
                        "'time' should be more relevant than 'jelly' because its rarer in the corpus")

    def test_without_tokenize(self):
        rez = mldb.get("/v1/query",
                       q="select tfidffunction_augmented_inverse({{jelly: 1, time : 1} as input}) as *  ")

        self.assertTrue(TfIdfTest.get_column(rez, 'output.time') > TfIdfTest.get_column(rez, 'output.jelly'),
                        "'time' should be more relevant than 'jelly' because its rarer in the corpus")

    def test_compare_with_expected_values(self):
        time_tfidf = 0.4054651 # 1 * ln(3/2)
        jelly_tfidf = -0.5753641 # 2 * ln(3/4)

        rez = mldb.get("/v1/query",
                       q="select tfidffunction_raw_inverse({{jelly: 2, time : 1} as input}) as *  ")

        mldb.log(rez)
        self.assertAlmostEqual(TfIdfTest.get_column(rez, 'output.time'), time_tfidf,
                        msg = "'time' tfidf is not equal to the one returned by scikit learn")
        self.assertAlmostEqual(TfIdfTest.get_column(rez, 'output.jelly'), jelly_tfidf,
                        msg = "'jelly' tfidg is not equal to the one returned by scikit learn")


mldb.run_tests()
