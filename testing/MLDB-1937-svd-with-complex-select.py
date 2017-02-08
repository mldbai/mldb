#
# MLDB-1937-svd-with-complex-select.py
# Guy Dumais, 2016-09-14
# This file is part of MLDB. Copyright 2016 mldb.ai inc. All rights reserved.
#

import random

mldb = mldb_wrapper.wrap(mldb)  # noqa

class MLDB1937SvdWithComplexSelect(MldbUnitTest):  # noqa

    @classmethod
    def setUpClass(cls):
        dataset = mldb.create_dataset({
            "type": "tabular",
            "id": "data"
        })
        for r in range(100):
            rand = random.randint(0, 100)
            dataset.record_row(
                "r%d"%r,
                [ ["a", rand, 0], ["b", "test" if rand % 3 == 0 else "TEST" if rand % 3 == 1 else "Lombric", 0] ]
            )
        dataset.commit()


    def test_svd_with_function_calls(self):
        # this is using the fact that SVD training on string values
        # creates a new sparse column for each string value seens
        mldb.put('/v1/procedures/train_svd', {
            "type" : "svd.train",
            "params" : {
                "trainingData": "select a, lower(b) from data",
                "rowOutputDataset": "svd_row_embedding",
                "columnOutputDataset": "svd_column_embedding",
                "modelFileUrl": "file://tmp/MLDB-1937-svd-with-complex-select.svd"
            }
        })

        # column rowName_, a, lower(b).stringEquals.test and lower(b).stringEquals.lombric
        self.assertEqual(len(mldb.query("select * from svd_column_embedding")[0]), 4)

    def test_svd_with_aritmetic_ops(self):
        mldb.put('/v1/procedures/train_svd', {
            "type" : "svd.train",
            "params" : {
                "trainingData": "select a + 2, b from data",
                "rowOutputDataset": "svd_row_embedding",
                "columnOutputDataset" : "svd_column_embedding"
            }
        })

        self.assertEqual(len(mldb.query("select * from svd_column_embedding")[0]), 5)

    def test_svd_with_column_expression(self):
        mldb.put('/v1/procedures/train_svd', {
            "type" : "svd.train",
            "params" : {
                "trainingData": "select column expr(where rowCount() = 100) from data",
                "rowOutputDataset": "svd_row_embedding",
                "columnOutputDataset" : "svd_column_embedding"
            }
        })

        self.assertEqual(len(mldb.query("select * from svd_column_embedding")[0]), 5)

        mldb.put('/v1/procedures/train_svd', {
            "type" : "svd.train",
            "params" : {
                "trainingData": "select column expr(where columnName() = 'a') from data",
                "rowOutputDataset": "svd_row_embedding",
                "columnOutputDataset" : "svd_column_embedding"
            }
        })

        mldb.log(mldb.query("select * from svd_column_embedding"))
        self.assertEqual(len(mldb.query("select * from svd_column_embedding")[0]), 1)

        mldb.put('/v1/procedures/train_svd', {
            "type" : "svd.train",
            "params" : {
                "trainingData": "select column expr(where columnName() = 'b') from data",
                "rowOutputDataset": "svd_row_embedding",
                "columnOutputDataset" : "svd_column_embedding"
            }
        })

        self.assertEqual(len(mldb.query("select * from svd_column_embedding")[0]), 4)


if __name__ == '__main__':
    mldb.run_tests()
