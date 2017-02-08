#
# MLDB-1718-invalid-utf-8-string-long.py
# Michael Burkat 2016-01-01
# This file is part of MLDB. Copyright 2016 mldb.ai inc. All rights reserved.
#

mldb = mldb_wrapper.wrap(mldb) # noqa

class Mldb1718(MldbUnitTest):
    def test_it(self):
        res = mldb.put("/v1/procedures/testset", {
            "type": "import.text",
            "params": {
                "dataFileUrl": "file://mldb/testing/dataset/MLDB-1718-long_string.txt",
                "delimiter": "\t",
                "headers": ['0'],
                "outputDataset": {
                    "id": "testset",
                    "type": "sparse.mutable",
                },
                "runOnCreation": True
            }
        })

        res = mldb.query("""SELECT "0" FROM testset WHERE rowName() = '1' """)
        self.assertEqual(res[1][1], "x")

        # test that the query works
        mldb.query("""
            SELECT count(*) FROM (SELECT "0" FROM testset) GROUP BY "0"
        """)

if __name__ == '__main__':
    mldb.run_tests()
