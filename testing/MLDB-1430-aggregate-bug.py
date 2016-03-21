#
# MLDB-1430-aggregate-bug.py
# Mathieu Bolduc, 2016-02-29
# This file is part of MLDB. Copyright 2016 Datacratic. All rights reserved.
#

mldb = mldb_wrapper.wrap(mldb) # noqa


import unittest
class HavingTest(unittest.TestCase):

    def test_having(self):

        csv_conf = {
            "type": "import.text",
            "params": {
                'dataFileUrl' : 'https://raw.githubusercontent.com/datacratic/mldb-pytanic-plugin/master/titanic_train.csv',
                "outputDataset": {
                    "id": "titanic",
                },
                "runOnCreation": True
            }
        }
        mldb.put("/v1/procedures/csv_proc", csv_conf)

        res = mldb.query('''
            select count(*) as x, sum(Age) / count(Age) as y, count(*) as z
            from titanic
        ''')

        expected = [["_rowName","x","y","z"],["[]",891,29.69911764705882,891]]

        self.assertEqual(res, expected)

    #MLDB-1478
    def test_error(self):

        csv_conf = {
            "type": "import.text",
            "params": {
                'dataFileUrl' : 'https://raw.githubusercontent.com/datacratic/mldb-pytanic-plugin/master/titanic_train.csv',
                "outputDataset": {
                    "id": "titanic2",
                },
                "runOnCreation": True
            }
        }
        mldb.put("/v1/procedures/csv_proc", csv_conf)

        with self.assertRaises(mldb_wrapper.ResponseException) as re:
            res = mldb.get("/v1/query", q='select COLUMN EXPR (WHERE regex_match(columnName(), "P.*") ) from titanic2')

        mldb.log(re.exception.response.json()["error"])

        expected = 'Binding error: Cannot read column "P.*" inside COLUMN EXPR.'

        self.assertEqual(re.exception.response.json()["error"], expected)

mldb.run_tests()
