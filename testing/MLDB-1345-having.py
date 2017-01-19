# This file is part of MLDB. Copyright 2015 mldb.ai inc. All rights reserved.

#
# MLDB-1322-sum_stem_token.py
# Copyright (c) 2016 mldb.ai inc. All rights reserved.
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
            select count(*) as x
            from titanic
            group by Sex, PClass, Embarked
            having count(*) > 5
        ''')

        mldb.log(res)

        for row in res[1:]:
            assert row[1] > 5

        res = mldb.query('''
            select max(Age), count(*), Embarked 
            from titanic
            group by Sex, Embarked
            having max(Age) < 64 and (count(*) > 5 or Embarked ='C')
            ''')
        mldb.log(res)

        for row in res[1:]:
            assert row[3] < 64 and (row[2] > 5 or row[1] == 'C')

mldb.run_tests()
