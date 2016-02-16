# This file is part of MLDB. Copyright 2015 Datacratic. All rights reserved.

#
# MLDB-1322-sum_stem_token.py
# Copyright (c) 2016 Datacratic Inc. All rights reserved.
#

mldb = mldb_wrapper.wrap(mldb) # noqa


import unittest
class HavingTest(unittest.TestCase):

    def test_having(self):

        ds = mldb.create_dataset({
            'type': 'text.csv.tabular',
            'id': 'titanic',
            'params':{
            'dataFileUrl':'https://raw.githubusercontent.com/datacratic/mldb-pytanic-plugin/master/titanic_train.csv'
        }})

        res = mldb.query('''
            select count(*) as x
            from titanic
            group by Sex, PClass, Embarked
            having count(*) > 5
        ''')

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
            assert row[1] < 64 and (row[2] > 5 or row[3] == 'C')

mldb.run_tests()
