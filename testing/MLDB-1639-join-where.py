#
# MLDB-1639-join-where.py
# Mathieu Marquis Bolduc, 2016-16-05
# This file is part of MLDB. Copyright 2016 mldb.ai inc. All rights reserved.
#

import unittest
import json

mldb = mldb_wrapper.wrap(mldb) # noqa

class SampleTest(MldbUnitTest):

    @classmethod
    def setUpClass(self):
        # create a dummy dataset
        ds = mldb.create_dataset({ "id": "dataset1", "type": "sparse.mutable" })
        ds.record_row("a",[["x", "toy story", 0]])
        ds.commit()
        
        ds = mldb.create_dataset({ "id": "dataset2", "type": "sparse.mutable" })
        ds.record_row("row_a",[["x", "toy story", 0]])
        ds.record_row("row_b",[["x", "terminator", 0]])
        ds.commit()
        
    def test_join_no_on_clause(self):

        res = mldb.query('select test1.x from (select \'toy story\' as x) as test1 join atom_dataset({"toy story": 1, "terminator": 5}) as test2 where regex_search(test1.x, test2.column)')
        mldb.log(res)

        expected = [["_rowName","test1.x"],["[result]-[1]","toy story"]]

        self.assertEqual(res, expected);

    def test_join_outer_where(self):

        res = mldb.query('select * from (select \'toy story\' as x) as test1 right join atom_dataset({"toy story": 1, "terminator": 5}) as test2 where CAST (test1.x AS PATH) = test2.column')
        mldb.log(res)

        expected = [["_rowName","test1.x","test2.column","test2.value"],["[result]-[1]","toy story","toy story",1]]

        mldb.log(expected)

        self.assertEqual(res, expected);

        # Flip it

        res = mldb.query('select * from atom_dataset({"toy story": 1, "terminator": 5}) as test2 left join (select \'toy story\' as x) as test1 where CAST (test1.x AS PATH) = test2.column')
        mldb.log(res)

        expected = [["_rowName","test1.x","test2.column","test2.value"],["[1]-[result]","toy story","toy story",1]]

        self.assertEqual(res, expected);

    def test_join_outer_on(self):

       res = mldb.query('select * from (select \'everythingisawesome\' as x) as test1 right join atom_dataset({"toy story": 1, "terminator": 5}) as test2 on CAST (test2.column AS STRING) = \'toy story\'')
       mldb.log(res)

       expected = [["_rowName","test2.column","test2.value","test1.x"],["[]-[0]","terminator",5,None],["[result]-[1]","toy story",1,"everythingisawesome"]]

       self.assertEqual(res, expected);

       res = mldb.query('select * from atom_dataset({"toy story": 1, "terminator": 5}) as test2 left join (select \'everythingisawesome\' as x) as test1 on CAST (test2.column AS STRING) = \'toy story\'')
       mldb.log(res)

       expected = [["_rowName","test2.column","test2.value","test1.x"],["[0]-[]","terminator",5,None],["[1]-[result]","toy story",1,"everythingisawesome"]]

       self.assertEqual(res, expected);

mldb.run_tests()
