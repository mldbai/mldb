#
# MLDB-1710-left-right-rowname.py
# Mathieu Marquis Bolduc, 2016-15-06
# This file is part of MLDB. Copyright 2016 mldb.ai inc. All rights reserved.
#

import unittest
import json

mldb = mldb_wrapper.wrap(mldb) # noqa

class LikeTest(MldbUnitTest):

    @classmethod
    def setUpClass(cls):
        ds = mldb.create_dataset({'id' : 'ds1', 'type' : 'sparse.mutable'})
        ds.record_row('x', [['a', 1, 0]])
        ds.commit()

        ds = mldb.create_dataset({'id' : 'ds2', 'type' : 'sparse.mutable'})
        ds.record_row('y', [['a', 1, 0]])
        ds.commit()

        ds = mldb.create_dataset({'id' : 'ds3', 'type' : 'sparse.mutable'})
        ds.record_row('z', [['a', 1, 0]])
        ds.commit()

    def test_left(self):

        res1 = mldb.query("select leftRowName() from ds1 join ds2")
        mldb.log(res1)

        expected = [["_rowName", "leftRowName()"],["[x]-[y]","x"]]

        self.assertEqual(res1, expected);

    def test_right(self):

        res1 = mldb.query("select rightRowName() from ds1 join ds2")
        mldb.log(res1)

        expected = [["_rowName", "rightRowName()"],["[x]-[y]","y"]]

        self.assertEqual(res1, expected);

    def test_nojoin(self):
        with self.assertMldbRaises(expected_regexp="Function 'leftRowName' is not available outside of a join"):
            res1 = mldb.query("select leftRowName() from ds1")

        with self.assertMldbRaises(expected_regexp="Function 'rightRowName' is not available outside of a join"):
            res1 = mldb.query("select rightRowName() from ds1")

    def test_nested(self):

        res1 = mldb.query("select leftRowName() from (ds1 join ds2) join ds3")
        mldb.log(res1)

        expected = [["_rowName","leftRowName()"],["[x]-[y]-[z]","[x]-[y]"]]

        self.assertEqual(res1, expected);

        res1 = mldb.query("select rightRowName() from (ds1 join ds2) join ds3")
        mldb.log(res1)

        expected = [["_rowName","rightRowName()"],["[x]-[y]-[z]","z"]]

        self.assertEqual(res1, expected);

    def test_outer(self):

        res1 = mldb.query("select leftRowName() from ds1 left join ds2 on ds1.a = ds2.a + 1")
        mldb.log(res1)

        expected = [["_rowName", "leftRowName()"],["[x]-[]","x"]]

        self.assertEqual(res1, expected);

        res1 = mldb.query("select rightRowName() from ds1 left join ds2 on ds1.a = ds2.a + 1")
        mldb.log(res1)

        expected = [["_rowName", "rightRowName()"], ["[x]-[]", '""']]

        self.assertEqual(res1, expected);

mldb.run_tests()
