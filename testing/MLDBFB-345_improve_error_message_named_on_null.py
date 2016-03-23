#
# MLDBFB-345_improve_error_message_named_on_null.py
# Mich, 2016-02-01
# This file is part of MLDB. Copyright 2016 Datacratic. All rights reserved.
#
import unittest

mldb = mldb_wrapper.wrap(mldb) # noqa

class ImproveErrorMessageNamedOnNullTest(MldbUnitTest): # noqa

    @classmethod
    def setUpClass(cls):
        ds = mldb.create_dataset({
            'id' : 'ds',
            'type' : 'sparse.mutable'
        })

        ds.record_row('row1', [['behA', 'a', 0]])
        ds.record_row('row2', [['behB', 'b', 0]])
        ds.commit()

    def test_working_case(self):
        # works because we only work on the row where behA != null
        mldb.query('SELECT * NAMED behA FROM ds WHERE behA IS NOT NULL')

    def test_non_working_case(self):
        expect = "Can't create a row with a null or empty name."
        with self.assertMldbRaises(expected_regexp=expect) as re:
           mldb.query('SELECT * NAMED behA FROM ds')

    def test_non_working_case_2(self):
        expect = "Can't create a row with an empty name."
        with self.assertMldbRaises(expected_regexp=expect) as re:
           mldb.query("SELECT * NAMED '' FROM ds")

    def test_non_working_case_3(self):
        expect = "NAMED expression must evaluate to a single value"
        with self.assertMldbRaises(expected_regexp=expect) as re:
           mldb.query("SELECT * NAMED {1} FROM ds")

    def test_named_without_dataset(self):
        expect = [["_rowName","1"],["the one", 1 ]]
        res = mldb.query("SELECT 1 NAMED 'the one'")
        self.assertEqual(expect, res);

if __name__ == '__main__':
    mldb.run_tests()
