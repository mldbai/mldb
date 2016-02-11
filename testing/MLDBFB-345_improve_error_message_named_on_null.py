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

    @unittest.expectedFailure
    def test_non_working_case(self):
        # FIXME: It's ok to fail, but the error message should be helpful
        # currently returns "Can't convert from empty to string"
        expect = "Can't create a row with a null or empty name."
        with self.assertMldbRaises(expected_regexp=expect):
            mldb.query('SELECT * NAMED behA FROM ds')

if __name__ == '__main__':
    mldb.run_tests()
