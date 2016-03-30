#
# MLDBFB-440_error_on_ds_wo_cols.py
# Mich, 2016-03-30
# Copyright (c) 2016 Datacratic Inc. All rights reserved.
#

import unittest

mldb = mldb_wrapper.wrap(mldb)  # noqa

class TestErrorOnDsWoCols(MldbUnitTest):  # noqa

    @unittest.expectedFailure
    def test_it(self):
        ds = mldb.create_dataset({'id' : 'noColDs', 'type' : 'sparse.mutable'})
        ds.record_row('row1', [])
        ds.commit()

        res = mldb.query("SELECT sum({*}) FROM noColDs")
        self.assertEqual(res[1][1], 0)


if __name__ == '__main__':
    mldb.run_tests()
