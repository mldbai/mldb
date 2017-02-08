#
# MLDB-1788_select_col_as_star_err.py
# Francois-Michel L'Heureux, 2016-09-30
# This file is part of MLDB. Copyright 2016 mldb.ai inc. All rights reserved.
#

mldb = mldb_wrapper.wrap(mldb)  # noqa

class Mldb1788SelectColAsStarErr(MldbUnitTest):  # noqa

    @classmethod
    def setUpClass(cls):
        ds = mldb.create_dataset({'id' : 'ds', 'type' : 'sparse.mutable'})
        ds.record_row('row1', [['col', 1, 0]])
        ds.commit()

    @unittest.expectedFailure
    def test_it(self):
        msg = 'TO BE DETERMINED'
        with self.assertRaisesRegexp(mldb_wrapper.ResponseException, msg) as exc:
            mldb.query("SELECT col AS * FROM ds")

        self.assertEqual(exc.exception.response.status_code, 400)

if __name__ == '__main__':
    mldb.run_tests()
