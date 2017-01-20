#
# MLDB-1834_select_row_expr_star_err_msg.py
# Francois-Michel L Heureux, 2016-07-21
# This file is part of MLDB. Copyright 2016 mldb.ai inc. All rights reserved.
#

mldb = mldb_wrapper.wrap(mldb)  # noqa

class Mldb1834SelectRowExprStarErrMsg(MldbUnitTest):  # noqa

    def test_it(self):
        msg = "Cannot use wildcards with no FROM clause"
        with self.assertRaisesRegexp(mldb_wrapper.ResponseException, msg):
            mldb.query("SELECT {*}")

if __name__ == '__main__':
    mldb.run_tests()
