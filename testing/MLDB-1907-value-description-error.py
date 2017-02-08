#
# MLDB-1907-value-description-error.py
# Guy Dumais, 2016-08-24
# This file is part of MLDB. Copyright 2016 mldb.ai inc. All rights reserved.
#

mldb = mldb_wrapper.wrap(mldb)  # noqa

class MLDB1907ValueDescriptionError(MldbUnitTest):  # noqa

    @classmethod
    def setUpClass(cls):
        pass

    def test_value_desc_on_wrong_params(self):
        with self.assertRaisesRegexp(mldb_wrapper.ResponseException,
                                     'Binding builtin function sqrt: expected 1 argument, got 2'):
            mldb.query("select sqrt(2, NULL)")

        with self.assertRaisesRegexp(mldb_wrapper.ResponseException,
                                     'Binding builtin function sqrt: expected 1 argument, got 2'):
            mldb.query("select sqrt(2, 1)")

if __name__ == '__main__':
    mldb.run_tests()
