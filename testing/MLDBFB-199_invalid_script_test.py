#
# MLDBFB-199_invalid_script_test.py
# Mich, 2016-02-02
# This file is part of MLDB. Copyright 2016 mldb.ai inc. All rights reserved.
#
import unittest

mldb = mldb_wrapper.wrap(mldb) # noqa

class InvalidScriptTest(MldbUnitTest): # noqa

    @unittest.expectedFailure
    def test_it(self):
        with self.assertRaises(mldb_wrapper.ResponseException): # noqa
            mldb.put('/v1/functions/foo', {
                "type": "script.apply",
                "params": {
                    "language": "python",
                    "scriptConfig": {
                        "source" : "This script source is foo bar!"
                    }
                }
            })

if __name__ == '__main__':
    mldb.run_tests()
