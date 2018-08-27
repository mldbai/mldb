#
# MLDBFB-199_invalid_script_test.py
# Mich, 2016-02-02
# This file is part of MLDB. Copyright 2016 mldb.ai inc. All rights reserved.
#
import unittest

from mldb import mldb, MldbUnitTest, ResponseException

class InvalidScriptTest(MldbUnitTest): # noqa

    @unittest.expectedFailure
    def test_it(self):
        with self.assertRaises(ResponseException): # noqa
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
    request.set_return(mldb.run_tests())
