#
# MLDBFB-318_misleading_error_msg.py
# Mich, 2016-03-01
# This file is part of MLDB. Copyright 2016 Datacratic. All rights reserved.
#

import unittest

mldb = mldb_wrapper.wrap(mldb) # noqa


class MisleadingErrorMessageTest(MldbUnitTest): # noqa

    @unittest.expectedFailure
    def test_misleading_error_message(self):
        msg = "Function eat doesn't exist"
        with self.assertMldbRaises(status_code=400, expected_regexp=msg):
            mldb.get('/v1/query', q="SELECT eat('food')")


if __name__ == '__main__':
    mldb.run_tests()
