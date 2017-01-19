#
# mongo_temp_server_wrapping_test.py
# Mich, 2015-07-02
# This file is part of MLDB. Copyright 2015 mldb.ai inc. All rights reserved.
#

import unittest
import python_mongo_temp_server_wrapping

class MongoTempServerWrappingTest(unittest.TestCase):
    def test_monto_temp_server_wrapping(self):
        srv = python_mongo_temp_server_wrapping.MongoTemporaryServerPtr("", 0)
        srv.get_port_num()


if __name__ == '__main__':
    unittest.main()
