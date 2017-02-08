# -*- coding: utf-8 -*-

# This file is part of MLDB. Copyright 2015 mldb.ai inc. All rights reserved.

#
# python_cell_converter_test.py
# Sunil Rottoo - 26 mars 2015
# Copyright (c) 2015 mldb.ai inc. All rights reserved.
#

import py_cell_conv_test_module as Tester
import unittest


tester = Tester.Tester()

class PythonCellConverterTest(unittest.TestCase):
    def test_mldb_cell(self):
        val = u'Mutua Madrileña Madrid Open'
        self.assertEqual(tester.cellValueToCpp(val), True)

    def test_rest_params(self):
        val = [["warp", "10"]]
        self.assertEqual(tester.getRestParams(val), val)

        self.assertEqual(tester.getRestParamsFromCpp(), [["patate", "pwel"]])
        self.assertEqual(tester.getRestParams(tester.getRestParamsFromCpp()),
                [["patate", "pwel"]])

if __name__ == '__main__':
    unittest.main()

