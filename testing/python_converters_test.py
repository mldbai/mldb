#
# python_converters_test.py
# Francois Maillet - 10 mars 2015
# This file is part of MLDB. Copyright 2015 mldb.ai inc. All rights reserved.
#

import py_conv_test_module as Tester
import unittest


tester = Tester.Tester()

class PythonConverterTest(unittest.TestCase):
    def test_string(self):
        val = "I am a STRINGGG"
        self.assertEqual(tester.getString(val), val)

    def test_float(self):
        val = 42.5
        self.assertEqual(tester.getFloat(val), val)
    
    def test_int(self):
        val = 42
        self.assertEqual(tester.getFloat(val), val)

    def test_vector_string(self):
        val = ["a", "b", "c"]
        self.assertEqual(tester.getVectorString(val), val)

    def test_pair_string_int(self):
        val = ["a", 45]
        self.assertEqual(tester.getPairStringInt(val), val)

    def test_json_val_to_cpp(self):
        val = {"hoho": 5}
        self.assertEqual(tester.getJsonVal(val), val)
        
        val = {"hoho": [5, 6, "asdf"], "flt": 5.5, "int": 5}
        rtn_val = tester.getJsonVal(val)
        self.assertEqual(rtn_val, val)
        self.assertEqual(type(val["int"]), type(rtn_val["int"]))
        self.assertEqual(type(val["flt"]), type(rtn_val["flt"]))
        
        val = {"hoho": {"a":5, "b":[5, 6, "asdf"]}}
        self.assertEqual(tester.getJsonVal(val), val)
        
        val = {"hoho": {"a":5, "b":[5, 6, {"zz": True}]}}
        print tester.getJsonVal(val)
        self.assertEqual(tester.getJsonVal(val), val)

if __name__ == '__main__':
    unittest.main()

