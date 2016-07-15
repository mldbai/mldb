#
# std_dev_builtin_fct_test.py
# Francois-Michel L Heureux, 2016-07-13
# This file is part of MLDB. Copyright 2016 Datacratic. All rights reserved.
#
import unittest
import numpy
import random

mldb = mldb_wrapper.wrap(mldb)  # noqa

class StdDevBuiltinFctTest(MldbUnitTest):  # noqa

    @classmethod
    def setUpClass(cls):
        ds = mldb.create_dataset({'id' : 'ds', 'type' : 'sparse.mutable'})
        ds.record_row('1', [['a', 1, 0]])
        ds.record_row('2', [['a', 2, 0]])
        ds.record_row('3', [['a', 3, 0]])
        ds.record_row('4', [['a', 10, 0]])
        ds.record_row('5', [['b', 10, 0]])
        ds.commit()

    def test_base(self):
        mldb_res = mldb.query("SELECT stddev(a) FROM ds")[1][1]
        numpy_res = numpy.var([1,2,3,10], ddof=1)
        float_res = float(numpy_res)
        self.assertAlmostEqual(mldb_res, float_res)

        mldb_res = mldb.query("SELECT vertical_stddev(a) FROM ds")[1][1]
        self.assertAlmostEqual(mldb_res, float_res)

    @unittest.expectedFailure # MLDB-1808
    def test_random_sequences(self):
        """
        Generate random sequences and compare the result with numpy.
        """
        for size in xrange(2, 100):
            ds = mldb.create_dataset({'id' : 'rand', 'type' : 'tabular'})
            sequence = []
            for row in xrange(size):
                sequence.append(random.random() * 1000000)
                ds.record_row(row, [['a', sequence[-1], 0]])
            ds.commit()

            mldb.log(sequence)
            mldb_res = mldb.query("SELECT stddev(a) FROM rand")[1][1]
            numpy_res = float(numpy.var(sequence, ddof=1))
            mldb.log("mldb:  {}".format(mldb_res))
            mldb.log("numpy: {}".format(numpy_res))
            self.assertAlmostEqual(mldb_res, numpy_res)
            ds.delete()
            mldb.log("Success with a size of {}".format(size))

    def test_nan(self):
        ds = mldb.create_dataset({'id' : 'null_ds', 'type' : 'tabular'})
        ds.record_row('1', [['a', 1, 0]])
        ds.commit()
        res = mldb.query("SELECT stddev(b) FROM null_ds")
        self.assertEqual(res[1][1], "NaN")

        res = mldb.query("SELECT stddev(c) FROM null_ds")
        self.assertEqual(res[1][1], "NaN")

    def test_some_numbers(self):
        ds = mldb.create_dataset({'id' : 'some_numbers', 'type' : 'tabular'})
        number_1 = 71218.50311678024
        number_2 = 255650.6226198759
        arr = ['a', number_1, 0]
        mldb.log(arr)
        ds.record_row('1', [arr])
        ds.record_row('2', [['a', number_2, 0]])
        ds.commit()
        res = mldb.query("SELECT stddev(a) FROM some_numbers")
        mldb.log(res)

        res = mldb.query("SELECT a FROM some_numbers ORDER BY rowName()")
        self.assertTableResultEquals(res, [
            ['_rowName', 'a'],
            ['1', number_1],
            ['2', number_2]
        ])

if __name__ == '__main__':
    mldb.run_tests()
