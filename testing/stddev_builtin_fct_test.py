#
# stddev_builtin_fct_test.py
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

    def test_nan(self):
        ds = mldb.create_dataset({'id' : 'null_ds', 'type' : 'tabular'})
        ds.record_row('1', [['a', 1, 0]])
        ds.commit()
        res = mldb.query("SELECT stddev(b) FROM null_ds")
        self.assertEqual(res[1][1], "NaN")

        res = mldb.query("SELECT stddev(c) FROM null_ds")
        self.assertEqual(res[1][1], "NaN")

    def test_random_sequences(self):
        """
        Generate random sequences and compare the result with numpy.
        """
        lines = [""]
        lines.append(
            '| {:^4} | {:^20} | {:^20} | {:^20} | {:^10} | {:^3} |'
            .format('size', 'MLDB', 'Numpy', 'Diff (%)', 'Diff', 'Err'))
        err_cnt = 0
        for size in xrange(2, 100):
            ds = mldb.create_dataset({'id' : 'rand', 'type' : 'tabular'})
            sequence = []
            for row in xrange(size):
                sequence.append(random.random() * 1000000)
                ds.record_row(row, [['a', sequence[-1], 0]])
            ds.commit()

            mldb_res = mldb.query("SELECT stddev(a) FROM rand")[1][1]
            numpy_res = float(numpy.var(sequence, ddof=1))
            if (numpy_res == 0):
                mldb.log("Skipping case where numpy_re == 0")
            else:
                rx = abs(mldb_res - numpy_res) / numpy_res * 100
                err = ''
                # The diff % to consider MLDB is "too" off compared to numpy
                if rx > 0.0000000000005:
                    err_cnt += 1
                    err = 'ERR'
                    mldb.log(sequence)
                lines.append(
                    '| {:>4} | {:>20f} | {:>20f} | {:>.18f} | {:>10f} | {:^3} |'
                    .format(size, mldb_res, numpy_res, rx,
                            abs(mldb_res - numpy_res), err))
            mldb.delete('/v1/datasets/rand')
        mldb.log('\n'.join(lines))
        mldb.log(err_cnt)
        self.assertEqual(err_cnt, 0)

if __name__ == '__main__':
    mldb.run_tests()
