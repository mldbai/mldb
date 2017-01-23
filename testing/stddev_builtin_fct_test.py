#
# stddev_builtin_fct_test.py
# Francois-Michel L Heureux, 2016-07-13
# This file is part of MLDB. Copyright 2016 mldb.ai inc. All rights reserved.
#
import unittest
import random
import math

mldb = mldb_wrapper.wrap(mldb)  # noqa

class StdDevBuiltinFctTest(MldbUnitTest):  # noqa

    @classmethod
    def setUpClass(cls):
        ds = mldb.create_dataset({'id' : 'ds', 'type' : 'sparse.mutable'})
        for i in xrange(100):
            ds.record_row('a%d-1' % i, [['a', 1, 0]])
            ds.record_row('a%d-2' % i, [['a', 2, 0]])
            ds.record_row('a%d-3' % i, [['a', 3, 0]])
            ds.record_row('a%d-4' % i, [['a', 10, 0]])
            ds.record_row('a%d-5' % i, [['a', 10, 0]])
        ds.commit()

    def test_base(self):
        var = 15.791583166332668
        res = mldb.query("SELECT variance(a) FROM ds")[1][1]
        self.assertAlmostEqual(res, var)

        res = mldb.query("SELECT vertical_variance(a) FROM ds")[1][1]
        self.assertAlmostEqual(res, var)

        res = mldb.query("SELECT stddev(a) FROM ds")[1][1]
        self.assertAlmostEqual(res, math.sqrt(var))

        res = mldb.query("SELECT vertical_stddev(a) FROM ds")[1][1]
        self.assertAlmostEqual(res, math.sqrt(var))

    def test_nan(self):
        ds = mldb.create_dataset({'id' : 'null_ds', 'type' : 'tabular'})
        ds.record_row('1', [['a', 1, 0]])
        ds.commit()
        res = mldb.query("SELECT stddev(b) FROM null_ds")
        self.assertEqual(res[1][1], "NaN")

        res = mldb.query("SELECT stddev(c) FROM null_ds")
        self.assertEqual(res[1][1], "NaN")

        res = mldb.query("SELECT variance(b) FROM null_ds")
        self.assertEqual(res[1][1], "NaN")

        res = mldb.query("SELECT variance(c) FROM null_ds")
        self.assertEqual(res[1][1], "NaN")

    @unittest.skip("Run manually if you want numpy comparison test")
    def test_random_sequences(self):
        """
        Generate random sequences and compare the result with numpy.
        """
        import numpy
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
            if (size == 20):
                mldb.log(sequence)

            mldb_res = mldb.query("SELECT stddev(a) FROM rand")[1][1]
            numpy_res = float(numpy.std(sequence, ddof=1))
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

    def test_pre_generated_sequence(self):
        """
        Use a sequence that was randomly generated.
        """
        sequence = [208427.44720839578, 457112.4117661105, 382059.51760122814,
                    665800.0456080714, 467338.1109353526, 213330.03276811822,
                    511618.87320035807, 479816.93290939386, 299103.40031107765,
                    473251.9045436747, 76189.30209577834, 886893.3898863205,
                    943297.756950757, 613434.874169999, 114575.37447960586,
                    683344.908275345, 719435.7021704618, 112303.13453557184,
                    646095.3802013887, 394881.5084234503]
        ds = mldb.create_dataset({
            'id' : 'pre_gen_seq_input',
            'type' : 'sparse.mutable'
        })
        for idx, num in enumerate(sequence):
            ds.record_row(idx, [['col', num, 0]])

        ds.commit()
        res = mldb.query("SELECT stddev(col) FROM pre_gen_seq_input")
        self.assertAlmostEqual(res[1][1], 249587.74043152996)

if __name__ == '__main__':
    mldb.run_tests()
