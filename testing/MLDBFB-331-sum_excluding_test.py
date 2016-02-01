#
# MLDBFB-331-sum_excluding_test.py
# Datacratic, 2015
# This file is part of MLDB. Copyright 2015 Datacratic. All rights reserved.
#
import unittest

mldb = mldb_wrapper.wrap(mldb) # noqa

class SumExcludingTest(unittest.TestCase):

    @classmethod
    def setUpClass(cls):
        dataset_config = {
            'type'    : 'sparse.mutable',
            'id'      : "example"
        }
        dataset = mldb.create_dataset(dataset_config)
        dataset.record_row("row1", [["colA", 1, 0]])
        dataset.record_row("row2", [["colA", 1, 0], ["colB", 1, 0]])
        dataset.commit()

    def test_it(self):
        mldb.query("SELECT sum({* EXCLUDING colB}) FROM example")

if __name__ == '__main__':
    mldb.run_tests()
