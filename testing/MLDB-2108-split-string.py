#
# MLDB-2108-split-string.py
# Mathieu Marquis Bolduc, 2017-01-11
# This file is part of MLDB. Copyright 2017 Datacratic. All rights reserved.
#

mldb = mldb_wrapper.wrap(mldb)  # noqa

class MLDB2108SplitStringTest(MldbUnitTest):  # noqa

    @classmethod
    def setUpClass(cls):
        ds = mldb.create_dataset({'id' : 'ds', 'type' : 'sparse.mutable'})
        ds.record_row('row0', [['x', 'A', 0]])
        ds.record_row('row1', [['x', 'B', 0]])
        ds.commit()

    def test_row(self):
        res = mldb.query("SELECT token_split(x, '::') AS x FROM (SELECT 'A::B::C' as x)")
        self.assertTableResultEquals(res, [
            [ "_rowName", "x.0", "x.1", "x.2" ],
            [ "result", "A", "B", "C" ]
        ])

    def test_row(self):
        res = mldb.query("SELECT token_split(x, ' ')[2] AS x FROM (SELECT 'The Quick Brown Fox' as x)")
        self.assertTableResultEquals(res, [
            [ "_rowName", "x" ],
            [ "result", 2 ]
        ])

if __name__ == '__main__':
    mldb.run_tests()
