#
# MLDB-2108-split-string.py
# Mathieu Marquis Bolduc, 2017-01-11
# This file is part of MLDB. Copyright 2017 Datacratic. All rights reserved.
#

mldb = mldb_wrapper.wrap(mldb)  # noqa

class MLDB2108SplitStringTest(MldbUnitTest):  # noqa

    def test_row(self):
        res = mldb.query("SELECT token_split(x, '::') AS x FROM (SELECT 'A::B::C' as x)")
        self.assertTableResultEquals(res, [
            [ "_rowName", "x.0", "x.1", "x.2" ],
            [ "result", "A", "B", "C" ]
        ])

    def test_single(self):
        res = mldb.query("SELECT token_split(x, ' ')[\"2\"] AS x FROM (SELECT 'The Quick Brown Fox' as x)")
        self.assertTableResultEquals(res, [
            [ "_rowName", "x" ],
            [ "result", "Brown" ]
        ])

if __name__ == '__main__':
    mldb.run_tests()
