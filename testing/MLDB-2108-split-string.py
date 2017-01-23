# -*- coding: utf-8 -*-
#
# MLDB-2108-split-string.py
# Mathieu Marquis Bolduc, 2017-01-11
# This file is part of MLDB. Copyright 2017 mldb.ai inc. All rights reserved.
#

mldb = mldb_wrapper.wrap(mldb)  # noqa

class MLDB2108SplitStringTest(MldbUnitTest):  # noqa

    def test_row(self):
        res = mldb.query("SELECT split_part(x, '::') AS x FROM (SELECT 'A::B::C' as x)")
        self.assertTableResultEquals(res, [
            [ "_rowName", "x.0", "x.1", "x.2" ],
            [ "result", "A", "B", "C" ]
        ])

    def test_row_no_as(self):
        res = mldb.query("""
            SELECT split_part(x, '::')
            FROM (SELECT 'A::B::C' as x)
        """)
        self.assertTableResultEquals(res, [
            [ "_rowName",
             "split_part(x, '::').0",
             "split_part(x, '::').1",
             "split_part(x, '::').2" ],
            [ "result", "A", "B", "C" ]
        ])

    def test_single(self):
        res = mldb.query("SELECT split_part(x, ' ')[\"2\"] AS x FROM (SELECT 'The Quick Brown Fox' as x)")
        self.assertTableResultEquals(res, [
            [ "_rowName", "x" ],
            [ "result", "Brown" ]
        ])

    #Those are anglo-saxon runes, and apparently it means "I can eat glass and it does not hurt"

    def test_utf8(self):
        res = mldb.query(u"SELECT split_part(x, ' ')[\"4\"] AS x FROM (SELECT 'ᛖᚴ ᚷᛖᛏ ᛖᛏᛁ ᚧ ᚷᛚᛖᚱ ᛘᚾ ᚦᛖᛋᛋ ᚨᚧ ᚡᛖ ᚱᚧᚨ ᛋᚨᚱ' as x)".encode('utf-8'))
        self.assertEqual(u"ᚷᛚᛖᚱ".encode('utf-8'), res[1][1].encode('utf-8'))

    def test_utf8_split(self):
        res = mldb.query(u"SELECT split_part(x, 'ᚧ')[\"4\"] AS x FROM (SELECT 'ᛖᚴᚧᚷᛖᛏᚧᛖᛏᛁᚧᚷᛚᛖᚱᚧᛘᚾᚧᚦᛖᛋᛋᚧᚨᚧᚧᚡᛖᚧᚱᚧᚨᚧᛋᚨᚱ' as x)".encode('utf-8'))
        self.assertEqual(u"ᛘᚾ".encode('utf-8'), res[1][1].encode('utf-8'))

if __name__ == '__main__':
    mldb.run_tests()
