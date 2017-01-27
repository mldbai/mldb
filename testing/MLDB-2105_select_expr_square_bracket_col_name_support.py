# -*- coding: utf-8 -*-
#
# MLDB-2105_select_expr_square_bracket_col_name_support.py
# Francois-Michel L'Heureux, 2017-01-27
# This file is part of MLDB. Copyright 2017 mldb.ai inc. All rights reserved.
#

mldb = mldb_wrapper.wrap(mldb)  # noqa

class Mldb2105SelectExprSquareBracketColNameSupport(MldbUnitTest):  # noqa

    def test_match(self):
        res = mldb.query("SELECT a['foo'] FROM (SELECT {foo: 123} AS a)")
        self.assertEqual(res, [
            ['_rowName', 'a.foo'],
            ['result', 123]
        ])

    def test_no_match(self):
        res = mldb.query("SELECT a['bar'] FROM (SELECT {foo: 123} AS a)")
        self.assertEqual(res, [
            ['_rowName', 'a.bar'],
            ['result', None]
        ])

    def test_chaining(self):
        res = mldb.query("SELECT a['f']['b'] FROM (SELECT {f: {b: 123}} AS a)")
        self.assertEqual(res, [
            ['_rowName', 'a.f.b'],
            ['result', 123]
        ])

    def test_mixing(self):
        res = mldb.query("SELECT a.f['b'] FROM (SELECT {f: {b: 123}} AS a)")
        self.assertEqual(res, [
            ['_rowName', 'a.f.b'],
            ['result', 123]
        ])

        res = mldb.query("SELECT a['f'].b FROM (SELECT {f: {b: 123}} AS a)")
        self.assertEqual(res, [
            ['_rowName', 'a.f.b'],
            ['result', 123]
        ])

    def test_pitter_whitespaces_ignored(self):
        res = mldb.query("SELECT a[ 'foo'   ] FROM (SELECT {foo: 123} AS a)")
        self.assertEqual(res, [
            ['_rowName', 'a.foo'],
            ['result', 123]
        ])

    def test_inner_whitespaces(self):
        res = mldb.query("""
            SELECT a['f b'] FROM (
                SELECT parse_json('{"f b" : 123}') AS a
            )
                         """)
        self.assertEqual(res, [
            ['_rowName', 'a.f b'],
            ['result', 123]
        ])

    def test_utf8(self):
        res = mldb.query("""
            SELECT a['utéf8'] FROM (
                SELECT parse_json('{"utéf8" : 123}') AS a
            )
                         """)
        self.assertEqual(res, [
            [u'_rowName', u'a.utéf8'],
            [u'result', 123]
        ])


if __name__ == '__main__':
    mldb.run_tests()
