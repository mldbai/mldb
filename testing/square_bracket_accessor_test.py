# -*- coding: utf-8 -*-
#
# square_bracket_accessor_test.py
# Francois-Michel L'Heureux, 2017-01-27
# This file is part of MLDB. Copyright 2017 mldb.ai inc. All rights reserved.
#
import unittest

mldb = mldb_wrapper.wrap(mldb)  # noqa

class Mldb2105SelectExprSquareBracketColNameSupport(MldbUnitTest):  # noqa

    def test_string_with_match(self):
        """
        It doesn't matter that it maches, it's a string.
        """
        res = mldb.query("SELECT a['foo'] FROM (SELECT {foo: 123} AS a)")
        self.assertEqual(res, [
            ['_rowName', "a['foo']"],
            ['result', 'foo']
        ])

    def test_string_no_match(self):
        res = mldb.query("SELECT a['bar'] FROM (SELECT {foo: 123} AS a)")
        self.assertEqual(res, [
            ['_rowName', "a['bar']"],
            ['result', 'bar']
        ])

    def test_string_chaining(self):
        res = mldb.query("SELECT a['f']['b']['z'] FROM (SELECT {f: {b: 123}} AS a)")
        self.assertEqual(res, [
            ['_rowName', "a['f']['b']['z']"],
            ['result', 'z']
        ])

    def test_string_mixing(self):
        res = mldb.query("SELECT a.f['b'] FROM (SELECT {f: {b: 123}} AS a)")
        self.assertEqual(res, [
            ['_rowName', u'"a.f[\'b\']"'],
            ['result', 'b']
        ])

        with self.assertRaises(mldb_wrapper.ResponseException):
            res = mldb.query("SELECT a['f'].b FROM (SELECT {f: {b: 123}} AS a)")

    def test_string_outter_whitespaces(self):
        res = mldb.query("SELECT a[ 'foo'   ] FROM (SELECT {foo: 123} AS a)")
        self.assertEqual(res, [
            ['_rowName', "a[ 'foo'   ]"],
            ['result', 'foo']
        ])

    def test_string_inner_whitespaces(self):
        res = mldb.query("""
            SELECT a['f b'] FROM (
                SELECT parse_json('{"f b" : 123}') AS a
            )
                         """)
        self.assertEqual(res, [
            ['_rowName', "a['f b']"],
            ['result', 'f b']
        ])

    def test_string_utf8(self):
        res = mldb.query("""
            SELECT a['utéf8'] FROM (
                SELECT parse_json('{"utéf8" : 123}') AS a
            )
                         """)
        self.assertEqual(res, [
            [u'_rowName', u"a['utéf8']"],
            [u'result', u'utéf8']
        ])

    @unittest.expectedFailure
    def test_match_no_double_quotes(self):
        """
        It doesn't matter that it maches, it's a string.
        """
        res = mldb.query("SELECT a[foo] FROM (SELECT {foo: 123} AS a)")
        self.assertEqual(res, [
            ['_rowName', "a[foo]"],
            ['result', 123]
        ])

    @unittest.expectedFailure
    def test_match_w_double_quotes(self):
        """
        It doesn't matter that it maches, it's a string.
        """
        res = mldb.query("""SELECT a["foo"] FROM (SELECT {foo: 123} AS a)""")
        self.assertEqual(res, [
            ['_rowName', "a[foo]"],
            ['result', 123]
        ])

    @unittest.expectedFailure
    def test_no_match_no_quotes(self):
        res = mldb.query("SELECT a[bar] FROM (SELECT {foo: 123} AS a)")
        self.assertEqual(res, [
            ['_rowName', "a[bar]"],
            ['result', None]
        ])

    @unittest.expectedFailure
    def test_no_match_double_quotes(self):
        res = mldb.query("""SELECT a["bar"] FROM (SELECT {foo: 123} AS a)""")
        self.assertEqual(res, [
            ['_rowName', "a[bar]"],
            ['result', None]
        ])

    @unittest.expectedFailure
    def test_chaining_no_quotes(self):
        res = mldb.query("SELECT a[f][b] FROM (SELECT {f: {b: 123}} AS a)")
        self.assertEqual(res, [
            ['_rowName', "a[f][b]"],
            ['result', 123]
        ])

    @unittest.expectedFailure
    def test_chaining_double_quotes(self):
        res = mldb.query("""
            SELECT a["f"]["b"] FROM (SELECT {f: {b: 123}} AS a)""")
        self.assertEqual(res, [
            ['_rowName', "a[f][b]"],
            ['result', 123]
        ])

    @unittest.expectedFailure
    def test_mixing_no_quotes(self):
        res = mldb.query("SELECT a.f[b] FROM (SELECT {f: {b: 123}} AS a)")
        self.assertEqual(res, [
            ['_rowName', 'a.f[b]'],
            ['result', 123]
        ])

        res = mldb.query("SELECT a[f].b FROM (SELECT {f: {b: 123}} AS a)")
        self.assertEqual(res, [
            ['_rowName', 'a.f[b]'],
            ['result', 123]
        ])

    @unittest.expectedFailure
    def test_mixing_double_quotes(self):
        res = mldb.query(
            """SELECT a.f["b"] FROM (SELECT {f: {b: 123}} AS a)""")
        self.assertEqual(res, [
            ['_rowName', 'a.f[b]'],
            ['result', 123]
        ])

        res = mldb.query(
            """SELECT a["f"].b FROM (SELECT {f: {b: 123}} AS a)""")
        self.assertEqual(res, [
            ['_rowName', 'a.f[b]'],
            ['result', 123]
        ])

    @unittest.expectedFailure
    def test_outter_whitespaces_no_quotes(self):
        res = mldb.query("SELECT a[ foo   ] FROM (SELECT {foo: 123} AS a)")
        self.assertEqual(res, [
            ['_rowName', "a[foo]"],
            ['result', 123]
        ])

    @unittest.expectedFailure
    def test_outter_whitespaces_double_quotes(self):
        res = mldb.query(
            """SELECT a[ "foo"   ] FROM (SELECT {foo: 123} AS a)""")
        self.assertEqual(res, [
            ['_rowName', "a[foo]"],
            ['result', 123]
        ])

    @unittest.expectedFailure
    def test_inner_whitespaces_no_quotes(self):
        res = mldb.query("""
            SELECT a[f b] FROM (
                SELECT parse_json('{"f b" : 123}') AS a
            )
                         """)
        self.assertEqual(res, [
            ['_rowName', "a[f b]"],
            ['result', 123]
        ])
    @unittest.expectedFailure
    def test_inner_whitespaces_double_quotes(self):
        res = mldb.query("""
            SELECT a["f b"] FROM (
                SELECT parse_json('{"f b" : 123}') AS a
            )
                         """)
        self.assertEqual(res, [
            ['_rowName', "a[f b]"],
            ['result', 123]
        ])


    @unittest.expectedFailure
    def test_utf8_no_quotes(self):
        res = mldb.query("""
            SELECT a[utéf8] FROM (
                SELECT parse_json('{"utéf8" : 123}') AS a
            )
                         """)
        self.assertEqual(res, [
            [u'_rowName', u"a[utéf8]"],
            [u'result', 123]
        ])

    @unittest.expectedFailure
    def test_utf8_double_quotes(self):
        res = mldb.query("""
            SELECT a["utéf8"] FROM (
                SELECT parse_json('{"utéf8" : 123}') AS a
            )
                         """)
        self.assertEqual(res, [
            [u'_rowName', u"a[utéf8]"],
            [u'result', 123]
        ])

    def test_internal_ws_no_quotes(self):
        with self.assertRaises(mldb_wrapper.ResponseException):
            mldb.query(
                """SELECT a[foo bar] FROM (SELECT {"foo bar": 123} AS a)""")

    @unittest.expectedFailure
    def test_internal_ws_double_quotes(self):
        mldb.query(
            """SELECT a["foo bar"] FROM (SELECT {"foo bar": 123} AS a)""")
        self.assertEqual(res, [
            ['_rowName', """a["foo bar"]"""],
            ['result', 123]
        ])


if __name__ == '__main__':
    mldb.run_tests()
