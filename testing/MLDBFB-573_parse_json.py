#
# MLDBFB-573_parse_json.py
# 12 juin 2016
# This file is part of MLDB. Copyright 2016 mldb.ai inc. All rights reserved.
#

import unittest

mldb = mldb_wrapper.wrap(mldb) # noqa

class MldbFb573(MldbUnitTest):  
    @classmethod
    def setUpClass(self):
        ds = mldb.create_dataset({ "id": "sample", "type": "sparse.mutable" })
        ds.record_row("a",[["x", '{"artist": "Champion Jack Dupree with TS McPhee", "timestamp": "2011-09-07 18:44:46.442194", "similars": [], "tags": [], "track_id": "TRAKMUG128F9328F8B", "title": "No Meat Blues"}', 0]])
        ds.commit()

        mldb.put("/v1/procedures/json_import", {
            "type": "import.text",
            "params": {
                "dataFileUrl": "file://mldb/testing/dataset/MLDB-1773_utf8.json",
                "headers": ["json"],
                "outputDataset": "utf8_js",
                "delimiter": "\n",
                "runOnCreation": True
            }
        })
        
        
    def test_ignore_errors(self):
        for arrays in ["parse", "encode"]:
            self.assertTableResultEquals(
                    mldb.query("select parse_json('{\"asdf:', {arrays: '%s', ignoreErrors:1}) as * from sample" % arrays),
                [[ "_rowName", "__parse_json_error__"],
                 ["a", True]])

    def test_null_input(self):
        self.assertTableResultEquals(
            mldb.query("select parse_json(y, {arrays: 'parse'}) as pwet from sample"),
            [[ "_rowName", "pwet"],
             ["a", None]])

    def test_null_arrays(self):
        with self.assertRaisesRegexp(mldb_wrapper.ResponseException,
            'NULL value found'):
            mldb.query("SELECT parse_json(x, {arrays: parse}) from sample")

    def test_parse_empty_list(self):
        self.assertTableResultEquals(
            mldb.query("SELECT parse_json(x, {arrays: 'encode'}) from sample"),
            [
                [
                    "_rowName",
                    "parse_json(x, {arrays: 'encode'}).artist",
                    "parse_json(x, {arrays: 'encode'}).timestamp",
                    "parse_json(x, {arrays: 'encode'}).title",
                    "parse_json(x, {arrays: 'encode'}).track_id"
                ],
                [
                    "a",
                    "Champion Jack Dupree with TS McPhee",
                    "2011-09-07 18:44:46.442194",
                    "No Meat Blues",
                    "TRAKMUG128F9328F8B"
                ]
            ]
        )
    
    def test_types(self):
        self.assertTableResultEquals(
            mldb.query("""
                select parse_json('[{"a": 5}, {"b": 2}]') as *
            """),
            [["_rowName", "0.a", "1.b"],
             ["result",5,2]]
        )

        self.assertTableResultEquals(
            mldb.query("""
                select parse_json('[{"a": 5}, {"b": 2}]', {arrays: 'encode'}) as *
            """),
            [["_rowName","0","1"],
             ["result","{\"a\":5}","{\"b\":2}"]]
        )

        self.assertTableResultEquals(
            mldb.query("""
                select parse_json('{}') as *
            """),
            [["_rowName"],["result"]]
        )
 
        self.assertTableResultEquals(
            mldb.query("""
                select parse_json('[]') as *
            """),
            [["_rowName"],["result"]]
        )

        self.assertTableResultEquals(
            mldb.query("""
                select parse_json(y) as nullz
                from (
                    row_dataset({x: 1})
                )
                """),
            [["_rowName","nullz"],
            ["0",None]]
        )

        with self.assertRaisesRegexp(mldb_wrapper.ResponseException, 'must be an object'):
            mldb.query("select parse_json('\"hola\"') as rez")
 
        with self.assertRaisesRegexp(mldb_wrapper.ResponseException, 'must be an object'):
            mldb.query("select parse_json(5) as rez")
 

    def test_utf8_parse(self):
        mldb.log(mldb.query("""
            select parse_json('{"data": '+ json + '}', {arrays: 'encode'}) from utf8_js
        """))


mldb.run_tests()

