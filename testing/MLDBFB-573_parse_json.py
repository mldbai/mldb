#
# MLDBFB-573_parse_json.py
# 12 juin 2016
# This file is part of MLDB. Copyright 2016 Datacratic. All rights reserved.
#

import unittest

mldb = mldb_wrapper.wrap(mldb) # noqa

class MldbFb573(MldbUnitTest):  
    @classmethod
    def setUpClass(self):
        ds = mldb.create_dataset({ "id": "sample", "type": "sparse.mutable" })
        ds.record_row("a",[["x", '{"artist": "Champion Jack Dupree with TS McPhee", "timestamp": "2011-09-07 18:44:46.442194", "similars": [], "tags":       [], "track_id": "TRAKMUG128F9328F8B", "title": "No Meat Blues"}', 0]])
        ds.commit()
    
    def test_null_arrays(self):
        with self.assertRaisesRegexp(mldb_wrapper.ResponseException,
            'got: NULL'):
            mldb.query("SELECT parse_json(x, {arrays: string}) from sample")
        
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

mldb.run_tests()

