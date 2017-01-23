#
# MLDB-461_horizontal_ops_test.py
# mldb.ai inc, 2015
# this file is part of mldb. copyright 2015 mldb.ai inc. all rights reserved.
#
import unittest

mldb = mldb_wrapper.wrap(mldb) # noqa

class HorizontalTest(MldbUnitTest):

    sometime = '2016-01-02T12:23:34Z'
    before = '2016-01-01T12:23:34Z'
    after = '2016-01-03T12:23:34Z'

    @classmethod
    def setUpClass(self):
        # create a dummy dataset
        dataset = mldb.create_dataset({
            "type": "sparse.mutable",
            "id": "dataset"
        })

        # Horizontal functions first flatten to the latest value
        # of each column and then operate on the resulting row.
        # As an example, horizontal_count with first flatten col1 in row
        # x and then count the resulting atoms.  The count will therefore
        # be 3 not 4.
        dataset.record_row("x",[
            ["col1", 1, HorizontalTest.sometime],
            ["col1", 2, HorizontalTest.before],
            ["col2", 1, HorizontalTest.sometime],
            ["pwet", 1, HorizontalTest.sometime]])
        dataset.record_row("y",[
            ["col1", 0, HorizontalTest.sometime],
            ["col1", 1, HorizontalTest.after],
            ["col2", 1, HorizontalTest.sometime],
            ["prout", 1, HorizontalTest.sometime]])
        dataset.record_row("z",[
            ["col1", 5, HorizontalTest.sometime],
            ["col1", 1, HorizontalTest.before],
            ["col1", 10, HorizontalTest.after],
            ["col2", 1, HorizontalTest.sometime]])

        dataset.commit()


    def test_horizontal_count(self):
        resp = mldb.get("/v1/query", q="select horizontal_count({*}) from dataset order by rowName()").json()

        self.assertFullResultEquals(resp,
            [
                {
                    "rowName": "x", "rowHash": "4fc82888d13de398",
                    "columns": [ [ "horizontal_count({*})", 3, HorizontalTest.sometime ] ]
                },
                {
                    "rowName": "y", "rowHash": "75c422100f18a043",
                    "columns": [ [ "horizontal_count({*})", 3, HorizontalTest.after ] ]
                },
                {
                    "rowName": "z", "rowHash": "5e7ad9221c640e96",
                    "columns": [ [ "horizontal_count({*})", 2, HorizontalTest.after ] ]
                }
             ]
        )


    def test_horizontal_count_prefix(self):
        resp = mldb.get("/v1/query", q="select horizontal_count({p*}) from dataset order by rowName()").json()

        self.assertFullResultEquals(resp,
            [
                {
                    "rowName": "x", "rowHash": "4fc82888d13de398",
                    "columns": [ [ "horizontal_count({p*})", 1, HorizontalTest.sometime ] ]
                },
                {
                    "rowName": "y", "rowHash": "75c422100f18a043",
                    "columns": [ [ "horizontal_count({p*})", 1, HorizontalTest.sometime ] ]
                },
                {
                    "rowName": "z", "rowHash": "5e7ad9221c640e96",
                    "columns": [ [ "horizontal_count({p*})", 0, "-Inf" ] ]
                }
             ]
        )

    def test_horizontal_sum(self):
        resp = mldb.get("/v1/query", q="select horizontal_sum({*}) from dataset order by rowName()").json()

        self.assertFullResultEquals(resp,
            [
                {
                    "rowName": "x", "rowHash": "4fc82888d13de398",
                    "columns": [ [ "horizontal_sum({*})", 3, HorizontalTest.sometime ] ]
                },
                {
                    "rowName": "y", "rowHash": "75c422100f18a043",
                    "columns": [ [ "horizontal_sum({*})", 3, HorizontalTest.after ] ]
                },
                {
                    "rowName": "z", "rowHash": "5e7ad9221c640e96",
                    "columns": [ [ "horizontal_sum({*})", 11, HorizontalTest.after ] ]
                }
             ]
        )

    def test_horizontal_avg(self):
        resp = mldb.get("/v1/query", q="select horizontal_avg({*}) from dataset order by rowName()").json()

        self.assertFullResultEquals(resp,
            [
                {
                    "rowName": "x", "rowHash": "4fc82888d13de398",
                    "columns": [ [ "horizontal_avg({*})", 1, HorizontalTest.sometime ] ]
                },
                {
                    "rowName": "y", "rowHash": "75c422100f18a043",
                    "columns": [ [ "horizontal_avg({*})", 1, HorizontalTest.after ] ]
                },
                {
                    "rowName": "z", "rowHash": "5e7ad9221c640e96",
                    "columns": [ [ "horizontal_avg({*})", 5.5, HorizontalTest.after ] ]
                }
             ]
        )

        self.assertTableResultEquals(
            mldb.query("select horizontal_avg({superPatate*}) from dataset order by rowName()"),
            [["_rowName","horizontal_avg({superPatate*})"],
             ["x",None], ["y",None], ["z",None]])


    def test_horizontal_min(self):
        resp = mldb.get("/v1/query", q="select horizontal_min({*}) from dataset order by rowName()").json()

        self.assertFullResultEquals(resp,
            [
                {
                    "rowName": "x", "rowHash": "4fc82888d13de398",
                    "columns": [ [ "horizontal_min({*})", 1, HorizontalTest.sometime ] ]
                },
                {
                    "rowName": "y", "rowHash": "75c422100f18a043",
                    "columns": [ [ "horizontal_min({*})", 1, HorizontalTest.after ] ]
                },
                {
                    "rowName": "z", "rowHash": "5e7ad9221c640e96",
                    "columns": [ [ "horizontal_min({*})", 1, HorizontalTest.sometime ] ]
                }
             ]
        )

    def test_horizontal_max(self):
        resp = mldb.get("/v1/query", q="select horizontal_max({*}) from dataset order by rowName()").json()

        self.assertFullResultEquals(resp,
            [
                {
                    "rowName": "x", "rowHash": "4fc82888d13de398",
                    "columns": [ [ "horizontal_max({*})", 1, HorizontalTest.sometime ] ]
                },
                {
                    "rowName": "y", "rowHash": "75c422100f18a043",
                    "columns": [ [ "horizontal_max({*})", 1, HorizontalTest.after ] ]
                },
                {
                    "rowName": "z", "rowHash": "5e7ad9221c640e96",
                    "columns": [ [ "horizontal_max({*})", 10, HorizontalTest.after ] ]
                }
             ]
        )


    def test_horizontal_earliest(self):
        resp = mldb.get("/v1/query", q="select horizontal_earliest({*}) from dataset order by rowName()").json()

        self.assertFullResultEquals(resp,
            [
                {
                    "rowName": "x", "rowHash": "4fc82888d13de398",
                    "columns": [ [ "horizontal_earliest({*})", 1, HorizontalTest.sometime ] ]
                },
                {
                    "rowName": "y", "rowHash": "75c422100f18a043",
                    "columns": [ [ "horizontal_earliest({*})", 1, HorizontalTest.sometime ] ]
                },
                {
                    "rowName": "z", "rowHash": "5e7ad9221c640e96",
                    "columns": [ [ "horizontal_earliest({*})", 1, HorizontalTest.sometime ] ]
                }
            ]
        )

    def test_horizontal_latest(self):
        resp = mldb.get("/v1/query", q="select horizontal_latest({*}) from dataset order by rowName()").json()

        self.assertFullResultEquals(resp,
            [
                {
                    "rowName": "x", "rowHash": "4fc82888d13de398",
                    "columns": [ [ "horizontal_latest({*})", 1, HorizontalTest.sometime ] ]
                },
                {
                    "rowName": "y", "rowHash": "75c422100f18a043",
                    "columns": [ [ "horizontal_latest({*})", 1, HorizontalTest.after ] ]
                },
                {
                    "rowName": "z", "rowHash": "5e7ad9221c640e96",
                    "columns": [ [ "horizontal_latest({*})", 10, HorizontalTest.after ] ]
                }
            ]
        )

    def test_mldbfb_558_no_need_to_cast_ts_on_data(self):
        res = mldb.query("""
            SELECT horizontal_min({'a', 'b'})
        """)
        self.assertEqual(res[1][1], 'a')

        res = mldb.query("""
            SELECT horizontal_max({'a', 'b'})
        """)
        self.assertEqual(res[1][1], 'b')

        res = mldb.query("""
            SELECT horizontal_min({TIMESTAMP '2015-01-01T00:00:00Z',
                                   TIMESTAMP '2016-01-01T00:00:00Z'})
        """)
        self.assertEqual(res[1][1], '2015-01-01T00:00:00Z')

        res = mldb.query("""
            SELECT horizontal_max({TIMESTAMP '2015-01-01T00:00:00Z',
                                   TIMESTAMP '2016-01-01T00:00:00Z'})
        """)
        self.assertEqual(res[1][1], '2016-01-01T00:00:00Z')


mldb.run_tests()
