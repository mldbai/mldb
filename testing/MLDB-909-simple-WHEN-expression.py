#
# MLDB-909-simple-WHEN-expression.py
# mldb.ai inc, 2015
# This file is part of MLDB. Copyright 2015 mldb.ai inc. All rights reserved.
#
import datetime
import unittest

mldb = mldb_wrapper.wrap(mldb) # noqa

now = datetime.datetime.now()
a_second_before_now = now + datetime.timedelta(seconds=-1)
same_time_tomorrow = now + datetime.timedelta(days=1)
in_two_hours = now + datetime.timedelta(hours=2)

year_ago_str = (now - datetime.timedelta(days=365)).strftime("%Y-%m-%d")
week_ago_str = (now - datetime.timedelta(days=7)).strftime("%Y-%m-%d")
year_from_now_str = (now + datetime.timedelta(days=365)).strftime("%Y-%m-%d")


def log(thing):
    mldb.log(str(thing))

def query(qry):
    return mldb.get('/v1/query', q=qry, format='full').json()

class SimpleWhenExpressionTest(unittest.TestCase):

    def __init__(self, *args, **kwargs):
        super(SimpleWhenExpressionTest, self).__init__(*args, **kwargs)

    @classmethod
    def setUpClass(cls):
        ds1 = mldb.create_dataset({
            'type': 'sparse.mutable',
            'id': 'dataset1'
        })

        row_count = 10
        for i in xrange(row_count - 1):
            # row name is x's value
            ds1.record_row(str(i), [['x', str(i), now]])
        ds1.record_row(str(row_count - 1), [['x', "9", same_time_tomorrow]])
        ds1.commit()

        ds = mldb.create_dataset({
            'type': 'sparse.mutable',
            'id': 'dataset2'
        })

        ds.record_row("row1", [['colA', 1, '1970-01-02T00:00:00Z'],
                               ['colA', 1, '1970-01-04T00:00:00Z'],
                               ['colA', 1, '1970-01-06T00:00:00Z']])

        ds.commit()

    def test_get_all_tuples(self):
        rows = query("SELECT * FROM dataset1")
        for row in rows:
            self.assertEqual(
                row["rowName"], row["columns"][0][1],
                'expected tuple matching row name %s' % row["rowName"])

        rows = query("SELECT * FROM dataset1 WHEN value_timestamp() BETWEEN "
                          "TIMESTAMP '{}' AND TIMESTAMP '{}'"
                          .format(year_ago_str, year_from_now_str))        

        for row in rows:
            self.assertEqual(
                row["rowName"], row["columns"][0][1],
                'expected tuple matching row name %s' % row["rowName"])

    def test_get_no_tuples(self):
        rows = query(
            "SELECT * FROM dataset1 WHEN value_timestamp() BETWEEN "
            "TIMESTAMP '{}' AND TIMESTAMP '{}'".format(year_ago_str, week_ago_str))
        for row in rows:
            self.assertTrue(
                "columns" not in row,
                'no tuple should be returned on row name %s' % row["rowName"])

    def test_get_single_row(self):
        rows = query("SELECT x FROM dataset1 WHERE x = '9'")
        self.assertEqual(
            len(rows), 1,
            "expecting only one row to be selected by the where clause")
        self.assertEqual(rows[0]["columns"][0][1], '9',
                         "expecting row 9 to be selected by where clause")

    def test_last_tuple_filtered_out(self):
        rows = query(
            "SELECT x FROM dataset1 WHEN value_timestamp() BETWEEN "
            "TIMESTAMP '%s' and TIMESTAMP '%s'" % (a_second_before_now, in_two_hours))
        for row in rows:
            if row['rowName'] is 9:
                self.assertTrue(
                    row["columns"][0][1] is None,
                    "expecting row 9 to be filtered out by the when clause")

    def test_when_exec_after_where(self):
        # check that the when clause is executed after the where one
        rows = query("SELECT x FROM dataset1 WHEN value_timestamp() BETWEEN "
                          "TIMESTAMP '%s' and TIMESTAMP '%s' WHERE x = '9'"
                          % (a_second_before_now, in_two_hours))
        self.assertTrue(
            rows[0]["columns"][0][1] is None and len(rows) == 1,
            "expecting the tuple to be filtered out by when clause")
        self.assertEqual(rows[0]['rowName'], "9",
                         "expecting only row 9 to remain")

    def test_multiple_ts_same_row_col(self):
        res = None

        def expect():
            self.assertEqual(len(res), 1)
            self.assertEqual(len(res[0]['columns']), 1)
            self.assertEqual(res[0]['columns'][0][2], '1970-01-04T00:00:00Z')

        res = query('SELECT * FROM dataset2 WHEN value_timestamp() BETWEEN '
                         "TIMESTAMP '1970-01-03T00:00:00Z' AND TIMESTAMP '1970-01-05T00:00:00Z'")
        expect()

        res = query("SELECT * FROM dataset2 WHEN "
                         "value_timestamp() >= TIMESTAMP '1970-01-03T00:00:00Z' AND "
                         "value_timestamp() <= TIMESTAMP '1970-01-05T00:00:00Z'")
        expect()

        res = query('SELECT * FROM dataset2 WHEN value_timestamp() BETWEEN '
                        "TIMESTAMP '1970-01-04T00:00:00Z' AND TIMESTAMP '1970-01-04T00:00:00Z'")
        expect()

        res = query('SELECT * FROM dataset2 WHEN value_timestamp() BETWEEN '
                         "TIMESTAMP '1970-01-03T23:59:59Z' AND TIMESTAMP '1970-01-04T23:59:59Z'")
        expect()

        res = query('SELECT * FROM dataset2 WHEN '
                         "value_timestamp() = TIMESTAMP '1970-01-04T00:00:00Z'")
        expect()

        res = query('SELECT * FROM dataset2 WHEN value_timestamp() BETWEEN '
                         "TIMESTAMP '1970-01-04T23:59:59Z' AND TIMESTAMP '1970-01-03T23:59:59Z'")
        self.assertTrue('columns' not in res[0])

    def test_multiple_ts(self):
        ds = mldb.create_dataset({
            'type': 'sparse.mutable',
            'id': 'dataset3'
        })

        ds.record_row("row1", [['colA', 1, '1970-01-02T00:00:00Z'],
                               ['colB', 1, '1970-01-04T00:00:00Z'],
                               ['colB', 1, '1970-01-06T00:00:00Z']])

        ds.commit()

        res = query('SELECT * FROM dataset3 WHEN value_timestamp() < '
                        "TIMESTAMP '1970-01-03T00:00:00Z'")
        self.assertEqual(len(res), 1)
        self.assertEqual(len(res[0]['columns']), 1)
        self.assertEqual(res[0]['columns'][0][2], '1970-01-02T00:00:00Z')

    def test_timezone(self):
        def expect():
            self.assertEqual(len(res), 1)
            self.assertEqual(len(res[0]['columns']), 1)
            self.assertEqual(res[0]['columns'][0][2], '1970-01-04T00:00:00Z')

        res = query('SELECT * FROM dataset2 WHEN value_timestamp() BETWEEN '
                    "TIMESTAMP '1970-01-04T01:00:00+01:00' AND "
                    "TIMESTAMP '1970-01-04T01:00:00+01:00'")
        expect()

        res = query('SELECT * FROM dataset2 WHEN value_timestamp() '
                    "= TIMESTAMP '1970-01-04T01:00:00+01:00'")
        expect()

        res = query('SELECT * FROM dataset2 WHEN value_timestamp() < '
                         "TIMESTAMP '1970-01-02T00:00:00+01:00'")
        self.assertTrue('columns' not in res[0])

        res = query('SELECT * FROM dataset2 WHEN value_timestamp() BETWEEN '
                         "TIMESTAMP '1970-01-03T23:00:00-01:00' AND "
                         "TIMESTAMP '1970-01-03T23:00:00-01:00'")
        expect()

        res = query('SELECT * FROM dataset2 WHEN value_timestamp() '
                    "= TIMESTAMP '1970-01-03T23:00:00-01:00'")
        expect()


if __name__ == '__main__':
    mldb.run_tests()
