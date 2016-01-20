# This file is part of MLDB. Copyright 2015 Datacratic. All rights reserved.


import json
import datetime
import unittest

if False:
    mldb = None

now = datetime.datetime.now()
a_second_before_now = now + datetime.timedelta(seconds=-1)
same_time_tomorrow = now + datetime.timedelta(days=1)
in_two_hours = now + datetime.timedelta(hours=2)

year_ago_str = (now - datetime.timedelta(days=365)).strftime("%Y-%m-%d")
week_ago_str = (now - datetime.timedelta(days=7)).strftime("%Y-%m-%d")
year_from_now_str = (now + datetime.timedelta(days=365)).strftime("%Y-%m-%d")


def log(thing):
    mldb.log(str(thing))


def perform(*args, **kwargs):
    res = mldb.perform(*args, **kwargs)
    assert res['statusCode'] in [200, 201], str(res)
    return res


def query(query, fmt='full'):
    res = perform('GET', '/v1/query', [['q', query], ['format', fmt]])
    return json.loads(res['response'])


class SimpleWhenExpressionTest(unittest.TestCase):

    def __init__(self, *args, **kwargs):
        super(SimpleWhenExpressionTest, self).__init__(*args, **kwargs)

    @classmethod
    def setUpClass(cls):
        ds1 = mldb.create_dataset({
            'type': 'sparse.mutable',
            'id': 'dataset1'})

        row_count = 10
        for i in xrange(row_count - 1):
            # row name is x's value
            ds1.record_row(str(i), [['x', i, now]])
        ds1.record_row(str(row_count - 1), [['x', 9, same_time_tomorrow]])
        ds1.commit()

    def test_get_all_tuples(self):
        rows = query("SELECT * FROM dataset1")
        for row in rows:
            self.assertEqual(
                row["rowName"], row["columns"][0][1],
                'expected tuple matching row name %s' % row["rowName"])

        rows = query("SELECT * FROM dataset1 WHEN timestamp() BETWEEN "
                     "'{}' AND '{}'".format(year_ago_str, year_from_now_str))

        for row in rows:
            self.assertEqual(
                row["rowName"], row["columns"][0][1],
                'expected tuple matching row name %s' % row["rowName"])

    def test_get_no_tuples(self):
        rows = query("SELECT * FROM dataset1 WHEN timestamp() BETWEEN "
                     "'{}' AND '{}'".format(year_ago_str, week_ago_str))
        log(rows)
        for row in rows:
            self.assertTrue(
                "columns" not in row,
                'no tuple should be returned on row name %s' % row["rowName"])

    def test_get_single_row(self):
        rows = query("SELECT x FROM dataset1 WHERE x = 9")
        self.assertEqual(
            len(rows), 1,
            "expecting only one row to be selected by the where clause")
        self.assertEqual(rows[0]["columns"][0][1], 9,
                         "expecting row 9 to be selected by where clause")

    def test_last_tuple_filtered_out(self):
        rows = query("SELECT x FROM dataset1 WHEN timestamp() BETWEEN "
                     "'%s' and '%s'" % (a_second_before_now, in_two_hours))
        log(rows)
        for row in rows:
            if row['rowName'] is 9:
                self.assertTrue(
                    row["columns"][0][1] is None,
                    "expecting row 9 to be filtered out by the when clause")

    def test_when_exec_after_where(self):
        # check that the when clause is executed after the where one
        rows = query("SELECT x FROM dataset1 WHEN timestamp() BETWEEN "
                     "'%s' and '%s' WHERE x = 9"
                     % (a_second_before_now, in_two_hours))
        self.assertTrue(
            rows[0]["columns"][0][1] is None and len(rows) == 1,
            "expecting the tuple to be filtered out by when clause")
        self.assertEqual(rows[0]['rowName'], 9,
                         "expecting only row 9 to remain")

if __name__ == '__main__':
    res = unittest.main(exit=False).result
    got_err = False
    for err in res.errors + res.failures:
        got_err = True
        log(str(err[0]) + "\n" + err[1])

    if not got_err:
        mldb.script.set_return("success")
