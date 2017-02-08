#
# MLDB-906-date-function.py
# mldb.ai inc, 2015
# this file is part of mldb. copyright 2015 mldb.ai inc. all rights reserved.
#

import datetime
import unittest

mldb = mldb_wrapper.wrap(mldb) # noqa

def log(thing):
    mldb.log(str(thing))

def query(query, fmt='full'):
    res = mldb.get('/v1/query', q=query, format=fmt)
    return res.json()

d = datetime.datetime.now()


class DateFunctionTest(unittest.TestCase):

    @classmethod
    def setUpClass(cls):
        global d
        dataset_config = {
            'type'    : 'sparse.mutable',
            'id'      : 'example'
        }

        dataset = mldb.create_dataset(dataset_config)

        dataset.record_row('row1', [["x", "2015-01-01T15:14:39.123456Z", d]])

        log("Committing dataset")
        dataset.commit()

        dataset_config = {
            'type'    : 'sparse.mutable',
            'id'      : 'example2'
        }

        dataset2 = mldb.create_dataset(dataset_config)
        dataset2.record_row('row1', [["x", "2014-12-31T15:14:39.123456Z", d]])
        log("Committing dataset")
        dataset2.commit()

        # previous year
        dataset_config = {
            'type'    : 'sparse.mutable',
            'id'      : 'example3'
        }

        dataset3 = mldb.create_dataset(dataset_config)
        d = datetime.datetime.now()
        dataset3.record_row('row1', [["x", "2014-12-28T15:14:39.123456Z", d]])
        log("Committing dataset")
        dataset3.commit()

    def test_date_part_year(self):
        res = query("SELECT date_part('year', latest_timestamp(x)) AS year "
                    "FROM example")
        self.assertEqual(res[0]['columns'][0][1], d.year)

    def test_date_part_month(self):
        res = query("SELECT date_part('month', latest_timestamp(x)) AS month FROM example")
        self.assertEqual(res[0]['columns'][0][1], d.month)

    def test_date_part_quarter(self):
        res = query("SELECT date_part('quarter', latest_timestamp(x)) AS quarter "
                    "FROM example")
        self.assertEqual(res[0]['columns'][0][1], (d.month / 4) + 1)

    def test_date_part_day(self):
        res = query("SELECT date_part('day', latest_timestamp(x)) AS day FROM example")
        self.assertEqual(res[0]['columns'][0][1], d.day)

    def test_date_part_hour(self):
        res = query("SELECT date_part('hour', latest_timestamp(x)) AS hour FROM example")
        self.assertEqual(res[0]['columns'][0][1], d.hour)

    def test_date_part_minute(self):
        res = query("SELECT date_part('minute', latest_timestamp(x)) AS minute "
                    "FROM example")
        self.assertEqual(res[0]['columns'][0][1], d.minute)

    ##
    # these ones use X instead of latest_timestamp(x) because its not directly in date_time
    def test_date_part_second(self):
        res = query("SELECT date_part('second', x) AS second FROM example")
        self.assertEqual(res[0]['columns'][0][1], 39)

    def test_date_part_millisecond(self):
        res = query("SELECT date_part('millisecond', x) AS millisecond "
                    "FROM example")
        self.assertEqual(res[0]['columns'][0][1], 123)

    def test_date_part_microsecond(self):
        res = query("SELECT date_part('microsecond', x) AS microsecond "
                    "FROM example")
        self.assertEqual(res[0]['columns'][0][1], 123456)

    def test_date_part_dow(self):
        res = query("SELECT date_part('dow', x) AS dow FROM example")
        self.assertEqual(res[0]['columns'][0][1], 4)

    def test_date_part_doy(self):
        res = query("SELECT date_part('doy', x) AS doy FROM example")
        self.assertEqual(res[0]['columns'][0][1], 0) #"days since january 1st"

    def test_date_part_isodow(self):
        res = query("SELECT date_part('isodow', x) AS isodow FROM example")
        self.assertEqual(res[0]['columns'][0][1], 4)

    def test_date_part_isodoy(self):
        res = query("SELECT date_part('isodoy', x) AS isodoy FROM example")
        self.assertEqual(res[0]['columns'][0][1], 4)

    def test_date_part_week(self):
        res = query("SELECT date_part('week', x) AS week FROM example")
        self.assertEqual(res[0]['columns'][0][1], 0)

    def test_date_part_isoweek(self):
        res = query("SELECT date_part('isoweek', x) AS isoweek FROM example")
        self.assertEqual(res[0]['columns'][0][1], 1)

    def test_date_part_isoyear(self):
        res = query("SELECT date_part('isoyear', x) AS isoyear FROM example")
        self.assertEqual(res[0]['columns'][0][1], 2015)

    ##
    # try with a december date
    def test_december_date_part_second(self):
        res = query("SELECT date_part('second', x) AS second FROM example2")
        self.assertEqual(res[0]['columns'][0][1], 39)

    def test_december_date_part_millisecond(self):
        res = query("SELECT date_part('millisecond', x) AS millisecond "
                    "FROM example2")
        self.assertEqual(res[0]['columns'][0][1], 123)

    def test_december_date_part_microsecond(self):
        res = query("SELECT date_part('microsecond', x) AS microsecond "
                    "FROM example2")
        self.assertEqual(res[0]['columns'][0][1], 123456)

    def test_december_date_part_dow(self):
        res = query("SELECT date_part('dow', x) AS dow FROM example2")
        self.assertEqual(res[0]['columns'][0][1], 3)

    def test_december_date_part_doy(self):
        res = query("SELECT date_part('doy', x) AS doy FROM example2")
        # days since january 1st
        self.assertEqual(res[0]['columns'][0][1], 364)

    def test_december_date_part_isodow(self):
        res = query("SELECT date_part('isodow', x) AS isodow FROM example2")
        self.assertEqual(res[0]['columns'][0][1], 3)

    def test_december_date_part_isodoy(self):
        res = query("SELECT date_part('isodoy', x) AS isodoy FROM example2")
        self.assertEqual(res[0]['columns'][0][1], 3)

    def december_date_part_week(self):
        res = query("SELECT date_part('week', x) AS week FROM example2")
        self.assertEqual(res[0]['columns'][0][1], 52)

    def test_december_date_part_isoweek(self):
        res = query("SELECT date_part('isoweek', x) AS isoweek FROM example2")
        self.assertEqual(res[0]['columns'][0][1], 1)

    def test_december_date_part_isoyear(self):
        res = query("SELECT date_part('isoyear', x) AS isoyear FROM example2")
        self.assertEqual(res[0]['columns'][0][1], 2015)

    def test_december_date_part_minute(self):
        res = query("SELECT date_trunc('minute', x) AS minute FROM example2")
        self.assertEqual(res[0]['columns'][0][1]['ts'], "2014-12-31T15:14:00Z")

    def test_december_date_trunc_hour(self):
        res = query("SELECT date_trunc('hour', x) AS hour FROM example2")
        self.assertEqual(res[0]['columns'][0][1]['ts'], "2014-12-31T15:00:00Z")

    def test_december_date_trunc_day(self):
        res = query("SELECT date_trunc('day', x) AS day FROM example2")
        self.assertEqual(res[0]['columns'][0][1]['ts'], "2014-12-31T00:00:00Z")

    def test_december_date_trunc_month(self):
        res = query("SELECT date_trunc('month', x) AS month FROM example2")
        self.assertEqual(res[0]['columns'][0][1]['ts'], "2014-12-01T00:00:00Z")

    def test_december_date_trunc_quarter(self):
        res = query("SELECT date_trunc('quarter', x) AS quarter FROM example2")
        self.assertEqual(res[0]['columns'][0][1]['ts'], "2014-09-01T00:00:00Z")

    def test_december_date_trunc_year(self):
        res = query("SELECT date_trunc('year', x) AS year FROM example2")
        self.assertEqual(res[0]['columns'][0][1]['ts'], "2014-01-01T00:00:00Z")

    def test_december_date_trunc_dow(self):
        res = query("SELECT date_trunc('dow', x) AS dow FROM example2")
        self.assertEqual(res[0]['columns'][0][1]['ts'], "2014-12-31T00:00:00Z")

    def test_december_date_trunc_doy(self):
        res = query("SELECT date_trunc('doy', x) AS doy FROM example2")
        self.assertEqual(res[0]['columns'][0][1]['ts'], "2014-12-31T00:00:00Z")

    def test_december_date_trunc_isodow(self):
        res = query("SELECT date_trunc('isodow', x) AS isodow FROM example2")
        self.assertEqual(res[0]['columns'][0][1]['ts'], "2014-12-31T00:00:00Z")

    def test_december_date_trunc_isodoy(self):
        res = query("SELECT date_trunc('isodoy', x) AS isodoy FROM example2")
        self.assertEqual(res[0]['columns'][0][1]['ts'], "2014-12-31T00:00:00Z")

    def test_december_date_trunc_week(self):
        res = query("SELECT date_trunc('week', x) AS week FROM example2")
        # previous sunday
        self.assertEqual(res[0]['columns'][0][1]['ts'], "2014-12-28T00:00:00Z")

    def test_december_date_trunc_isoweek(self):
        res = query("SELECT date_trunc('isoweek', x) AS isoweek FROM example2")
        # previous monday
        self.assertEqual(res[0]['columns'][0][1]['ts'], "2014-12-29T00:00:00Z")

    def test_december_date_trunc_isoyear(self):
        res = query("SELECT date_trunc('isoyear', x) AS isoyear FROM example2")
        # first monday of the first iso week
        self.assertEqual(res[0]['columns'][0][1]['ts'], "2014-12-29T00:00:00Z")

    ##
    # previous year
    def test_py_date_part_isoweek(self):
        res = query("SELECT date_part('isoweek', x) AS isoweek FROM example3")
        self.assertEqual(res[0]['columns'][0][1], 52)

    def test_py_date_part_isodoy(self):
        res = query("SELECT date_part('isodoy', x) AS isodoy FROM example3")
        self.assertEqual(res[0]['columns'][0][1], 364)

    ##
    # date_trunc
    def test_date_trunc_second(self):
        res = query("SELECT date_trunc('second', x) AS second FROM example2")
        self.assertEqual(res[0]['columns'][0][1]['ts'], "2014-12-31T15:14:39Z")

    def test_date_trunc_ms_date_part_ms(self):
        # problematic because floating points
        res = query("""
            SELECT date_part('millisecond', date_trunc('millisecond', x))
            AS millisecond FROM example2""")
        self.assertTrue(122 <= res[0]['columns'][0][1] <= 124)

    def test_date_trunc_ms_date_part_us(self):
        res = query("""
            SELECT date_part('microsecond',date_trunc('millisecond', x))
            AS millisecond FROM example2""")
        self.assertTrue(122999 <= res[0]['columns'][0][1] <= 123001)

    def test_date_trunc_us_date_part_us(self):
        # problematic because floating points
        res = query("""
            SELECT date_part('microsecond',date_trunc('microsecond', x))
            AS microsecond FROM example2""")
        self.assertTrue(123455 <= res[0]['columns'][0][1] <= 123457)

    ##
    #
    def test_date_part_tz(self):
        # MLDB-990 - timezone support
        res = query("SELECT date_part('hour', x, '-0100') AS hour "
                    "FROM example")
        self.assertEqual(res[0]['columns'][0][1], 16)

        res = query("SELECT date_part('hour', x, '+01') AS hour FROM example")
        self.assertEqual(res[0]['columns'][0][1], 14)

        res = query("SELECT date_part('hour', x, '-05:50') AS hour "
                    "FROM example")
        self.assertEqual(res[0]['columns'][0][1], 21)

        res = query("SELECT date_part('hour', x, '-12:00') AS hour "
                    "FROM example")
        self.assertEqual(res[0]['columns'][0][1], 3)

        res = query("SELECT date_part('day', x, '-12:00') AS hour "
                    "FROM example")
        self.assertEqual(res[0]['columns'][0][1], 2)

        res = query("SELECT date_trunc('minute', x, '-00:30') AS minute "
                    "FROM example2")
        self.assertEqual(res[0]['columns'][0][1]['ts'], "2014-12-31T15:44:00Z")

        res = query("SELECT date_trunc('hour', x, '+08:00') AS hour "
                    "FROM example2")
        self.assertEqual(res[0]['columns'][0][1]['ts'], "2014-12-31T07:00:00Z")

if __name__ == '__main__':
    mldb.run_tests()
