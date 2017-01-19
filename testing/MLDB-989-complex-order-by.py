#
# MLDB-989-complex-order-by.py
# mldb.ai inc, 2015
# This file is part of MLDB. Copyright 2015 mldb.ai inc. All rights reserved.
#

import datetime
import unittest

mldb = mldb_wrapper.wrap(mldb) # noqa


class ComplexOrderByTest(unittest.TestCase):

    @classmethod
    def setUpClass(cls):
        dataset_config = {"id": "test_data", "type": "sparse.mutable"}
        dataset = mldb.create_dataset(dataset_config)
        now = datetime.datetime.now()

        for index in range(4):
            dataset.record_row("row" + str(index), [["index", index, now]])

        dataset.commit()

    def order_by(self, order_by):
        output = mldb.get(
            "/v1/query",
            q='select * from test_data order by ' + order_by).json()
        mldb.log(output)
        return [row['columns'][0][1] for row in output]

    def compare(self, left, right, op='<'):
        result = mldb.get(
            '/v1/query',
            q="select tokenize('" +
              left + "', {splitChars:' '}) " + op + " tokenize('"
              + right + "', {splitChars:' '}) as comp")
        response = result.json()
        mldb.log(response)
        return response[0]['columns'][0][1]

    def test_order_row_type_sorted_columns(self):
        mldb.log('testing ordering of row type with sorted columns...')
        order = self.order_by(
            'case when index % 4 = 1 then {a:1, b:2} when index % 4 = 2 then {a:0, b:3} when index % 4 = 3 then {a:0, c:4} else null end')
        self.assertEqual(order, [0, 2, 3, 1])

    def test_order_row_type_unsorted_columns(self):
        mldb.log('testing ordering of row type with unsorted columns...')
        order = self.order_by(
            'case when index % 4 = 1 then {a:1, b:2} when index % 4 = 2 then {b:3, a:0} when index % 4 = 3 then {c:4, a:0} else null end')
        self.assertEqual(order, [0, 2, 3, 1])

    def test_order_different_types(self):
        mldb.log('testing ordering of different types...')
        order = self.order_by(
            'case when index % 4 = 1 then 3 when index % 4 = 2 then null when index % 4 = 3 then {a:1, b:2} else [0,3] end')
        self.assertEqual(order, [2, 1, 3, 0])

    def test_order_embedding_types(self):
        mldb.log('testing ordering of embedding types...')
        order = self.order_by(
            'case when index % 4 = 1 then [3.3, 34.0] when index % 4 = 2 then [64.2, 34.0] when index % 4 = 3 then [64.1999, 34.0] else [3.3,3.0] end')
        self.assertEqual(order, [0, 1, 3, 2])

    def test_comparisons(self):
        smaller = self.compare('string string zoo', 'string zoo')
        self.assertFalse(smaller, 'left string is greater than right string')

        smaller = self.compare('string zoo', 'string zoo')
        self.assertFalse(smaller, 'left string is equal to right string')

        smaller = self.compare('zoo string string', 'string zoo')
        self.assertFalse(smaller, 'left string is greater than right string')

        equal = self.compare('zoo string string', 'string zoo string', '=')
        self.assertTrue(equal, 'left string is equal to right string')

        equal = self.compare('zoo string str', 'zoo string string', '=')
        self.assertFalse(equal, 'left string is not equal to right string')

        greater = self.compare('zoo string string', 'string zoo string', '>')
        self.assertFalse(greater,
                         'left string is not greater than right string')

        greater = self.compare(
            'zoo string string string', 'zoo string string', '>')
        self.assertTrue(greater, 'left string is greater than right string')

    def test_somthing(self):
        str_dataset_config = {
            "id": "str_test_data",
            "type": "sparse.mutable"
        }
        str_dataset = mldb.create_dataset(str_dataset_config)

        now = datetime.datetime.now()
        str_dataset.record_row("row1",
                               [["terms", "c++,python,c++,java,c++", now]])
        str_dataset.record_row(
            "row2",
            [["terms", "scala,scala,java,java,scala,java,scala,c++", now]])
        str_dataset.record_row("row3", [["terms", "python,ada,ada", now]])
        str_dataset.commit()

        output = mldb.get(
            "/v1/query",
            q="select tokenize(terms) as term from str_test_data "
              "order by tokenize(terms)").json()
        mldb.log(output)
        self.assertEqual([row['rowName'] for row in output],
                         ['row3', 'row2', 'row1'])

if __name__ == '__main__':
    mldb.run_tests()
