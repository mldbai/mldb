#
# MLDB-1594-aggregator-empty-row.py
# mldb.ai inc, 2016
# this file is part of mldb. copyright 2016 mldb.ai inc. all rights reserved.
#

import unittest

mldb = mldb_wrapper.wrap(mldb) # noqa

class Mldb1594(MldbUnitTest):  

    def test_simple(self):
        res1 = mldb.query("select {}")
        res2 = mldb.query("select sum({*}) named 'result' from (select {})")
        self.assertEqual(res1,res2)


    def test_multi_row(self):
        dataset_config = {
            'type'    : 'tabular',
            'id'      : 'toy'
        }
        dataset = mldb.create_dataset(dataset_config)
        dataset.record_row("rowA", [["txt", "hoho things are great!", 0]])
        dataset.record_row("rowB", [["txt", "! ", 0]])
        dataset.record_row("rowC", [["txt", "things are great, great", 0]])

        dataset.commit()

        expected = [
            ["_rowName", "are", "great", "hoho", "things"],
            ["pwet", 2, 3, 1, 2]
        ]

        # skipping the null row
        self.assertTableResultEquals(
            mldb.query("""
             select sum({*}) as *
             named 'pwet'
             from (
                SELECT tokenize(lower(txt), {splitChars: ' ,.!;:"?', minTokenLength: 2}) as *
                from toy
                where rowName() != 'rowB'
            )
            """),
            expected)
       
        # passing the empty row (rowB) to sum
        self.assertTableResultEquals(
            mldb.query("""
             select sum({*}) as *
             named 'pwet'
             from (
                SELECT tokenize(lower(txt), {splitChars: ' ,.!;:"?', minTokenLength: 2}) as *
                from toy
            )
            """),
            expected)


mldb.run_tests()


